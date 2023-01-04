
use std::sync::{Arc, Weak};
use enumflags2::BitFlags;
use event_listener::Event;
use parking_lot::Mutex;
use anyhow::{Result, bail, Context};
use tokio::{task::JoinHandle, sync::mpsc::{Sender, self}};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tracing::{info, Instrument, debug};

use crate::uid::{NodeId, next_instance_id, NodeOffset};

use super::{proto::{
    RpcClient,
    NodeClientStream,
    node_client_stream,
    NodeServerStream,
    node_server_stream,
    NodeSubscribeRequest,
    NodeSubscribeResponse,
    NodeStatePacket,
    node_state_packet,
    NodeDeltaPacket,
    node_delta_packet,
}, ClusterInner, common::Flag};


pub(crate) async fn client_sync_node(
    recver: &NodeSyncRecver,
    client: &mut RpcClient, 
) -> Result<()> {
    let (tx, rx) = mpsc::channel(128);
    let req = ReceiverStream::new(rx);

    let rx_stream = client.sync_node(req).await?.into_inner();


    recver.shutdown().await;
    let stream_info = {
        let mut data = recver.shared.data.lock(); 
        data.flags.remove(Flag::Shutdown);
        data.flags.remove(Flag::Fininshed);
        RecverStreamInfo{tx, rx_stream, offset: data.last_offset}
    };

    recver.kick(stream_info);

    Ok(())

}

#[derive(Default)]
pub(crate) struct NodeSyncRecver { 
    shared: Arc<RecverShared>,
}

impl NodeSyncRecver {

    pub(crate) fn new(node_id: NodeId, cluster: Weak<ClusterInner>) -> Self { 
        Self {
            shared: RecverShared::new(node_id, cluster),
        }
    }

    async fn shutdown(&self) { 
        let task = {
            let mut data = self.shared.data.lock(); 
            data.flags |= Flag::Shutdown;
            data.task.take()
        };

        if let Some(task) = task {
            let _r = task.await;
        }
    }

    fn kick(&self, mut stream_info: RecverStreamInfo) { 
        // let self0 = NodeSync::new(target_node_id);
    
        let mut data = self.shared.data.lock();
    
        if data.flags.contains(Flag::Shutdown | Flag::Fininshed) {
            return ;
        }
    
        let task = {
            let name = format!("node({})-recver({})", self.shared.node_id, self.shared.inst_id);
            let span = tracing::span!(parent:None, tracing::Level::DEBUG, "", s = name.as_str());
            let weak = Arc::downgrade(&self.shared);
    
            let fut = async move {
    
                info!("start working");
                let task_r = recver_task(&weak, &mut stream_info).await;
    
                {
                    // 设置结束标志，并且唤醒等待的 task
                    // 更新 offset 到 shared 里
                    let r = weak.upgrade();
                    if let Some(shared) = r {
                        let mut data = shared.data.lock();
                        data.last_offset = stream_info.offset;
                        data.flags |= Flag::Fininshed;
                        shared.event.notify(usize::MAX);
                    }
                }
                
                debug!("finished with [{:?}]", task_r);
                task_r
            }.instrument(span);
    
            tokio::spawn(fut)
        };
    
        data.task = Some(task);
    } 
}



async fn recver_task(weak: &Weak<RecverShared>, stream_info: &mut RecverStreamInfo) -> Result<()> { 
    // TODO： 注意退出条件：没有关心时； 接收失败； 更新时

    let _rsp = request_first(weak, stream_info).await?;
    loop {
        let r = stream_info.rx_stream.message().await?
        .with_context(||"receive stream message fail")?
        .data
        .with_context(||"receive stream message but empty data")?;
    
        match r {
            node_server_stream::Data::State(packet) => { 
                process_state_packet(weak, stream_info, packet)?;
            },
            node_server_stream::Data::Delta(packet) => { 
                process_delta_packet(weak, stream_info, packet)?;
            },
            node_server_stream::Data::SubscribeRsp(rsp) => { 
                bail!("receive stream message but got [{:?}]", rsp)
            },
        }
    }
}

fn process_state_packet(weak: &Weak<RecverShared>, stream_info: &mut RecverStreamInfo, packet: NodeStatePacket) -> Result<bool> { 

    let offset: NodeOffset = packet.offset.into();


    let shared = acquire_shared(weak)?;
    let cluster = acquire_cluster(&shared.cluster)?;

    let r = packet.state_data.with_context(||"empty state data")?;
    match r {
        node_state_packet::StateData::Node(info) => { 
            if offset > stream_info.offset {
                cluster.try_add_node(info)?;
                stream_info.offset = packet.offset.into();
            }
 
            Ok(false)
        },
        node_state_packet::StateData::Last(last) => {
            if offset > stream_info.offset {
                stream_info.offset = packet.offset.into();
            }
            Ok(last)
        },
    }
}

fn process_delta_packet(weak: &Weak<RecverShared>, stream_info: &mut RecverStreamInfo, packet: NodeDeltaPacket) -> Result<()> { 

    let offset: NodeOffset = packet.offset.into();

    if offset > stream_info.offset {
        let shared = acquire_shared(weak)?;
        let cluster = acquire_cluster(&shared.cluster)?;
    
        let r = packet.delta_data.with_context(||"empty state data")?;
        match r {
            node_delta_packet::DeltaData::UpdateNode (node) => { 
                cluster.try_add_node(node)?;
            },
        }
    }

    Ok(())
}


async fn request_first(weak: &Weak<RecverShared>, stream_info: &mut RecverStreamInfo) -> Result<NodeSubscribeResponse> {
    let shared = acquire_shared(weak)?;
    let offset = {
        shared.data.lock().last_offset
    };

    let _r = stream_info.tx.send(NodeClientStream {
        data: Some(node_client_stream::Data::SubscribeReq(NodeSubscribeRequest {
            offset: offset.into(),
            client_node_id: Some(shared.node_id.into()),
        }))
    }).await?;

    let r = stream_info.rx_stream.message().await?
    .with_context(||"receive stream first fail")?
    .data
    .with_context(||"receive stream first but empty data")?;

    match r {
        node_server_stream::Data::SubscribeRsp(rsp) => { 
            Ok(rsp)
        },
        _ => bail!("expect header but got [{:?}]", r)
    }
}

#[derive(Default)]
struct RecverShared { 
    node_id: NodeId,
    inst_id: u64,
    cluster: Weak<ClusterInner>,
    event: Event,
    data: Mutex<RecverData>,
}

impl RecverShared {
    fn new(node_id: NodeId, cluster: Weak<ClusterInner>) -> Arc<Self> {
        let inst_id = next_instance_id();
        Arc::new(Self {
            node_id,
            inst_id,
            cluster,
            event: Default::default(),
            data: Default::default(),
        })
    }
}


#[derive(Default)]
struct RecverData {
    task: Option<JoinHandle<Result<()>>>, 
    flags: BitFlags<Flag>,
    last_offset: NodeOffset,
}

struct RecverStreamInfo {
    tx: Sender<NodeClientStream>, 
    rx_stream: Streaming<NodeServerStream>,
    offset: NodeOffset,
}


#[inline]
fn acquire_shared(weak: &Weak<RecverShared>) -> Result<Arc<RecverShared>> {
    weak.upgrade().with_context(||"no one care about me")
}

#[inline]
fn acquire_cluster(weak: &Weak<ClusterInner>) -> Result<Arc<ClusterInner>> {
    weak.upgrade().with_context(||"cluster has gone")
}



