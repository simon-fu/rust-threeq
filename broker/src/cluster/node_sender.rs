
use std::sync::{Arc, Weak};
use enumflags2::BitFlags;
use event_listener::Event;
use parking_lot::Mutex;
use anyhow::{Result, bail, Context};
use tokio::{task::JoinHandle, sync::mpsc::Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tracing::{info, Instrument, debug};

use crate::uid::{next_instance_id, NodeOffset};

use super::{proto::{
    NodeClientStream,
    node_client_stream,
    NodeServerStream,
    node_server_stream,
    NodeSubscribeRequest,
    NodeSubscribeResponse,
    NodeStatePacket,
    node_state_packet,
}, ClusterInner, GetDelta, common::Flag};



pub(crate) type NodeStreamItem = Result<NodeServerStream, tonic::Status>;
pub(crate) type PinNodeStream = std::pin::Pin<Box<dyn futures::Stream<Item = NodeStreamItem> + Send + Sync>>;

pub(crate) async fn server_sync_node(
    req_stream: Streaming<NodeClientStream>,
    cluster: Arc<ClusterInner>, 
) -> PinNodeStream {

    const CHANNEL_SIZE: usize = 128;
    let (tx, rx) = tokio::sync::mpsc::channel::<NodeStreamItem>(CHANNEL_SIZE);

    let sender = NodeSyncSender::new(Arc::downgrade(&cluster), tx);

    let rsp_stream = ReceiverStream::new(rx);
    let rsp_stream = Box::pin(rsp_stream) as PinNodeStream;
    sender.kick(cluster, req_stream);
    
    rsp_stream
}

#[derive(Clone)]
pub(crate) struct NodeSyncSender { 
    shared: Arc<SenderShared>,
}

impl NodeSyncSender {
    pub(crate) fn new(
        cluster: Weak<ClusterInner>,
        tx: Sender<NodeStreamItem>,
    ) -> Self { 

        Self { shared: SenderShared::new(cluster, tx) }
    }

    pub(crate) fn inst_id(&self) -> u64 {
        self.shared.inst_id
    }

    pub(crate) fn notify_delta(&self) {
        self.shared.event.notify(usize::MAX);
    }

    fn kick(
        &self, 
        cluster: Arc<ClusterInner>,
        req_stream: Streaming<NodeClientStream>, 
    ) { 
        // let self0 = NodeSync::new(target_node_id);
    
        let mut data = self.shared.data.lock();
    
        if data.flags.contains(Flag::Shutdown | Flag::Fininshed) {
            return ;
        }
    
        let task = { 
            let name = format!("node_sender({})", self.shared.inst_id);
            let span = tracing::span!(parent:None, tracing::Level::DEBUG, "", s = name.as_str());

            let weak = Arc::downgrade(&self.shared);
    
            let fut = async move {
    
                info!("start working");
                let task_r = sender_task(&weak, req_stream).await;
    
                {
                    // 设置结束标志，并且唤醒等待的 task
                    let r = weak.upgrade();
                    if let Some(shared) = r { 
                        {
                            shared.data.lock().flags |= Flag::Fininshed;
                            shared.event.notify(usize::MAX);
                        }

                        let r = shared.cluster.upgrade();
                        if let Some(cluster) = r {
                            cluster.remove_sync_sender(shared.inst_id);
                        }
                    }
                }
                
                debug!("finished with [{:?}]", task_r);
                task_r
            }.instrument(span);
    
            tokio::spawn(fut)
        };
    
        data.task = Some(task);
        cluster.add_sync_sender(self.clone());
    } 
}



async fn sender_task(
    weak: &Weak<SenderShared>,
    mut req_stream: Streaming<NodeClientStream>, 
) -> Result<()> { 
    // TODO： 注意退出条件：没有人关心时； 发送失败 

    let req = recv_first_req(weak, &mut req_stream).await?;
    info!("got req {:?}", req);

    match req.client_node_id {
        Some(node_id) => {
            
            let name = {
                let shared = acquire_shared(weak)?;
                format!("node({})-sender({})", node_id, shared.inst_id)
            };
            let span = tracing::span!(parent:None, tracing::Level::DEBUG, "", s = name.as_str());
            send_loop(weak, req.offset.into()).instrument(span).await
        },
        None => {
            send_loop(weak, req.offset.into()).await
        },
    }

}

async fn send_loop(
    weak: &Weak<SenderShared>,
    mut offset: NodeOffset,
) -> Result<()> { 
    offset = send_next(&acquire_shared(weak)?, offset).await?.into();

    loop {
        let ev_listener = acquire_shared(weak)?.event.listen(); 
        
        tokio::select! {
            _r = ev_listener => { 
                let shared = acquire_shared(weak)?;
                {
                    let data = shared.data.lock();
                    if data.flags.contains(Flag::Shutdown) {
                        bail!("got shutdown request");
                    }
                }

                offset = send_next(&shared, offset).await?;
            },
        }
    }
}

async fn recv_first_req(weak: &Weak<SenderShared>, req_stream: &mut Streaming<NodeClientStream>) -> Result<NodeSubscribeRequest> {
    let shared = acquire_shared(weak)?; 
    let req = req_stream.message().await
    .with_context(||"recving first req fail")?
    .with_context(||"recving first req but EOF")?;
    
    let data = req.data.with_context(||"recved first but empty data")?;
    let req = match data {
        node_client_stream::Data::SubscribeReq(req) => {
            shared.tx.send(Ok(NodeServerStream{
                data: Some(node_server_stream::Data::SubscribeRsp(NodeSubscribeResponse {
                    sync_id: shared.inst_id,
                })),
            })).await?;
            req
        },
        // _ => bail!("expect first subcribe req but [{:?}]", data),
    };
    Ok(req)
}

async fn send_next(
    shared: &Arc<SenderShared>,
    offset: NodeOffset,
) -> Result<NodeOffset> { 

    let mut next_offset = offset;
    loop {
        
        let cluster = acquire_cluster(&shared.cluster)?;
        
        let r = cluster.get_delta(next_offset);
        next_offset = match r {
            GetDelta::Delta(delta) => {
                shared.tx.send(Ok(NodeServerStream{
                    data: Some(node_server_stream::Data::Delta(delta)),
                })).await?;
                offset + 1
            },
            GetDelta::Lagged(offset, nodes) => {
                let mut seq = 0;
                for node in nodes {
                    shared.tx.send(Ok(NodeServerStream{
                        data: Some(node_server_stream::Data::State(NodeStatePacket {
                            offset,
                            seq,
                            state_data: Some(node_state_packet::StateData::Node(node)),
                        })),
                    })).await?;
                    seq += 1;
                }
                shared.tx.send(Ok(NodeServerStream{
                    data: Some(node_server_stream::Data::State(NodeStatePacket {
                        offset,
                        seq,
                        state_data: Some(node_state_packet::StateData::Last(true)),
                    })),
                })).await?;
                (offset + 1).into()
            },
            GetDelta::Latest => {
                return Ok(offset)
            }
        };
    }
}

#[inline]
fn acquire_shared(weak: &Weak<SenderShared>) -> Result<Arc<SenderShared>> {
    weak.upgrade().with_context(||"no one care about me")
}

#[inline]
fn acquire_cluster(weak: &Weak<ClusterInner>) -> Result<Arc<ClusterInner>> {
    weak.upgrade().with_context(||"cluster has gone")
}

struct SenderShared { 
    inst_id: u64,
    tx: Sender<NodeStreamItem>,
    cluster: Weak<ClusterInner>,
    event: Event,
    data: Mutex<SenderData>,
}

impl SenderShared {
    fn new(
        cluster: Weak<ClusterInner>,
        tx: Sender<NodeStreamItem>,
    ) -> Arc<Self> {
        let inst_id = next_instance_id();
        Arc::new(Self {
            inst_id,
            tx,
            cluster,
            event: Default::default(),
            data: Default::default(),
        })
    }
}


#[derive(Default)]
struct SenderData {
    task: Option<JoinHandle<Result<()>>>, 
    flags: BitFlags<Flag>,
}
