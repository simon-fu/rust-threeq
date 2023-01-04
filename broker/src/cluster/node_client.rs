use std::{sync::{Arc, Weak}, time::{Duration, Instant}, net::IpAddr};
use anyhow::{Result, bail, Context};
use enumflags2::BitFlags;
use parking_lot::Mutex;
use reqwest::Url;
use tokio::task::JoinHandle;
use tracing::{debug, Instrument, info};
use event_listener::Event;

use crate::{uid::{NodeId, next_instance_id}, util::time::now_ms};

use super::proto::{
    MergeAddrs,
    RpcClient,
    MeetRequest,
    PingRequest,
    NodeInfo,
    NodeStatus, 
};
use super::node_stream::{client_sync_node, NodeSyncRecver};
use super::ClusterInner;
use super::common::Flag;


#[derive(Clone)]
pub(crate) struct NodeClient { 
    shared: Arc<Shared>,
}

impl NodeClient {
    pub(crate) fn new(node_id: NodeId, cluster: Weak<ClusterInner>, target: NodeInfo) -> Self { 
        let shared = Shared::new(node_id, cluster);
        kick(&shared, target);
        Self { shared }
    }

    pub(crate) fn inst_id(&self) -> u64 {
        self.shared.inst_id
    }

    pub(crate) fn node_id(&self) -> NodeId {
        self.shared.node_id
    }

    pub(crate) fn status(&self) -> NodeStatus {
        // TODO: 
        NodeStatus::Online
    }

    pub(crate) fn to_info(&self) -> NodeInfo {
        self.shared.data.lock().target_node.clone()
    }

    // /// 当自身节点的 rpc 地址有变更时都要调用这个接口 (状态变更也调用？)
    // pub fn update_self_node(&self, self_node: NodeInfo) { 
    //     let mut data = self.shared.data.lock();
    //     data.self_node = self_node;
    // }

    /// 当 target 节点的 rpc 地址有变更时都要调用这个接口
    pub fn update_target_node(&self, target_node: NodeInfo) -> bool {
        let mut data = self.shared.data.lock();
        data.is_target_updated = data.target_node.merge_addrs(target_node.addrs.into_iter());
        self.shared.event.notify(usize::MAX);
        data.is_target_updated
    }

    pub fn get_rpc_client(&self) -> Option<RpcClient> {
        let data = self.shared.data.lock();
        data.rpc_client.clone()
    }

}



#[derive(Default)]
struct Shared { 
    node_id: NodeId, // target node_id
    cluster: Weak<ClusterInner>,
    inst_id: u64,
    event: Event,
    recver: NodeSyncRecver,
    data: Mutex<ClientData>,
}

fn kick(shared: &Arc<Shared>, target: NodeInfo) { 
        
    let mut data = shared.data.lock();

    if data.flags.contains(Flag::Shutdown | Flag::Fininshed) {
        return ;
    }

    // data.self_node = self_node;
    data.target_node = target;
    data.is_target_updated = true;

    let task = {
        let name = format!("node({})-client({})", shared.node_id, shared.inst_id);
        let span = tracing::span!(parent:None, tracing::Level::DEBUG, "", s = name.as_str());
        let weak = Arc::downgrade(shared);

        let fut = async move {

            info!("start working");
            let task_r = client_task(&weak).await;

            {
                // 设置结束标志，并且唤醒等待的 task
                let r = weak.upgrade();
                if let Some(shared) = r {
                    shared.data.lock().flags |= Flag::Fininshed;
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

impl Shared {
    fn new(node_id: NodeId, cluster: Weak<ClusterInner>) -> Arc<Self> {
        let inst_id = next_instance_id();
        Arc::new(Self {
            node_id,
            cluster: cluster.clone(),
            inst_id,
            recver: NodeSyncRecver::new(node_id, cluster),
            ..Default::default()
        })
    }
}

#[derive(Default)]
struct ClientData { 
    task: Option<JoinHandle<Result<()>>>, 
    // self_node: NodeInfo,
    target_node: NodeInfo,
    is_target_updated: bool,
    rpc_client: Option<RpcClient>, // None 代表 Fault 
    flags: BitFlags<Flag>,
}


async fn client_task(weak: &Weak<Shared>) -> Result<()> { 

    let mut last_addr = None;
    let mut target_node = NodeInfo::default();
    loop {
        check_target_node(weak, &mut target_node)?;

        let _r = connect_and_handshake(weak, &mut last_addr, &target_node).await?;


        let mut listen = {
            weak_acquire_shared(weak)?.event.listen()
        };

        // 重连间隔时间
        let interval = Duration::from_millis(1000);
        let updated = check_target_node(weak, &mut target_node)?;
        
        if !updated {
            let start = Instant::now();
            loop {
                let elapsed = start.elapsed();
                if elapsed >= interval { 
                    check_target_node(weak, &mut target_node)?;
                    break;
                }
                let d = interval - elapsed;

                tokio::select! {
                    _r = tokio::time::sleep(d) => {}
        
                    _r = &mut listen => {
                        let updated = check_target_node(weak, &mut target_node)?;
                        if updated {
                            break;
                        }
                    }
                }
            }
        }
    }    

}


async fn loop_ping(weak: &Weak<Shared>, mut client: RpcClient) -> Result<()> { 
    
    let interval = Duration::from_millis(1000); // ping 间隔
    let mut last_print_ts = 0;
    let mut last_ping_time = Instant::now() - interval;

    loop {
        
        let sleep_duration = {
            let elapsed = last_ping_time.elapsed();
            if elapsed < interval {
                interval - elapsed
            } else {
                Duration::ZERO
            }
        };

        let mut listen = {
            weak_acquire_shared(weak)?.event.listen()
        };
        
        tokio::select! {
            _r = tokio::time::sleep(sleep_duration) => { 
                call_ping(&mut client, &mut last_print_ts).await?;
                last_ping_time = Instant::now();
            }

            _r = &mut listen => { 
                let shared = weak_acquire_shared(weak)?;
                let data = shared.data.lock();
                if data.flags.contains(Flag::Shutdown) {
                    bail!("got shutdown request");
                }
            }
        }     
    }
}

async fn call_ping(client: &mut RpcClient, last_print_ts: &mut i64) -> Result<()> {

    let rsp = client.ping(tonic::Request::new(PingRequest {
        ts_milli: now_ms(),
    })).await?.into_inner();

    // TODO: 实现类似“施密特触发器滞回曲线”的策略，避免打印太多日志

    let now = now_ms();
    let elapsed = now - rsp.origin_ts_milli;
    let est_diff = rsp.ts_milli - rsp.origin_ts_milli - elapsed/2; // 估计值 = 对方时间 - 本地时间

    if elapsed > 1500 || est_diff.abs() > 1000 { 
        if (now - *last_print_ts) > 3000 {
            debug!("ping elapsed [{}] ms, diff [{}] ms", elapsed, est_diff);
            *last_print_ts = now;
        }                    
    } else {
        *last_print_ts = 0;
    }
    Ok(())
}

fn check_target_node(weak: &Weak<Shared>, node: &mut NodeInfo) -> Result<bool> {
    let shared = weak_acquire_shared(weak)?;
    let mut data = shared.data.lock();
    
    if data.flags.contains(Flag::Shutdown) {
        bail!("got shutdown request");
    }

    if data.is_target_updated {
        data.is_target_updated = false;
        info!("update target [{:?}]", data.target_node);
        let r = sort_addrs(&mut node.addrs);
        if let Err(e) = r {
            info!("sort addrs error [{:?}]", e);
        }
        *node = data.target_node.clone();
        Ok(true)
    } else {
        Ok(false)
    }

    // if let Some(v) = data.target_node.take() { 
    //     info!("update target [{:?}]", v);
    //     let r = sort_addrs(&mut node.addrs);
    //     if let Err(e) = r {
    //         info!("sort addrs error [{:?}]", e);
    //     }
    //     *node = v;
    //     Ok(true)
    // } else {
    //     Ok(false)
    // }
}

async fn connect_and_handshake(weak: &Weak<Shared>, last_addr: &mut Option<String>, target_node: &NodeInfo) -> Result<()> {
    let r = try_connect_and_handshake(weak, last_addr, target_node).await?;
    if let Some(client) = r { 

        {
            let shared = weak_acquire_shared(weak)?;

            let cluster = {
                let mut data = shared.data.lock();
                data.rpc_client = Some(client.clone());
                shared_acquire_cluster(&shared)?
            };

            cluster.on_client_connected(shared.node_id, shared.inst_id)
            .with_context(||"connected but processed fail")?;

            // 唤醒等待连接成功的 task
            shared.event.notify(usize::MAX);
        }

        let r = loop_ping(weak, client).await;
        if let Err(e) = r {
            info!("loop_ping return [{:?}]", e);
        }

        {
            let shared = weak_acquire_shared(weak)?;

            let cluster = {
                let mut data = shared.data.lock();
                data.rpc_client = None;
                shared_acquire_cluster(&shared)?
            };

            cluster.on_client_disconnected(shared.node_id, shared.inst_id);
        }
        
    }

    Ok(())
}

async fn try_connect_and_handshake(weak: &Weak<Shared>, last_addr: &mut Option<String>, target_node: &NodeInfo) -> Result<Option<RpcClient>> {
    // TODO: config connect timeout 

    if let Some(dst) = last_addr.as_deref() {
        let r = try_addr_once(weak, dst).await;
        match r {
            Ok(return_value) => {
                info!("re-handshaked with [{}]", dst); 
                return Ok(Some(return_value))
            },
            Err(e) => {
                debug!("fail to re-handshake with [{}], [{:?}]", dst, e);
            }
        }
    }

    for dst in target_node.addrs.iter() { 
        if Some(dst.as_str()) == last_addr.as_deref() {
            // 上面已经尝试过，不再尝试
            continue;
        }

        // let r = RpcClient::connect(dst.to_string()).await;
        let r = try_addr_once(weak, dst).await;
        match r {
            Ok(return_value) => { 
                // established
                if Some(dst.as_str()) == last_addr.as_deref() {
                    info!("again handshaked with [{}]", dst);
                } else {
                    if last_addr.is_none() {
                        info!("first handshaked with [{}]", dst);
                    } else {
                        info!("handshaked with another [{}]", dst);
                    }
                }                
                
                *last_addr = Some(dst.to_string());

                return Ok(Some(return_value))
            },
            Err(e) => {
                debug!("fail to handshake with [{}], [{:?}]", dst, e);
            }
        }
    }    

    Ok(None)
}

async fn try_addr_once(weak: &Weak<Shared>, dst: &str) -> Result<RpcClient> {
    let mut client = RpcClient::connect(dst.to_string()).await?; 
    debug!("connected to [{}]", dst);

    let _r = try_meet(weak, &mut client, dst).await
    .with_context(||"try meet fail")?;

    {
        let shared = weak_acquire_shared(weak)?;
        client_sync_node(&shared.recver, &mut client).await
        .with_context(||"try sync node fail")?;   
    }

    // let recver = {
    //     let (self_node_id, offset, cluster) = {
    //         let shared = weak_acquire_shared(weak)?;
    //         let data = shared.data.lock();
    //         let cluster = data_acquire_cluster(data.deref())?;
    //         let self_node_id = cluster.node_id();
    //         let target_offset = cluster.get_node_offset(shared.node_id).with_context(||"Not found node when get offset")?;
    //         (self_node_id, target_offset, data.cluster.clone())
    //     };

    //     let recver = client_sync_node(&mut client, self_node_id, offset, cluster).await
    //     .with_context(||"try sync node fail")?;
    //     recver
    // };

    Ok(client)
}


async fn try_meet(weak: &Weak<Shared>, client: &mut RpcClient, server_addr: &str) -> Result<()> {
    let req = {
        let shared = weak_acquire_shared(weak)?;
        let server_node_id = shared.node_id.into();
        // let data = shared.data.lock();
        let self_node = shared_acquire_cluster(&shared)?.self_node();
        tonic::Request::new(MeetRequest {
            client_node: Some(self_node),
            server_addr: server_addr.to_string(),
            server_node_id,
        })
    };
    let rsp = client.meet(req).await?.into_inner();

    {
        let cluster = weak_acquire_cluster(weak)?;
        cluster.try_add_nodes(rsp.nodes.into_iter())?;
    }

    Ok(())
}

#[inline]
fn weak_acquire_shared(weak: &Weak<Shared>) -> Result<Arc<Shared>> {
    weak.upgrade().with_context(||"no one care about me")
}

#[inline]
fn weak_acquire_cluster(weak: &Weak<Shared>) -> Result<Arc<ClusterInner>> { 
    shared_acquire_cluster(&weak_acquire_shared(weak)?)
}

#[inline]
fn shared_acquire_cluster(shared: &Arc<Shared>) -> Result<Arc<ClusterInner>> {
    shared.cluster.upgrade()
    .with_context(||"owner had gone")
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum HostType { 
    IpPrivate = 1, 
    // IpGlobal,
    IpOther,
    IpLoopback,
    Domain,
}

fn sort_addrs(addrs: &mut Vec<String>) -> Result<()> { 
    let mut typed_addrs = Vec::with_capacity(addrs.len());
    
    while let Some(s) = addrs.pop() {
        let url = Url::parse(s.as_str())
        .with_context(||format!("invalid url [{}]", s))?;

        let host = url.host_str()
        .with_context(||format!("no host of url [{}]", s))?;

        let r = host.parse::<IpAddr>();
        
        match r {
            Ok(ip) => {
                if ip.is_unspecified() {
                    
                } else if ip.is_loopback() {
                    typed_addrs.push((HostType::IpLoopback, s));

                } else if is_private_ip(&ip) {
                    typed_addrs.push((HostType::IpPrivate, s));

                } 
                // else if ip.is_global() { // 要用到 use of unstable library feature 'ip'
                //     typed_addrs.push((HostType::IpGlobal, s));

                // } 
                else { 
                    typed_addrs.push((HostType::IpOther, s));
                }
            },
            Err(_e) => {
                typed_addrs.push((HostType::Domain, s));
            },
        };
    }

    typed_addrs.sort_by(|x, y| x.0.cmp(&y.0) );

    for v in typed_addrs {
        addrs.push(v.1);
    }

    Ok(())
}

fn is_private_ip(ip: &IpAddr) -> bool {
    if let IpAddr::V4(v4) = ip {
        v4.is_private()
    } else {
        false
    }
}





