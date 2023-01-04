///
/// TODO:
/// - NodeClient 的 status
/// - 断开连接时， 实现类似“施密特触发器滞回曲线”的策略，避免打印太多日志，连接状态也可用类似策略
/// 


use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::ops::DerefMut;
use anyhow::{Result, Context, bail};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server};
use tokio::sync::oneshot;
use tonic::transport::Endpoint;
use tracing::{info, debug};
use futures::FutureExt;
use async_recursion::async_recursion;
use crate::uid::{NodeId, NodeOffset};
use crate::util::AddrUrl;
use crate::util::time::now_ms;
use super::id_pool::NodeIdPool;
use super::node_client::NodeClient;
use super::node_sender::NodeSyncSender;
use super::proto::{
    MergeAddrs,
    RpcClient,
    rpc_server::RpcServer,
    PrepareNodeRequest,
    PrepareNodeReply,
    prepare_node_response,
    NodeInfo,
    NodeStatus,
    NodeDeltaPacket,
    node_delta_packet,
};


pub async fn bootstrap<C>(cfg: &C) -> Result<Cluster> 
where
    C: ClusterConfig
{   
    
    let mut opt_node_id = cfg.cluster_node_id();
    let mut opt_loopback_id = None;
    let mut opt_reflex_ip = None;
    let mut opt_exist_nodes = None;
    // let mut opt_bootstrap_client = None;

    let default_listen_url = cfg.cluster_listen_default().with_context(||"invalid default cluster listen address")?;
    let listen_url = cfg.cluster_listen_url().with_context(||"invalid cluster listen address")?;
    let bootstrap_url = cfg.cluster_bootstrap_url().with_context(||"invalid cluster bootstrap address")?;

    if let Some(bootstrap_url) = &bootstrap_url { 
        let url_str = bootstrap_url.listen_url().to_string();

        let (_client, rsp) = prepare_node(&url_str, opt_node_id.is_none()).await?;

        // opt_bootstrap_client = Some((client, url_str));

        if let Some(id) = rsp.node_id {
            opt_node_id = Some(id.into());
        }

        opt_loopback_id = rsp.loopback_id;

        opt_exist_nodes = Some(rsp.nodes);

        let my_addr: SocketAddr = rsp.reflex_addr.as_deref()
        .with_context(||format!("prepare_node reply without addr"))?
        .parse()?;

        opt_reflex_ip = Some(my_addr.ip());
    }

    let node_id = opt_node_id.unwrap_or_else(||0.into());

    // let cluster0 = Cluster::new(node_id);
    let cluster0 = ClusterInner::new(node_id);

    if let Some(pool) = cfg.cluster_node_id_pool() {
        cluster0.add_node_id_pool(pool);
        let r = cluster0.try_alloc_my_node_id();
        if r {
            debug!("alloc my node_id [{}]", cluster0.node_id());
        }
    } else {
        if *node_id == 0 {
            info!("standalone mode");
            return Ok(Cluster::new(0.into()));
        }
    }
    
    let (server, url) = match listen_url {
        Some(url) => { 
            let server = cluster0.try_listen(url.to_sockaddr()?).await?;
            (server, url)
        },
        None => {
            let url = match opt_loopback_id {
                Some(id) => {
                    cluster0.merge_loopback_id(id);
                    cfg.cluster_listen_by_id(id)?
                },
                None => default_listen_url,
            };

            let addr = url.to_sockaddr()?;
            let server = cluster0.try_listen(addr).await?;
            (server, url)
        },
    };

    if let Some(ip) = opt_reflex_ip {
        let addr = format!("http://{}:{}", ip, server.listen_addr().port());
        cluster0.data.lock().check_add_my_addr(addr); 
    }
    info!("cluster rpc service at [{}]", url.listen_url());

    match opt_exist_nodes {
        Some(nodes) => {
            cluster0.data.lock().self_node.set_status(NodeStatus::PrepareOnline);

            cluster0.try_add_nodes(nodes.into_iter())?;

            // let rsp = client.meet(tonic::Request::new(MeetRequest {
            //     request_addr: addr,
            //     node: Some(cluster0.inner.data.lock().self_node.to_info()),
            // })).await?;
    
            // let reply = rsp.into_inner();
            // cluster0.try_add_nodes(reply.nodes.into_iter())?;
        },
        None => {
            cluster0.data.lock().self_node.set_status(NodeStatus::Alone);
        }
    }


    // if let Some((mut client, addr)) = opt_bootstrap_client {
    //     cluster0.inner.data.lock().self_node.set_status(NodeStatus::PrepareOnline);
    //     let rsp = client.meet(tonic::Request::new(MeetRequest {
    //         request_addr: addr,
    //         node: Some(cluster0.inner.data.lock().self_node.to_info()),
    //     })).await?;

    //     let reply = rsp.into_inner();
    //     cluster0.try_add_nodes(reply.nodes.into_iter())?;
    // } else {
    //     cluster0.inner.data.lock().self_node.set_status(NodeStatus::Alone);
    // }

    cluster0.data.lock().server = Some(server);

    Ok(Cluster { inner: cluster0 })
}

#[async_recursion]
async fn prepare_node(url_str: &str, is_alloc_node_id: bool) -> Result<(RpcClient, PrepareNodeReply)> {

    // let is_alloc_node_id = opt_node_id.is_none();

    let channel = Endpoint::from_shared(url_str.to_string())?
    .connect_lazy()?;
    
    let mut client = RpcClient::new(channel);

    let rsp = client.prepare_node(tonic::Request::new(PrepareNodeRequest {
        is_alloc_node_id,
        is_alloc_loopback_id: true,
        server_addr: url_str.to_string(),
    })).await?;
    
    let data = rsp.into_inner().data.with_context(||"prepare_node reply None")?;
    let rsp = match data {
        prepare_node_response::Data::RedirectNode(info) => {
            info!("prepare_node redirect to [{:?}]", info);
            for addr in info.addrs.iter() {
                let r = prepare_node(addr, is_alloc_node_id).await;
                if r.is_ok() {
                    return r;
                }
            }
            bail!("prepare_node redirect but all fail");
        },
        prepare_node_response::Data::Reply(rsp) => rsp,
    };

    debug!("prepare with [{}] and reply [{:?}]", url_str, rsp);
    if is_alloc_node_id && rsp.node_id.is_none() {
        bail!("prepare alloc node_id but reply has no node_id")
    }
    
    Ok((client, rsp))
}


#[derive(Clone)]
pub struct Cluster {
    inner: Arc<ClusterInner>,
}

impl Cluster {
    pub fn new(id: NodeId) -> Self {
        Self { 
            inner: ClusterInner::new(id)
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.inner.node_id()
    }
    
    pub fn loopback_id(&self) -> u64 {
        self.inner.data.lock().loopback_id
    }

}



pub(crate) struct ClusterInner { 
    weak: Weak<Self>,
    data: Mutex<ClusterData>,
}

impl ClusterInner {
    pub(crate) fn new(id: NodeId) -> Arc<Self> { 
        Arc::new_cyclic(|weak| {
            Self {
                weak: weak.clone(),
                data: Mutex::new(ClusterData {
                    self_node: NodeInfo { 
                        node_id: id.into(),
                        ..Default::default()
                    },  // Node::new(id),
                    ..Default::default()
                }),
            }
        })
    }

    pub(crate) fn acquire_me(&self) -> Result<Arc<Self>> {
        self.weak.upgrade().with_context(||"cluster already gone")
    }

    pub(crate) fn node_id(&self) -> NodeId {
        self.data.lock().node_id()
    }

    pub(crate) fn self_node(&self) -> NodeInfo {
        self.data.lock().self_node.clone()
    }

    pub(crate) fn merge_loopback_id(&self, id: u64) {
        let mut data = self.data.lock();
        if id > data.loopback_id {
            data.loopback_id = id;
        }
    }

    pub(crate) fn next_loopback_id(&self, ) -> u64 {
        let mut data = self.data.lock();
        if data.loopback_id == 0 {
            data.loopback_id += 1;    
        }
        data.loopback_id += 1;
        let id = data.loopback_id;
        debug!("alloc loopback id [{}]", id);
        id
    }

    pub(crate) fn alloc_node_id(&self) -> Option<NodeId> {
        let mut data = self.data.lock();
        data.node_id_pool.pop()
    }

    pub(crate) fn try_alloc_my_node_id(&self) -> bool { 
        let mut data = self.data.lock();
        if NodeId::new(0) == data.node_id() {
            if let Some(id) = data.node_id_pool.pop() {
                data.set_node_id(id);
                return true;
            }
        } 
        false
    }

    pub(crate) fn add_node_id_pool(&self, pool: &NodeIdPool) {
        
        let mut data = self.data.lock(); 
        let data = data.deref_mut();
        
        data.node_id_pool.merge(pool);

        for (id, _node) in data.nodes.iter() {
            data.node_id_pool.remove(id);
        }
        data.node_id_pool.remove(&data.node_id());
        data.node_id_pool.remove(&0.into());
    }

    /// 获取当前正常工作 且 node_id 最小的节点
    /// 
    /// 返回空代表当前节点是最小节点
    pub(crate) fn get_min_working_node_client(&self) -> Result<Option<(RpcClient, NodeId)>> {
        let mut data = self.data.lock();
        let data = data.deref_mut();
        let self_node_id = data.node_id();
        for (_id, node) in data.nodes.iter_mut() { 
            if node.node_id() >= self_node_id { 
                break;
            } else if node.status() == NodeStatus::Online  {
                if let Some(client) = node.get_rpc_client() {
                    return Ok(Some((client, node.node_id())))
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn try_add_node(&self, info: NodeInfo) -> Result<()> { 
        self.try_add_nodes(Some(info).into_iter())
    }

    pub(crate) fn try_add_nodes<I: Iterator<Item = NodeInfo>>(&self, nodes: I) -> Result<()> { 
        let mut updated_self = false;
        let mut updated_node = false;

        let mut data = self.data.lock();
        let my_id = data.node_id();
        for info in nodes {
            let node_id = info.node_id.into(); 
            if node_id == my_id { 
                data.self_node.merge_addrs(info.addrs.into_iter());
                updated_self = true;
                continue;
            }

            let r = data.nodes.get_mut(&node_id);
            match r {
                Some(exist) => {
                    let updated = exist.update_target_node(info.clone());
                    
                    if updated { 
                        let info = exist.to_info();
                        info!("update node addrs [{:?}]", info.addrs);
                        data.push_delta_node(info);
                        updated_node = true;
                    }
                },
                None => {
                    info!("add node [{:?}]", info);
                    let node = NodeClient::new(node_id, self.weak.clone(), info);

                    let target = node.to_info();

                    // let mut node = Node::from_info(info)?;
                    // node.set_status(NodeStatus::PrepareOnline);
                    // let target = node.to_info();
                    // let me = Arc::downgrade(&self.acquire_me()?);
                    // node.client().kick(data.self_node.to_info(), target.clone(), me);

                    data.nodes.insert(node_id, node);
                    data.push_delta_node(target);
                    updated_node = true;
                },
            }
        }

        if updated_self { 
            data.update_node_clients();
        }

        if updated_self || updated_node {
            data.wakeup_sync_senders();
        }

        Ok(())
    }

    pub(crate) fn on_client_connected(&self, node_id: NodeId, inst_id: u64) -> Result<()> { 
        
        let mut data = self.data.lock();
        let r = data.nodes.get_mut(&node_id);
        if let Some(node) = r { 
            if node.inst_id() == inst_id { 
                match node.status() {
                    NodeStatus::Unknown => {},
                    NodeStatus::Alone => {},
                    NodeStatus::PrepareOnline => {},
                    NodeStatus::Online => {},
                    NodeStatus::Fault => { 
                        info!("recover node [{:?}]", node_id);
                        // node.set_status(NodeStatus::Online);
                    },
                    NodeStatus::PrepareOffline => {},
                    NodeStatus::Offline => {},
                }
                // return Some(node.offset())
            }
        }
        // None
        Ok(())
    }

    pub(crate) fn on_client_disconnected(&self, node_id: NodeId, inst_id: u64) { 
        let mut data = self.data.lock();
        let r = data.nodes.get_mut(&node_id);
        if let Some(node) = r { 
            if node.inst_id() == inst_id {
                match node.status() {
                    NodeStatus::Unknown => {},
                    NodeStatus::Alone => {},
                    NodeStatus::PrepareOnline => {},
                    NodeStatus::Online => {
                        info!("fault node [{:?}]", node_id);
                        // node.set_status(NodeStatus::Fault);
                    },
                    NodeStatus::Fault => { },
                    NodeStatus::PrepareOffline => {},
                    NodeStatus::Offline => {},
                }
            }
        }
    }

    pub(crate) fn get_nodes_info(&self) -> Vec<NodeInfo> { 
        let data = self.data.lock();
        data.get_nodes_info()
        // let self_info = Some(data.self_node.to_info());

        // data.nodes.iter()
        // .map(|x|x.1.to_info())
        // .chain(self_info.into_iter())
        // .collect()
    }

    pub(crate) fn try_alone_to_online(&self, addr: String) -> bool { 
        let mut data = self.data.lock(); 
        
        let added_addr = data.check_add_my_addr(addr);
        
        let changed_status = data.check_change_my_status(NodeStatus::Alone, NodeStatus::Online);

        if added_addr || changed_status { 
            data.update_node_clients();
        }

        changed_status
    }

    pub(crate) fn add_sync_sender(&self, sender: NodeSyncSender) {
        self.data.lock().sync_senders.insert(sender.inst_id(), sender);
    }

    pub(crate) fn remove_sync_sender(&self, sync_id: u64 ) -> Option<NodeSyncSender> {
        self.data.lock().sync_senders.remove(&sync_id).map(|x|x.clone())
    }

    pub(crate) fn get_delta(&self, offset: NodeOffset ) -> GetDelta { 
        let data = self.data.lock();
        if let Some(front) = data.delta_que.front() {
            if offset >= front.offset.into() {
                let index = (offset - front.offset.into()) as usize;
                if index < data.delta_que.len() {
                    return GetDelta::Delta(data.delta_que[index].clone());
                } else {
                    return GetDelta::Latest;
                }
            }
        }
        GetDelta::Lagged(data.delta_offset, data.get_nodes_info())
    }

}

impl ClusterInner {
    async fn try_listen(&self, addr: SocketAddr) -> Result<ClusterServer> {
        
        let listener = TcpListener::bind(addr).await
        .with_context(||format!("fail to bind cluster listen at [{}]", addr))?;

        let listener = TcpListenerStream::new(listener);

        let (tx, rx) = oneshot::channel::<()>();

        let inner = self.acquire_me()?;

        let task = tokio::spawn(async move { 

            // refer https://github.com/hyperium/tonic/blob/master/examples/src/autoreload/server.rs
            let _r = Server::builder()
            .add_service(RpcServer::new(inner))
            .serve_with_incoming_shutdown(listener, rx.map(drop)).await?;
            Result::Ok(())
        });

    
        let server = ClusterServer {
            task,
            tx,
            addr,
        };
    
        Ok(server)
    }

}

pub trait ClusterConfig { 

    /// 设置的当前节点 id
    fn cluster_node_id(&self) -> Option<NodeId> ;

    /// 默认 rpc 监听地址
    fn cluster_listen_default(&self) -> Result<AddrUrl>;

    /// 根据 id 获取 监听地址
    fn cluster_listen_by_id(&self, id: u64) -> Result<AddrUrl>;

    /// 设置的 监听地址
    fn cluster_listen_url(&self) -> Result<Option<AddrUrl>>;

    /// 设置的 bootstrap 地址
    fn cluster_bootstrap_url(&self) -> Result<Option<AddrUrl>>;

    /// 设置的节点 id 池
    fn cluster_node_id_pool(&self) -> &Option<NodeIdPool>;
}


#[derive(Default)]
struct ClusterData {
    self_node: NodeInfo,
    // work_nodes: Vec<NodeId>, 
    nodes: BTreeMap<NodeId, NodeClient>,
    loopback_id: u64, // 在本机起多个node分配端口之用，生产环境避免使用
    node_id_pool: NodeIdPool,
    server: Option<ClusterServer>,
    sync_senders: HashMap<u64, NodeSyncSender>, // 只是暂存，
                                                // TODO: 
                                                // - 增加时间戳， 定期扫描清除超时（1分钟）的 sender，
                                                // - 或者创建 NodeSyncSender 时创建内部 task ， 由内部task 接收失败时移除
    delta_que: VecDeque<NodeDeltaPacket>,
    delta_offset: u64,
}

impl ClusterData { 
    fn node_id(&self) -> NodeId {
        self.self_node.node_id.into()
    }

    fn set_node_id(&mut self, node_id: NodeId) {
        self.self_node.node_id = node_id.into();
    }

    fn check_add_my_addr(&mut self, addr: String) -> bool {
        let r = self.self_node.addrs.iter().position(|x| *x == addr);
        if r.is_none() { 
            info!("add my rpc addr [{}]", addr);
            self.self_node.addrs.push(addr);
            true
        } else {
            false
        }
    }

    fn check_change_my_status(&mut self, expect_status: NodeStatus, new_status: NodeStatus) -> bool {
        if self.self_node.status() == expect_status {
            self.self_node.set_status(new_status);
            true
        } else {
            false
        }
    }

    fn update_node_clients(&self) { 
        // let self_node = self.self_node.to_info();
        // for (_id, node) in self.nodes.iter() {
        //     node.client().update_self_node(self_node.clone());
        // }
    }

    fn push_delta_node(&mut self, node: NodeInfo) { 
        self.trim_delta_que();

        self.delta_offset += 1;
        self.delta_que.push_back(NodeDeltaPacket {
            offset: self.delta_offset,
            timestamp: now_ms(),
            delta_data: Some(node_delta_packet::DeltaData::UpdateNode(node)),
        });
    }

    fn trim_delta_que(&mut self) { 
        const MAX: usize = 63; 
        if self.delta_que.len() > MAX {
            let num = self.delta_que.len() - MAX;
            for _i in 0..num {
                self.delta_que.pop_front();
            }
        }
    }

    fn wakeup_sync_senders(&self) {
        for (_id, sender) in self.sync_senders.iter() {
            sender.notify_delta();
        }
    }

    fn get_nodes_info(&self) -> Vec<NodeInfo> {
        let self_info = Some(self.self_node.clone());

        self.nodes.iter()
        .map(|x|x.1.to_info())
        .chain(self_info.into_iter())
        .collect()
    }
}


pub struct ClusterServer {
    task: JoinHandle<Result<()>>,
    tx: oneshot::Sender<()>,
    addr: SocketAddr,
}

impl ClusterServer {

    pub fn listen_addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub async fn shutdown(self) -> Result<()> {
        let _r = self.tx.send(());
        self.task.await?
    }
}

pub(crate) enum GetDelta {
    Lagged(u64, Vec<NodeInfo>),
    Delta(NodeDeltaPacket),
    Latest,
}



