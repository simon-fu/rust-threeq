use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;

use super::znodes::{
    self, DeltaSyncReply, DeltaSyncRequest, FullSyncReply, FullSyncRequest, NodeInfo,
};
use super::zrpc;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{broadcast, watch, RwLock};
use tracing::Instrument;
use tracing::{debug, error, info, trace, warn};

// macro_rules! define_var {
//     ($val:expr,) => {

//     };
//     ($val:expr, $id:ident, $($ids:ident),*$(,)?) => {
//         let $id = $val;
//         define_var!($val + 1, $($ids,)*);
//     };
// }

// fn test() {
//     define_var!(100, a, b, c);
//     println!("{}", a);
//     println!("{}", b);
//     println!("{}", c);
// }

// pub struct Node {
//     phantom: PhantomData<bool>, // prevent construct from outside
//     pub info: NodeInfo,
//     htask: Option<JoinHandle<()>>,
// }

// impl Node {
//     fn with_info(info: NodeInfo) -> Self {
//         Self {
//             phantom: PhantomData,
//             info,
//             htask: None,
//         }
//     }

//     fn new(uid: String) -> Self {
//         Self {
//             phantom: PhantomData,
//             info: NodeInfo {
//                 uid: uid,
//                 addrs: Vec::new(),
//             },
//             htask: None,
//         }
//     }

//     fn add_addr<S: Into<String>>(&mut self, addr: S) -> bool {
//         let addr = addr.into();
//         if !self.info.addrs.contains(&addr) {
//             self.info.addrs.push(addr);
//             true
//         } else {
//             false
//         }
//     }

//     fn add_addrs(&mut self, addrs: &Vec<String>) {
//         for addr in addrs {
//             self.add_addr(addr);
//         }
//     }
// }

pub type NodeId = u32;

// #[derive(Debug, Default, Clone)]
// struct NodeId(u32);
// impl NodeId {
//     fn inner(self) -> u32 {
//         self.0
//     }
// }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    Disconnect,
    Running,
}

impl Default for NodeState {
    fn default() -> Self {
        NodeState::Disconnect
    }
}

#[derive(Default, Clone)]
struct InnerNode {
    info: NodeInfo,
    addr: String,
    state: NodeState,
}

impl InnerNode {
    fn with_info(info: &NodeInfo) -> Self {
        Self {
            info: info.clone(),
            ..Default::default()
        }
    }

    fn with_uid(uid: &NodeId) -> Self {
        let mut info = NodeInfo::default();
        info.uid = uid.clone();
        Self {
            info,
            ..Default::default()
        }
    }
}

#[derive(Default)]
struct Delta {
    data: HashMap<NodeId, NodeInfo>,
    ver: u64,
    src: NodeId,
}

#[derive(Default, Clone)]
struct InnerNodes {
    data: HashMap<NodeId, InnerNode>,
    ver: u64,
}

impl InnerNodes {
    fn merge_node(&mut self, info: &NodeInfo, delta: &mut Delta) -> bool {
        let exist = self.data.get_mut(&info.uid);
        if exist.is_none() {
            //debug!("merge_node: add node {:?}", info);
            self.data
                .insert(info.uid.clone(), InnerNode::with_info(&info));
            delta.data.insert(info.uid.clone(), info.clone());
            return true;
        }

        let node = exist.unwrap();
        for addr in &info.addrs {
            if !node.info.addrs.contains(addr) {
                node.info.addrs.push(addr.clone());
                let delta_node = delta
                    .data
                    .entry(node.info.uid.clone())
                    .or_insert(NodeInfo::default());
                delta_node.addrs.push(addr.clone());
            }
        }
        return false;
    }

    fn merge_addr(&mut self, uid: &NodeId, addr: String, delta: &mut Delta) -> bool {
        let mut is_add_node = false;
        if !self.data.contains_key(uid) {
            let node = InnerNode::with_uid(uid);
            //debug!("merge_addr: add node {}", uid);
            self.data.insert(uid.clone(), node);
            is_add_node = true;
        }

        let node = self.data.get_mut(uid).unwrap();
        if !node.info.addrs.contains(&addr) {
            node.info.addrs.push(addr.clone());
            let delta_node = delta
                .data
                .entry(node.info.uid.clone())
                .or_insert(NodeInfo::default());
            delta_node.addrs.push(addr);
        }
        is_add_node
    }
}

#[derive(Default)]
struct SeqData {
    nodes: InnerNodes,
    history: VecDeque<Arc<Delta>>,
}

impl SeqData {
    fn new(uid: NodeId) -> Self {
        let node = InnerNode::with_uid(&uid);
        let mut self0: SeqData = Default::default();
        self0.nodes.data.insert(uid, node);
        self0
    }

    fn merge_full(
        &mut self,
        local_uid: &NodeId,
        req: FullSyncRequest,
        addr: String,
    ) -> (u64, Option<HashSet<NodeId>>) {
        if req.src == *local_uid {
            return (self.nodes.ver, None);
        }

        let mut uids: HashSet<NodeId> = HashSet::default();
        let mut delta = Delta::default();
        let nodes = &mut self.nodes;
        for info in &req.nodes {
            if nodes.merge_node(info, &mut delta) {
                uids.insert(info.uid.clone());
            }
        }

        nodes.merge_addr(local_uid, req.your_active_addr, &mut delta);
        // let addr = format!("{}:{}", from_ip, req.port);
        if nodes.merge_addr(&req.uid, addr, &mut delta) {
            uids.insert(req.uid.clone());
        }

        if !delta.data.is_empty() {
            nodes.ver += 1;
            delta.ver = nodes.ver;
            delta.src = req.src.clone();
            self.history.push_back(Arc::new(delta));
            return (nodes.ver, Some(uids));
        } else {
            return (nodes.ver, None);
        }
    }

    fn merge_delta(
        &mut self,
        local_uid: &NodeId,
        req: DeltaSyncRequest,
    ) -> (Option<u64>, Option<HashSet<NodeId>>) {
        if req.src == *local_uid {
            return (None, None);
        }

        let mut uids: HashSet<NodeId> = HashSet::default();
        let mut delta = Delta::default();
        let nodes = &mut self.nodes;
        for info in &req.nodes {
            if nodes.merge_node(info, &mut delta) {
                uids.insert(info.uid.clone());
            }
        }

        if !delta.data.is_empty() {
            nodes.ver += 1;
            delta.ver = nodes.ver;
            delta.src = req.src.clone();
            self.history.push_back(Arc::new(delta));
            return (Some(nodes.ver), Some(uids));
        } else {
            return (None, None);
        }
    }

    fn get_full(&self) -> (Vec<NodeInfo>, u64) {
        let mut nodes = Vec::new();
        for (_, node) in &self.nodes.data {
            nodes.push(node.info.clone());
        }
        return (nodes, self.nodes.ver);
    }

    fn get_delta(&self, ver: u64) -> Option<Arc<Delta>> {
        if self.history.is_empty() {
            return None;
        }

        let first_ver = self.history.front().unwrap().ver;
        if ver < first_ver {
            return None;
        }

        let index = (ver - first_ver) as usize;
        if index >= self.history.len() {
            return None;
        }

        return Some(self.history[index].clone());
    }

    fn get_node_addrs(&self, uid: &NodeId) -> Option<Vec<String>> {
        let r = self.nodes.data.get(uid);
        if let Some(node) = r {
            return Some(node.info.addrs.clone());
        } else {
            return None;
        }
    }
}

#[derive(Default)]
struct LocalNode {
    uid: NodeId,
    port: i32,
    data: Arc<RwLock<SeqData>>,
}

impl LocalNode {
    fn new(uid: NodeId, port: i32) -> Self {
        let data = SeqData::new(uid);
        let data: Arc<RwLock<SeqData>> = Arc::new(RwLock::new(data));
        Self { uid, port, data }
    }

    async fn build_full_sync(&self) -> FullSyncRequest {
        let mut req = FullSyncRequest::default();
        req.uid = self.uid.clone();
        req.port = self.port;
        req.src = self.uid.clone();
        let (nodes, ver) = self.data.read().await.get_full();
        req.nodes = nodes;
        req.data_ver = ver;
        return req;
    }

    async fn build_delta_sync(&self, ver: u64) -> Option<DeltaSyncRequest> {
        let r = self.data.read().await.get_delta(ver);
        if let Some(delta) = r {
            let mut req = DeltaSyncRequest::default();
            for (_s, info) in &delta.data {
                req.nodes.push(info.clone());
            }
            req.src = delta.src.clone();
            req.data_ver = delta.ver;
            return Some(req);
        } else {
            None
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    IoError(#[from] std::io::Error),

    #[error("{0}")]
    ZRpcError(#[from] zrpc::Error),

    #[error("TryConnectFail")]
    TryConnectFail,

    #[error("error: {0}")]
    Generic(String),
}

struct NodeSyncer {
    uid: NodeId,
    local: Arc<LocalNode>,
    ver: u64,
}

impl NodeSyncer {
    async fn sync_with_addr(
        &mut self,
        client: &mut zrpc::Client,
        addr: &String,
        delta_rx: &mut watch::Receiver<u64>,
    ) -> Result<(), Error> {
        let (tx, mut disconn_rx) = broadcast::channel(2);

        #[derive(Debug)]
        struct Watcher(broadcast::Sender<()>);

        #[async_trait]
        impl zrpc::ClientWatcher for Watcher {
            async fn on_disconnect(&mut self, reason: zrpc::Reason, detail: &zrpc::Error) {
                warn!("disconnect with [{:?}], detail [{:?}]", reason, detail);
                let _r = self.0.send(());
            }
        }

        client.watch(Box::new(Watcher(tx))).await?;

        {
            let mut req = self.local.build_full_sync().await;
            req.your_active_addr = addr.clone();
            let r = client.call(&req).await?;
            let _r = r.await?;
            self.ver = req.data_ver;
        }

        loop {
            tokio::select! {
                r = delta_rx.changed() => {
                    if r.is_err() {
                        break;
                    }
                }
                _r = disconn_rx.recv() => {
                    break;
                }
            }

            let r = self.local.build_delta_sync(self.ver).await;
            if let Some(req) = r {
                let r = client.call(&req).await?;
                let _reply = r.await?;
            } else {
                let mut req = self.local.build_full_sync().await;
                req.your_active_addr = addr.clone();
                debug!("out of sync, {} -> {} ", self.ver, req.data_ver);

                let r = client.call(&req).await?;
                let _reply = r.await?;
                self.ver = req.data_ver;
            }
        }
        Ok(())
    }

    async fn try_connect(&mut self) -> Result<(zrpc::Client, String), Error> {
        let r = self.local.data.read().await.get_node_addrs(&self.uid);

        if let Some(addrs) = r {
            for addr in addrs {
                let mut client = zrpc::Client::builder()
                    .service_type(znodes::service_type())
                    .keep_alive(2)
                    .build();

                let r = client.connect(&addr).await;
                if let Err(e) = r {
                    warn!("try fail, {:?}", e);
                    continue;
                }
                return Ok((client, addr));
            }
        }
        return Err(Error::TryConnectFail);
    }

    async fn run(
        &mut self,
        delta_rx: &mut watch::Receiver<u64>,
        node_tx: &mut NodesSender,
    ) -> Result<(), Error> {
        let mut wait_secs = 1;
        loop {
            let r = self.try_connect().await;
            match r {
                Ok((mut client, addr)) => {
                    info!("connected to node [{}], addr [{}]", self.uid, addr);
                    wait_secs = 1; // reset timeout
                    {
                        let mut data = self.local.data.write().await;
                        let r = data.nodes.data.get_mut(&self.uid);
                        if let Some(node) = r {
                            node.addr = addr.clone();
                            node.state = NodeState::Running;
                        }
                    }
                    let _r = node_tx.send(());
                    let _r = self.sync_with_addr(&mut client, &addr, delta_rx).await;

                    {
                        let mut data = self.local.data.write().await;
                        let r = data.nodes.data.get_mut(&self.uid);
                        if let Some(node) = r {
                            node.state = NodeState::Disconnect;
                        }
                    }
                }
                Err(_e) => {
                    wait_secs *= 2;
                    if wait_secs > 16 {
                        wait_secs = 16;
                    }
                }
            }

            debug!("wait for next round in seconds {}", wait_secs);
            tokio::select! {
                _r = delta_rx.changed() => { }
                _r = tokio::time::sleep(Duration::from_secs(wait_secs)) => { }
            }
        }
    }
}

struct Handler {
    tx: watch::Sender<u64>,
    rx: watch::Receiver<u64>,
    node_tx: NodesSender,
    local: Arc<LocalNode>,
}

#[async_trait]
impl znodes::Handler for Handler {
    async fn handle_full_sync(
        &self,
        session: &zrpc::Session,
        req: FullSyncRequest,
    ) -> FullSyncReply {
        trace!("serivce: <= full sync, {:?}", req);

        let mut data = self.local.data.write().await;
        let mut reply = FullSyncReply::default();

        let mut exist = false;
        if req.uid == self.local.uid {
            exist = true;
        } else if let Some(node) = data.nodes.data.get(&req.uid) {
            if node.state == NodeState::Running {
                exist = true;
            }
        }

        if exist {
            reply.code = -1;
            reply.msg = format!("already exist node {}", req.uid);
        } else {
            let addr = format!("{}:{}", session.remote_addr().ip(), req.port);
            let (ver, uids) = data.merge_full(&self.local.uid, req, addr.clone());
            let _r = self.tx.send(ver); // always kick
            self.launch_syncers(uids).await;

            reply.uid = self.local.uid.clone();
            reply.your_reflex_addr = addr.clone();
        }

        trace!("serivce: => full sync, {:?}", reply);

        reply
    }

    async fn handle_delta_sync(
        &self,
        _session: &zrpc::Session,
        req: DeltaSyncRequest,
    ) -> DeltaSyncReply {
        trace!("serivce: <= delta sync, {:?}", req);

        let mut data = self.local.data.write().await;
        let (ver, uids) = data.merge_delta(&self.local.uid, req);
        if let Some(ver) = ver {
            let _r = self.tx.send(ver);
        }
        self.launch_syncers(uids).await;

        let reply = DeltaSyncReply::default();
        trace!("serivce: => delta sync, {:?}", reply);
        reply
    }
}

impl Handler {
    fn new(uid: &NodeId, port: i32) -> Self {
        let (tx, rx) = watch::channel(0);
        let (node_tx, _rx) = broadcast::channel(1);
        let data = LocalNode::new(uid.clone(), port);
        Handler {
            tx,
            rx,
            node_tx,
            local: Arc::new(data),
        }
    }

    async fn kick_seed(&self, addr: &str) -> Result<(), Error> {
        let mut client = zrpc::Client::builder()
            .service_type(znodes::service_type())
            .keep_alive(2)
            .build();

        client.connect(&addr).await?;
        // .expect(&format!("fail to connect to seed [{}]", addr));

        let mut req = self.local.build_full_sync().await;
        req.your_active_addr = addr.to_string();

        let r = client.call(&req).await?;
        let reply = r.await?;
        client.close().await;

        if reply.code != 0 {
            return Err(Error::Generic(reply.msg.clone()));
        }

        Ok(())
    }

    async fn launch_syncers(&self, uids: Option<HashSet<NodeId>>) {
        if let Some(uids) = uids {
            for uid in uids {
                if uid == self.local.uid {
                    continue;
                }

                info!("found node [{}]", uid);
                let mut syncer = NodeSyncer {
                    uid: uid.clone(),
                    local: self.local.clone(),
                    ver: 0,
                };
                let mut rx = self.rx.clone();
                let mut tx = self.node_tx.clone();

                let uid = &uid;
                let span = tracing::span!(tracing::Level::INFO, "", sync = &uid);
                let f = async move {
                    let r = syncer.run(&mut rx, &mut tx).await;
                    if let Err(e) = r {
                        debug!("node task finished with {:?}", e);
                    }
                };
                tokio::spawn(Instrument::instrument(f, span));
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Node {
    pub uid: NodeId,
    pub addr: String,
    pub state: NodeState,
}

#[derive(Default, Clone)]
pub struct Nodes(pub HashMap<NodeId, Node>);

// impl Nodes {
//     fn sub(&self, other: &Self) -> Vec<NodeId> {
//         let mut uids = Vec::new();
//         for (uid, _) in &self.0 {
//             if !other.0.contains_key(uid) {
//                 uids.push(uid.clone());
//             }
//         }
//         uids
//     }
// }

pub type NodesReceiver = broadcast::Receiver<()>;
type NodesSender = broadcast::Sender<()>;

#[derive(Clone)]
pub struct Service {
    service: Arc<znodes::Service<Handler>>,
    // local_addr: SocketAddr,
}

impl Service {
    pub fn id(&self) -> &NodeId {
        &self.service.inner().local.uid
    }

    // pub fn local_addr(&self) -> &SocketAddr {
    //     &self.local_addr
    // }

    // pub async fn get_nodes(&self) -> Nodes {
    //     let mut nodes = Nodes::default();
    //     let data = self.service.inner().local.data.read().await;
    //     for (uid, v) in &data.nodes.data {
    //         nodes.0.insert(uid.clone(), Node{uid: uid.clone(), addr: v.addr.clone()});
    //     }
    //     nodes
    // }

    pub async fn nodes_delta(&self, current: &Nodes) -> (Vec<NodeId>, Vec<Node>) {
        let mut remove_uids = Vec::new();
        let data = self.service.inner().local.data.read().await;
        for (uid, _v) in &current.0 {
            if !data.nodes.data.contains_key(uid) {
                remove_uids.push(uid.clone());
            }
        }

        let mut add_nodes = Vec::new();
        for (uid, v) in &data.nodes.data {
            if !current.0.contains_key(uid) {
                if !v.addr.is_empty() {
                    add_nodes.push(Node {
                        uid: uid.clone(),
                        addr: v.addr.clone(),
                        state: v.state.clone(),
                    });
                }
            }
        }
        (remove_uids, add_nodes)
    }

    pub fn watch(&self) -> NodesReceiver {
        self.service.inner().node_tx.subscribe()
    }

    pub fn service(&self) -> Arc<dyn zrpc::Service> {
        self.service.clone()
    }

    pub fn new(uid: &NodeId, local_addr: &SocketAddr) -> Self {
        let handler = Handler::new(uid, local_addr.port() as i32);
        let service = znodes::Service::new(handler);
        Self {
            service,
            // local_addr: local_addr.clone(),
        }
    }

    pub async fn kick_seed(&self, seed: &str) -> Result<(), Error> {
        let seed = seed.trim();
        if !seed.is_empty() {
            self.service.inner().kick_seed(seed).await
        } else {
            Ok(())
        }
    }

    // pub async fn launch(uid: &NodeId, addr: &str, seed: &str) -> Result<Self, Error> {
    //     let seed = seed.trim();

    //     let seed = if !seed.is_empty() {
    //         seed.to_string()
    //     } else {
    //         "".to_string()
    //     };

    //     let mut server = zrpc::Server::builder().build();
    //     server.server_mut().bind(addr).await?;

    //     let local_addr = server.server().local_addr().unwrap();
    //     //let handler = Handler::new(uid, sock_addr.port() as i32);
    //     let handler = Handler::new(uid, local_addr.port() as i32);
    //     let service = znodes::Service::new(handler);

    //     let server = Arc::new(server);
    //     server.add_service(service.clone());

    //     let server0 = server.clone();
    //     let f = async move {
    //         server0.server().run().await;
    //     };
    //     let span = tracing::span!(tracing::Level::INFO, "cluster");
    //     tokio::spawn(Instrument::instrument(f, span));

    //     if !seed.is_empty() {
    //         service.inner().kick_seed(&seed).await?;
    //     }

    //     Ok(Self {
    //         service,
    //         local_addr,
    //     })
    // }
}

// pub async fn launch(uid: &str, addr: &str, seed: &str) -> core::result::Result<(), Box<dyn std::error::Error>> {
//     info!("local: node_id = [{}], seed = [{}]", uid, seed);

//     let r = Service::launch(uid, addr, seed).await;
//     match r {
//         Ok(service) => {
//             info!("discovery service at {}, id [{}]", service.local_addr(), service.id());
//             tokio::time::sleep(Duration::from_secs(999999)).await;
//             Ok(())
//         },
//         Err(e) => {
//             return Err(Box::new(e));
//         },
//     }
// }
