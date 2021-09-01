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

#[derive(Default)]
struct Delta {
    data: HashMap<String, NodeInfo>,
    ver: u64,
    src: String,
}

#[derive(Default, Clone)]
struct Nodes {
    data: HashMap<String, NodeInfo>,
    ver: u64,
}

impl Nodes {
    fn merge_node(&mut self, info: &NodeInfo, delta: &mut Delta) -> bool {
        let exist = self.data.get_mut(&info.uid);
        if exist.is_none() {
            //debug!("merge_node: add node {:?}", info);
            self.data.insert(info.uid.clone(), info.clone());
            delta.data.insert(info.uid.clone(), info.clone());
            return true;
        }

        let node = exist.unwrap();
        for addr in &info.addrs {
            if !node.addrs.contains(addr) {
                node.addrs.push(addr.clone());
                let delta_node = delta
                    .data
                    .entry(node.uid.clone())
                    .or_insert(NodeInfo::default());
                delta_node.addrs.push(addr.clone());
            }
        }
        return false;
    }

    fn merge_addr(&mut self, uid: &str, addr: String, delta: &mut Delta) -> bool {
        let mut is_add_node = false;
        if !self.data.contains_key(uid) {
            let mut node = NodeInfo::default();
            node.uid = uid.to_string();
            //debug!("merge_addr: add node {}", uid);
            self.data.insert(uid.to_string(), node);
            is_add_node = true;
        }

        let node = self.data.get_mut(uid).unwrap();
        if !node.addrs.contains(&addr) {
            node.addrs.push(addr.clone());
            let delta_node = delta
                .data
                .entry(node.uid.clone())
                .or_insert(NodeInfo::default());
            delta_node.addrs.push(addr);
        }
        is_add_node
    }
}

#[derive(Default)]
struct SeqData {
    nodes: Nodes,
    history: VecDeque<Arc<Delta>>,
}

impl SeqData {
    fn merge_full(
        &mut self,
        local_uid: &str,
        req: FullSyncRequest,
        addr: String,
    ) -> (u64, Option<HashSet<String>>) {
        if req.src == local_uid {
            return (self.nodes.ver, None);
        }

        let mut uids: HashSet<String> = HashSet::default();
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
        local_uid: &str,
        req: DeltaSyncRequest,
    ) -> (Option<u64>, Option<HashSet<String>>) {
        if req.src == local_uid {
            return (None, None);
        }

        let mut uids: HashSet<String> = HashSet::default();
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
        for (_, info) in &self.nodes.data {
            nodes.push(info.clone());
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

    fn get_node_addrs(&self, uid: &str) -> Option<Vec<String>> {
        let r = self.nodes.data.get(uid);
        if let Some(info) = r {
            return Some(info.addrs.clone());
        } else {
            return None;
        }
    }
}

#[derive(Default)]
struct LocalNode {
    uid: String,
    port: i32,
    data: Arc<RwLock<SeqData>>,
}

impl LocalNode {
    fn new(uid: String, port: i32) -> Self {
        Self {
            uid,
            port,
            ..Default::default()
        }
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
    // #[error("error: {0}")]
    // Generic(String),
}

struct NodeSyncer {
    uid: String,
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
            let _r: FullSyncReply = r.await?;
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
                let _reply: DeltaSyncReply = r.await?;
            } else {
                let mut req = self.local.build_full_sync().await;
                req.your_active_addr = addr.clone();
                debug!("out of sync, {} -> {} ", self.ver, req.data_ver);

                let r = client.call(&req).await?;
                let _reply: FullSyncReply = r.await?;
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

    async fn run(&mut self, delta_rx: &mut watch::Receiver<u64>) -> Result<(), Error> {
        let mut wait_secs = 1;
        loop {
            let r = self.try_connect().await;
            match r {
                Ok((mut client, addr)) => {
                    info!("connected to node [{}], addr [{}]", self.uid, addr);
                    wait_secs = 1; // reset timeout
                    let _r = self.sync_with_addr(&mut client, &addr, delta_rx).await;
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

        let addr = format!("{}:{}", session.remote_addr().ip(), req.port);

        let mut data = self.local.data.write().await;
        let (ver, uids) = data.merge_full(&self.local.uid, req, addr.clone());
        let _r = self.tx.send(ver); // always kick
        self.launch_syncers(uids).await;

        let mut reply = FullSyncReply::default();
        reply.uid = self.local.uid.clone();
        reply.your_reflex_addr = addr.clone();
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
    fn new<S: Into<String>>(uid: S, port: i32) -> Self {
        let (tx, rx) = watch::channel(0);
        let data = LocalNode::new(uid.into(), port);
        Handler {
            tx,
            rx,
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
        let _r: FullSyncReply = r.await?;
        client.close().await;

        Ok(())
    }

    async fn launch_syncers(&self, uids: Option<HashSet<String>>) {
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

                let uid: &str = &uid;
                let span = tracing::span!(tracing::Level::INFO, "", sync = uid);
                let f = async move {
                    let r = syncer.run(&mut rx).await;
                    if let Err(e) = r {
                        debug!("node task finished with {:?}", e);
                    }
                };
                tokio::spawn(Instrument::instrument(f, span));
            }
        }
    }
}

pub struct Service {
    service: Arc<znodes::Service<Handler>>,
    local_addr: SocketAddr,
}

impl Service {
    pub fn id(&self) -> &str {
        &self.service.inner().local.uid
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    pub async fn launch(uid: &str, addr: &str, seed: &str) -> Result<Self, Error> {
        let uid = uid.trim();
        let seed = seed.trim();

        let uid = if !uid.is_empty() {
            uid.to_string()
        } else {
            mac_address::get_mac_address().unwrap().unwrap().to_string()
        };
        let seed = if !seed.is_empty() {
            seed.to_string()
        } else {
            "".to_string()
        };

        let mut server = zrpc::Server::builder().build();
        server.server_mut().bind(addr).await?;

        let local_addr = server.server().local_addr().unwrap();
        //let handler = Handler::new(uid, sock_addr.port() as i32);
        let handler = Handler::new(uid, local_addr.port() as i32);
        let service = znodes::Service::new(handler);

        let server = Arc::new(server);
        server.add_service(service.clone());

        let server0 = server.clone();
        let f = async move {
            server0.server().run().await;
        };
        let span = tracing::span!(tracing::Level::INFO, "cluster");
        tokio::spawn(Instrument::instrument(f, span));

        if !seed.is_empty() {
            service.inner().kick_seed(&seed).await?;
        }

        Ok(Self {
            service,
            local_addr,
        })
    }
}

// pub async fn launch(uid: &str, addr: &str, seed: &str) -> core::result::Result<(), Box<dyn std::error::Error>> {
//     info!("local: node_id = [{}], seed = [{}]", uid, seed);

//     let r = Service::launch(uid, addr, seed).await;
//     match r {
//         Ok(service) => {
//             info!("cluster service at {}, id [{}]", service.local_addr(), service.id());
//             tokio::time::sleep(Duration::from_secs(999999)).await;
//             Ok(())
//         },
//         Err(e) => {
//             return Err(Box::new(e));
//         },
//     }
// }
