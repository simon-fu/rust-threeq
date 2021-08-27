use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::info;
use tracing::warn;

tonic::include_proto!("cluster");

// #[derive(Default)]
pub struct Node {
    phantom: PhantomData<bool>, // prevent construct from outside
    pub info: NodeInfo,
    htask: Option<JoinHandle<()>>,
}

impl Node {
    fn with_info(info: NodeInfo) -> Self {
        Self {
            phantom: PhantomData,
            info,
            htask: None,
        }
    }

    fn new(uid: String) -> Self {
        Self {
            phantom: PhantomData,
            info: NodeInfo {
                uid: uid,
                addrs: Vec::new(),
            },
            htask: None,
        }
    }

    fn add_addr<S: Into<String>>(&mut self, addr: S) -> bool {
        let addr = addr.into();
        if !self.info.addrs.contains(&addr) {
            self.info.addrs.push(addr);
            true
        } else {
            false
        }
    }

    fn add_addrs(&mut self, addrs: &Vec<String>) {
        for addr in addrs {
            self.add_addr(addr);
        }
    }
}

// type Nodes = HashMap< String, Arc<RwLock<Node> > >;
type Nodes = HashMap<String, Node>;

async fn rpc_sync_with(
    request: NodeSyncRequest,
    timeout_d: Duration,
) -> Result<NodeSyncReply, String> {
    let r = timeout(
        timeout_d,
        discovery_client::DiscoveryClient::connect(request.your_active_addr.clone()),
    )
    .await;

    match r {
        Ok(r) => match r {
            Ok(mut client) => {
                let request = tonic::Request::new(request);
                let r = client.sync_nodes(request).await;
                match r {
                    Ok(r) => {
                        return Ok(r.into_inner());
                    }
                    Err(e) => {
                        warn!("rpc sync nodes return {:?}", e);
                    }
                }
            }
            Err(e) => {
                warn!("rpc connect return {:?}", e);
            }
        },
        Err(e) => {
            warn!("rpc connect timeout {:?}", e);
        }
    }
    Err("none".to_string())
}

async fn try_sync_with_node(owner: &Arc<RwLock<LocalNode>>, uid: &str) -> Result<(), String> {
    let addrs = owner.read().unwrap().get_node_addrs(uid);
    let timeout = Duration::from_secs(2);
    for addr in &addrs {
        info!("client sync nodes with {}", addr);
        let request = owner.read().unwrap().build_sync_request(addr);
        let r = rpc_sync_with(request, timeout).await;
        match r {
            Ok(r) => {
                info!("client sync nodes return {:?}", r);
                if let Some(tx) = {
                    let mut local = owner.write().unwrap();
                    local.add_addr("", &r.your_reflex_addr);
                    let tx = local.add_nodes(&r.nodes);
                    tx
                } {
                    let _r = tx.send(Event::Check).await;
                }
                return Ok(());
            }
            Err(e) => {
                info!("sync nodes return {}", e);
            }
        }
    }
    Err("none".to_string())
}

async fn remote_node_task(owner: Arc<RwLock<LocalNode>>, uid: String) {
    info!("remote node task [{}] ", uid);

    let mut delay_sec = 1;
    loop {
        if let Ok(_r) = try_sync_with_node(&owner, &uid).await {
            break;
        }

        delay_sec = delay_sec * 2;
        info!(
            "next sync with node [{}] after [{}] seconds",
            uid, delay_sec
        );
        tokio::time::sleep(Duration::from_secs(delay_sec)).await;
    }
}

async fn local_node_task(owner: Arc<RwLock<LocalNode>>, mut rx: mpsc::Receiver<Event>) {
    loop {
        let seed_addr = owner.read().unwrap().seed_addr.clone();
        if seed_addr.is_empty() {
            break;
        }

        let timeout = Duration::from_secs(2);
        let request = owner.read().unwrap().build_sync_request(&seed_addr);
        if let Ok(r) = rpc_sync_with(request, timeout).await {
            if let Some(tx) = {
                let mut local = owner.write().unwrap();
                local.add_addr("", &r.your_reflex_addr);
                let tx = local.add_nodes(&r.nodes);
                tx
            } {
                let _r = tx.send(Event::Check).await;
            }
            break;
        }

        info!("next sync with seed [{}] after [{}] seconds", seed_addr, 2);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    loop {
        let ev = match rx.recv().await {
            Some(ev) => ev,
            None => break,
        };

        match ev {
            Event::Check => {
                let mut local = owner.write().unwrap();
                for (_uid, node) in &mut local.nodes {
                    if node.htask.is_none() {
                        let h =
                            tokio::spawn(remote_node_task(owner.clone(), node.info.uid.clone()));
                        node.htask = Some(h);
                    }
                }
            }
        }
    }
}

enum Event {
    Check,
}

struct LocalNode {
    my_node: Node,
    my_port: i32,
    seed_addr: String,
    nodes: Nodes,
    update_seq: u64,
    tx: mpsc::Sender<Event>,
}

impl LocalNode {
    pub fn new<S: Into<String>>(uid: S, port: i32, seed_addr: S, tx: mpsc::Sender<Event>) -> Self {
        Self {
            my_node: Node::new(uid.into()),
            my_port: port,
            seed_addr: seed_addr.into(),
            nodes: Nodes::default(),
            update_seq: 0,
            tx,
        }
    }

    fn get_nodes_info(&self) -> Vec<NodeInfo> {
        let mut nodes = Vec::new();
        nodes.push(self.my_node.info.clone());
        for (_, node) in &self.nodes {
            nodes.push(node.info.clone());
        }
        nodes
    }

    pub fn build_sync_request<S: Into<String>>(&self, addr: S) -> NodeSyncRequest {
        NodeSyncRequest {
            uid: self.my_node.info.uid.clone(),
            update_seq: self.update_seq,
            nodes: self.get_nodes_info(),
            your_active_addr: addr.into(),
            port: self.my_port,
        }
    }

    pub fn get_node_addrs(&self, uid: &str) -> Vec<String> {
        let mut v = Vec::new();
        if let Some(node) = self.nodes.get(uid) {
            v.append(&mut node.info.addrs.clone().into_iter().collect());
        }
        v
    }

    pub fn check_add(&mut self, info: &NodeInfo) -> bool {
        match self.nodes.get_mut(&info.uid) {
            Some(node) => {
                node.add_addrs(&info.addrs);
                return false;
            }
            None => {
                let node = Node::with_info(info.clone());
                self.nodes.insert(info.uid.clone(), node);
                return true;
            }
        }
    }

    pub fn add_nodes(&mut self, nodes: &Vec<NodeInfo>) -> Option<mpsc::Sender<Event>> {
        let mut added = false;
        for info in nodes {
            if info.uid == self.my_node.info.uid {
                self.my_node.add_addrs(&info.addrs);
            } else if self.check_add(info) {
                added = true;
            }
        }
        if added {
            Some(self.tx.clone())
        } else {
            None
        }
    }

    pub fn add_addr(&mut self, uid: &str, addr: &str) {
        if addr.is_empty() {
            return;
        }
        if uid.is_empty() || uid == self.my_node.info.uid {
            self.my_node.add_addr(addr);
        } else if let Some(node) = self.nodes.get_mut(uid) {
            node.add_addr(addr);
        }
    }
}

pub struct DiscoveryService {
    inner: Arc<RwLock<LocalNode>>,
}

impl DiscoveryService {
    pub fn new<S: Into<String>>(uid: S, port: i32, seed_addr: S) -> Self {
        let (tx, rx) = mpsc::channel(128);
        let inner = Arc::new(RwLock::new(LocalNode::new(uid, port, seed_addr, tx)));
        tokio::spawn(local_node_task(inner.clone(), rx));
        Self { inner }
    }
}

#[tonic::async_trait]
impl discovery_server::Discovery for DiscoveryService {
    async fn sync_nodes(
        &self,
        request: tonic::Request<self::NodeSyncRequest>,
    ) -> Result<tonic::Response<self::NodeSyncReply>, tonic::Status> {
        info!("got sync nodes request {:?}", request);

        let reflex_addr = format!(
            "http://{}:{}",
            request.remote_addr().unwrap().ip().to_string(),
            request.get_ref().port
        );
        let mut reply = NodeSyncReply::default();

        let tx = {
            let mut inner = self.inner.write().unwrap();
            reply.uid = inner.my_node.info.uid.clone();
            if request.get_ref().uid == inner.my_node.info.uid {
                return Ok(tonic::Response::new(reply));
            }

            reply.your_reflex_addr = reflex_addr;
            reply.nodes = inner.get_nodes_info();

            inner.add_nodes(&request.get_ref().nodes);
            inner.add_addr(&request.get_ref().uid, &reply.your_reflex_addr);
            inner.tx.clone()
        };
        let _r = tx.send(Event::Check).await;

        Ok(tonic::Response::new(reply))
    }

    async fn dummy(
        &self,
        request: tonic::Request<self::Empty>,
    ) -> Result<tonic::Response<self::Empty>, tonic::Status> {
        info!("got dummy request {:?}", request);
        Ok(tonic::Response::new(Empty::default()))
    }
}

#[derive(Default)]
pub struct SubServiceImpl {}

#[tonic::async_trait]
impl sub_service_server::SubService for SubServiceImpl {
    async fn subscribe(
        &self,
        request: tonic::Request<self::SubRequest>,
    ) -> Result<tonic::Response<self::Empty>, tonic::Status> {
        info!("got sub request {:?}", request);

        Ok(tonic::Response::new(Empty {}))
    }

    async fn unsubscribe(
        &self,
        request: tonic::Request<self::UnsubRequest>,
    ) -> Result<tonic::Response<self::Empty>, tonic::Status> {
        info!("got unsub request {:?}", request);
        Err(tonic::Status::already_exists(""))
    }

    // async fn say_hello(
    //     &self,
    //     request: Request<HelloRequest>, // Accept request of type HelloRequest
    // ) -> Result<Response<HelloReply>, Status> { // Return an instance of type HelloReply
    //     println!("Got a request: {:?}", request);

    //     let reply = hello_world::HelloReply {
    //         message: format!("Hello {}!", request.into_inner().name).into(), // We must use .into_inner() as the fields of gRPC requests and responses are private
    //     };
    //     tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    //     Ok(Response::new(reply)) // Send back our formatted greeting
    // }
}
