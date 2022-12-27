
use std::net::SocketAddr;
use std::time::Duration;
use std::{sync::Arc, collections::HashMap};
use dashmap::DashMap;
use anyhow::{Result, Context, bail};
use tokio::task::JoinHandle;
use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::oneshot;
use tonic::transport::Endpoint;
use tracing::{info, error, debug};
use futures::FutureExt;
use crate::uid::NodeId;
use crate::util::AddrUrl;
use super::node::Node;
use super::proto::{
    rpc_client::RpcClient, 
    rpc_server::{RpcServer, Rpc},
    PrepareNodeRequest,
    PrepareNodeResponse,
};


pub async fn bootstrap<C>(cfg: &C) -> Result<(Cluster, Option<ClusterServer>)> 
where
    C: ClusterConfig
{ 
    let mut opt_node_id = cfg.cluster_node_id();
    let mut opt_loopback_id = None;

    let default_listen_url = cfg.cluster_listen_default().with_context(||"invalid default cluster listen address")?;
    let listen_url = cfg.cluster_listen_url().with_context(||"invalid cluster listen address")?;
    let boostrap_url = cfg.cluster_boostrap_url().with_context(||"invalid cluster boostrap address")?;

    if let Some(boostrap_url) = &boostrap_url {
        let _ip ={
            let url = boostrap_url.to_string();

            let channel = Endpoint::from_shared(url.to_string())?
            .connect_lazy()?;
            
            let mut client = RpcClient::new(channel);

            let rsp = client.prepare_node(tonic::Request::new(PrepareNodeRequest {
                is_alloc_node_id: opt_node_id.is_none(),
                is_alloc_loopback_id: true,
                request_addr: url.to_string(),
            })).await?;
            
            debug!("prepare with [{}] and response [{:?}]", url, rsp.get_ref());

            if let Some(id) = rsp.get_ref().node_id {
                opt_node_id = Some(id.into());
            }

            opt_loopback_id = rsp.get_ref().loopback_id;

            let reflex_addr = rsp.into_inner().reflex_addr.with_context(||format!("can't exchange address with [{}]", url))?;

            let sock_addr: SocketAddr = reflex_addr.parse()?;
            sock_addr.ip()
        };
    }

    let node_id = match opt_node_id {
        Some(id) => id,
        None => {
            0.into()
        },
    };

    if *node_id == 0 {
        info!("standalone mode");
        return Ok((Cluster::new(0.into()), None));
    }

    let cluster0 = Cluster::new(node_id);

    if boostrap_url.is_none() && *node_id == 1 {
        // first node in cluster
        debug!("first node, add node_id pool range [2-512]");
        cluster0.self_node().add_node_id_range(2.into(), 512.into());
    }
    
    let (server, url) = match listen_url {
        Some(url) => { 
            let server = cluster0.try_listen(url.to_sockaddr()?).await?;
            (server, url)
        },
        None => {
            let url = match opt_loopback_id {
                Some(id) => {
                    cluster0.self_node().merge_loopback_id(id);
                    cfg.cluster_listen_by_id(id)?
                },
                None => default_listen_url,
            };

            let addr = url.to_sockaddr()?;
            let server = cluster0.try_listen(addr).await?;
            (server, url)

        },
    };
    

    info!("cluster rpc service at [{}]", url.listen_url());
    Ok((cluster0, Some(server)))
}


#[derive(Clone)]
pub struct Cluster {
    inner: Arc<ClusterInner>,
}

impl Cluster {
    pub fn new(id: NodeId) -> Self {
        Self { 
            inner: Arc::new(ClusterInner {
                self_node: Node::new(id),
                nodes: Default::default(),
            })
        }
    }

    pub fn self_node(&self) -> &Node {
        &self.inner.self_node
    }

    pub fn nodes_snapshot(&self, nodes: &mut HashMap<NodeId, Node>) {
        for node in self.inner.nodes.iter() {
            nodes.insert(node.id(), node.clone());
        }
    }

    async fn try_listen(&self, addr: SocketAddr) -> Result<ClusterServer> {
            
        let (tx, rx) = oneshot::channel::<()>();
        let (tx2, rx2) = oneshot::channel::<()>();
        let cluster = self.clone();

        let task = tokio::spawn(async move {
            let serve_r = Server::builder()
            .add_service(RpcServer::new(cluster))
            .serve_with_shutdown(addr, rx.map(drop))
            .await;
            
            let send_r = tx2.send(());
            
            if let Err(e) = &serve_r {
                match send_r {
                    Ok(_r) => {},
                    Err(_e) => {
                        error!("cluster service finish error [{:?}]", e);
                        serve_r?;
                    },
                } 
            }

            Result::Ok(())
        });

    
        // 等待服务结束事件
        let r = tokio::time::timeout(Duration::from_millis(200), rx2).await;
        match r {
            Ok(_r) => { // 有收到事件说明监听失败
                bail!("fail to bind cluster listen at [{}]", addr);
            },
            Err(_e) => {}, // 超时说明监听成功
        }
    
        let server = ClusterServer {
            task,
            tx,
            addr,
        };
    
        Ok(server)
    }
    
}

pub trait ClusterConfig { 
    fn cluster_node_id(&self) -> Option<NodeId> ;

    fn cluster_listen_default(&self) -> Result<AddrUrl>;

    // fn cluster_listen_with(&self, ip: SocketAddr) -> Result<AddrUrl>;
    fn cluster_listen_by_id(&self, id: u64) -> Result<AddrUrl>;

    fn cluster_listen_url(&self) -> Result<Option<AddrUrl>>;

    fn cluster_boostrap_url(&self) -> Result<Option<AddrUrl>>;
}

struct ClusterInner {
    self_node: Node,
    nodes: DashMap<NodeId, Node>,
}


#[tonic::async_trait]
impl Rpc for Cluster {
    // async fn exchange_addr(
    //     &self,
    //     request: Request<StringObj>, 
    // ) -> Result<Response<ReflexAddr>, Status> { 

    //     info!("got exchange address from [{:?}], my address is [{}]", request.remote_addr(), request.get_ref().value);
    //     let (loopback_id, addr) = match request.remote_addr() {
    //         Some(v) => {
    //             if v.ip().is_loopback() {
    //                 (self.self_node().next_loopback_id(), Some(v.to_string()))
    //             } else {
    //                 (0, None)
    //             }
    //         },
    //         None => (0, None),
    //     };

    //     let reply = ReflexAddr {
    //         addr,
    //         loopback_id,
    //     };
    //     self.self_node().put_addr(request.into_inner().value);

    //     Ok(Response::new(reply)) 
    // }

    async fn prepare_node(
        &self,
        request: Request<PrepareNodeRequest>, 
    ) -> Result<Response<PrepareNodeResponse>, Status> { 
        debug!("got [prepare_node] from [{:?}], req [{:?}]", request.remote_addr(), request.get_ref());

        let reflex_addr = match request.remote_addr() {
            Some(v) => {
                Some(v.to_string())
            },
            None => None,
        };

        let loopback_id = if request.get_ref().is_alloc_loopback_id {
            Some(self.self_node().next_loopback_id())
        } else {
            None
        };

        let node_id = if request.get_ref().is_alloc_node_id {
            self.self_node().alloc_node_id()
        } else {
            None
        };

        let reply = PrepareNodeResponse {
            reflex_addr,
            loopback_id,
            node_id: node_id.map(|x|x.into()),
        };
        self.self_node().put_addr(request.into_inner().request_addr);

        Ok(Response::new(reply)) 
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


