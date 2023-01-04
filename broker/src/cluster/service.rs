

use std::sync::Arc;
use anyhow::Result;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, debug};


use crate::util::time::now_ms;

use super::ClusterInner;
use super::node_sender::{PinNodeStream, server_sync_node};
use super::proto::{
    rpc_server::Rpc,
    PrepareNodeRequest,
    PrepareNodeResponse,
    PrepareNodeReply,
    prepare_node_response,
    MeetRequest,
    MeetResponse,
    PingRequest,
    PingResponse,
    NodeClientStream,
};

#[tonic::async_trait]
impl Rpc for Arc<ClusterInner> {

    async fn prepare_node(
        &self,
        request: Request<PrepareNodeRequest>, 
    ) -> Result<Response<PrepareNodeResponse>, Status> { 
        debug!("got [prepare_node] from [{:?}], req [{:?}]", request.remote_addr(), request.get_ref());

        let (reflex_addr, is_loopback) = match request.remote_addr() {
            Some(v) => {
                (Some(v.to_string()), v.ip().is_loopback())
            },
            None => (None, false),
        };
    
        let req = request.into_inner();
    
        let r = self.get_min_working_node_client()
        .map_err(|e|Status::aborted(format!("get_min_working_node_client [{:?}]", e)))?;
    
        match r {
            Some((mut client, id)) => {
                debug!("forward [prepare_node] to node [{}]", id);
                return client.prepare_node(Request::new(req)).await
            },
            None => {}
        }
    
        let loopback_id = if req.is_alloc_loopback_id && is_loopback {
            Some(self.next_loopback_id())
        } else {
            None
        };
    
        let node_id = if req.is_alloc_node_id {
            self.alloc_node_id()
        } else {
            None
        };
    
        let ok = self.try_alone_to_online(req.server_addr.clone());
        if ok {
            info!("update my rpc addr to [{}]", req.server_addr)
        }
    
        let reply = PrepareNodeResponse {
            data: Some(prepare_node_response::Data::Reply(PrepareNodeReply{
                reflex_addr,
                loopback_id,
                node_id: node_id.map(|x|x.into()),
                nodes: self.get_nodes_info(),
            }))
        };
    
        Ok(Response::new(reply)) 
    }

    async fn meet(
        &self,
        request: Request<MeetRequest>, 
    ) -> Result<Response<MeetResponse>, Status> { 

        let req = request.into_inner();

        debug!("got [meet] req [{:?}]", req);

        let self_node_id = self.node_id();
        if *self_node_id != req.server_node_id {
            return Err(Status::invalid_argument(format!("expect node id [{}] but [{}]", self_node_id, req.server_node_id)))
        }

        match req.client_node {
            Some(node) => {
                let _added = self.try_add_node(node).map_err(|x|Status::invalid_argument(x.to_string()))?;
            },
            None => {
                return Err(Status::invalid_argument("meet but no node"));
            }
        }
        
        let reply = MeetResponse {
            nodes: self.get_nodes_info(),
        };
        Ok(Response::new(reply)) 
    }

    async fn ping(
        &self,
        request: Request<PingRequest>, 
    ) -> Result<Response<PingResponse>, Status> { 
        let reply = PingResponse {
            ts_milli: now_ms(),
            elapsed_milli: 0,
            origin_ts_milli: request.get_ref().ts_milli,
        };
        Ok(Response::new(reply)) 
    }

    type SyncNodeStream = PinNodeStream;
    // type SyncNodeStream = Streaming<NodeServerStream>;

    async fn sync_node(
        &self,
        request: Request<Streaming<NodeClientStream>>, 
    ) -> Result<Response<Self::SyncNodeStream>, Status> { 
        
        let req = request.into_inner();
        let cluster = self.acquire_me().map_err(|e|Status::aborted(e.to_string()))?;
        let rx = server_sync_node(req, cluster).await;
        Ok(Response::new(rx))
    }

}

// type ResponseItem = Result<NodeServerStream, Status>;
// type ResponseStream = std::pin::Pin<Box<dyn futures::Stream<Item = ResponseItem> + Send + Sync>>;


