use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

use super::zrpc;
use crate::define_msgs;
use crate::define_msgs_;
use paste::paste;
use zrpc::Id32;
use zrpc::MESSAGE_ID_BASE;

include!(concat!(env!("OUT_DIR"), "/znodes.rs"));
define_msgs!(NodeSyncRequest, NodeSyncReply);

#[async_trait]
pub trait Handler {
    async fn handle_nodes_sync(&self, req: NodeSyncRequest) -> NodeSyncReply;
}

pub struct Service<T: Handler> {
    handler: T,
}

impl<T: Handler> Service<T> {
    pub fn new(handler: T) -> Arc<Self> {
        Arc::new(Self { handler })
    }
}

pub fn service_type() -> &'static str {
    "Znodes"
}

#[async_trait]
impl<T: Sync + Send + Handler> zrpc::Service for Service<T> {
    fn type_name(&self) -> &'static str {
        service_type()
    }

    async fn handle_request(&self, ptype: i32, mut bytes: Bytes) -> Result<(i32, Bytes), String> {
        match ptype {
            NODESYNCREQUEST_ID => {
                let req: NodeSyncRequest = zrpc::decode(&mut bytes)?;
                let reply = self.handler.handle_nodes_sync(req).await;
                return zrpc::reply_result(NODESYNCREPLY_ID, reply);
            }
            _ => {
                return Err(format!("unexpect message {:?}", ptype));
            }
        }
    }
}
