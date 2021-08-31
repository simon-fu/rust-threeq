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
define_msgs!(FullSyncRequest, FullSyncReply, DeltaSyncRequest, DeltaSyncReply);

#[async_trait]
pub trait Handler {
    async fn handle_full_sync(&self, session: &zrpc::Session, req: FullSyncRequest) -> FullSyncReply;
    async fn handle_delta_sync(&self, session: &zrpc::Session, req: DeltaSyncRequest) -> DeltaSyncReply;
}

pub struct Service<T: Handler> {
    inner: T,
}

impl<T: Handler> Service<T> {
    pub fn new(inner: T) -> Arc<Self> {
        Arc::new(Self { inner })
    }

    pub fn inner(&self) -> &T {
        &self.inner
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

    async fn handle_request(&self, session: &zrpc::Session, ptype: i32, mut bytes: Bytes) -> Result<(i32, Bytes), String> {
        match ptype {
            FULLSYNCREQUEST_ID => {
                let req: FullSyncRequest = zrpc::decode(&mut bytes)?;
                let reply = self.inner.handle_full_sync(session, req).await;
                return zrpc::reply_result(reply.id(), reply);
            },
            DELTASYNCREQUEST_ID => {
                let req: DeltaSyncRequest = zrpc::decode(&mut bytes)?;
                let reply = self.inner.handle_delta_sync(session, req).await;
                return zrpc::reply_result(reply.id(), reply);
            },
            _ => {
                return Err(format!("unexpect message {:?}", ptype));
            }
        }
    }
}
