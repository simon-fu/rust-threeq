// TODO:
// - check all unwrap
// - impl zrpc::ClientSync

use anyhow::Result;
use anyhow::{bail, Context};
use async_raft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, Entry, EntryPayload,
        InstallSnapshotRequest, InstallSnapshotResponse, MembershipConfig, VoteRequest,
        VoteResponse,
    },
    storage::{CurrentSnapshotData, HardState, InitialState},
    AppData, AppDataResponse, Raft, RaftNetwork, RaftStorage,
};
use prost::Message;
use serde::{Deserialize, Serialize};

use tokio::sync::RwLock;
use tracing::{debug, error, Instrument};

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::Cursor,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;

use bytes::Bytes;

use crate::{discovery, zrpc};

use rust_threeq::tq3::tt::mqtree::Mqtree;

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Action {
    Sub,
    Unsub,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ClientRequest {
    uid: async_raft::NodeId,
    req_id: u64,
    hub_name: String,
    filter: String,
    action: Action,
}

impl AppData for ClientRequest {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientResponse {}

impl AppDataResponse for ClientResponse {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubStoreSnapshot {
    /// The last index covered by this snapshot.
    pub index: u64,
    /// The term of the last index covered by this snapshot.
    pub term: u64,
    /// The last memberhsip config included in this snapshot.
    pub membership: MembershipConfig,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct SubStoreStateMachine {
    last_applied_log: u64,

    client_serial_responses: HashMap<String, (u64, Option<String>)>,

    filter_tree: Mqtree<()>,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ShutdownError {
    // #[error("unsafe storage error")]
// UnsafeStorageError,
}

const ERR_INCONSISTENT_LOG: &str =
    "a query was received which was expecting data to be in place which does not exist in the log";

const TRACE_LEVEL: tracing::Level = tracing::Level::DEBUG;

#[derive(Default)]
struct LocalStore {
    id: async_raft::NodeId,
    log: RwLock<BTreeMap<u64, Entry<ClientRequest>>>,
    sm: RwLock<SubStoreStateMachine>,
    hs: RwLock<Option<HardState>>,
    current_snapshot: RwLock<Option<SubStoreSnapshot>>,
}

impl LocalStore {
    fn new(id: async_raft::NodeId) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    async fn get_membership(&self) -> Result<MembershipConfig> {
        let log = self.log.read().await;
        let cfg_opt = log.values().rev().find_map(|entry| match &entry.payload {
            EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
            EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
            _ => None,
        });
        Ok(match cfg_opt {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.id),
        })
    }
}

#[async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for LocalStore {
    type Snapshot = Cursor<Vec<u8>>;

    type ShutdownError = ShutdownError;

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        debug!("");
        self.get_membership().await
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn get_initial_state(&self) -> Result<async_raft::storage::InitialState> {
        debug!("");
        let membership = self.get_membership().await?;
        let mut hs = self.hs.write().await;
        let log = self.log.read().await;
        let sm = self.sm.read().await;
        match &mut *hs {
            Some(inner) => {
                let (last_log_index, last_log_term) = match log.values().rev().next() {
                    Some(log) => (log.index, log.term),
                    None => (0, 0),
                };
                let last_applied_log = sm.last_applied_log;
                Ok(InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: inner.clone(),
                    membership,
                })
            }
            None => {
                let new = InitialState::new_initial(self.id);
                *hs = Some(new.hard_state.clone());
                Ok(new)
            }
        }
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self, hs), fields(local = %self.id))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        debug!("hard state = {:?}", hs);
        *self.hs.write().await = Some(hs.clone());
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<ClientRequest>>> {
        debug!("");
        if start > stop {
            tracing::error!("invalid request, start > stop");
            return Ok(vec![]);
        }
        let log = self.log.read().await;
        Ok(log.range(start..stop).map(|(_, val)| val.clone()).collect())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        debug!("");
        if stop.as_ref().map(|stop| &start > stop).unwrap_or(false) {
            tracing::error!("invalid request, start > stop");
            return Ok(());
        }
        let mut log = self.log.write().await;

        // If a stop point was specified, delete from start until the given stop point.
        if let Some(stop) = stop.as_ref() {
            for key in start..*stop {
                log.remove(&key);
            }
            return Ok(());
        }
        // Else, just split off the remainder.
        log.split_off(&start);
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, fields(local = %self.id), skip(self, entry))]
    async fn append_entry_to_log(&self, entry: &Entry<ClientRequest>) -> Result<()> {
        debug!("{:?}", entry);
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self, entries), fields(local = %self.id))]
    async fn replicate_to_log(&self, entries: &[Entry<ClientRequest>]) -> Result<()> {
        debug!("{:?}", entries);
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self, data), fields(local = %self.id))]
    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &ClientRequest,
    ) -> Result<ClientResponse> {
        debug!("{:?}", data);
        let mut sm = self.sm.write().await;
        sm.last_applied_log = *index;
        Ok(ClientResponse {})
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self, entries), fields(local = %self.id))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &ClientRequest)]) -> Result<()> {
        debug!("{:?}", entries);
        let mut sm = self.sm.write().await;
        for (index, _data) in entries {
            sm.last_applied_log = **index;
        }
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        debug!("");
        let (data, last_applied_log);
        {
            // Serialize the data of the state machine.
            let sm = self.sm.read().await;
            data = serde_json::to_vec(&*sm)?;
            last_applied_log = sm.last_applied_log;
        } // Release state machine read lock.

        let membership_config;
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let log = self.log.read().await;
            membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > last_applied_log)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));
        } // Release log read lock.

        let snapshot_bytes: Vec<u8>;
        let term;
        {
            let mut log = self.log.write().await;
            let mut current_snapshot = self.current_snapshot.write().await;
            term = log
                .get(&last_applied_log)
                .map(|entry| entry.term)
                .ok_or_else(|| anyhow::anyhow!(ERR_INCONSISTENT_LOG))?;
            *log = log.split_off(&last_applied_log);
            log.insert(
                last_applied_log,
                Entry::new_snapshot_pointer(
                    last_applied_log,
                    term,
                    "".into(),
                    membership_config.clone(),
                ),
            );

            let snapshot = SubStoreSnapshot {
                index: last_applied_log,
                term,
                membership: membership_config.clone(),
                data,
            };
            snapshot_bytes = serde_json::to_vec(&snapshot)?;
            *current_snapshot = Some(snapshot);
        } // Release log & snapshot write locks.

        tracing::trace!(
            { snapshot_size = snapshot_bytes.len() },
            "log compaction complete"
        );
        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: membership_config.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        })
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        debug!("");
        Ok((String::from(""), Box::new(Cursor::new(Vec::new())))) // Snapshot IDs are insignificant to this storage engine.
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self, snapshot), fields(local = %self.id))]
    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        debug!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let raw = serde_json::to_string_pretty(snapshot.get_ref().as_slice())?;
        println!("JSON SNAP:\n{}", raw);
        let new_snapshot: SubStoreSnapshot = serde_json::from_slice(snapshot.get_ref().as_slice())?;
        // Update log.
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let mut log = self.log.write().await;
            let membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > index)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));

            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear(),
            }
            log.insert(
                index,
                Entry::new_snapshot_pointer(index, term, id, membership_config),
            );
        }

        // Update the state machine.
        {
            let new_sm: SubStoreStateMachine = serde_json::from_slice(&new_snapshot.data)?;
            let mut sm = self.sm.write().await;
            *sm = new_sm;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn get_current_snapshot(
        &self,
    ) -> Result<Option<async_raft::storage::CurrentSnapshotData<Self::Snapshot>>> {
        debug!("");
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let reader = serde_json::to_vec(&snapshot)?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(reader)),
                }))
            }
            None => Ok(None),
        }
    }
}

#[derive(Default)]
struct LocalRouter {
    clients: Arc<RwLock<HashMap<async_raft::NodeId, zrpc::Client>>>,
}

// struct BufMutToWrite<'a, B> {
//     buf: &'a B
// }

// impl<'a, B: bytes::BufMut> std::io::Write for BufMutToWrite<'a, B> {
//     fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
//         self.buf.put(buf);
//         Ok(buf.len())
//     }

//     fn flush(&mut self) -> std::io::Result<()> {
//         Ok(())
//     }
// }

// struct BufToRead<'a, B> {
//     buf: &'a B
// }

// impl<'a, B: bytes::Buf> std::io::Read for BufToRead<'a, B> {
//     fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
//         self.buf.copy_to_slice(buf);
//         Ok(buf.len())
//     }
// }

#[derive(Clone, PartialEq, Message)]
struct AppendRequest {
    #[prost(bytes, tag = "1")]
    data: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConflictOpt0 {
    #[prost(uint64, tag = "1")]
    term: u64,
    #[prost(uint64, tag = "2")]
    index: u64,
}

#[derive(Clone, PartialEq, Message)]
struct AppendResponse {
    #[prost(uint64, tag = "1")]
    term: u64,
    #[prost(bool, tag = "2")]
    success: bool,
    #[prost(message, tag = "3")]
    conflict_opt: Option<ConflictOpt0>,
}

impl AppendResponse {
    fn to(&self) -> AppendEntriesResponse {
        AppendEntriesResponse {
            term: self.term,
            success: self.success,
            conflict_opt: match &self.conflict_opt {
                Some(o) => Some(async_raft::raft::ConflictOpt {
                    term: o.term,
                    index: o.index,
                }),
                None => None,
            },
        }
    }

    fn from(rsp: &AppendEntriesResponse) -> Self {
        Self {
            term: rsp.term,
            success: rsp.success,
            conflict_opt: match &rsp.conflict_opt {
                Some(o) => Some(ConflictOpt0 {
                    term: o.term,
                    index: o.index,
                }),
                None => None,
            },
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct InstallRequest {
    #[prost(uint64, tag = "1")]
    term: u64,
    #[prost(uint64)]
    leader_id: u64,
    #[prost(uint64)]
    last_included_index: u64,
    #[prost(uint64)]
    last_included_term: u64,
    #[prost(uint64)]
    offset: u64,
    #[prost(bytes)]
    data: Vec<u8>,
    #[prost(bool)]
    done: bool,
}

impl InstallRequest {
    fn to(self) -> InstallSnapshotRequest {
        InstallSnapshotRequest {
            term: self.term,
            leader_id: self.leader_id,
            last_included_index: self.last_included_index,
            last_included_term: self.last_included_term,
            offset: self.offset,
            data: self.data,
            done: self.done,
        }
    }

    fn from(o: InstallSnapshotRequest) -> Self {
        Self {
            term: o.term,
            leader_id: o.leader_id,
            last_included_index: o.last_included_index,
            last_included_term: o.last_included_term,
            offset: o.offset,
            data: o.data,
            done: o.done,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct InstallResponse {
    #[prost(uint64, tag = "1")]
    term: u64,
}

impl InstallResponse {
    fn to(self) -> InstallSnapshotResponse {
        InstallSnapshotResponse { term: self.term }
    }

    fn from(o: InstallSnapshotResponse) -> Self {
        Self { term: o.term }
    }
}

#[derive(Clone, PartialEq, Message)]
struct AVoteRequest {
    #[prost(uint64, tag = "1")]
    term: u64,
    #[prost(uint64)]
    candidate_id: u64,
    #[prost(uint64)]
    last_log_index: u64,
    #[prost(uint64)]
    last_log_term: u64,
}

impl AVoteRequest {
    fn to(self) -> VoteRequest {
        VoteRequest {
            term: self.term,
            candidate_id: self.candidate_id,
            last_log_index: self.last_log_index,
            last_log_term: self.last_log_term,
        }
    }

    fn from(o: VoteRequest) -> Self {
        Self {
            term: o.term,
            candidate_id: o.candidate_id,
            last_log_index: o.last_log_index,
            last_log_term: o.last_log_term,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct AVoteResponse {
    #[prost(uint64, tag = "1")]
    term: u64,
    #[prost(bool)]
    vote_granted: bool,
}

impl AVoteResponse {
    fn to(self) -> VoteResponse {
        VoteResponse {
            term: self.term,
            vote_granted: self.vote_granted,
        }
    }

    fn from(o: VoteResponse) -> Self {
        Self {
            term: o.term,
            vote_granted: o.vote_granted,
        }
    }
}

use crate::define_msg_pair;
use crate::define_msgs;
use crate::define_msgs_;
use paste::paste;
use zrpc::Id32;
use zrpc::MPair;
use zrpc::MESSAGE_ID_BASE;

define_msgs!(
    AppendRequest,
    AppendResponse,
    InstallRequest,
    InstallResponse,
    AVoteRequest,
    AVoteResponse,
);
define_msg_pair!(AppendRequest, AppendResponse);
define_msg_pair!(InstallRequest, InstallResponse);
define_msg_pair!(AVoteRequest, AVoteResponse);

// #[derive(Debug)]
// struct ARequest0<'a> {
//     rpc: &'a AppendEntriesRequest<ClientRequest>,

// }

// impl Message for ARequest0<'_> {
//     fn encode_raw<B>(&self, buf: &mut B)
//     where
//         B: bytes::BufMut,
//         Self: Sized {
//         let buf = BufMutToWrite{buf};
//         bincode::serialize_into(&mut buf, &self.rpc);
//     }

//     fn merge_field<B>(
//         &mut self,
//         tag: u32,
//         wire_type: prost::encoding::WireType,
//         buf: &mut B,
//         ctx: prost::encoding::DecodeContext,
//     ) -> Result<(), prost::DecodeError>
//     where
//         B: bytes::Buf,
//         Self: Sized {
//         todo!()
//     }

//     fn encoded_len(&self) -> usize {
//         bincode::serialized_size(&self.rpc).unwrap() as usize
//     }

//     fn clear(&mut self) {
//         todo!()
//     }
// }

// #[derive(Debug)]
// struct AResponse {
//     rsp: AppendEntriesResponse
// }

// impl Default for AResponse {
//     fn default() -> Self {
//         Self { rsp: AppendEntriesResponse{
//             term: 0,
//             success: false,
//             conflict_opt: None,
//         } }
//     }
// }

// impl Message for AResponse {
//     fn decode<B>(mut buf: B) -> Result<Self, prost::DecodeError>
//     where
//     B: Buf,
//     Self: Default,
//     {
//         bincode::deserialize_from(reader);
//     }

//     fn encode_raw<B>(&self, buf: &mut B)
//     where
//         B: BufMut,
//         Self: Sized {
//         todo!()
//     }

//     fn merge_field<B>(
//         &mut self,
//         tag: u32,
//         wire_type: prost::encoding::WireType,
//         buf: &mut B,
//         ctx: prost::encoding::DecodeContext,
//     ) -> Result<(), prost::DecodeError>
//     where
//         B: bytes::Buf,
//         Self: Sized {
//         todo!()
//     }

//     fn encoded_len(&self) -> usize {
//         todo!()
//     }

//     fn clear(&mut self) {
//         todo!()
//     }
// }

#[async_trait]
impl RaftNetwork<ClientRequest> for LocalRouter {
    /// Send an AppendEntries RPC to the target Raft node (§5).
    #[tracing::instrument(level = TRACE_LEVEL, skip(self, rpc))]
    async fn append_entries(
        &self,
        target: u64,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        debug!("call remote {:?}", rpc);
        let clients = self.clients.read().await;
        let client = clients.get(&target).unwrap();
        let req = AppendRequest {
            data: bincode::serialize(&rpc).unwrap(),
        };
        let r = client.call(&req).await?;
        let r = r.await?;
        Ok(r.to())

        // bail!("Not connected");
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self, rpc))]
    async fn install_snapshot(
        &self,
        target: u64,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        debug!("call remote {:?}", rpc);
        let clients = self.clients.read().await;
        let client = clients.get(&target).unwrap();
        let req = InstallRequest::from(rpc);
        let r = client.call(&req).await?;
        let r = r.await?;
        Ok(r.to())
    }

    /// Send a RequestVote RPC to the target Raft node (§5).
    async fn vote(&self, target: u64, rpc: VoteRequest) -> Result<VoteResponse> {
        debug!("call remote {:?}", rpc);
        let clients = self.clients.read().await;
        let client = clients.get(&target).unwrap();
        let req = AVoteRequest::from(rpc);
        let r = client.call(&req).await?;
        let r = r.await?;
        Ok(r.to())
    }
}

#[derive(Default)]
struct DummyRouter {}

#[async_trait]
impl RaftNetwork<ClientRequest> for DummyRouter {
    async fn append_entries(
        &self,
        _target: u64,
        _rpc: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        bail!("DummyRouter: Not implmented append_entries");
    }

    async fn install_snapshot(
        &self,
        _target: u64,
        _rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        bail!("DummyRouter: Not implmented install_snapshot");
    }

    async fn vote(&self, _target: u64, _rpc: VoteRequest) -> Result<VoteResponse> {
        bail!("DummyRouter: Not implmented vote");
    }
}

pub fn service_type() -> &'static str {
    "SyncSub"
}

#[async_trait]
impl zrpc::Service for InnerService {
    fn type_name(&self) -> &'static str {
        service_type()
    }

    async fn handle_request(
        &self,
        _session: &zrpc::Session,
        ptype: i32,
        mut bytes: Bytes,
    ) -> Result<(i32, Bytes), String> {
        match ptype {
            APPENDREQUEST_ID => {
                let mut req: AppendRequest = zrpc::decode(&mut bytes)?;
                let rpc: AppendEntriesRequest<ClientRequest> =
                    bincode::deserialize(&mut req.data).unwrap();
                // if rpc.entries.len() > 0 {
                //     let mut found = false;
                //     for e in &rpc.entries {
                //         if let EntryPayload::Normal(_n) = &e.payload {
                //             found = true;
                //             break;
                //         }
                //     }
                //     if found {
                //         debug!("aaaaaaaa: delay for handle append entries");
                //         tokio::time::sleep(Duration::from_millis(100_000)).await;
                //         debug!("aaaaaaaa: now handle append entries");
                //     }
                // }

                let (_added, node) = self.get_or_add_remote(&rpc.leader_id).await.unwrap();
                let rsp = node.raft.append_entries(rpc).await.unwrap();
                let reply = AppendResponse::from(&rsp);
                return zrpc::reply_result(reply.id(), reply);
            }
            INSTALLREQUEST_ID => {
                let req: InstallRequest = zrpc::decode(&mut bytes)?;
                let rpc = req.to();
                let (_added, node) = self.get_or_add_remote(&rpc.leader_id).await.unwrap();
                let rsp = node.raft.install_snapshot(rpc).await.unwrap();
                let reply = InstallResponse::from(rsp);
                return zrpc::reply_result(reply.id(), reply);
            }
            AVOTEREQUEST_ID => {
                let req: AVoteRequest = zrpc::decode(&mut bytes)?;
                let rpc = req.to();
                let (_added, node) = self.get_or_add_remote(&rpc.candidate_id).await.unwrap();
                let rsp = node.raft.vote(rpc).await.unwrap();
                let reply = AVoteResponse::from(rsp);
                return zrpc::reply_result(reply.id(), reply);
            }
            _ => {
                return Err(format!("unexpect message {:?}", ptype));
            }
        }
    }
}

struct RemoteNode {
    raft: Raft<ClientRequest, ClientResponse, DummyRouter, LocalStore>,
}

type RemoteNodes = HashMap<async_raft::NodeId, Arc<RemoteNode>>;

struct InnerService {
    discovery: discovery::Service,
    store: Arc<LocalStore>,
    router: Arc<LocalRouter>,
    raft: Raft<ClientRequest, ClientResponse, LocalRouter, LocalStore>,
    remote_nodes: Arc<RwLock<RemoteNodes>>,
}

impl InnerService {
    async fn get_or_add_remote(&self, uid: &async_raft::NodeId) -> Result<(bool, Arc<RemoteNode>)> {
        {
            let nodes = self.remote_nodes.read().await;
            if let Some(n) = nodes.get(uid) {
                return Ok((false, n.clone()));
            }
        }

        {
            let mut nodes = self.remote_nodes.write().await;
            if let Some(n) = nodes.get(uid) {
                return Ok((false, n.clone()));
            }

            let config = build_raft_config("sub-remote-raft".into())?;

            let router = Arc::new(DummyRouter::default());
            let store = Arc::new(LocalStore::new(uid.clone()));
            let raft = Raft::new(store.id, config, router.clone(), store.clone());

            // let mut uids = HashSet::new();
            // uids.insert(uid.clone());
            // let r = raft.initialize(uids).await;

            let node = Arc::new(RemoteNode { raft });
            nodes.insert(uid.clone(), node.clone());

            return Ok((true, node));
        }
    }

    async fn run(self: Arc<Self>) -> Result<()> {
        let mut rx = self.discovery.watch();
        let mut nodes = discovery::Nodes::default();

        let raft = &self.raft;
        let mut uids = HashSet::new();
        uids.insert(self.store.id);

        // loop {
        //     let _r = rx.recv().await?;
        //     let (_remove_uids, add_nodes) = self.discovery.nodes_delta(&nodes).await;
        //     for node in &add_nodes {
        //         uids.insert(node.uid.clone() as async_raft::NodeId);
        //     }
        //     if uids.len() >= 3 {
        //         break;
        //     }
        // }

        let r = raft.initialize(uids).await;
        debug!("raft init {:?}", r);
        loop {
            let r = raft.current_leader().await;
            debug!("current_leader {:?}", r);
            if let Some(uid) = r {
                debug!("become leader {}", uid);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        {
            let raft = raft.clone();
            let uid = self.store.id;
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(15_000)).await;
                for req_id in 1..3u64 {
                    let req = ClientRequest {
                        uid: uid.clone(),
                        req_id,
                        hub_name: "hub1".to_string(),
                        filter: "t1/#".to_string(),
                        action: Action::Sub,
                    };

                    let _r = raft.client_write(ClientWriteRequest::new(req)).await;
                }
            });
        }

        loop {
            let _r = rx.recv().await?;
            let (_remove_uids, add_nodes) = self.discovery.nodes_delta(&nodes).await;
            if add_nodes.len() > 0 {
                let mut add_uids = HashSet::new();
                {
                    let mut clients = self.router.clients.write().await;
                    for node in &add_nodes {
                        add_uids.insert(node.uid.clone() as async_raft::NodeId);
                        let mut client =
                            zrpc::Client::builder().service_type(service_type()).build();
                        let r = client.connect(&node.addr).await;
                        debug!("connect to {:?}, {:?}", node, r);
                        clients.insert(node.uid.clone() as async_raft::NodeId, client);
                        nodes.0.insert(node.uid.clone(), node.clone());
                    }
                }

                let mut mship = self.store.get_membership().await?;
                for uid in &add_uids {
                    mship.members.insert(uid.clone());
                    // let r = raft.add_non_voter(uid.clone()).await;
                    // debug!("add_non_voter {}, {:?}", uid, r);
                }

                debug!("change_membership {:?}", mship.members);
                raft.change_membership(mship.members).await?;
            }
        }
    }
}

fn build_raft_config(name: String) -> Result<Arc<async_raft::Config>> {
    let builder = async_raft::Config::build(name)
    // .election_timeout_min(50_000_000_000)
    // .election_timeout_max(100_000_000_000)
    // .heartbeat_interval(300_000)
    ;
    let config = Arc::new(builder.validate().context("failed to build Raft config")?);
    Ok(config)
}

pub async fn build(discovery: &discovery::Service) -> Result<Arc<dyn zrpc::Service>> {
    let config = build_raft_config("sub-local-raft".into())?;

    let router = Arc::new(LocalRouter::default());
    let store = Arc::new(LocalStore::new(*discovery.id() as async_raft::NodeId));

    debug!("build raft -> ");
    let raft = Raft::new(store.id, config, router.clone(), store.clone());
    debug!("build raft <- ");
    let service = Arc::new(InnerService {
        discovery: discovery.clone(),
        raft,
        router,
        store,
        remote_nodes: Default::default(),
    });

    let service0 = service.clone();
    let f = async move {
        debug!("service begin");
        let r = service0.run().await;
        if let Err(e) = r {
            error!("finish with {:?}", e);
        }
    };
    let span = tracing::span!(tracing::Level::INFO, "sync_sub");
    tokio::spawn(Instrument::instrument(f, span));

    Ok(service)
}

pub async fn run() -> Result<()> {
    let node_id: discovery::NodeId = 1;

    let mut server = zrpc::Server::builder().build();
    server.server_mut().bind("0:0").await?;
    let local_addr = server.server().local_addr().unwrap();
    let discovery = discovery::Service::new(&node_id, &local_addr);
    //discovery.kick_seed(&cfg.seed).await?;
    let _service = build(&discovery).await?;
    tokio::time::sleep(Duration::from_secs(9999999)).await;
    Ok(())
}
