

// use anyhow::{Result, Context};

// use crate::{uid::{NodeId, NodeOffset}, util::time::now_ms};
// use super::{proto::{NodeInfo, NodeStatus}, node_client::NodeClient};


// pub struct Node {
//     id: NodeId,

//     uptime_milli: i64,

//     addrs: Vec<String>,

//     status: NodeStatus,

//     offset: NodeOffset,

//     // client: NodeClient,
// }

// impl Default for Node {
//     fn default() -> Self { 
//         Self { 
//             id: 0.into(),
//             uptime_milli: now_ms(),  
//             status: NodeStatus::Unknown, 
//             addrs: Default::default(), 
//             offset: Default::default(), 
//             // rpc_endpoint: Default::default(), 
//             // client: NodeClient::new(0.into()),
//         }
//     }
// }

// impl Node {
//     pub fn new(id: NodeId) -> Self {
//         Self{
//             id,
//             // client: NodeClient::new(id),
//             ..Default::default()
//         }
//     }

//     pub fn from_addr(id: NodeId, addr: String) -> Self { 
//         Self {
//             id,
//             addrs: vec![addr],
//             // client: NodeClient::new(id),
//             ..Default::default()
//         }
//     }

//     pub fn from_info(info: NodeInfo) -> Result<Self> { 
//         let status = NodeStatus::from_i32(info.status)
//         .with_context(||format!("build node but invalid status [{}]", info.status))?;
//         let id = info.node_id.into();
//         Ok(Self {
//             id,
//             uptime_milli: info.uptime_milli,
//             addrs: info.addrs,
//             offset: info.offset.into(),
//             status,
//             // client: NodeClient::new(id),
//             ..Default::default()
//         })
//     }

//     pub fn to_info(&self) -> NodeInfo {
//         NodeInfo {
//             node_id: self.id.into(),
//             uptime_milli: self.uptime_milli,
//             addrs: self.addrs.clone(),
//             offset: self.offset.into(),
//             status: self.status.into(),
//         }
//     }

//     pub fn id(&self) -> NodeId {
//         self.id
//     }

//     pub fn status(&self) -> NodeStatus {
//         self.status
//     }

//     pub fn addrs(&self) -> &[String] {
//         &self.addrs
//     }

//     pub fn offset(&self) -> NodeOffset {
//         self.offset
//     }

//     // pub fn is_working(&self) -> bool {
//     //     self.status() == NodeStatus::Online
//     // }

//     pub(crate) fn push_addr(&mut self, addr: String) {
//         self.addrs.push(addr);
//     }

//     pub(crate) fn set_status(&mut self, s: NodeStatus) {
//         self.status = s;
//     }

//     pub(crate) fn change_id(&mut self, id: NodeId) {
//         self.id = id;
//     }

//     // pub(crate) fn client(&self) -> &NodeClient {
//     //     &self.client
//     // }

//     pub(crate) fn merge_addrs<I: Iterator<Item = String>>(&mut self, addrs: I) -> bool { 
//         let mut updated = false;
//         for s in addrs {
//             let exist = self.addrs.iter().position(|x| *x == s).is_some();
//             if !exist {
//                 self.addrs.push(s);
//                 updated = true;
//             }
//         }
//         updated
//     }
// }


// // #[derive(Clone)]
// // pub struct Node {
// //     inner: Arc<NodeInner>,
// // }

// // impl Node {
// //     pub fn new(id: NodeId) -> Self {
// //         Self {
// //             inner: Arc::new(NodeInner {
// //                 data: Mutex::new(NodeData{
// //                     id,
// //                     ..Default::default()
// //                 }),
// //             }),
// //         }
// //     }

// //     pub fn from_addr(id: NodeId, addr: String) -> Self { 
// //         Self {
// //             inner: Arc::new(NodeInner {
// //                 data: Mutex::new(NodeData{
// //                     id,
// //                     addr,
// //                     ..Default::default()
// //                 }),
// //             }),
// //         }
// //     }

// //     pub fn from_info(info: NodeInfo) -> Result<Self> { 
// //         let status = NodeStatus::from_i32(info.status)
// //         .with_context(||format!("build node but invalid status [{}]", info.status))?;

// //         Ok(Self {
// //             inner: Arc::new(NodeInner {
// //                 data: Mutex::new(NodeData{
// //                     id: info.node_id.into(),
// //                     up_time: info.up_time,
// //                     addr: info.addr,
// //                     offset: info.offset,
// //                     status,
// //                     ..Default::default()
// //                 }),
// //             }),
// //         })
// //     }

// //     pub fn to_info(&self) -> NodeInfo {
// //         let data = self.inner.data.lock();
// //         NodeInfo {
// //             node_id: data.id.into(),
// //             up_time: data.up_time,
// //             addr: data.addr.clone(),
// //             offset: data.offset,
// //             status: data.status.into(),
// //         }
// //     }

// //     pub fn id(&self) -> NodeId {
// //         self.inner.data.lock().id
// //     }

// //     pub fn status(&self) -> NodeStatus {
// //         self.inner.data.lock().status
// //     }

// //     pub fn is_working(&self) -> bool {
// //         self.status() == NodeStatus::Online
// //     }

// //     pub(crate) fn put_addr(&self, addr: String) -> bool {
// //         let mut data = self.inner.data.lock(); 
// //         if data.addr.is_empty() {
// //             data.addr = addr;
// //             true
// //         } else {
// //             false
// //         }
// //     }

// //     pub(crate) fn try_alone_to_online(&self, addr: String) -> bool { 
// //         let mut data = self.inner.data.lock(); 
// //         if data.status == NodeStatus::Alone {
// //             data.addr = addr;
// //             data.status = NodeStatus::Online;
// //             true
// //         } else {
// //             false
// //         }
// //     }

// //     pub(crate) fn change_status(&self, s: NodeStatus) {
// //         self.inner.data.lock().status = s;
// //     }

// //     pub(crate) fn change_id(&self, id: NodeId) {
// //         self.inner.data.lock().id = id;
// //     }

// //     pub(crate) fn rpc_client(&self) -> Result<RpcClient<RpcChannel>> { 
// //         let mut data = self.inner.data.lock(); 
// //         if data.rpc_endpoint.is_none() {
// //             if data.addr.is_empty() {
// //                 bail!("get rpc client but address empty")
// //             }
// //             let ep = Endpoint::from_shared(data.addr.to_string())?;
// //             data.rpc_endpoint = Some(ep);
// //         }

// //         if let Some(ep) = &data.rpc_endpoint {
// //             let ch = ep.connect_lazy()?;
// //             return Ok(RpcClient::new(ch))
// //         }

// //         bail!("get rpc client but none")
// //     }
// // }


// // struct NodeInner {
// //     data: Mutex<NodeData>,
// // }

// // struct NodeData {
// //     id: NodeId,

// //     up_time: u64,

// //     addr: String,

// //     offset: u64,

// //     status: NodeStatus,

// //     rpc_endpoint: Option<Endpoint>,
// // }

// // impl Default for NodeData {
// //     fn default() -> Self {
// //         Self { 
// //             id: 0.into(),
// //             up_time: now_ms(),  
// //             status: NodeStatus::Unknown, 
// //             addr: Default::default(), 
// //             offset: Default::default(), 
// //             rpc_endpoint: Default::default() 
// //         }
// //     }
// // }


// // // type Addrs = heapless::Vec<String, 2>;

// // // type AddrStr = heapless::String<50>;    // len(111.111.111.111:60000) = 21
// // //                                         // len(2001:0db8:3c4d:0015:0000:0000:1a2f:1a2b:60000) = 45

