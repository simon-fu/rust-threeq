use std::{sync::Arc, collections::BTreeSet};

use parking_lot::Mutex;

use crate::uid::NodeId;


#[derive(Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
}

impl Node {
    pub fn new(id: NodeId) -> Self {
        Self {
            inner: Arc::new(NodeInner {
                id,
                data: Default::default(),
            }),
        }
    }

    pub fn id(&self) -> NodeId {
        self.inner.id
    }

    pub fn loopback_id(&self) -> u64 {
        self.inner.data.lock().loopback_id
    }

    pub(crate) fn merge_loopback_id(&self, id: u64) {
        let mut data = self.inner.data.lock();
        if id > data.loopback_id {
            data.loopback_id = id;
        }
    }

    pub(crate) fn next_loopback_id(&self, ) -> u64 {
        let mut data = self.inner.data.lock();
        if data.loopback_id == 0 {
            data.loopback_id += 1;    
        }
        data.loopback_id += 1;
        let id = data.loopback_id;
        id
    }

    pub(crate) fn alloc_node_id(&self) -> Option<NodeId> {
        let mut data = self.inner.data.lock();
        data.node_id_pool.pop_first()
    }

    pub(crate) fn add_node_id_range(&self, start: NodeId, end: NodeId) {
        let mut data = self.inner.data.lock();
        for id in *start..*end {
            data.node_id_pool.insert(id.into());
        }
        data.node_id_pool.remove(&self.id());
    }

    pub(crate) fn put_addr(&self, addr: String) {
        let mut data = self.inner.data.lock(); 
        if data.addrs.iter().position(|x|*x == addr).is_none() {
            let _r = data.addrs.push(addr);
        }        
    }
}


struct NodeInner {
    id: NodeId,
    data: Mutex<MutData>,
}

#[derive(Default)]
struct MutData {
    addrs: Addrs,
    loopback_id: u64, // 为了在本机起多个node分配端口之用，生产环境避免使用
    node_id_pool: BTreeSet<NodeId>,
}

type Addrs = heapless::Vec<String, 2>;

// type AddrStr = heapless::String<50>;    // len(111.111.111.111:60000) = 21
//                                         // len(2001:0db8:3c4d:0015:0000:0000:1a2f:1a2b:60000) = 45

