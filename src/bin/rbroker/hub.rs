use rust_threeq::tq3::tt;
use rust_threeq::tq3::tt::mqtree::{Mqtree, MqtreeR};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;
// use crate::hub::{BcData, BcSender};

#[derive(Debug)]
struct PubTopicInner {
    senders: HashMap<u64, BcSender>,
}

impl PubTopicInner {
    fn new() -> Self {
        Self {
            senders: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct PubTopicNode {
    name: String,
    inner: RwLock<PubTopicInner>,
    refc: AtomicU64,
}

impl PubTopicNode {
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline(always)]
    pub async fn broadcast(&self, d: Arc<BcData>) {
        let inner = self.inner.read().await;
        for (_, tx) in inner.senders.iter() {
            if let Err(_e) = send(tx, d.clone()).await {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct PubTopic {
    hub: Hub,
    node: Arc<PubTopicNode>,
}

impl PubTopic {
    fn new(hub: Hub, node: Arc<PubTopicNode>) -> Self {
        Self { hub, node }
    }
}

impl std::ops::Deref for PubTopic {
    type Target = PubTopicNode;
    fn deref(&self) -> &PubTopicNode {
        &self.node
    }
}

impl Drop for PubTopic {
    fn drop(&mut self) {
        self.hub.release_pub_topic(&self.name);
    }
}

#[derive(Debug, Clone)]
struct HubInner {
    filter_tree: Mqtree<HashMap<u64, BcSender>>,
    topic_tree: MqtreeR<Arc<PubTopicNode>>,
}

type HubRwLock<T> = std::sync::RwLock<T>;

#[derive(Debug, Clone)]
pub struct Hub {
    inner: Arc<HubRwLock<HubInner>>,
}

impl Hub {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HubRwLock::new(HubInner {
                filter_tree: Mqtree::new(),
                topic_tree: MqtreeR::new(),
            })),
        }
    }

    fn subscribe0(&self, filter: &str, uid: u64, tx: BcSender) -> Vec<Arc<PubTopicNode>> {
        let mut hub = self.inner.write().unwrap();
        let senders = hub.filter_tree.entry(filter).get_or_insert(HashMap::new());
        senders.insert(uid, tx.clone());

        let mut topics: Vec<Arc<PubTopicNode>> = Vec::new();
        hub.topic_tree.rmatch_with(filter, &mut |t| {
            topics.push(t.clone());
        });
        topics
    }

    pub async fn subscribe(&self, filter: &str, uid: u64, tx: BcSender) {
        for t in self.subscribe0(filter, uid, tx.clone()) {
            let mut ti = t.inner.write().await;
            ti.senders.insert(uid, tx.clone());
        }
    }

    pub fn unsubscribe0(&self, filter: &str, uid: u64) -> Vec<Arc<PubTopicNode>> {
        let mut topics: Vec<Arc<PubTopicNode>> = Vec::new();

        let mut inner = self.inner.write().unwrap();
        if let Some(senders) = inner.filter_tree.entry(filter) {
            senders.remove(&uid);
            if senders.is_empty() {
                inner.filter_tree.remove(filter);
            }

            inner.topic_tree.rmatch_with(filter, &mut |t| {
                topics.push(t.clone());
            });
        }

        topics
    }

    pub async fn unsubscribe(&self, filter: &str, uid: u64) -> bool {
        let topics = self.unsubscribe0(filter, uid);
        for t in &topics {
            let mut ti = t.inner.write().await;
            ti.senders.remove(&uid);
        }
        topics.len() > 0
    }

    pub fn acquire_pub_topic<S: Into<String>>(&self, path: S) -> PubTopic {
        let path = path.into();
        {
            let hub = self.inner.read().unwrap();
            if let Some(node) = hub.topic_tree.get(&path) {
                node.refc.fetch_add(1, Ordering::Relaxed);
                return PubTopic::new(self.clone(), node.clone());
            }
        }

        {
            let mut hub = self.inner.write().unwrap();
            let mut topic_inner = PubTopicInner::new();
            hub.filter_tree.match_with(&path, &mut |senders| {
                for r in senders {
                    topic_inner.senders.insert(*r.0, r.1.clone());
                }
            });

            let node = Arc::new(PubTopicNode {
                name: path,
                inner: RwLock::new(topic_inner),
                refc: AtomicU64::new(1),
            });

            hub.topic_tree.entry(&node.name).get_or_insert(node.clone());

            return PubTopic::new(self.clone(), node);
        }
    }

    fn release_pub_topic(&self, path: &str) {
        {
            let hub = self.inner.read().unwrap();
            if let Some(t) = hub.topic_tree.get(path) {
                let value = t.refc.fetch_sub(1, Ordering::Relaxed);
                if value > 1 {
                    return;
                }
            } else {
                return; // NOT found topic
            }
        }

        {
            let mut hub = self.inner.write().unwrap();
            if let Some(t) = hub.topic_tree.get(path) {
                if t.refc.load(Ordering::Relaxed) == 0 {
                    hub.topic_tree.remove(path);
                }
            }
        }
    }
}

pub fn get() -> &'static Hub {
    lazy_static::lazy_static! {
        static ref INST: Hub = Hub::new();
    }
    return &*INST;
}

pub enum BcData {
    PUB(tt::Publish),
}

// RUSTFLAGS='--cfg channel_type="broadcast"' cargo build --release
#[cfg(channel_type = "broadcast")]
//#[cfg( any(not(channel_type), channel_type="broadcast") )]
pub mod channel {
    use super::*;
    use tokio::sync::broadcast;
    pub type BcSender = broadcast::Sender<Arc<BcData>>;
    pub type BcRecver = broadcast::Receiver<Arc<BcData>>;

    #[inline(always)]
    pub fn channel_type_name() -> String {
        "broadcast".to_string()
    }

    #[inline(always)]
    pub fn make_pair() -> (BcSender, BcRecver) {
        broadcast::channel(16)
    }

    #[inline(always)]
    pub async fn recv(rx: &mut BcRecver) -> Result<Arc<BcData>, broadcast::error::RecvError> {
        rx.recv().await
    }

    #[inline(always)]
    pub async fn send(tx: &BcSender, d: Arc<BcData>) -> Result<(), String> {
        let r = tx.send(d);
        match r {
            Ok(_d) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

// RUSTFLAGS='--cfg channel_type="mpsc"' cargo build --release
#[cfg(any(not(channel_type), channel_type = "mpsc"))]
// #[cfg(channel_type = "mpsc")]
pub mod channel {
    use super::*;
    use tokio::sync::broadcast;
    use tokio::sync::mpsc;
    pub type BcSender = mpsc::Sender<Arc<BcData>>;
    pub type BcRecver = mpsc::Receiver<Arc<BcData>>;

    #[inline(always)]
    pub fn channel_type_name() -> String {
        "mpsc".to_string()
    }

    #[inline(always)]
    pub fn make_pair() -> (BcSender, BcRecver) {
        mpsc::channel(16)
    }

    #[inline(always)]
    pub async fn recv(rx: &mut BcRecver) -> Result<Arc<BcData>, broadcast::error::RecvError> {
        let r = rx.recv().await;
        match r {
            Some(d) => Ok(d),
            None => (Err(broadcast::error::RecvError::Closed)),
        }
    }

    #[inline(always)]
    pub async fn send(tx: &BcSender, d: Arc<BcData>) -> Result<(), String> {
        let r = tx.send(d).await;
        match r {
            Ok(_d) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

pub use channel::*;
