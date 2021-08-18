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
struct TopicInner {
    senders: HashMap<u64, BcSender>,
}

impl TopicInner {
    fn new() -> Self {
        Self {
            senders: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct Topic {
    inner: RwLock<TopicInner>,
    refc: AtomicU64,
}

impl Topic {
    fn new() -> Self {
        Self {
            inner: RwLock::new(TopicInner::new()),
            refc: AtomicU64::new(1),
        }
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
struct RegistryInner {
    filter_tree: Mqtree<HashMap<u64, BcSender>>,
    topic_tree: MqtreeR<Arc<Topic>>,
}

#[derive(Debug)]
pub struct Hub {
    inner: Arc<RwLock<RegistryInner>>,
}

impl Hub {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(RegistryInner {
                filter_tree: Mqtree::new(),
                topic_tree: MqtreeR::new(),
            })),
        }
    }

    pub async fn subscribe(&self, filter: &str, uid: u64, tx: BcSender) {
        let mut inner = self.inner.write().await;
        let senders = inner
            .filter_tree
            .entry(filter)
            .get_or_insert(HashMap::new());
        senders.insert(uid, tx.clone());

        let mut topics: Vec<Arc<Topic>> = Vec::new();
        inner.topic_tree.rmatch_with(filter, &mut |t| {
            topics.push(t.clone());
        });
        for t in topics {
            let mut ti = t.inner.write().await;
            ti.senders.insert(uid, tx.clone());
        }
    }

    pub async fn unsubscribe(&self, filter: &str, uid: u64) -> bool {
        let mut inner = self.inner.write().await;
        if let Some(senders) = inner.filter_tree.entry(filter) {
            let exist = senders.remove(&uid).is_some();
            if senders.is_empty() {
                inner.filter_tree.remove(filter);
            }

            let mut topics: Vec<Arc<Topic>> = Vec::new();
            inner.topic_tree.rmatch_with(filter, &mut |t| {
                topics.push(t.clone());
            });

            for t in topics {
                let mut ti = t.inner.write().await;
                ti.senders.remove(&uid);
            }

            return exist;
        } else {
            return false;
        }
    }

    pub async fn acquire_topic(&self, path: &str) -> Arc<Topic> {
        {
            let inner = self.inner.read().await;
            if let Some(t) = inner.topic_tree.get(path) {
                t.refc.fetch_add(1, Ordering::Relaxed);
                return t.clone();
            }
        }

        {
            let mut inner = self.inner.write().await;
            let t = Arc::new(Topic::new());
            {
                let mut topic_inner = t.inner.write().await;
                inner.filter_tree.match_with(path, &mut |senders| {
                    for r in senders {
                        topic_inner.senders.insert(*r.0, r.1.clone());
                    }
                });
            }

            inner.topic_tree.entry(path).get_or_insert(t.clone());
            return t;
        }
    }

    pub async fn release_topic(&self, path: &str) {
        {
            let inner = self.inner.read().await;
            if let Some(t) = inner.topic_tree.get(path) {
                let value = t.refc.fetch_sub(1, Ordering::Relaxed);
                if value > 1 {
                    return;
                }
            } else {
                return; // NOT found topic
            }
        }

        {
            let mut inner = self.inner.write().await;
            inner.topic_tree.remove(path);
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
