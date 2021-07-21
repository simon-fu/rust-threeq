use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tokio_stream::StreamMap;

use super::Message;
// pub type Packet = u64;

pub type VerType = u64;

#[derive(Debug, Default)]
pub struct VerQue<T> {
    max_size: usize,
    curr_ver: VerType,
    data: VecDeque<(VerType, T)>,
}

impl<T> VerQue<T> {
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            curr_ver: 0,
            data: VecDeque::default(),
        }
    }

    pub fn next_ver(&self) -> VerType {
        self.curr_ver + 1
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn push(&mut self, v: T) -> VerType {
        self.curr_ver += 1;
        self.data.push_back((self.curr_ver, v));
        while self.data.len() > self.max_size {
            self.data.pop_front();
        }
        self.curr_ver
    }

    pub fn inc_ver(&mut self) -> VerType {
        self.curr_ver += 1;
        self.curr_ver
    }

    pub fn next(&self, ver: VerType) -> Option<&(VerType, T)> {
        if self.data.len() == 0 || ver > self.curr_ver {
            return None;
        }

        let reversed_index = (self.curr_ver - ver + 1) as usize;
        let index = if reversed_index <= self.data.len() {
            self.data.len() - reversed_index
        } else {
            0
        };
        return self.data.get(index);

        // let (s0, s1) = self.que.as_slices();
        // if let Some(item) = s0.last() {
        //     if ver <= item.0{
        //         let first = s0.first().unwrap();
        //         if ver < first.0 {
        //             return Some(first.clone());
        //         } else {
        //             let index = (ver - first.0) as usize;
        //             return Some(s0[index].clone());
        //         }
        //     }
        // }

        // if let Some(item) = s1.last() {
        //     if ver <= item.0{
        //         let index = s1.len() - 1 - (item.0 - ver) as usize;
        //         return Some(s1[index].clone());
        //     }
        // }

        // return None;
    }
}

#[derive(Debug)]
pub struct QueItem {
    refs: AtomicUsize,
    msg: Arc<Message>,
}

impl QueItem {
    fn new(refs: usize, msg: Arc<Message>) -> Self {
        Self {
            refs: AtomicUsize::new(refs),
            msg,
        }
    }
}

pub type QueData = VerQue<QueItem>;
pub type RWQue = RwLock<QueData>;

#[derive(Debug)]
pub struct Que {
    name: String,
    rw: RWQue,
}

impl Que {
    pub fn new(name: &str, max_size: usize) -> Self {
        Self {
            name: name.to_string(),
            rw: RwLock::new(VerQue::new(max_size)),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn next_ver(&self) -> VerType {
        self.rw.read().await.next_ver()
    }

    pub async fn read<'a>(&'a self) -> tokio::sync::RwLockReadGuard<'a, QueData> {
        let guard = self.rw.read().await;
        return guard;
    }
}

#[derive(Debug)]
pub struct Hub {
    que: Arc<Que>,
    tx: broadcast::Sender<u64>,
    // rx: broadcast::Receiver<u64>,
}

impl Hub {
    pub fn new(que: Arc<Que>) -> Self {
        let (tx, _rx) = broadcast::channel(2);
        Self {
            que,
            tx,
            // rx,
        }
    }

    pub fn with_name(name: &str, max_size: usize) -> Self {
        let que = Que::new(name, max_size);
        Self::new(Arc::new(que))
    }

    pub fn que(&self) -> &Arc<Que> {
        &self.que
    }

    pub async fn push(&self, packet: Message) -> VerType {
        let mut que = self.que.rw.write().await;
        let ver = if self.tx.receiver_count() > 0 {
            que.push(QueItem::new(self.tx.receiver_count(), Arc::new(packet)))
        } else {
            que.inc_ver()
        };
        let _r = self.tx.send(ver);
        ver
    }

    pub async fn subscribe(&self, subs: &mut Subscriptions) -> Option<Arc<Suber>> {
        let rx = BroadcastStream::new(self.tx.subscribe());
        let sub = Arc::new(Suber::new(self.que.clone(), self.que.next_ver().await));
        if subs.map.contains_key(&sub) {
            return None;
        }
        subs.map.insert(sub.clone(), rx);
        Some(sub)
    }
}

impl PartialEq for Hub {
    fn eq(&self, other: &Self) -> bool {
        self.que.name == other.que.name
    }
}

impl Eq for Hub {}

impl std::hash::Hash for Hub {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.que.name.hash(state);
    }
}

#[derive(Debug)]
pub struct Suber {
    que: Arc<Que>,
    ver: AtomicU64,
}

impl Suber {
    fn new(que: Arc<Que>, ver: VerType) -> Self {
        Self {
            que,
            ver: AtomicU64::new(ver),
        }
    }

    pub fn topic(&self) -> &str {
        self.que.name()
    }

    pub fn que(&self) -> &Arc<Que> {
        &self.que
    }

    pub async fn next(&self) -> Option<(VerType, Arc<Message>)> {
        let que = self.que.rw.read().await;
        let r = que.next(self.ver.load(Ordering::Relaxed));
        if let Some((ver, item)) = r {
            self.ver.store(ver + 1, Ordering::Relaxed);
            item.refs.fetch_sub(1, Ordering::Relaxed);
            return Some((*ver, item.msg.clone()));
        }
        return None;
    }
}

impl PartialEq for Suber {
    fn eq(&self, other: &Self) -> bool {
        self.que.name == other.que.name
    }
}

impl Eq for Suber {}

impl std::hash::Hash for Suber {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.que.name.hash(state);
    }
}

#[derive(Debug, Default)]
pub struct Subscriptions {
    map: StreamMap<Arc<Suber>, BroadcastStream<u64>>,
}

impl Subscriptions {
    pub async fn recv(&mut self) -> Option<(Arc<Suber>, Result<u64, BroadcastStreamRecvError>)> {
        return self.map.next().await;
    }
}
