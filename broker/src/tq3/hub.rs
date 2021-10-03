// use async_trait::async_trait;
use std::marker::PhantomData;
use std::sync::Mutex;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tokio::sync::Notify;

// #[async_trait]
pub trait Sub<T: Send> {
    fn push(&self, msg: &T);
}

#[derive(Debug, Default)]
pub struct Hub<T: Send, K: std::cmp::Eq + std::hash::Hash, SubT: Sub<T>> {
    subs: std::sync::Mutex<HashMap<K, Arc<SubT>>>,
    _p: PhantomData<T>,
}

impl<T: Send, K: std::cmp::Eq + std::hash::Hash, SubT: Sub<T>> Hub<T, K, SubT> {
    pub fn new() -> Self {
        Self {
            subs: std::sync::Mutex::new(HashMap::new()),
            _p: PhantomData,
        }
    }

    pub fn with_suber(k: K, suber: Arc<SubT>) -> Self {
        let mut hm = HashMap::new();
        hm.insert(k, suber);
        Self {
            subs: std::sync::Mutex::new(hm),
            _p: PhantomData,
        }
    }

    pub fn add(&self, k: K, suber: Arc<SubT>) -> Option<Arc<SubT>> {
        self.subs.lock().unwrap().insert(k, suber)
    }

    pub fn remove(&self, k: &K) -> Option<Arc<SubT>> {
        self.subs.lock().unwrap().remove(&k)
    }

    pub async fn push(&self, msg: &T) {
        let map = self.subs.lock().unwrap();
        for (_k, v) in map.iter() {
            v.push(msg);
        }
    }
}

// drop oldest item if queue is full

#[derive(Debug)]
pub enum Error {
    Closed,
    // Lagged(usize),
}

#[derive(Debug, Default)]
struct SlidingData<T> {
    que: VecDeque<T>,
    drops: usize,
    closed: bool,
}

#[derive(Debug, Default)]
pub struct SlidingQue<T> {
    max_qsize: usize,
    notify: Notify,
    data: Mutex<SlidingData<T>>,
}

impl<T: Send> SlidingQue<T> {
    pub fn new(max_qsize: usize) -> Self {
        Self {
            max_qsize,
            data: Mutex::new(SlidingData {
                que: VecDeque::new(),
                drops: 0,
                closed: false,
            }),
            notify: Notify::new(),
        }
    }

    pub fn max_qsize(&self) -> usize {
        self.max_qsize
    }

    pub fn close(&self) {
        {
            let mut data = self.data.lock().unwrap();
            data.closed = true;
        }
        self.notify.notify_one();
    }

    pub async fn recv(&self) -> Result<(T, usize), Error> {
        loop {
            {
                let mut data = self.data.lock().unwrap();

                if !data.que.is_empty() {
                    let r = Ok((data.que.pop_front().unwrap(), data.drops));
                    data.drops = 0;
                    return r;
                } else {
                    if data.closed {
                        return Err(Error::Closed);
                    }
                }
                drop(data);
            }

            self.notify.notified().await;
        }
    }
}

// #[async_trait]
impl<T: Send + Sync + Clone> Sub<T> for SlidingQue<T> {
    fn push(&self, msg: &T) {
        {
            let mut data = self.data.lock().unwrap();
            if data.closed {
                // closed, do not produce data any more
                return;
            }

            data.que.push_back(msg.clone());
            while data.que.len() > self.max_qsize {
                data.que.pop_front();
                data.drops += 1;
            }
        }
        self.notify.notify_one();
    }
}
