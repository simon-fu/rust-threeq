use std::{collections::{HashMap, VecDeque}, sync::Arc};
use tokio::sync::{Mutex, Notify, RwLock};
use async_trait::async_trait;
use std::marker::PhantomData;

#[async_trait]
pub trait Sub<T: Send>{
    async fn push<>(&self, msg: &T);
}

#[derive(Debug, Default)]
pub struct Hub < 
    T: Send,
    K:std::cmp::Eq + std::hash::Hash, 
    SubT: Sub<T> > 
{
    subs: RwLock< HashMap<K, Arc<SubT> > > ,
    _p: PhantomData<T>,
}

impl<
    T: Send, 
    K:std::cmp::Eq+ std::hash::Hash, 
    SubT: Sub<T> 
    > 
    Hub<T, K, SubT> {
    pub fn new() -> Self{
        Self {
            subs: RwLock::new(HashMap::new()),
            _p: PhantomData,
        }
    }

    pub async fn add(&self, k: K, suber: Arc<SubT>) -> Option<Arc<SubT>> {
        self.subs.write().await.insert(k, suber)
    }

    pub async fn remove(&self, k: &K) -> Option<Arc<SubT>> {
        self.subs.write().await.remove(&k)
    }

    pub async fn push(&self, msg: &T) {
        let map = self.subs.write().await;
        for (_k, v) in map.iter() {
            v.push(msg).await;
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Closed,
    // Lagged(usize),
}

#[derive(Debug, Default)]
struct LatestFirstData<T>{
    que: VecDeque<T>,
    drops: usize,
    closed: bool ,
}


#[derive(Debug, Default)]
pub struct LatestFirstQue<T> {
    max_qsize: usize,
    notify: Notify,
    data: Mutex<LatestFirstData<T>>,
}

impl<T: Send> LatestFirstQue<T> {
    pub fn new(max_qsize: usize) -> Self {
        Self {
            max_qsize,
            data: Mutex::new(LatestFirstData{
                que: VecDeque::new(),
                drops: 0,
                closed: false,
            }),
            notify: Notify::new(),
        }
    }

    pub async fn close(&self) {
        let mut data = self.data.lock().await;
        data.closed = true;
    }

    pub async fn recv(&self) -> Result<(T, usize), Error> {
        loop {
            {
                let mut data = self.data.lock().await;

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

#[async_trait]
impl<T: Send+Sync+Clone> Sub<T> for LatestFirstQue<T> {
    async fn push(&self, msg: &T) {
        {
            let mut data = self.data.lock().await;
            if data.closed {
                // closed, do not produce data any more
                return ;
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
