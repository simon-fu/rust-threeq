use anyhow::Result;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::time::Duration;
use tracing::info;

pub mod tt;

pub mod tbytes;

pub mod log;

pub mod limit;

pub mod hub;

pub mod histogram;

lazy_static::lazy_static! {
    static ref BASE:std::time::Instant = std::time::Instant::now();

    static ref SINCE_THE_EPOCH: std::time::Duration = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .expect("Time went backwards");
}

pub struct TS;
impl TS {
    // monotone increasing
    pub fn mono_ms() -> i64 {
        let t = *BASE;
        let d0 = *SINCE_THE_EPOCH;
        let d1 = std::time::Instant::now() - t;
        let ms = (d0 + d1).as_millis();
        return ms as i64;
    }

    pub fn now_ms() -> i64 {
        // chrono::Utc::now().timestamp_millis();

        let now = std::time::SystemTime::now();
        let since_the_epoch = now
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards");
        //let ms = since_the_epoch.as_secs() as i64 * 1000i64 + (since_the_epoch.subsec_nanos() as i64 / 1_000_000) as i64;
        let ms =
            since_the_epoch.as_secs() * 1000 + (since_the_epoch.subsec_nanos() as u64 / 1_000_000);
        ms as i64
    }

    pub fn wait_for_next_milli() {
        let now = std::time::SystemTime::now();
        let since_the_epoch = now
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards");

        let mut nanos = since_the_epoch.subsec_nanos() as u64;
        nanos = nanos - 1_000_000 * (nanos / 1_000_000);
        nanos = 1_000_000 - nanos;
        std::thread::sleep(std::time::Duration::from_nanos(nanos));
    }
}

pub async fn measure_async<T>(num: u64, fut: T) -> (Duration, u128, u128, u128)
where
    T: core::future::Future,
{
    let now = std::time::Instant::now();
    let _r = fut.await;

    let elapsed = now.elapsed();
    drop(now);
    let num = num as u128;
    let each = elapsed.as_nanos() / num;
    let rate = num * 1_000_000_000 / elapsed.as_nanos();
    return (elapsed, num, each, rate);
}

pub async fn measure_and_print<T>(name: &str, num: u64, fut: T)
where
    T: core::future::Future,
{
    let (elapsed, num, each, rate) = measure_async(num, fut).await;

    info!(
        "{}: num {}, elapsed {:?}, each {} ns, estimate {}/sec",
        name, num, elapsed, each, rate
    );
}

// snowflake revised edition
// reserved: 1 bit
// time-id: 39bit(origin 41bit), duration years = 2^39/1000/60/60/24/365 ≈ 17
// node-id: 12bit(origin 10bit), max nodes = 2^12 = 4096
// seq-id: 12bit, max qps = 2^12 = 4096/ms = 4096000/sec
#[derive(Debug)]
pub struct SnowflakeId {
    node_id: u64,
    last_ts: u64,
    last_seq: u64,
}

const SNOWFLAKE_BASE: u64 = 1609430400000; // 2021-01-01 00:00:00
const SNOWFLAKE_MAX_SEQ: u64 = 1 << 12;

impl SnowflakeId {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id: node_id << 12,
            last_ts: 0,
            last_seq: 0,
        }
    }

    pub fn next(&mut self) -> Result<u64, u64> {
        let now = std::time::SystemTime::now();
        let since_the_epoch = now
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards");

        let ts =
            since_the_epoch.as_secs() * 1000 + (since_the_epoch.subsec_nanos() as u64 / 1_000_000);
        let ts = ts - SNOWFLAKE_BASE;

        if ts <= self.last_ts {
            self.last_seq += 1;
            if self.last_seq >= SNOWFLAKE_MAX_SEQ {
                let mut nanos = since_the_epoch.subsec_nanos() as u64;
                nanos = nanos - 1_000_000 * (nanos / 1_000_000);
                nanos = 1_000_000 - nanos;
                nanos = nanos + (self.last_ts - ts) * 1_000_000;
                return Err(nanos);
            }
        } else {
            self.last_ts = ts;
            self.last_seq = 0;
        }

        let mid = (self.last_ts << 24) | self.node_id | self.last_seq;
        Ok(mid)
    }

    pub fn next_or_wait(&mut self) -> u64 {
        loop {
            match self.next() {
                Ok(n) => {
                    return n;
                }
                Err(nanos) => {
                    std::thread::sleep(std::time::Duration::from_nanos(nanos));
                }
            }
        }
    }

    pub fn next_or_borrow(&mut self) -> u64 {
        let now = std::time::SystemTime::now();
        let since_the_epoch = now
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards");
        let ts =
            since_the_epoch.as_secs() * 1000 + (since_the_epoch.subsec_nanos() as u64 / 1_000_000);
        let ts = ts - SNOWFLAKE_BASE;

        if ts <= self.last_ts {
            self.last_seq += 1;
            if self.last_seq >= SNOWFLAKE_MAX_SEQ {
                self.last_ts += 1;
                self.last_seq = 0;
            }
        } else {
            self.last_ts = ts;
            self.last_seq = 0;
        }

        let mid = (self.last_ts << 24) | self.node_id | self.last_seq;
        mid
    }
}

// pub type SnowflakeIdSafe = std::sync::Mutex<SnowflakeId>;
pub struct SnowflakeIdSafe {
    real: std::sync::Mutex<SnowflakeId>,
}

impl SnowflakeIdSafe {
    pub fn new(node_id: u64) -> Self {
        Self {
            real: std::sync::Mutex::new(SnowflakeId::new(node_id)),
        }
    }

    pub fn next(&self) -> Result<u64, u64> {
        self.real.lock().unwrap().next()
    }

    pub fn next_or_wait(&self) -> u64 {
        self.real.lock().unwrap().next_or_wait()
    }

    pub fn next_or_borrow(&self) -> u64 {
        self.real.lock().unwrap().next_or_borrow()
    }
}

use core::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::sync::mpsc;
// refer from ReceiverStream::next()
#[derive(Debug)]
pub struct TryRecv<'a, T> {
    rx: &'a mut mpsc::Receiver<T>,
}

#[derive(Debug, PartialEq)]
pub enum TryRecvResult<T> {
    Value(T),
    Empty,
    NoSender,
}

impl<'a, T> TryRecv<'a, T> {
    pub fn new(rx: &'a mut mpsc::Receiver<T>) -> Self {
        Self { rx }
    }
}

impl<T> Future for TryRecv<'_, T> {
    type Output = TryRecvResult<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(o) => match o {
                Some(v) => Poll::Ready(TryRecvResult::Value(v)),
                None => Poll::Ready(TryRecvResult::NoSender),
            },
            Poll::Pending => Poll::Ready(TryRecvResult::Empty),
        }
    }
}

struct TryPoll<'a, T: futures::Future + Unpin>(&'a mut T);

impl<'a, T: futures::Future + Unpin> futures::Future for TryPoll<'a, T> {
    type Output = Option<T::Output>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // std::pin::Pin::new(&mut self.0).poll(cx);
        let r = futures::FutureExt::poll_unpin(self.0, cx);
        match r {
            std::task::Poll::Ready(r) => std::task::Poll::Ready(Some(r)),
            std::task::Poll::Pending => std::task::Poll::Ready(None),
        }
    }
}

#[inline]
pub async fn try_poll<'a, T: futures::Future + Unpin>(o: &'a mut T) -> Option<T::Output> {
    TryPoll(o).await
}

#[async_trait]
pub trait Flight {
    type Output;
    async fn try_recv_ack(&mut self) -> Result<Option<Self::Output>>;
    async fn recv_ack(self) -> Result<Self::Output>;
}

pub struct Inflights<T> {
    que: VecDeque<T>,
}

impl<T: Flight> Inflights<T> {
    pub fn new() -> Self {
        Self {
            que: VecDeque::new(),
        }
    }

    // #[inline]
    // pub fn is_empty(&self) -> bool {
    //     self.que.is_empty()
    // }

    // #[inline]
    // pub fn len(&mut self) -> usize{
    //     self.que.len()
    // }

    #[inline]
    pub async fn add_and_check(&mut self, value: T, max: usize) -> Result<Vec<T::Output>> {
        let mut outputs = Vec::new();
        if max <= 1 {
            outputs.push(value.recv_ack().await?);
            return Ok(outputs);
        }

        self.que.push_back(value);
        if self.que.len() >= max {
            return self.wait_for_oldest_and_check().await;
        } else {
            return self.check().await;
        }
    }

    pub async fn check(&mut self) -> Result<Vec<T::Output>> {
        let mut outputs = Vec::new();
        while !self.que.is_empty() {
            let flight = self.que.front_mut().unwrap();
            let r = flight.try_recv_ack().await?;
            match r {
                Some(r) => {
                    self.que.pop_front();
                    outputs.push(r);
                }
                None => {
                    break;
                }
            }
        }
        return Ok(outputs);
    }

    pub async fn wait_for_oldest_and_check(&mut self) -> Result<Vec<T::Output>> {
        if self.que.is_empty() {
            return Ok(Vec::new());
        }

        let mut outputs = Vec::new();
        {
            let flight = self.que.pop_front().unwrap();
            let r = flight.recv_ack().await?;
            outputs.push(r);
        }
        self.check().await
    }

    pub async fn wait_for_newest(&mut self) -> Result<Option<T::Output>> {
        if self.que.is_empty() {
            return Ok(None);
        }
        let flight = self.que.pop_back().unwrap();
        self.que.clear();
        let r = flight.recv_ack().await?;
        return Ok(Some(r));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::error;

    #[test]
    fn test_snowflake_id_normal() {
        // RUST_LOG=debug  cargo test test_snowflake_id -- --nocapture

        super::log::init();

        const NODE_ID: u64 = 0x123;
        const MAX_ROUNDS: u64 = 10000;

        {
            // warm up
            let mut gen = SnowflakeId::new(111);
            for _ in 0..1000 {
                let _r = gen.next();
            }
        }

        let mut expect_seq: u64 = 0;
        let mut expect_ts = TS::now_ms() as u64 - SNOWFLAKE_BASE;
        let mut expect_err = false;
        let mut max_seq: u64 = 0;

        let mut gen = SnowflakeId::new(NODE_ID);

        for round in 0..MAX_ROUNDS {
            for _ in 0..4096 {
                let r = gen.next();
                if r.is_err() {
                    assert_eq!(expect_err, true);
                    let nanos = r.unwrap_err();
                    assert!(nanos < 1_000_000, "error nanos {}", nanos);
                    continue;
                }

                let uid = r.unwrap();
                let u_ts = uid >> 12 >> 12;
                let u_node_id = (uid >> 12) & 0x0FFF;
                let u_seq = uid & 0x0FFF;

                if u_seq > max_seq {
                    max_seq = u_seq;
                }

                assert_eq!(u_node_id, NODE_ID);

                if expect_err {
                    assert_eq!(expect_seq, 0);
                    expect_err = false;
                    //error!("round[{}]: expect_ts {}, u_ts {}, expect_seq {}, u_seq {}", round, expect_ts, u_ts, expect_seq, u_seq);
                    //assert_eq!(expect_err, false);
                }

                if u_ts == expect_ts {
                } else if u_ts == (expect_ts + 1) {
                    assert_eq!(round, 0);
                    expect_ts = u_ts;
                    expect_seq = 0;
                } else {
                    error!("round[{}]: expect ts {}, but {}", round, expect_ts, u_ts);
                    assert!(false);
                }

                if u_seq != expect_seq {
                    error!(
                        "round[{}]: expect_ts {}, u_ts {}, expect_err {}",
                        round, expect_ts, u_ts, expect_err
                    );
                    assert_eq!(u_seq, expect_seq);
                }

                expect_seq += 1;
                if expect_seq == 4096 {
                    expect_seq = 0;
                    expect_ts = expect_ts + 1;
                    expect_err = true;
                }
            }
        }

        assert_eq!(max_seq, 4095);
    }

    #[test]
    fn test_snowflake_id_borrow() {
        // RUST_LOG=debug  cargo test test_snowflake_id -- --nocapture

        super::log::init();
        {
            // warm up
            let mut gen = SnowflakeId::new(111);
            for _ in 0..1000 {
                let _r = gen.next();
            }
        }

        const NODE_ID: u64 = 0x987;
        const MAX_ROUNDS: u64 = 10000;

        let mut max_seq: u64 = 0;
        let mut expect_seq: u64 = 0;
        let mut expect_ts = TS::now_ms() as u64 - SNOWFLAKE_BASE;
        let mut gen = SnowflakeId::new(NODE_ID);

        for round in 0..MAX_ROUNDS {
            for _ in 0..4096 {
                let uid = gen.next_or_borrow();

                let u_ts = uid >> 12 >> 12;
                let u_node_id = (uid >> 12) & 0x0FFF;
                let u_seq = uid & 0x0FFF;

                if u_seq > max_seq {
                    max_seq = u_seq;
                }

                assert_eq!(u_node_id, NODE_ID);

                if u_ts == expect_ts {
                } else if u_ts == (expect_ts + 1) {
                    expect_ts = u_ts;
                    expect_seq = 0;
                } else {
                    error!("round[{}]: expect ts {}, but {}", round, expect_ts, u_ts);
                    assert!(false);
                }

                assert_eq!(u_seq, expect_seq);
                expect_seq += 1;
                if expect_seq == 4096 {
                    expect_seq = 0;
                }
            }
        }

        assert_eq!(max_seq, 4095);
    }

    #[tokio::test]
    async fn test_try_next() {
        // RUST_LOG=debug  cargo test test_try_next -- --nocapture
        let (tx, mut rx) = mpsc::channel(100);
        assert_eq!(TryRecv::new(&mut rx).await, TryRecvResult::Empty);
        let _r = tx.send(1u64).await;
        assert_eq!(TryRecv::new(&mut rx).await, TryRecvResult::Value(1));
        assert_eq!(TryRecv::new(&mut rx).await, TryRecvResult::Empty);
        drop(tx);
        assert_eq!(TryRecv::new(&mut rx).await, TryRecvResult::NoSender);
    }
}
