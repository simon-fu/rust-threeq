use serde::Serialize;
use std::time::{Duration, Instant};
// use num_rational::Ratio;
// use num::Integer;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Ratio<T> {
    numer: T,
    denom: T,
}

impl<T> Ratio<T> {
    pub fn new(numer: T, denom: T) -> Self {
        Self { numer, denom }
    }

    #[inline]
    pub const fn numer(&self) -> &T {
        &self.numer
    }

    #[inline]
    pub const fn denom(&self) -> &T {
        &self.denom
    }
}

pub type Rate = Ratio<u64>;

impl Default for Rate {
    fn default() -> Self {
        Self { numer: 0, denom: 1 }
    }
}

#[derive(Debug)]
pub struct Pacer {
    kick_time: Instant,
    rate: Rate,
}

impl Pacer {
    pub fn new(rate: Rate) -> Self {
        Pacer {
            kick_time: Instant::now(),
            rate,
        }
    }

    pub fn with_time(mut self, t: Instant) -> Self {
        self.kick_time = t;
        self
    }

    pub fn kick(&mut self) {
        self.kick_time = Instant::now();
        // self.kick_time.elapsed();
    }

    pub fn kick_time(&self) -> &Instant {
        &self.kick_time
    }

    // if let Some(d) = pacer.get_sleep_duration(n) {
    //     tokio::time::sleep(d).await;
    // }
    pub fn get_sleep_duration(&self, n: u64) -> Option<Duration> {
        if *self.rate.denom() == 0 || *self.rate.numer() == 0 {
            return Some(Duration::from_millis(std::u64::MAX / 2));
        }

        let expect = 1000 * n * self.rate.denom() / self.rate.numer();
        let diff = expect as i64 - self.kick_time.elapsed().as_millis() as i64;
        if diff > 0 {
            Some(Duration::from_millis(diff as u64))
        } else {
            None
        }
    }

    // pub fn get_wait_milli(&self, n : u64) -> i64{
    //     if self.rate == 0 {
    //         return std::i64::MAX/2;
    //     }

    //     let expect = 1000 * n / self.rate;
    //     let diff = expect as i64 - self.kick_time.elapsed().as_millis() as i64;
    //     return diff;
    // }

    // pub fn check<F, T>(&self, n : u64, mut f: F) -> Option<T>
    // where F: FnMut(std::time::Duration) -> T,
    // {
    //     let diff = self.get_wait_milli(n);
    //     if diff > 0 {
    //         let r = f(std::time::Duration::from_millis(diff as u64));
    //         return Some(r);
    //     } else {
    //         None
    //     }
    // }

    // pub async fn check_and_wait(&self, n : u64) {
    //     let diff = self.get_wait_milli(n);
    //     if diff > 0 {
    //         tokio::time::sleep(tokio::time::Duration::from_millis(diff as u64)).await;
    //     }
    // }

    // pub async fn run_if_wait<F>(&self, n : u64, mut f: F)
    // where
    //     F: FnMut() -> bool,
    // {
    //     let mut diff = self.get_wait_milli(n);
    //     let mut is_run_next = true;
    //     while diff > 0 {
    //         if is_run_next {
    //             is_run_next = f();
    //         } else {
    //             tokio::time::sleep(tokio::time::Duration::from_millis(diff as u64)).await;
    //         }
    //         diff = self.get_wait_milli(n);
    //     }
    // }
}

#[derive(Debug)]
pub struct Interval {
    time: Instant,
    milli: u64,
}

impl Interval {
    pub fn new(milli: u64) -> Self {
        Interval {
            time: Instant::now(),
            milli,
        }
    }

    pub fn check(&mut self) -> bool {
        let next = self.time + std::time::Duration::from_millis(self.milli);
        let now = Instant::now();
        if now >= next {
            self.time = now + std::time::Duration::from_millis(self.milli);
            return true;
        } else {
            return false;
        }
    }
}