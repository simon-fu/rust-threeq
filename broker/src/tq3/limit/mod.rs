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

const INTERVAL: Duration = Duration::from_millis(1000);

#[derive(Debug, Default, Clone)]
pub struct Traffic {
    pub packets: u64,
    pub bytes: u64,
}

impl Traffic {
    pub fn inc(&mut self, bytes: u64) {
        self.packets += 1;
        self.bytes += bytes;
    }
}

#[derive(Debug)]
pub struct TrafficSpeed {
    last_time: Instant,
    next_time: Instant,
    traffic: Traffic,
}

impl Default for TrafficSpeed {
    fn default() -> Self {
        Self {
            last_time: Instant::now(),
            next_time: Instant::now() + INTERVAL,
            traffic: Traffic::default(),
        }
    }
}

impl TrafficSpeed {
    fn reset(&mut self, now: Instant) {
        self.last_time = now;
        self.next_time = now + INTERVAL;
    }

    pub fn check_float(&mut self, now: Instant, t: &Traffic) -> Option<(f64, f64)> {
        if now < self.next_time {
            return None;
        }
        let d = now - self.last_time;
        let d = d.as_millis() as u64;
        if d == 0 {
            return None;
        }
        let r = (
            (t.packets - self.traffic.packets) as f64 * 1000.0 / d as f64,
            (t.bytes - self.traffic.bytes) as f64 * 1000.0 / d as f64 / 1000.0,
        );

        self.traffic.packets = t.packets;
        self.traffic.bytes = t.bytes;
        self.reset(now);

        return Some(r);
    }
}

pub struct Quota {
    rate: u64,      // 常量
    last_time: u64, // 状态， 最后一次时间
    duration: u64,  // 常量， 最大可以使用时间范围
    max_quota: u64, // 常量， 最大可使用额度
}

impl Quota {
    pub fn new(rate: u64, last_time: u64, duration: u64) -> Self {
        Self {
            rate,
            last_time,
            duration,
            max_quota: rate * duration / 1000,
        }
    }

    /*
        说明：
            申请额度，并更新可用额度
        参数：
            now: 当前时间
            req_quota: 申请额度
        返回：
            实际申请到的额度
    */
    pub fn acquire_quota(&mut self, now: u64, req_quota: u64) -> u64 {
        let real_quota = self.try_quota(now, req_quota);
        self.sub_quota(real_quota);
        return real_quota;
    }

    /*
        说明：
            尝试申请额度，但不更新可用额度
        参数：
            now: 当前时间
            req_quota: 申请额度
        返回：
            实际可以申请的额度
    */
    pub fn try_quota(&mut self, now: u64, req_quota: u64) -> u64 {
        if now <= self.last_time {
            return 0; // 没有可用额度
        }

        // 计算可用额度
        let mut available_quota = self.rate * (now - self.last_time) / 1000;

        // 限制最大可用额度
        if available_quota >= self.max_quota {
            available_quota = self.max_quota;
            self.last_time = now - self.duration;
        }

        // 分配额度， 不能超过最大额度
        let real_quota = if req_quota > available_quota {
            available_quota
        } else {
            req_quota
        };

        // 返回分配的额度
        return real_quota;
    }

    /*
        说明：
            减少可用额度
        参数：
            real_quota: 要减少的可用额度
        返回：
            无
    */
    pub fn sub_quota(&mut self, real_quota: u64) {
        // 更新时间，也就是减掉可用额度
        self.last_time = self.last_time + (1000 * real_quota / self.rate);
    }
}

#[cfg(test)]
mod test {

    use super::Quota;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn test_quota_silence() {
        let last = 10u64;
        let mut state = Quota::new(10000, last, 3000);

        // 尝试分配额度
        let req_quota = 2000u64;
        let req_duration = 200u64;

        // 和上次时间一样，没有额度
        assert!(state.acquire_quota(last, req_quota) == 0);

        // 比上次时间小，没有额度
        assert!(state.acquire_quota(last - last / 2, req_quota) == 0);

        // 可用额度只有申请额度的一半
        assert!(state.acquire_quota(last + req_duration / 2, req_quota) == req_quota / 2);

        // 同样的时间再申请一次，没有额度
        assert!(state.acquire_quota(last + req_duration / 2, req_quota) == 0);

        // 还是只有一半额度
        assert!(state.acquire_quota(last + req_duration, req_quota) == req_quota / 2);

        // 同样的时间再申请一次，没有额度
        assert!(state.acquire_quota(last + req_duration, req_quota) == 0);
    }

    fn get_milli() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[test]
    fn test_quota_with_log() {
        // cargo test --release test_quota_with_log -- --nocapture

        let mut state = Quota::new(10000, get_milli(), 1000);
        let req_quota = 100;

        let mut num = 0u64;
        let mut last_print_time = get_milli();
        loop {
            let realq = state.acquire_quota(get_milli(), req_quota);
            num += realq;

            if realq == 0 {
                // 没有额度时短暂休息一下，避免占用太多cpu
                thread::sleep(Duration::from_millis(1));
            }

            // 打印 qps
            let now = get_milli();
            let elapsed = now - last_print_time;
            if elapsed >= 1000 {
                let speed = num * 1000 / elapsed;
                num = 0;
                last_print_time = now;
                println!("speed: {} q/s", speed);
            }
        }

        // use super::{Traffic, TrafficSpeed};
        // let mut speed = TrafficSpeed::default();
        // let mut traffic = Traffic::default();
        // loop {
        //     let realq = state.acquire_quota(get_milli(), req_quota);
        //     traffic.inc(realq);
        //     if let Some(r) = speed.check_float(std::time::Instant::now(), &traffic) {
        //         println!("speed: [{:.2} q/s, {:.2} KB/s]", r.0, r.1,);
        //     }
        //     if realq == 0 {
        //         thread::sleep(Duration::from_millis(1));
        //     }
        // }

        // let subs = 1000; // 假如有1000个人订阅
        // let realq = state.try_quota(get_milli(), subs);
        // if realq >= subs {
        //     state.sub_quota(subs);
        //     // 发送消息给各个session进程
        // } else {
        //     // 丢弃消息
        // }
    }
}
