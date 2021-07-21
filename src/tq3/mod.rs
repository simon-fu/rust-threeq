pub mod tt;

pub mod tbytes;

pub mod log;

pub mod limit;

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
}

pub async fn measure_async<T>(name: &str, num: u64, fut: T)
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

    println!(
        "{}: num {}, elapsed {:?}, each {} ns, estimate {}/sec",
        name, num, elapsed, each, rate
    );
}
