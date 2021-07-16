pub mod tt;

pub mod tbytes;

pub mod log;

pub mod limit;

lazy_static::lazy_static! {
    static ref BASE:std::time::Instant = std::time::Instant::now();
    static ref SINCE_THE_EPOCH: u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards").as_millis();
}

pub struct TS;
impl TS {
    pub fn init() {
        let t1 = *BASE;
        let t2 = std::time::Instant::now();
        let d1 = *SINCE_THE_EPOCH;
        if (t2-t1).as_millis() > d1 {

        }
    }
    
    pub fn now_ms() -> i64{
        // chrono::Utc::now().timestamp_millis();
        let ms = (std::time::Instant::now() - *BASE).as_millis() + *SINCE_THE_EPOCH;
        return ms as i64;
    }
}

// pub struct TS;
// impl TS {
//     thread_local! {
//         static BASE:std::time::Instant = std::time::Instant::now();
//         static SINCE_THE_EPOCH: u128 = std::time::SystemTime::now()
//             .duration_since(std::time::UNIX_EPOCH)
//             .expect("Time went backwards").as_millis();
//     }

//     pub fn now_ms() -> i64{
//         let ms = Self::BASE.with(|base|{
//             std::time::Instant::now() - *base
//         }).as_millis();

//         let ms = Self::SINCE_THE_EPOCH.with(|v|{
//             v + ms
//         });
//         return ms as i64;
//     }
// }
