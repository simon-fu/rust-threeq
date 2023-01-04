use chrono::{DateTime, Utc, TimeZone};


fn base() -> &'static DateTime<Utc> {
    lazy_static::lazy_static!(
        // 1970/01/01 08:00:00 ==> 0
        // static ref INSTANCE: LocalTime = LocalTime {
        //     base: Local.ymd(1970, 01, 01).and_hms(8, 0, 0),
        // };

        static ref INSTANCE: DateTime<Utc> = Utc.ymd(1970, 01, 01).and_hms(0, 0, 0);

    );
    &*INSTANCE
}

#[inline]
pub fn now_ms() -> i64 {
    let now = Utc::now();
    let base = base();
    if now < *base {
        0
    } else {
        (now - *base).num_milliseconds()
    }
}
