use chrono::{Datelike, Duration, Local, NaiveDateTime, NaiveTime, Weekday};

pub fn next_data_expire_in_china(expire_days: i64) -> NaiveDateTime {
    let now = Local::now();

    if let Some(market_close_time) = NaiveTime::from_hms_opt(15, 0, 0) {
        let today = now.date_naive();

        let mut expire_date = if expire_days == 0 && now.time() > market_close_time {
            today + Duration::days(1)
        } else {
            today + Duration::days(expire_days)
        };

        while matches!(expire_date.weekday(), Weekday::Sat | Weekday::Sun) {
            expire_date += Duration::days(1);
        }

        expire_date.and_time(market_close_time)
    } else {
        (now + Duration::days(expire_days)).naive_local()
    }
}
