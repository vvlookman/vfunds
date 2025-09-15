use std::collections::HashMap;

use chrono::{Datelike, Duration, Local, NaiveTime, Weekday};
use log::debug;
use serde_json::Value;
use tokio::time::sleep;

use crate::{
    cache,
    error::VfResult,
    utils::{compress, net::http_get},
};

pub async fn call_api(
    path: &str,
    params: &serde_json::Value,
    stable: bool,
) -> VfResult<serde_json::Value> {
    let api_url = std::env::var("QMT_API")
        .as_deref()
        .unwrap_or("http://192.168.0.222:9000")
        .to_string();

    let cache_key = format!("qmt:{path}?{params}");

    let bytes = if let Some(data) = cache::get(&cache_key).await? {
        compress::decode(&data)?
    } else {
        let mut query = HashMap::new();
        if let Some(params) = params.as_object() {
            for (k, v) in params.iter() {
                let s = match v {
                    Value::Bool(b) => {
                        if *b {
                            "true".to_string()
                        } else {
                            "false".to_string()
                        }
                    }
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => s.to_string(),
                    _ => "".to_string(),
                };
                query.insert(k.to_string(), s);
            }
        }

        let request_delay_secs: f64 = std::env::var("QMT_DELAY")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1.0);
        if request_delay_secs > 0.0 {
            sleep(tokio::time::Duration::from_secs(request_delay_secs as u64)).await;
        }

        let headers: HashMap<String, String> = HashMap::new();
        let bytes = http_get(&api_url, Some(path), &query, &headers, 30, 3).await?;
        debug!(
            "[HTTP OK] {api_url}{path}?{}",
            query
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&")
        );

        let now = Local::now();
        let expire = if stable {
            (now + Duration::days(30)).naive_local()
        } else {
            if let Some(market_close_time) = NaiveTime::from_hms_opt(15, 0, 0) {
                let today = now.date_naive();
                let today_close = today.and_time(market_close_time);

                if now.time() < today_close.time() {
                    today_close
                } else {
                    let mut next_day = today + Duration::days(1);
                    while matches!(next_day.weekday(), Weekday::Sat | Weekday::Sun) {
                        next_day += Duration::days(1);
                    }

                    next_day.and_time(market_close_time)
                }
            } else {
                (now + Duration::days(1)).naive_local()
            }
        };

        let data = compress::encode(&bytes)?;
        let _ = cache::upsert(&cache_key, &data, &expire).await;

        bytes
    };

    let json: serde_json::Value = serde_json::from_slice(&bytes)?;

    Ok(json)
}
