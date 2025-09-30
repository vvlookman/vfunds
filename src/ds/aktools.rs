use std::collections::HashMap;

use chrono::{Datelike, Duration, Local, NaiveTime, Weekday};
use fake_user_agent::get_rua;
use log::debug;
use rand::Rng;
use serde_json::Value;
use tokio::time::sleep;

use crate::{
    CACHE_ONLY, cache,
    error::{VfError, VfResult},
    utils::{
        compress,
        net::{http_get, join_url},
    },
};

pub async fn call_api(
    path: &str,
    params: &serde_json::Value,
    expire_days: Option<i64>,
    request_referer: Option<&str>,
) -> VfResult<serde_json::Value> {
    let api_url = join_url(
        std::env::var("AKTOOLS_API")
            .as_deref()
            .unwrap_or("http://127.0.0.1:8080"),
        "/api/public",
    )?;

    let cache_key = format!("aktools:{path}?{params}");

    let bytes = if let Some(data) = cache::get(&cache_key).await? {
        compress::decode(&data)?
    } else {
        if *CACHE_ONLY {
            return Err(VfError::NoData(
                "NO_CACHE_DATA",
                format!("Cache with key '{cache_key}' not exists"),
            ));
        }

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

        let request_delay_secs: f64 = std::env::var("AKTOOLS_DELAY")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(10.0);
        if request_delay_secs > 0.0 {
            let secs = (request_delay_secs * rand::rng().random_range(0.67..=1.33)) as u64;
            sleep(tokio::time::Duration::from_secs(secs)).await;
        }

        let mut headers: HashMap<String, String> = HashMap::new();
        headers.insert(
            reqwest::header::USER_AGENT.to_string(),
            get_rua().to_string(),
        );

        if let Some(referer) = request_referer {
            headers.insert(reqwest::header::REFERER.to_string(), referer.to_string());
        }

        let bytes = http_get(&api_url, Some(path), &query, &headers, 30, 3).await?;
        debug!(
            "[HTTP OK] {api_url}/{path}?{}",
            query
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&")
        );

        let now = Local::now();
        let expire = if let Some(expire_days) = expire_days {
            (now + Duration::days(expire_days)).naive_local()
        } else {
            if let Some(market_close_time) = NaiveTime::from_hms_opt(15, 0, 0) {
                let today = now.naive_local().date();
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
