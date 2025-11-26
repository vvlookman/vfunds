use std::collections::HashMap;

use fake_user_agent::get_rua;
use rand::Rng;
use serde_json::Value;
use tokio::time::sleep;

use crate::{
    CACHE_NO_EXPIRE, CONFIG, cache,
    error::{VfError, VfResult},
    market::next_data_expire_in_china,
    utils::{
        compress,
        net::{http_get, join_url},
    },
};

pub async fn call_api(
    path: &str,
    params: &serde_json::Value,
    expire_days: i64,
    request_referer: Option<&str>,
) -> VfResult<serde_json::Value> {
    let aktools_api = { &CONFIG.read().await.aktools_api };
    let api_url = join_url(aktools_api, "/api/public")?;

    let cache_key = format!("aktools:{path}?{params}");

    let bytes: VfResult<Vec<u8>> =
        if let Some(data) = cache::get(&cache_key, *CACHE_NO_EXPIRE).await? {
            Ok(compress::decode(&data)?)
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

            {
                let data = compress::encode(&bytes)?;
                let expire = next_data_expire_in_china(expire_days);
                let _ = cache::upsert(&cache_key, &data, &expire).await;
            }

            Ok(bytes)
        };

    let json: serde_json::Value = serde_json::from_slice(&bytes?)?;

    Ok(json)
}

pub async fn check_api() -> VfResult<()> {
    let aktools_api = { &CONFIG.read().await.aktools_api };
    let api_url = join_url(aktools_api, "/api/public/stock_zh_a_hist")?;

    let bytes = http_get(&api_url, None, &HashMap::new(), &HashMap::new(), 30, 3).await?;
    let json: serde_json::Value = serde_json::from_slice(&bytes)?;

    if let Some(array) = json.as_array() {
        if !array.is_empty() {
            return Ok(());
        }
    }

    Err(VfError::Invalid {
        code: "INVALID_RESPONSE",
        message: "Invalid response".to_string(),
    })
}
