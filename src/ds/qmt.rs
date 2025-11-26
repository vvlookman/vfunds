use std::collections::HashMap;

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
) -> VfResult<serde_json::Value> {
    let qmt_api = { &CONFIG.read().await.qmt_api };
    let api_url = qmt_api.to_string();

    let cache_key = format!("qmt:{path}?{params}");

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

            let request_delay_secs: f64 = std::env::var("QMT_DELAY")
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(2.0);
            if request_delay_secs > 0.0 {
                sleep(tokio::time::Duration::from_secs(request_delay_secs as u64)).await;
            }

            let headers: HashMap<String, String> = HashMap::new();

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
    let qmt_api = { &CONFIG.read().await.qmt_api };
    let api_url = join_url(qmt_api, "/stock_kline/000001.SH")?;

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
