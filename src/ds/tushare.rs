use serde_json::json;
use tokio::time::sleep;

use crate::{
    CACHE_NO_EXPIRE, CONFIG, cache,
    error::{VfError, VfResult},
    market::next_data_expire_in_china,
    utils::{compress, net::http_post},
};

pub async fn call_api(
    api_name: &str,
    params: &serde_json::Value,
    fields: Option<&str>,
    expire_days: i64,
) -> VfResult<serde_json::Value> {
    let cache_key = format!("tushare:{api_name}?{params}");

    if let Some(data) = cache::get(&cache_key, *CACHE_NO_EXPIRE).await? {
        let bytes: Vec<u8> = compress::decode(&data)?;
        let json: serde_json::Value = serde_json::from_slice(&bytes)?;

        Ok(json)
    } else {
        let request_delay_secs: f64 = std::env::var("TUSHARE_DELAY")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1.0);
        if request_delay_secs > 0.0 {
            sleep(tokio::time::Duration::from_secs(request_delay_secs as u64)).await;
        }

        let tushare_api = { &CONFIG.read().await.tushare_api };
        let tushare_token = { &CONFIG.read().await.tushare_token };

        let bytes = http_post(
            tushare_api,
            None,
            &json!({
                "api_name": api_name,
                "token": tushare_token,
                "params": params,
                "fields": fields.unwrap_or_default(),
            }),
            None,
            30,
            3,
        )
        .await?;
        let json: serde_json::Value = serde_json::from_slice(&bytes)?;

        if json["code"].as_i64() == Some(0) {
            if let Ok(data) = compress::encode(&bytes) {
                let expire = next_data_expire_in_china(expire_days);
                let _ = cache::upsert(&cache_key, &data, &expire).await;
            }

            Ok(json)
        } else {
            Err(VfError::Invalid {
                code: "INVALID_RESPONSE",
                message: format!("[{}]{}", json["code"], json["msg"]),
            })
        }
    }
}

pub async fn check_api() -> VfResult<()> {
    let tushare_api = { &CONFIG.read().await.tushare_api };
    let tushare_token = { &CONFIG.read().await.tushare_token };

    let bytes = http_post(
        tushare_api,
        None,
        &json!({
            "api_name": "index_daily",
            "token": tushare_token,
            "params": {
                "ts_code":"000001.SH",
            },
        }),
        None,
        30,
        3,
    )
    .await?;
    let json: serde_json::Value = serde_json::from_slice(&bytes)?;

    if json["code"].as_i64() == Some(0) {
        if let Some(items) = json["data"]["items"].as_array() {
            if !items.is_empty() {
                return Ok(());
            }
        }
    }

    Err(VfError::Invalid {
        code: "INVALID_RESPONSE",
        message: "Invalid response".to_string(),
    })
}
