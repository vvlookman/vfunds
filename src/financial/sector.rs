use std::{collections::HashMap, str::FromStr, sync::LazyLock};

use dashmap::DashMap;
use serde_json::json;

use crate::{ds::qmt, error::VfResult, ticker::Ticker};

pub async fn fetch_sector_tickers(sector_prefix: &str) -> VfResult<HashMap<Ticker, String>> {
    let cache_key = sector_prefix.to_string();
    if let Some(result) = TICKERS_SECTOR_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        "/stocks_sector",
        &json!({"sector_prefix": sector_prefix}),
        Some(30),
    )
    .await?;

    let mut tickers_sector: HashMap<Ticker, String> = HashMap::new();

    if let Some(obj) = json.as_object() {
        for (ticker_str, sector) in obj {
            if let Ok(ticker) = Ticker::from_str(ticker_str) {
                tickers_sector.insert(ticker, sector.to_string());
            }
        }
    }

    TICKERS_SECTOR_CACHE.insert(cache_key, tickers_sector.clone());

    Ok(tickers_sector)
}

static TICKERS_SECTOR_CACHE: LazyLock<DashMap<String, HashMap<Ticker, String>>> =
    LazyLock::new(DashMap::new);
