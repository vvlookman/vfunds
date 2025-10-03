use std::{str::FromStr, sync::LazyLock};

use chrono::NaiveDate;
use dashmap::DashMap;
use serde_json::json;

use crate::{ds::aktools, error::VfResult, ticker::Ticker};

pub async fn fetch_cnindex_tickers(symbol: &str, date: &NaiveDate) -> VfResult<Vec<Ticker>> {
    let query_date_str = date.format("%Y%m").to_string();
    let cache_key = format!("{symbol}/{query_date_str}");
    if let Some(result) = CNINDEX_TICKERS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = aktools::call_api(
        "/index_detail_cni",
        &json!({
            "symbol": symbol, // 通过 /index_all_cni 查询
            "date": query_date_str,
        }),
        Some(30),
        None,
    )
    .await?;

    let mut tickers: Vec<Ticker> = vec![];

    if let Some(array) = json.as_array() {
        for item in array {
            if let Some(obj) = item.as_object() {
                if let Some(ticker_str) = obj["样本代码"].as_str() {
                    if let Ok(ticker) = Ticker::from_str(ticker_str) {
                        tickers.push(ticker);
                    }
                }
            }
        }
    }

    CNINDEX_TICKERS_CACHE.insert(cache_key, tickers.clone());

    Ok(tickers)
}

pub async fn fetch_csindex_tickers(symbol: &str) -> VfResult<Vec<Ticker>> {
    let cache_key = symbol.to_string();
    if let Some(result) = CSINDEX_TICKERS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = aktools::call_api(
        "/index_stock_cons_weight_csindex",
        &json!({
            "symbol": symbol, // 通过 /index_stock_info 查询
        }),
        Some(30),
        None,
    )
    .await?;

    let mut tickers: Vec<Ticker> = vec![];

    if let Some(array) = json.as_array() {
        for item in array {
            if let Some(obj) = item.as_object() {
                if let Some(ticker_str) = obj["成分券代码"].as_str() {
                    if let Ok(ticker) = Ticker::from_str(ticker_str) {
                        tickers.push(ticker);
                    }
                }
            }
        }
    }

    CSINDEX_TICKERS_CACHE.insert(cache_key, tickers.clone());

    Ok(tickers)
}

static CNINDEX_TICKERS_CACHE: LazyLock<DashMap<String, Vec<Ticker>>> = LazyLock::new(DashMap::new);
static CSINDEX_TICKERS_CACHE: LazyLock<DashMap<String, Vec<Ticker>>> = LazyLock::new(DashMap::new);
