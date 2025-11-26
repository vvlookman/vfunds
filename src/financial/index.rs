use std::{collections::HashMap, str::FromStr, sync::LazyLock};

use chrono::NaiveDate;
use dashmap::DashMap;
use serde_json::json;

use crate::{
    ds::aktools,
    error::VfResult,
    ticker::Ticker,
    utils::datetime::{date_from_str, date_to_str},
};

pub async fn fetch_cnindex_tickers(symbol: &str, date: &NaiveDate) -> VfResult<Vec<Ticker>> {
    let cache_key = format!("{symbol}/{}", date_to_str(date));
    if let Some(result) = CNINDEX_TICKERS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = aktools::call_api(
        "/index_detail_hist_cni",
        &json!({
            "symbol": symbol, // 通过 /index_all_cni 查询
        }),
        0,
        None,
    )
    .await?;

    let mut hist_tickers: HashMap<NaiveDate, Vec<Ticker>> = HashMap::new();

    if let Some(array) = json.as_array() {
        for item in array {
            if let Some(obj) = item.as_object() {
                if let (Some(date_str), Some(ticker_str)) =
                    (obj["日期"].as_str(), obj["样本代码"].as_str())
                {
                    if let (Ok(date), Ok(ticker)) =
                        (date_from_str(date_str), Ticker::from_str(ticker_str))
                    {
                        hist_tickers.entry(date).or_default().push(ticker);
                    }
                }
            }
        }
    }

    let tickers = if let Some(latest_date) = hist_tickers.keys().filter(|d| **d < *date).max() {
        hist_tickers.get(latest_date).unwrap_or(&vec![]).clone()
    } else {
        vec![]
    };

    CNINDEX_TICKERS_CACHE.insert(cache_key, tickers.clone());

    Ok(tickers)
}

pub async fn fetch_csindex_tickers(symbol: &str, _date: &NaiveDate) -> VfResult<Vec<Ticker>> {
    // !!!!!!! NOTICE, unsupported for historical constituents yet
    let cache_key = symbol.to_string();
    if let Some(result) = CSINDEX_TICKERS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = aktools::call_api(
        "/index_stock_cons_weight_csindex",
        &json!({
            "symbol": symbol, // 通过 /index_stock_info 查询
        }),
        30,
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
