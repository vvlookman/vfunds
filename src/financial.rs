use std::{collections::HashMap, sync::LazyLock};

use chrono::NaiveDate;
use dashmap::DashMap;

use crate::{
    data::daily::*,
    error::*,
    financial::{
        index::fetch_cnindex_tickers,
        stock::{
            StockDetail, fetch_stock_daily_backward_adjusted_price, fetch_stock_daily_indicators,
            fetch_stock_detail,
        },
    },
    ticker::Ticker,
};

pub mod index;
pub mod stock;

#[derive(Debug)]
pub struct Portfolio {
    pub cash: f64,
    pub positions: HashMap<String, u64>,
}

#[derive(Debug, PartialEq, strum::Display, strum::EnumIter, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum Prospect {
    Bullish,
    Bearish,
    Neutral,
}

pub async fn get_cnindex_tickers(symbol: &str, date: &NaiveDate) -> VfResult<Vec<Ticker>> {
    fetch_cnindex_tickers(symbol, date).await
}

pub async fn get_stock_daily_backward_adjusted_price(ticker: &Ticker) -> VfResult<DailyDataset> {
    let key = ticker.to_string();

    if let Some(dataset) = STOCK_DAILY_BACKWARD_ADJUSTED_PRICE_CACHE.get(&key) {
        Ok(dataset.value().clone())
    } else {
        let dataset = fetch_stock_daily_backward_adjusted_price(ticker).await?;
        STOCK_DAILY_BACKWARD_ADJUSTED_PRICE_CACHE.insert(key, dataset.clone());
        Ok(dataset)
    }
}

pub async fn get_stock_daily_indicators(ticker: &Ticker) -> VfResult<DailyDataset> {
    let key = ticker.to_string();

    if let Some(dataset) = STOCK_DAILY_INDICATORS_CACHE.get(&key) {
        Ok(dataset.value().clone())
    } else {
        let dataset = fetch_stock_daily_indicators(ticker).await?;
        STOCK_DAILY_INDICATORS_CACHE.insert(key, dataset.clone());
        Ok(dataset)
    }
}

pub async fn get_stock_detail(ticker: &Ticker) -> VfResult<StockDetail> {
    fetch_stock_detail(ticker).await
}

impl Portfolio {
    pub fn new(cash: f64) -> Self {
        Self {
            cash,
            positions: HashMap::new(),
        }
    }
}

static STOCK_DAILY_BACKWARD_ADJUSTED_PRICE_CACHE: LazyLock<DashMap<String, DailyDataset>> =
    LazyLock::new(DashMap::new);

static STOCK_DAILY_INDICATORS_CACHE: LazyLock<DashMap<String, DailyDataset>> =
    LazyLock::new(DashMap::new);
