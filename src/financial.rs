use std::{collections::HashMap, sync::LazyLock};

use dashmap::DashMap;

use crate::{
    data::daily::*,
    error::*,
    financial::stock::{fetch_stock_daily_backward_adjusted_price, fetch_stock_daily_indicators},
    ticker::Ticker,
};

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
