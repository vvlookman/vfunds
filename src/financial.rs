use std::sync::LazyLock;

use dashmap::DashMap;

use crate::{
    data::daily::*, error::*, financial::stock::fetch_stock_daily_backward_adjust, ticker::Ticker,
};

pub mod stock;

#[derive(Debug, PartialEq, strum::Display, strum::EnumIter, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum Prospect {
    Bullish,
    Bearish,
    Neutral,
}

pub async fn get_stock_daily_backward_adjust(ticker: &Ticker) -> VfResult<DailyDataset> {
    let key = ticker.to_string();

    if let Some(dataset) = STOCK_DAILY_VALUATIONS_CACHE.get(&key) {
        Ok(dataset.value().clone())
    } else {
        let dataset = fetch_stock_daily_backward_adjust(ticker).await?;
        STOCK_DAILY_VALUATIONS_CACHE.insert(key, dataset.clone());
        Ok(dataset)
    }
}

static STOCK_DAILY_VALUATIONS_CACHE: LazyLock<DashMap<String, DailyDataset>> =
    LazyLock::new(DashMap::new);
