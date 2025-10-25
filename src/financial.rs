use std::collections::HashMap;

use chrono::NaiveDate;

use crate::{
    error::VfResult,
    financial::{
        bond::{ConvBondAnalysisField, fetch_conv_bond_analysis, fetch_conv_bond_detail},
        stock::{StockDividendAdjust, StockKlineField, fetch_stock_detail, fetch_stock_kline},
    },
    ticker::{Ticker, TickerType},
};

pub mod bond;
pub mod index;
pub mod sector;
pub mod stock;
pub mod tool;

#[derive(Debug, Clone)]
pub struct Portfolio {
    pub free_cash: f64,
    pub reserved_cash: HashMap<Ticker, f64>,
    pub positions: HashMap<Ticker, u64>,
}

#[derive(Debug, PartialEq, strum::Display, strum::EnumIter, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum Prospect {
    Bullish,
    Bearish,
    Neutral,
}

pub async fn get_ticker_price(ticker: &Ticker, date: &NaiveDate) -> VfResult<Option<f64>> {
    match ticker.r#type {
        TickerType::ConvBond => {
            let analysis = fetch_conv_bond_analysis(ticker).await?;
            Ok(analysis
                .get_latest_value::<f64>(date, &ConvBondAnalysisField::Price.to_string())
                .map(|(_, price)| price))
        }
        TickerType::Stock => {
            let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
            Ok(kline
                .get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
                .map(|(_, price)| price))
        }
    }
}
pub async fn get_ticker_title(ticker: &Ticker) -> VfResult<String> {
    match ticker.r#type {
        TickerType::ConvBond => Ok(fetch_conv_bond_detail(ticker).await?.title),
        TickerType::Stock => Ok(fetch_stock_detail(ticker).await?.title),
    }
}

impl Portfolio {
    pub fn new(cash: f64) -> Self {
        Self {
            free_cash: cash,
            reserved_cash: HashMap::new(),
            positions: HashMap::new(),
        }
    }
}
