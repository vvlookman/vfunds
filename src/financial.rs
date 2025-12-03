use std::collections::HashMap;

use chrono::NaiveDate;

use crate::{
    error::VfResult,
    financial::{
        bond::{ConvBondDailyField, fetch_conv_bond_daily, fetch_conv_bond_detail},
        stock::{StockDividendAdjust, fetch_stock_detail, fetch_stock_kline},
    },
    ticker::{Ticker, TickerType},
};

pub mod bond;
pub mod index;
pub mod sector;
pub mod stock;
pub mod tool;

#[derive(strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum KlineField {
    Open,
    Close,
    High,
    Low,
    Volume,
}

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

pub async fn get_ticker_price(
    ticker: &Ticker,
    date: &NaiveDate,
    include_today: bool,
    price_bias: i32, // >0 is high price, <0 is low price, =0 is close price
) -> VfResult<Option<f64>> {
    match ticker.r#type {
        TickerType::ConvBond => {
            let price_field = if price_bias > 0 {
                ConvBondDailyField::High
            } else if price_bias < 0 {
                ConvBondDailyField::Low
            } else {
                ConvBondDailyField::Close
            };

            let daily = fetch_conv_bond_daily(ticker).await?;
            Ok(daily
                .get_latest_value::<f64>(date, include_today, &price_field.to_string())
                .and_then(|(_, price)| if price > 0.0 { Some(price) } else { None }))
        }
        TickerType::Stock => {
            let price_field = if price_bias > 0 {
                KlineField::High
            } else if price_bias < 0 {
                KlineField::Low
            } else {
                KlineField::Close
            };

            let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
            Ok(kline
                .get_latest_value::<f64>(date, include_today, &price_field.to_string())
                .and_then(|(_, price)| if price > 0.0 { Some(price) } else { None }))
        }
    }
}

pub async fn get_ticker_title(ticker: &Ticker) -> String {
    if let Ok(name) = match ticker.r#type {
        TickerType::ConvBond => fetch_conv_bond_detail(ticker).await.map(|d| d.name),
        TickerType::Stock => fetch_stock_detail(ticker).await.map(|d| d.name),
    } {
        format!("{ticker}({name})")
    } else {
        ticker.to_string()
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::utils::datetime;

    #[tokio::test]
    async fn test_get_ticker_price() {
        let ticker = Ticker::from_str("123029").unwrap();
        assert_eq!(ticker.r#type, TickerType::ConvBond);

        let date = datetime::date_from_str("2021-09-16").unwrap();
        let price = get_ticker_price(&ticker, &date, true, -1)
            .await
            .unwrap()
            .unwrap_or(0.0);

        assert!(price > 0.0);
    }
}
