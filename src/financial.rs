use std::collections::HashMap;

use chrono::NaiveDate;

use crate::{
    STALE_DAYS_LONG,
    data::series::DailySeries,
    error::VfResult,
    financial::{
        bond::{
            ConvBondDailyField, fetch_conv_bond_basic, fetch_conv_bond_daily,
            fetch_conv_bond_kline, fetch_conv_bond_kline_ignore_cache,
        },
        stock::{
            StockDividendAdjust, fetch_stock_basic, fetch_stock_detail, fetch_stock_kline,
            fetch_stock_kline_ignore_cache, fetch_stock_kline_ignore_cache_with_ds,
            fetch_stock_kline_with_ds,
        },
    },
    ticker::{Ticker, TickerType},
};

pub mod bond;
pub mod helper;
pub mod index;
pub mod market;
pub mod sector;
pub mod stock;

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
    pub reserved_cash: HashMap<Ticker, (f64, NaiveDate)>,
    pub positions: HashMap<Ticker, u64>,
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

#[derive(PartialEq, strum::Display)]
pub enum PriceType {
    Close,
    High,
    Low,
    Mid, // (High + Low) / 2
    Open,
}

pub async fn get_ticker_kline(ticker: &Ticker, ignore_cache: bool) -> VfResult<DailySeries> {
    if ignore_cache {
        match ticker.r#type {
            TickerType::ConvBond => fetch_conv_bond_kline_ignore_cache(ticker).await,
            TickerType::Stock => {
                fetch_stock_kline_ignore_cache(ticker, StockDividendAdjust::Backward).await
            }
        }
    } else {
        match ticker.r#type {
            TickerType::ConvBond => fetch_conv_bond_kline(ticker).await,
            TickerType::Stock => fetch_stock_kline(ticker, StockDividendAdjust::Backward).await,
        }
    }
}

pub async fn get_ticker_kline_with_ds(
    ticker: &Ticker,
    ignore_cache: bool,
    ds_name: &str,
) -> VfResult<DailySeries> {
    if ignore_cache {
        match ticker.r#type {
            TickerType::ConvBond => fetch_conv_bond_kline_ignore_cache(ticker).await,
            TickerType::Stock => {
                fetch_stock_kline_ignore_cache_with_ds(
                    ticker,
                    StockDividendAdjust::Backward,
                    ds_name,
                )
                .await
            }
        }
    } else {
        match ticker.r#type {
            TickerType::ConvBond => fetch_conv_bond_kline(ticker).await,
            TickerType::Stock => {
                fetch_stock_kline_with_ds(ticker, StockDividendAdjust::Backward, ds_name).await
            }
        }
    }
}

pub async fn get_ticker_price(
    ticker: &Ticker,
    date: &NaiveDate,
    include_today: bool,
    price_type: &PriceType,
) -> VfResult<Option<f64>> {
    match ticker.r#type {
        TickerType::ConvBond => {
            let daily = fetch_conv_bond_daily(ticker).await?;
            if *price_type == PriceType::Mid {
                if let Some((date_high, high)) = daily.get_latest_value::<f64>(
                    date,
                    STALE_DAYS_LONG,
                    include_today,
                    &ConvBondDailyField::High.to_string(),
                ) && let Some((date_low, low)) = daily.get_latest_value::<f64>(
                    date,
                    STALE_DAYS_LONG,
                    include_today,
                    &ConvBondDailyField::Low.to_string(),
                ) {
                    if date_high == date_low {
                        Ok(Some((high + low) / 2.0))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            } else {
                let price_field = match price_type {
                    PriceType::Close => ConvBondDailyField::Close,
                    PriceType::High => ConvBondDailyField::High,
                    PriceType::Low => ConvBondDailyField::Low,
                    PriceType::Mid => unreachable!(),
                    PriceType::Open => ConvBondDailyField::Open,
                };

                Ok(daily
                    .get_latest_value::<f64>(
                        date,
                        STALE_DAYS_LONG,
                        include_today,
                        &price_field.to_string(),
                    )
                    .map(|(_, price)| price))
            }
        }
        TickerType::Stock => {
            let kline = fetch_stock_kline(ticker, StockDividendAdjust::Backward).await?;
            if *price_type == PriceType::Mid {
                if let Some((date_high, high)) = kline.get_latest_value::<f64>(
                    date,
                    STALE_DAYS_LONG,
                    include_today,
                    &KlineField::High.to_string(),
                ) && let Some((date_low, low)) = kline.get_latest_value::<f64>(
                    date,
                    STALE_DAYS_LONG,
                    include_today,
                    &KlineField::Low.to_string(),
                ) {
                    if date_high == date_low {
                        Ok(Some((high + low) / 2.0))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            } else {
                let price_field = match price_type {
                    PriceType::Close => KlineField::Close,
                    PriceType::High => KlineField::High,
                    PriceType::Low => KlineField::Low,
                    PriceType::Mid => unreachable!(),
                    PriceType::Open => KlineField::Open,
                };

                Ok(kline
                    .get_latest_value::<f64>(
                        date,
                        STALE_DAYS_LONG,
                        include_today,
                        &price_field.to_string(),
                    )
                    .map(|(_, price)| price))
            }
        }
    }
}

pub async fn get_ticker_title(ticker: &Ticker) -> String {
    if let Ok(name) = match ticker.r#type {
        TickerType::ConvBond => fetch_conv_bond_basic(ticker).await.map(|d| d.name),
        TickerType::Stock => fetch_stock_basic(ticker).await.map(|d| d.name),
    } {
        if !name.is_empty() {
            return format!("{ticker}({name})");
        }
    }

    if let Ok(name) = match ticker.r#type {
        TickerType::Stock => fetch_stock_detail(ticker).await.map(|d| d.name),
        _ => Ok("".to_string()),
    } {
        if !name.is_empty() {
            return format!("{ticker}({name})");
        }
    }

    ticker.to_string()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::utils::datetime::date_from_str;

    #[tokio::test]
    async fn test_get_ticker_price() {
        let ticker = Ticker::from_str("123029").unwrap();
        assert_eq!(ticker.r#type, TickerType::ConvBond);

        let price = get_ticker_price(
            &ticker,
            &date_from_str("2021-06-28").unwrap(),
            true,
            &PriceType::Mid,
        )
        .await
        .unwrap()
        .unwrap_or(0.0);

        assert!(price > 0.0);
    }

    #[tokio::test]
    async fn test_get_ticker_title() {
        let ticker = Ticker::from_str("600397").unwrap();
        let title = get_ticker_title(&ticker).await;

        assert_eq!(title, "600397.XSHG(江钨装备)");
    }
}
