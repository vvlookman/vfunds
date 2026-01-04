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

#[derive(PartialEq)]
pub enum PriceType {
    Close,
    High,
    Low,
    Mid, // (High + Low) / 2
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
                    include_today,
                    &ConvBondDailyField::High.to_string(),
                ) && let Some((date_low, low)) = daily.get_latest_value::<f64>(
                    date,
                    include_today,
                    &ConvBondDailyField::Low.to_string(),
                ) {
                    if date_high == date_low && high > 0.0 && low > 0.0 {
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
                };

                Ok(daily
                    .get_latest_value::<f64>(date, include_today, &price_field.to_string())
                    .and_then(|(_, price)| if price > 0.0 { Some(price) } else { None }))
            }
        }
        TickerType::Stock => {
            let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
            if *price_type == PriceType::Mid {
                if let Some((date_high, high)) = kline.get_latest_value::<f64>(
                    date,
                    include_today,
                    &KlineField::High.to_string(),
                ) && let Some((date_low, low)) =
                    kline.get_latest_value::<f64>(date, include_today, &KlineField::Low.to_string())
                {
                    if date_high == date_low && high > 0.0 && low > 0.0 {
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
                };

                Ok(kline
                    .get_latest_value::<f64>(date, include_today, &price_field.to_string())
                    .and_then(|(_, price)| if price > 0.0 { Some(price) } else { None }))
            }
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
        let price = get_ticker_price(&ticker, &date, true, &PriceType::Mid)
            .await
            .unwrap()
            .unwrap_or(0.0);

        assert!(price > 0.0);
    }
}
