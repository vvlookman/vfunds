use chrono::{Duration, Local, NaiveDate};

use crate::{
    data::{daily::*, stock::*},
    error::*,
    financial::stock::*,
    ticker::Ticker,
    utils::datetime::*,
};

pub mod stock;

#[derive(Debug, PartialEq, strum::Display, strum::EnumIter, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum Prospect {
    Bullish,
    Bearish,
    Neutral,
}

pub async fn get_stock_daily_valuations(ticker: &Ticker) -> VfResult<DailyDataset> {
    fetch_stock_daily_valuations(ticker).await
}

pub async fn get_stock_events(
    ticker: &Ticker,
    date: Option<&NaiveDate>,
    backward_days: i64,
) -> VfResult<StockEvents> {
    let date_end = date.copied().unwrap_or(Local::now().date_naive());
    let date_start = date_end - Duration::days(backward_days);

    let dividends = fetch_stock_dividends(ticker, &date_start, &date_end).await?;

    Ok(StockEvents { dividends })
}

pub async fn get_stock_fiscal_metricset(
    ticker: &Ticker,
    quater: Option<FiscalQuarter>,
) -> VfResult<StockFiscalMetricset> {
    let fiscal_quater = quater.unwrap_or_else(|| prev_fiscal_quarter(None));
    let financial_summary = fetch_stock_financial_summary(ticker, &fiscal_quater).await?;

    Ok((fiscal_quater, StockMetricset { financial_summary }))
}

pub async fn get_stock_info(ticker: &Ticker) -> VfResult<StockInfo> {
    fetch_stock_info(ticker).await
}
