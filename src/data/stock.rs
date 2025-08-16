use chrono::NaiveDate;
use serde::Serialize;

use crate::data::daily::DailyDataset;

#[derive(Clone, Debug, Serialize)]
pub struct StockDailyData {
    pub daily_valuations: DailyDataset,
}

#[derive(Clone, Debug, Serialize)]
pub struct StockDividend {
    pub date_announce: NaiveDate,
    pub date_record: NaiveDate,
    pub dividend_per_share: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StockEvents {
    pub dividends: Vec<StockDividend>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StockInfo {
    pub name: Option<String>,
    pub industry: Option<String>,
}
