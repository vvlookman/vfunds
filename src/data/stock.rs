use chrono::NaiveDate;
use serde::Serialize;

use crate::{data::daily::DailyDataset, utils::datetime::FiscalQuarter};

pub type StockFiscalMetricset = (FiscalQuarter, StockMetricset);

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
pub struct StockFinancialSummary {
    pub asset_turnover: Option<f64>,
    pub book_value_per_share: Option<f64>,
    pub cash_ratio: Option<f64>,
    pub cost_of_profit: Option<f64>,
    pub cost_of_revenue: Option<f64>,
    pub cost_of_sales: Option<f64>,
    pub current_ratio: Option<f64>,
    pub days_asset_outstanding: Option<f64>,
    pub days_inventory_outstanding: Option<f64>,
    pub days_sales_outstanding: Option<f64>,
    pub debt_to_assets: Option<f64>,
    pub debt_to_equity: Option<f64>,
    pub earnings_per_share: Option<f64>,
    pub free_cash_flow_per_share: Option<f64>,
    pub goodwill: Option<f64>,
    pub gross_margin: Option<f64>,
    pub inventory_turnover: Option<f64>,
    pub net_assets: Option<f64>,
    pub net_margin: Option<f64>,
    pub net_profit: Option<f64>,
    pub operating_cash_flow: Option<f64>,
    pub operating_costs: Option<f64>,
    pub operating_margin: Option<f64>,
    pub operating_revenue: Option<f64>,
    pub quick_ratio: Option<f64>,
    pub receivables_turnover: Option<f64>,
    pub return_on_assets: Option<f64>,
    pub return_on_equity: Option<f64>,
    pub return_on_invested_capital: Option<f64>,
    pub revenue_growth: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StockInfo {
    pub name: Option<String>,
    pub industry: Option<String>,
}

#[derive(Clone, Debug)]
pub struct StockMetricset {
    pub financial_summary: StockFinancialSummary,
}
