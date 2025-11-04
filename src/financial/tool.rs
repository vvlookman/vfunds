use std::{collections::HashSet, str::FromStr};

use chrono::NaiveDate;
use serde_json::json;

use crate::{
    ds::aktools,
    error::VfResult,
    financial::{
        KlineField,
        stock::{
            StockDividendAdjust, StockReportCapitalField, StockReportIncomeField,
            StockReportPershareField, fetch_stock_kline, fetch_stock_report_capital,
            fetch_stock_report_income, fetch_stock_report_pershare,
        },
        tool::datetime::{FiscalQuarter, date_from_str, date_to_fiscal_quarter},
    },
    ticker::Ticker,
    utils::datetime,
};

pub async fn calc_stock_pb(ticker: &Ticker, date: &NaiveDate) -> VfResult<Option<f64>> {
    let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
    let report_pershare = fetch_stock_report_pershare(ticker).await?;

    if let (Some((_, price)), Some((_, bps))) = (
        kline.get_latest_value::<f64>(date, false, &KlineField::Close.to_string()),
        report_pershare.get_latest_value::<f64>(
            date,
            false,
            &StockReportPershareField::Bps.to_string(),
        ),
    ) {
        let pb = price / bps;
        return Ok(Some(pb));
    }

    Ok(None)
}

pub async fn calc_stock_pe_ttm(ticker: &Ticker, date: &NaiveDate) -> VfResult<Option<f64>> {
    let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
    let report_pershare = fetch_stock_report_pershare(ticker).await?;

    if let (Some((_, price)), eps_values) = (
        kline.get_latest_value::<f64>(date, false, &KlineField::Close.to_string()),
        report_pershare.get_latest_values_with_label::<f64>(
            date,
            false,
            &StockReportPershareField::Eps.to_string(),
            &StockReportIncomeField::ReportDate.to_string(),
            5,
        ),
    ) {
        let mut fiscal_eps_values: Vec<(FiscalQuarter, f64)> = vec![];
        {
            let mut current_fiscal_quarter: Option<FiscalQuarter> = None;
            for (_, eps, report_date_label) in eps_values {
                if let Some(report_date_label_str) = report_date_label {
                    if let Ok(report_date) = date_from_str(&report_date_label_str) {
                        let fiscal_quarter = date_to_fiscal_quarter(&report_date);

                        if let Some(current_fiscal_quarter) = current_fiscal_quarter {
                            if fiscal_quarter.prev() != current_fiscal_quarter {
                                // Reports must be consistent
                                break;
                            }
                        }

                        current_fiscal_quarter = Some(fiscal_quarter.clone());
                        fiscal_eps_values.push((fiscal_quarter, eps));
                    }
                }
            }
        }

        if fiscal_eps_values.len() == 5 {
            let mut eps_ttm = 0.0;
            for i in 1..5 {
                let (_, prev_eps) = &fiscal_eps_values[i - 1];
                let (fiscal_quarter, eps) = &fiscal_eps_values[i];

                let quarter_eps: f64 = if fiscal_quarter.quarter == 1 {
                    *eps
                } else {
                    eps - prev_eps
                };

                eps_ttm += quarter_eps;
            }

            let pe_ttm = price / eps_ttm;
            return Ok(Some(pe_ttm));
        }
    }

    Ok(None)
}

pub async fn calc_stock_ps_ttm(ticker: &Ticker, date: &NaiveDate) -> VfResult<Option<f64>> {
    let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
    let report_capital = fetch_stock_report_capital(ticker).await?;
    let report_income = fetch_stock_report_income(ticker).await?;

    if let (Some((_, price)), Some((_, total_captical)), revenues) = (
        kline.get_latest_value::<f64>(date, false, &KlineField::Close.to_string()),
        report_capital.get_latest_value::<f64>(
            date,
            false,
            &StockReportCapitalField::Total.to_string(),
        ),
        report_income.get_latest_values_with_label::<f64>(
            date,
            false,
            &StockReportIncomeField::Revenue.to_string(),
            &StockReportIncomeField::ReportDate.to_string(),
            5,
        ),
    ) {
        let mut fiscal_revenues: Vec<(FiscalQuarter, f64)> = vec![];
        {
            let mut current_fiscal_quarter: Option<FiscalQuarter> = None;
            for (_, revenue, report_date_label) in revenues {
                if let Some(report_date_label_str) = report_date_label {
                    if let Ok(report_date) = date_from_str(&report_date_label_str) {
                        let fiscal_quarter = date_to_fiscal_quarter(&report_date);

                        if let Some(current_fiscal_quarter) = current_fiscal_quarter {
                            if fiscal_quarter.prev() != current_fiscal_quarter {
                                // Reports must be consistent
                                break;
                            }
                        }

                        current_fiscal_quarter = Some(fiscal_quarter.clone());
                        fiscal_revenues.push((fiscal_quarter, revenue));
                    }
                }
            }
        }

        if fiscal_revenues.len() == 5 {
            let mut revenue_ttm = 0.0;
            for i in 1..5 {
                let (_, prev_revenue) = &fiscal_revenues[i - 1];
                let (fiscal_quarter, revenue) = &fiscal_revenues[i];

                let quarter_revenue: f64 = if fiscal_quarter.quarter == 1 {
                    *revenue
                } else {
                    revenue - prev_revenue
                };

                revenue_ttm += quarter_revenue;
            }

            let ps_ttm = price * total_captical / revenue_ttm;
            return Ok(Some(ps_ttm));
        }
    }

    Ok(None)
}

pub async fn fetch_trade_dates() -> VfResult<HashSet<NaiveDate>> {
    let mut dates: HashSet<NaiveDate> = HashSet::new();

    if let Ok(json) = aktools::call_api("/tool_trade_date_hist_sina", &json!({}), 0, None).await {
        if let Some(array) = json.as_array() {
            for item in array {
                if let Some(obj) = item.as_object() {
                    if let Some(trade_date_str) = obj["trade_date"].as_str() {
                        if let Ok(date) = datetime::date_from_str(trade_date_str) {
                            dates.insert(date);
                        }
                    }
                }
            }
        }
    } else {
        let bench_ticker = Ticker::from_str("000001.SH")?;
        let bench_kline = fetch_stock_kline(&bench_ticker, StockDividendAdjust::No).await?;
        let bench_dates = bench_kline.get_dates();
        dates.extend(bench_dates);
    }

    Ok(dates)
}
