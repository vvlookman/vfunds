use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::NaiveDate;
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::stock::{
        StockDetail, StockDividendAdjust, StockKlineField, StockReportIncomeField,
        StockReportPershareField, fetch_stock_detail, fetch_stock_kline,
        fetch_stock_report_pershare,
    },
    rule::{BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor},
    ticker::Ticker,
    utils::{
        datetime::{FiscalQuarter, date_from_str, date_to_fiscal_quarter, date_to_str},
        financial::calc_annualized_return_rate,
    },
};

pub struct Executor {
    #[allow(dead_code)]
    options: HashMap<String, serde_json::Value>,
}

impl Executor {
    pub fn new(definition: &RuleDefinition) -> Self {
        Self {
            options: definition.options.clone(),
        }
    }
}

#[async_trait]
impl RuleExecutor for Executor {
    async fn exec(
        &mut self,
        context: &mut FundBacktestContext,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let rule_name = mod_name!();

        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let lookback_years = self
            .options
            .get("lookback_years")
            .and_then(|v| v.as_u64())
            .unwrap_or(3);
        let skip_same_sector = self
            .options
            .get("skip_same_sector")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        {
            if limit == 0 {
                panic!("limit must > 0");
            }

            if lookback_years == 0 {
                panic!("lookback_years must > 0");
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let date_str = date_to_str(date);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;
                for ticker in tickers_map.keys() {
                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                    let prices: Vec<f64> = kline
                        .get_latest_values::<f64>(
                            date,
                            &StockKlineField::Close.to_string(),
                            lookback_years as u32 * 365,
                        )
                        .iter()
                        .map(|&(_, v)| v)
                        .collect();
                    if prices.len() < lookback_years as usize * 180 {
                        let _ = event_sender
                            .send(BacktestEvent::Info(format!(
                                "[{date_str}] [{rule_name}] [No Enough Data] {ticker}"
                            )))
                            .await;
                        continue;
                    }

                    if let Some(arr) = calc_annualized_return_rate(
                        prices[0],
                        prices[prices.len() - 1],
                        prices.len() as u64,
                    ) {
                        let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
                        let report_pershare = fetch_stock_report_pershare(ticker).await?;

                        if let (Some((_, price)), epss) = (
                            kline
                                .get_latest_value::<f64>(date, &StockKlineField::Close.to_string()),
                            report_pershare.get_latest_values_with_label::<f64>(
                                date,
                                &StockReportPershareField::Eps.to_string(),
                                &StockReportIncomeField::ReportDate.to_string(),
                                5,
                            ),
                        ) {
                            let mut fiscal_epss: Vec<(FiscalQuarter, f64)> = vec![];
                            {
                                let mut current_fiscal_quarter: Option<FiscalQuarter> = None;
                                for (_, eps, report_date_label) in epss {
                                    if let Some(report_date_label_str) = report_date_label {
                                        if let Ok(report_date) =
                                            date_from_str(&report_date_label_str)
                                        {
                                            let fiscal_quarter =
                                                date_to_fiscal_quarter(&report_date);

                                            if let Some(current_fiscal_quarter) =
                                                current_fiscal_quarter
                                            {
                                                if fiscal_quarter.prev() != current_fiscal_quarter {
                                                    // Reports must be consistent
                                                    break;
                                                }
                                            }

                                            current_fiscal_quarter = Some(fiscal_quarter.clone());
                                            fiscal_epss.push((fiscal_quarter, eps));
                                        }
                                    }
                                }
                            }

                            if fiscal_epss.len() == 5 {
                                let mut eps_ttm = 0.0;
                                for i in 1..5 {
                                    let (_, prev_eps) = &fiscal_epss[i - 1];
                                    let (fiscal_quarter, eps) = &fiscal_epss[i];

                                    let quarter_eps: f64 = if fiscal_quarter.quarter == 1 {
                                        *eps
                                    } else {
                                        eps - prev_eps
                                    };

                                    eps_ttm += quarter_eps;
                                }

                                let pe_ttm = price / eps_ttm;

                                let indicator = arr / pe_ttm;
                                debug!("[{date_str}] [{rule_name}] {ticker}={indicator:.4}");

                                indicators.push((ticker.clone(), indicator));
                            }
                        }
                    }

                    calc_count += 1;

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        let calc_progress_pct =
                            calc_count as f64 / tickers_map.len() as f64 * 100.0;
                        let _ = event_sender
                            .send(BacktestEvent::Toast(format!(
                                "[{date_str}] [{rule_name}] Σ {calc_progress_pct:.2}% ..."
                            )))
                            .await;

                        last_time = Instant::now();
                    }
                }

                let _ = event_sender
                    .send(BacktestEvent::Toast(format!(
                        "[{date_str}] [{rule_name}] Σ 100%"
                    )))
                    .await;
            }

            indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

            let top_indicators = indicators
                .iter()
                .take(2 * limit as usize)
                .collect::<Vec<_>>();

            let mut tickers_detail: HashMap<Ticker, StockDetail> = HashMap::new();
            for (ticker, _) in &top_indicators {
                let detail = fetch_stock_detail(ticker).await?;
                tickers_detail.insert(ticker.clone(), detail);
            }

            let mut selected_tickers: Vec<(Ticker, f64)> = vec![];
            let mut candidate_tickers: Vec<(Ticker, f64)> = vec![];
            for (ticker, indicator) in &top_indicators {
                if selected_tickers.len() < limit as usize {
                    if skip_same_sector
                        && selected_tickers.iter().any(|(a, _)| {
                            let sector_a = tickers_detail.get(a).map(|v| v.sector.as_ref());
                            let sector_b = tickers_detail.get(ticker).map(|v| v.sector.as_ref());

                            sector_a == sector_b && sector_a.is_some()
                        })
                    {
                        candidate_tickers.push((ticker.clone(), *indicator));
                    } else {
                        selected_tickers.push((ticker.clone(), *indicator));
                    }
                } else {
                    candidate_tickers.push((ticker.clone(), *indicator));
                }
            }

            if !selected_tickers.is_empty() {
                let format_ticker = |(ticker, indicator): &(Ticker, f64)| {
                    if let Some(detail) = tickers_detail.get(ticker) {
                        let ticker_title = &detail.title;
                        let ticker_sector = detail
                            .sector
                            .as_ref()
                            .map(|v| format!("|{v}"))
                            .unwrap_or_default();
                        format!("{ticker}({ticker_title}{ticker_sector})={indicator:.4}")
                    } else {
                        format!("{ticker}={indicator:.4}")
                    }
                };

                let selected_strs = selected_tickers
                    .iter()
                    .map(format_ticker)
                    .collect::<Vec<_>>();
                let candidate_strs = candidate_tickers
                    .iter()
                    .map(format_ticker)
                    .collect::<Vec<_>>();

                let mut top_tickers_str = String::new();
                top_tickers_str.push_str(&selected_strs.join(" "));
                if !candidate_strs.is_empty() {
                    top_tickers_str.push_str(" (");
                    top_tickers_str.push_str(&candidate_strs.join(" "));
                    top_tickers_str.push(')');
                }

                let _ = event_sender
                    .send(BacktestEvent::Info(format!(
                        "[{date_str}] [{rule_name}] {top_tickers_str}"
                    )))
                    .await;
            }

            let mut targets_weight: Vec<(Ticker, f64)> = vec![];
            for (ticker, _) in &selected_tickers {
                if let Some((weight, _)) = tickers_map.get(ticker) {
                    targets_weight.push((ticker.clone(), *weight));
                }
            }

            context
                .rebalance(&targets_weight, date, event_sender)
                .await?;
        }

        Ok(())
    }
}
