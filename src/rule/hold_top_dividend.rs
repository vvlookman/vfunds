use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate};
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::stock::{
        StockDetail, StockDividendAdjust, StockDividendField, StockKlineField,
        StockReportPershareField, fetch_stock_detail, fetch_stock_dividends, fetch_stock_kline,
        fetch_stock_report_pershare,
    },
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
    ticker::Ticker,
    utils::datetime::date_to_str,
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
        context: &mut BacktestContext,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let rule_name = mod_name!();

        let filter_roe_floor = self
            .options
            .get("filter_roe_floor")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
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
            if filter_roe_floor < 0.0 {
                panic!("filter_roe_floor must >= 0");
            }

            if limit == 0 {
                panic!("limit must > 0");
            }

            if lookback_years == 0 {
                panic!("lookback_years must > 0");
            }
        }

        let tickers = context.fund_definition.all_tickers(date).await?;
        if !tickers.is_empty() {
            let date_str = date_to_str(date);
            let date_from =
                date.with_year(date.year() - lookback_years as i32).unwrap() + Duration::days(1);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;
                for ticker in tickers.keys() {
                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
                    if let Some(price) =
                        kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
                    {
                        if price > 0.0 {
                            let filter_roe = if filter_roe_floor > 0.0 {
                                let report_pershare = fetch_stock_report_pershare(ticker).await?;
                                let roe = report_pershare
                                    .get_latest_value::<f64>(
                                        date,
                                        &StockReportPershareField::RoeRate.to_string(),
                                    )
                                    .unwrap_or(0.0)
                                    / 100.0;

                                roe > filter_roe_floor
                            } else {
                                true
                            };

                            if filter_roe {
                                let dividends = fetch_stock_dividends(ticker).await?;

                                let interests = dividends.get_values::<f64>(
                                    &date_from,
                                    date,
                                    &StockDividendField::Interest.to_string(),
                                );
                                let interest_sum = interests.iter().map(|x| x.1).sum::<f64>();

                                if interest_sum > 0.0 && interests.len() as u64 >= lookback_years {
                                    let indicator = interest_sum / price;
                                    debug!("[{date_str}] [{rule_name}] {ticker}={indicator:.4}");

                                    indicators.push((ticker.clone(), indicator));
                                }
                            }
                        }
                    }

                    calc_count += 1;

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        let calc_progress_pct = calc_count as f64 / tickers.len() as f64 * 100.0;
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

            let total_value = context.calc_total_value(date).await?;
            let ticker_value = total_value / selected_tickers.len() as f64;
            let target: Vec<(Ticker, f64)> = selected_tickers
                .into_iter()
                .map(|(ticker, _)| (ticker, ticker_value))
                .collect();

            context.rebalance(&target, date, event_sender).await?;
        }

        Ok(())
    }
}
