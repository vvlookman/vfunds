use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate};
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    error::VfResult,
    financial::stock::{
        StockDividendAdjust, StockDividendField, StockKlineField, StockReportRershareField,
        fetch_stock_detail, fetch_stock_dividends, fetch_stock_kline, fetch_stock_report_pershare,
    },
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
    ticker::Ticker,
    utils,
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
            let date_str = utils::datetime::date_to_str(date);
            let date_from =
                date.with_year(date.year() - lookback_years as i32).unwrap() + Duration::days(1);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            let mut last_time = Instant::now();
            let mut calc_count: usize = 0;
            for ticker in &tickers {
                debug!("[{date_str}] {ticker}");

                let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
                if let Some(price) =
                    kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
                {
                    if price > 0.0 {
                        let report_pershare = fetch_stock_report_pershare(ticker).await?;

                        let roe = report_pershare
                            .get_latest_value::<f64>(
                                date,
                                &StockReportRershareField::RoeRate.to_string(),
                            )
                            .unwrap_or(0.0)
                            / 100.0;

                        if roe > filter_roe_floor {
                            let dividends = fetch_stock_dividends(ticker).await?;

                            let interests = dividends.get_values::<f64>(
                                &date_from,
                                date,
                                &StockDividendField::Interest.to_string(),
                            );
                            let interest_sum = interests.iter().map(|x| x.1).sum::<f64>();

                            if interest_sum > 0.0 && interests.len() as u64 >= lookback_years {
                                let indicator = interest_sum / price;
                                debug!("[{date_str}] {ticker} = {indicator:.4} (ROE={roe:.4})");

                                indicators.push((ticker.clone(), indicator));
                            }
                        }
                    }
                }

                calc_count += 1;

                if last_time.elapsed().as_secs() > 5 {
                    let calc_progress_pct = calc_count as f64 / tickers.len() as f64 * 100.0;
                    let _ = event_sender
                        .send(BacktestEvent::Info(format!(
                            "[{date_str}] Σ {calc_progress_pct:.2}% ..."
                        )))
                        .await;

                    last_time = Instant::now();
                }
            }
            indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

            if !indicators.is_empty() {
                let mut top_tickers_str = String::from("");
                for (ticker, indicator) in indicators.iter().take(2 * limit as usize) {
                    let ticker_title = fetch_stock_detail(ticker).await?.title;
                    top_tickers_str.push_str(&format!("{ticker}({ticker_title})={indicator:.4} "));
                }

                let _ = event_sender
                    .send(BacktestEvent::Info(format!(
                        "[{date_str}] {top_tickers_str}"
                    )))
                    .await;
            }

            let selected_tickers: Vec<Ticker> = indicators
                .iter()
                .take(limit as usize)
                .map(|(ticker, _)| ticker.clone())
                .collect();

            let total_value = context.calc_total_value(date).await?;
            let ticker_value = total_value / selected_tickers.len() as f64;

            let target: Vec<(Ticker, f64)> = selected_tickers
                .into_iter()
                .map(|ticker| (ticker, ticker_value))
                .collect();
            context.rebalance(&target, date, event_sender).await?;
        }

        Ok(())
    }
}
