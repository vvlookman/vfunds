use std::{cmp::Ordering, collections::HashMap, str::FromStr};

use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate};
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    backtest::{calc_buy_fee, calc_sell_fee},
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
        let tickers = context.fund_definition.all_tickers(date).await?;
        if !tickers.is_empty() {
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

            if filter_roe_floor < 0.0 {
                panic!("filter_roe_floor must >= 0");
            }

            if limit == 0 || limit > 100 {
                panic!("limit must > 0 and <= 100");
            }

            if lookback_years == 0 || lookback_years > 100 {
                panic!("lookback_years must > 0 and <= 100");
            }

            let date_str = utils::datetime::date_to_str(date);
            let date_from =
                date.with_year(date.year() - lookback_years as i32).unwrap() + Duration::days(1);

            let mut indicators: Vec<(String, f64)> = vec![];
            let mut last_time = Instant::now();
            let mut calc_count: usize = 0;
            for ticker in &tickers {
                let ticker_str = ticker.to_string();
                debug!("[{date_str}] {ticker_str}");

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
                                debug!("[{date_str}] {ticker_str} = {indicator:.4} (ROE={roe:.4})");

                                indicators.push((ticker_str, indicator));
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

            {
                let mut top_tickers_str = String::from("");
                for (ticker_str, indicator) in indicators.iter().take(2 * limit as usize) {
                    let ticker = Ticker::from_str(ticker_str)?;
                    let ticker_title = fetch_stock_detail(&ticker).await?.title;
                    top_tickers_str.push_str(&format!("{ticker}({ticker_title})={indicator:.4} "));
                }

                let _ = event_sender
                    .send(BacktestEvent::Info(format!(
                        "[{date_str}] {top_tickers_str}"
                    )))
                    .await;
            }

            let selected_tickers: Vec<_> = indicators
                .iter()
                .take(limit as usize)
                .map(|x| x.0.to_string())
                .collect();

            let holding_tickers: Vec<_> = context.portfolio.positions.keys().cloned().collect();
            for ticker_str in &holding_tickers {
                if !selected_tickers.contains(ticker_str) {
                    let ticker = Ticker::from_str(ticker_str)?;

                    let kline =
                        fetch_stock_kline(&ticker, StockDividendAdjust::ForwardProp).await?;
                    if let Some(price) =
                        kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
                    {
                        let sell_units =
                            *(context.portfolio.positions.get(ticker_str).unwrap_or(&0)) as f64;
                        if sell_units > 0.0 {
                            let value = sell_units * price;
                            let fee = calc_sell_fee(value, context.options);
                            let cash = value - fee;

                            context.portfolio.cash += cash;
                            context.portfolio.positions.remove(ticker_str);

                            let ticker_title = fetch_stock_detail(&ticker).await?.title;
                            let _ = event_sender
                                .send(BacktestEvent::Sell(format!(
                                    "[{date_str}] {ticker}({ticker_title}) {price:.2}x{sell_units} -> +${cash:.2}"
                                )))
                                .await;
                        }
                    }
                }
            }

            let total_value = context.calc_total_value(date).await?;
            let ticker_value = total_value / selected_tickers.len() as f64;
            for ticker_str in &selected_tickers {
                let ticker = Ticker::from_str(ticker_str)?;

                let kline = fetch_stock_kline(&ticker, StockDividendAdjust::ForwardProp).await?;
                if let Some(price) =
                    kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
                {
                    let holding_units = context.portfolio.positions.get(ticker_str).unwrap_or(&0);
                    let mut holding_value = *holding_units as f64 * price;
                    holding_value -= calc_sell_fee(holding_value, context.options);

                    if holding_value < ticker_value {
                        let mut buy_value = ticker_value - holding_value;
                        buy_value -= calc_buy_fee(buy_value, context.options);

                        let buy_units = (buy_value / price).floor();
                        if buy_units > 0.0 {
                            let value = buy_units * price;
                            let fee = calc_buy_fee(value, context.options);
                            let cost = value + fee;

                            context.portfolio.cash -= cost;
                            context
                                .portfolio
                                .positions
                                .insert(ticker_str.to_string(), holding_units + buy_units as u64);

                            let ticker_title = fetch_stock_detail(&ticker).await?.title;
                            let _ = event_sender
                                .send(BacktestEvent::Buy(format!(
                                    "[{date_str}] {ticker}({ticker_title}) {price:.2}x{buy_units} -> -${cost:.2}"
                                )))
                                .await;
                        }
                    } else {
                        let mut sell_value = holding_value - ticker_value;
                        sell_value -= calc_sell_fee(sell_value, context.options);

                        let sell_units = (sell_value / price).floor();
                        if sell_units > 0.0 {
                            let value = sell_units * price;
                            let fee = calc_sell_fee(value, context.options);
                            let cash = value - fee;

                            context.portfolio.cash += cash;
                            context
                                .portfolio
                                .positions
                                .insert(ticker_str.to_string(), holding_units - sell_units as u64);

                            let ticker_title = fetch_stock_detail(&ticker).await?.title;
                            let _ = event_sender
                                .send(BacktestEvent::Sell(format!(
                                    "[{date_str}] {ticker}({ticker_title}) {price:.2}x{sell_units} -> +${cash:.2}"
                                )))
                                .await;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
