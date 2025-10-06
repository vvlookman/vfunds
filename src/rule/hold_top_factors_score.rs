use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::NaiveDate;
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::stock::{
        StockDividendAdjust, StockKlineField, fetch_stock_detail, fetch_stock_kline,
    },
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
    ticker::Ticker,
    utils::{
        datetime::date_to_str,
        financial::{calc_max_drawdown, calc_momentum, calc_sharpe_ratio, calc_volatility},
        math::normalize_min_max,
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
        context: &mut BacktestContext,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let rule_name = mod_name!();

        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let lookback_trade_days = self
            .options
            .get("lookback_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(21);
        let weight_sharpe = self
            .options
            .get("weight_sharpe")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let weight_max_drawdown = self
            .options
            .get("weight_max_drawdown")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let weight_volatility = self
            .options
            .get("weight_volatility")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let weight_momentum = self
            .options
            .get("weight_momentum")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        {
            if limit == 0 {
                panic!("limit must > 0");
            }

            if lookback_trade_days == 0 {
                panic!("lookback_trade_days must > 0");
            }
        }

        let tickers = context.fund_definition.all_tickers(date).await?;
        if !tickers.is_empty() {
            let date_str = date_to_str(date);

            let mut factors: Vec<(Ticker, [f64; 4])> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;
                for ticker in tickers.keys() {
                    if context.portfolio.sideline_cash.contains_key(ticker) {
                        continue;
                    }

                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                    let prices = kline.get_latest_values::<f64>(
                        date,
                        &StockKlineField::Close.to_string(),
                        lookback_trade_days as u32,
                    );

                    if prices.len() < lookback_trade_days as usize {
                        let _ = event_sender
                            .send(BacktestEvent::Info(format!(
                                "[{date_str}] [{rule_name}] [No Enough Data] {ticker}"
                            )))
                            .await;
                        continue;
                    }

                    if let (Some(sharpe), Some(max_drawdown), Some(volatility), Some(momentum)) = (
                        calc_sharpe_ratio(&prices, context.options.risk_free_rate),
                        calc_max_drawdown(&prices),
                        calc_volatility(&prices),
                        calc_momentum(&prices),
                    ) {
                        factors
                            .push((ticker.clone(), [sharpe, max_drawdown, volatility, momentum]));
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

            let mut normalized_factor_values: Vec<Vec<f64>> = vec![];
            for j in 0..4 {
                let factor_values: Vec<f64> = factors.iter().map(|x| x.1[j]).collect();
                normalized_factor_values.push(normalize_min_max(&factor_values));
            }

            let mut indicators: Vec<(Ticker, f64)> = factors
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    let ticker = &x.0;

                    let sharpe = normalized_factor_values[0][i];
                    let max_drawdown = normalized_factor_values[1][i];
                    let volatility = normalized_factor_values[2][i];
                    let momentum = normalized_factor_values[3][i];

                    let indicator = weight_sharpe * sharpe
                        + weight_max_drawdown * (1.0 - max_drawdown)
                        + weight_volatility * (1.0 - volatility)
                        + weight_momentum * momentum;
                    debug!("[{date_str}] {ticker}={indicator:.4} (Sharpe={sharpe:.4} Vol={volatility:.4} MDD={max_drawdown:.4} Mom={momentum:.4})");

                    (ticker.clone(), indicator)
                })
                .collect();
            indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

            let filetered_indicators = indicators.iter().take(limit as usize).collect::<Vec<_>>();

            if !filetered_indicators.is_empty() {
                let mut top_tickers_strs: Vec<String> = vec![];
                for (ticker, indicator) in &filetered_indicators {
                    let ticker_title = fetch_stock_detail(ticker).await?.title;
                    top_tickers_strs.push(format!("{ticker}({ticker_title})={indicator:.4}"));
                }

                let top_tickers_str = top_tickers_strs.join(" ");
                let _ = event_sender
                    .send(BacktestEvent::Info(format!(
                        "[{date_str}] [{rule_name}] {top_tickers_str}"
                    )))
                    .await;
            }

            let selected_tickers: Vec<Ticker> = filetered_indicators
                .iter()
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
