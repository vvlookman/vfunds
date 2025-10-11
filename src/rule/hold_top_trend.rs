use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::NaiveDate;
use log::debug;
use smartcore::{
    linalg::basic::matrix::DenseMatrix,
    linear::ridge_regression::{RidgeRegression, RidgeRegressionParameters},
    metrics::r2,
};
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::stock::{
        StockDividendAdjust, StockKlineField, fetch_stock_detail, fetch_stock_kline,
    },
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
    ticker::Ticker,
    utils::{datetime::date_to_str, financial::calc_annualized_return_rate},
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
            .unwrap_or(30);
        let regression_alpha = self
            .options
            .get("regression_alpha")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let regression_test = self
            .options
            .get("regression_test")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.3);
        {
            if limit == 0 {
                panic!("limit must > 0");
            }

            if lookback_trade_days == 0 {
                panic!("lookback_trade_days must > 0");
            }

            if regression_alpha < 0.0 {
                panic!("regression_alpha must >= 0");
            }

            if regression_test <= 0.0 || regression_test >= 1.0 {
                panic!("regression_test must > 0 and < 1");
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
                    if context.portfolio.reserved_cash.contains_key(ticker) {
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

                    if let Some(arr) = calc_annualized_return_rate(
                        prices[0],
                        prices[prices.len() - 1],
                        prices.len() as u64,
                    ) {
                        let total_len = prices.len();
                        let train_len = (total_len as f64 * (1.0 - regression_test)) as usize;

                        let generate_feature = |i: usize| {
                            let x = i as f64;
                            vec![x, x.powi(2)]
                        };
                        let features_train: Vec<Vec<f64>> = Vec::from_iter(0..train_len)
                            .iter()
                            .map(|&i| generate_feature(i))
                            .collect();
                        let features_test: Vec<Vec<f64>> = Vec::from_iter(train_len..total_len)
                            .iter()
                            .map(|&i| generate_feature(i))
                            .collect();

                        if let (Ok(x_train), Ok(x_test)) = (
                            DenseMatrix::from_2d_array(
                                &features_train
                                    .iter()
                                    .map(|v| v.as_slice())
                                    .collect::<Vec<&[f64]>>(),
                            ),
                            DenseMatrix::from_2d_array(
                                &features_test
                                    .iter()
                                    .map(|v| v.as_slice())
                                    .collect::<Vec<&[f64]>>(),
                            ),
                        ) {
                            let y_train: Vec<f64> =
                                prices.iter().take(train_len).map(|&v| v.ln()).collect();
                            let y_test: Vec<f64> =
                                prices.iter().skip(train_len).map(|&v| v.ln()).collect();

                            let parameters =
                                RidgeRegressionParameters::default().with_alpha(regression_alpha);
                            if let Ok(model) = RidgeRegression::fit(&x_train, &y_train, parameters)
                            {
                                if let Ok(y_pred) = model.predict(&x_test) {
                                    let r2_score = r2(&y_test, &y_pred);
                                    let indicator = arr * (1.0 + r2_score);
                                    debug!(
                                        "[{date_str}] [{rule_name}] {ticker} = {indicator:.4} (ARR={arr:.4} R2={r2_score:.4})"
                                    );

                                    indicators.push((ticker.clone(), indicator));
                                }
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

            let filetered_indicators = indicators.iter().take(limit as usize).collect::<Vec<_>>();

            if !filetered_indicators.is_empty() {
                let mut top_tickers_strs: Vec<String> = vec![];
                for (ticker, indicator) in &filetered_indicators {
                    let ticker_title = fetch_stock_detail(ticker).await?.title;
                    top_tickers_strs.push(format!("{ticker}({ticker_title})={indicator:.4} "));
                }

                let top_tickers_str = top_tickers_strs.join(" ");
                let _ = event_sender
                    .send(BacktestEvent::Info(format!(
                        "[{date_str}] [{rule_name}] {top_tickers_str}"
                    )))
                    .await;
            }

            let mut targets_weight: Vec<(Ticker, f64)> = vec![];
            for (ticker, _) in &filetered_indicators {
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
