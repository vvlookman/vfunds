use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::{Datelike, NaiveDate};
use log::debug;
use smartcore::{
    linalg::basic::{arrays::Array, matrix::DenseMatrix},
    linear::ridge_regression::{RidgeRegression, RidgeRegressionParameters},
    metrics::{mean_absolute_error, r2},
};
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    CANDIDATE_TICKER_RATIO, PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::{
        KlineField,
        stock::{StockDividendAdjust, fetch_stock_kline},
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_warning,
    },
    ticker::Ticker,
    utils::{
        datetime::date_to_str,
        financial::{calc_annualized_return_rate, calc_ema},
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
        event_sender: &Sender<BacktestEvent>,
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
        let ma_exp = self
            .options
            .get("ma_exp")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let ma_period_fast = self
            .options
            .get("ma_period_fast")
            .and_then(|v| v.as_u64())
            .unwrap_or(5);
        let ma_period_slow = self
            .options
            .get("ma_period_slow")
            .and_then(|v| v.as_u64())
            .unwrap_or(20);
        let metric_r2_threshold = self
            .options
            .get("metric_r2_threshold")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.8);
        let regression_alpha = self
            .options
            .get("regression_alpha")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let weight_method = self
            .options
            .get("weight_method")
            .and_then(|v| v.as_str())
            .unwrap_or("equal");
        {
            if limit == 0 {
                panic!("limit must > 0");
            }

            if lookback_trade_days == 0 {
                panic!("lookback_trade_days must > 0");
            }

            if ma_period_fast == 0 {
                panic!("ma_period_fast must > 0");
            }

            if ma_period_slow == 0 {
                panic!("ma_period_slow must > 0");
            }

            if ma_period_fast >= ma_period_slow {
                panic!("ma_period_fast must < ma_period_slow");
            }

            if regression_alpha < 0.0 {
                panic!("regression_alpha must >= 0");
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
                    calc_count += 1;

                    if context.portfolio.reserved_cash.contains_key(ticker) {
                        continue;
                    }

                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::Forward).await?;
                    let prices_with_date = kline.get_latest_values::<f64>(
                        date,
                        false,
                        &KlineField::Close.to_string(),
                        lookback_trade_days as u32,
                    );
                    if prices_with_date.len() < lookback_trade_days as usize {
                        rule_send_warning(
                            rule_name,
                            &format!("[No Enough Data] {ticker}"),
                            date,
                            event_sender,
                        )
                        .await;
                        continue;
                    }

                    let prices: Vec<f64> = prices_with_date.iter().map(|&(_, v)| v).collect();
                    if let Some(arr) = calc_annualized_return_rate(&prices) {
                        let features: Vec<Vec<f64>> = prices_with_date
                            .iter()
                            .enumerate()
                            .map(|(i, &(date, _))| {
                                let x_i = i as f64;
                                let x_weekday = date.weekday().number_from_monday() as f64;
                                let x_monthday = date.day() as f64;

                                vec![x_i, x_weekday, x_monthday]
                            })
                            .collect();

                        if let Ok(x_train) = DenseMatrix::from_2d_array(
                            &features
                                .iter()
                                .map(|v| v.as_slice())
                                .collect::<Vec<&[f64]>>(),
                        ) {
                            let y_train: Vec<f64> = prices.iter().map(|&v| v.ln()).collect();

                            let parameters =
                                RidgeRegressionParameters::default().with_alpha(regression_alpha);
                            if let Ok(model) = RidgeRegression::fit(&x_train, &y_train, parameters)
                            {
                                if let Ok(y_train_pred) = model.predict(&x_train) {
                                    let r2_score = r2(&y_train, &y_train_pred);
                                    debug!(
                                        "[{date_str}] R2={r2_score:.4} MAE={:.4} SHAPE={:?}",
                                        mean_absolute_error(&y_train, &y_train_pred),
                                        x_train.shape(),
                                    );

                                    if r2_score > metric_r2_threshold && r2_score < 1.0 - 1e-8 {
                                        let emas_fast = calc_ema(&prices, ma_period_fast as usize);
                                        let emas_slow = calc_ema(&prices, ma_period_slow as usize);
                                        let ema_ratio = if let (Some(ema_fast), Some(ema_slow)) =
                                            (emas_fast.last(), emas_slow.last())
                                        {
                                            (ema_slow / ema_fast).powi(ma_exp as i32)
                                        } else {
                                            0.0
                                        };

                                        let indicator = arr * r2_score * ema_ratio;
                                        debug!(
                                            "[{date_str}] [{rule_name}] {ticker} = {indicator:.4} (ARR={arr:.4} R2={r2_score:.4} EMA_RATIO={ema_ratio:.4})"
                                        );

                                        if indicator > 0.0 {
                                            indicators.push((ticker.clone(), indicator));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        rule_notify_calc_progress(
                            rule_name,
                            calc_count as f64 / tickers_map.len() as f64 * 100.0,
                            date,
                            event_sender,
                        )
                        .await;

                        last_time = Instant::now();
                    }
                }

                rule_notify_calc_progress(rule_name, 100.0, date, event_sender).await;
            }

            indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

            let targets_indicator = indicators
                .iter()
                .filter(|(_, v)| *v > 0.0)
                .take(limit as usize)
                .map(|(t, v)| (t.clone(), *v))
                .collect::<Vec<_>>();

            rule_notify_indicators(
                rule_name,
                &targets_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &indicators
                    .iter()
                    .filter(|&(_, v)| *v > 0.0)
                    .skip(limit as usize)
                    .take(CANDIDATE_TICKER_RATIO * limit as usize)
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                date,
                event_sender,
            )
            .await;

            let weights = calc_weights(&targets_indicator, weight_method)?;
            context.rebalance(&weights, date, event_sender).await?;
        }

        Ok(())
    }
}
