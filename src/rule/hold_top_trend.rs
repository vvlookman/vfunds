use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::{Datelike, NaiveDate};
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
    financial::{
        KlineField,
        stock::{StockDividendAdjust, fetch_stock_kline},
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, notify_calc_progress,
        notify_tickers_indicator,
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
                    let prices_with_date = kline.get_latest_values::<f64>(
                        date,
                        false,
                        &KlineField::Close.to_string(),
                        lookback_trade_days as u32,
                    );
                    if prices_with_date.len() < lookback_trade_days as usize {
                        let _ = event_sender
                            .send(BacktestEvent::Info(format!(
                                "[{date_str}] [{rule_name}] [No Enough Data] {ticker}"
                            )))
                            .await;
                        continue;
                    }

                    let prices: Vec<f64> = prices_with_date.iter().map(|&(_, v)| v).collect();
                    if let Some(arr) = calc_annualized_return_rate(
                        prices[0],
                        prices[prices.len() - 1],
                        prices.len() as u64,
                    ) {
                        let total_len = prices.len();
                        let train_len = (total_len as f64 * (1.0 - regression_test)) as usize;

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

                        if let (Ok(x_train), Ok(x_test)) = (
                            DenseMatrix::from_2d_array(
                                &features
                                    .iter()
                                    .take(train_len)
                                    .map(|v| v.as_slice())
                                    .collect::<Vec<&[f64]>>(),
                            ),
                            DenseMatrix::from_2d_array(
                                &features
                                    .iter()
                                    .skip(train_len)
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
                                    let r2_normal = r2_score.max(0.0);

                                    let emas_fast = calc_ema(&prices, ma_period_fast as usize);
                                    let emas_slow = calc_ema(&prices, ma_period_slow as usize);
                                    let ema_ratio = if let (Some(ema_fast), Some(ema_slow)) =
                                        (emas_fast.last(), emas_slow.last())
                                    {
                                        (ema_slow / ema_fast).powi(ma_exp as i32)
                                    } else {
                                        0.0
                                    };

                                    let indicator = arr * r2_normal * ema_ratio;
                                    debug!(
                                        "[{date_str}] [{rule_name}] {ticker} = {indicator:.4} (ARR={arr:.4} R2={r2_normal:.4} EMA_RATIO={ema_ratio:.4})"
                                    );

                                    indicators.push((ticker.clone(), indicator));
                                }
                            }
                        }
                    }

                    calc_count += 1;

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        notify_calc_progress(
                            event_sender.clone(),
                            date,
                            rule_name,
                            calc_count as f64 / tickers_map.len() as f64 * 100.0,
                        )
                        .await;

                        last_time = Instant::now();
                    }
                }

                notify_calc_progress(event_sender.clone(), date, rule_name, 100.0).await;
            }

            indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

            let targets_indicator = indicators
                .iter()
                .filter(|&(_, v)| *v > 0.0)
                .take(limit as usize)
                .collect::<Vec<_>>();

            notify_tickers_indicator(
                event_sender.clone(),
                date,
                rule_name,
                &targets_indicator
                    .iter()
                    .map(|&(t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &indicators
                    .iter()
                    .filter(|&(_, v)| *v > 0.0)
                    .skip(limit as usize)
                    .take(limit as usize)
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
            )
            .await;

            let mut targets_weight: Vec<(Ticker, f64)> = vec![];
            for (ticker, indicator) in &targets_indicator {
                if let Some((weight, _)) = tickers_map.get(ticker) {
                    targets_weight.push((ticker.clone(), (*weight) * (*indicator)));
                }
            }

            context
                .rebalance(&targets_weight, date, event_sender)
                .await?;
        }

        Ok(())
    }
}
