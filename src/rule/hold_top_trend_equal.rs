use std::{cmp::Ordering, collections::HashMap, str::FromStr};

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
    backtest::{calc_buy_fee, calc_sell_fee},
    error::VfResult,
    financial::stock::{
        StockDividendAdjust, StockKlineField, fetch_stock_detail, fetch_stock_kline,
    },
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
    ticker::Ticker,
    utils,
    utils::financial::calc_annual_return_rate,
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
        let filter_quantile = self
            .options
            .get("filter_quantile")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.8);
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
            if filter_quantile <= 0.0 || filter_quantile >= 1.0 {
                panic!("filter_quantile must > 0 and < 1");
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

        let tickers = context.fund_definition.all_tickers(date).await?;
        if !tickers.is_empty() {
            let date_str = utils::datetime::date_to_str(date);

            let mut indicators: Vec<(String, f64)> = vec![];
            let mut last_time = Instant::now();
            let mut calc_count: usize = 0;
            for ticker in &tickers {
                let ticker_str = ticker.to_string();

                if context.portfolio.sideline_cash.contains_key(&ticker_str) {
                    continue;
                }

                let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                let prices = kline.get_latest_values::<f64>(
                    date,
                    &StockKlineField::Close.to_string(),
                    lookback_trade_days as usize,
                );

                if prices.len() < lookback_trade_days as usize {
                    let _ = event_sender
                        .send(BacktestEvent::Info(format!(
                            "[{date_str}] [{ticker_str}] No enough data!"
                        )))
                        .await;
                    continue;
                }

                if let Some(arr) = calc_annual_return_rate(
                    prices[0],
                    prices[prices.len() - 1],
                    prices.len() as u64,
                ) {
                    let total_len = prices.len();
                    let train_len = (total_len as f64 * (1.0 - regression_test)) as usize;

                    let generate_featue = |i: usize| {
                        let x = i as f64;
                        vec![x, x.powi(2)]
                    };
                    let features_train: Vec<Vec<f64>> = Vec::from_iter(0..train_len)
                        .iter()
                        .map(|&i| generate_featue(i))
                        .collect();
                    let features_test: Vec<Vec<f64>> = Vec::from_iter(train_len..total_len)
                        .iter()
                        .map(|&i| generate_featue(i))
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
                        if let Ok(model) = RidgeRegression::fit(&x_train, &y_train, parameters) {
                            if let Ok(y_pred) = model.predict(&x_test) {
                                let r2_score = r2(&y_test, &y_pred);
                                debug!("[{date_str}] {ticker_str} Better R2={r2_score:.4}");

                                let indicator = arr * (1.0 + r2_score);
                                debug!(
                                    "[{date_str}] {ticker_str} = {indicator:.4} (ARR={arr:.4} R2={r2_score:.4})"
                                );

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

            let threshold = utils::stats::quantile(
                &indicators.iter().map(|x| x.1).collect::<Vec<f64>>(),
                filter_quantile,
            )
            .unwrap_or(f64::MAX);

            let filetered_indicators = indicators
                .iter()
                .filter(|x| x.1 >= threshold)
                .collect::<Vec<_>>();

            if !filetered_indicators.is_empty() {
                let mut top_tickers_str = String::from("");
                for (ticker_str, indicator) in &filetered_indicators {
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

            let selected_tickers: Vec<_> = filetered_indicators
                .iter()
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
