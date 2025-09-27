use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    backtest::{calc_buy_fee, calc_sell_fee},
    error::VfResult,
    financial::stock::{
        StockDividendAdjust, StockKlineField, fetch_stock_detail, fetch_stock_kline,
    },
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
    ticker::Ticker,
    utils,
    utils::{financial::calc_annual_volatility, math::constraint_array},
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
            let lookback_trade_days = self
                .options
                .get("lookback_trade_days")
                .and_then(|v| v.as_u64())
                .unwrap_or(126);
            let weight_scale_max = self
                .options
                .get("weight_scale_max")
                .and_then(|v| v.as_f64())
                .unwrap_or(4.0);
            let weight_scale_min = self
                .options
                .get("weight_scale_min")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.25);

            if lookback_trade_days == 0 {
                panic!("lookback_trade_days must > 0");
            }

            if weight_scale_max < 1.0 {
                panic!("weight_scale_max must >= 1.0");
            }

            if weight_scale_min > 1.0 {
                panic!("weight_scale_min must <= 1.0");
            }

            let date_str = utils::datetime::date_to_str(date);

            let mut ticker_inverse_vols: HashMap<Ticker, f64> = HashMap::new();
            for ticker in tickers {
                let ticker_str = ticker.to_string();

                if context.portfolio.sideline_cash.contains_key(&ticker_str) {
                    continue;
                }

                let kline = fetch_stock_kline(&ticker, StockDividendAdjust::ForwardProp).await?;
                let prices = kline.get_latest_values::<f64>(
                    date,
                    &StockKlineField::Close.to_string(),
                    lookback_trade_days as usize,
                );
                if let Some(vol) = calc_annual_volatility(&prices) {
                    ticker_inverse_vols.insert(ticker, if vol > 0.0 { 1.0 / vol } else { 0.0 });
                }
            }

            let ticker_inverse_vols_sum = ticker_inverse_vols.values().sum::<f64>();
            let ticker_weights: HashMap<Ticker, f64> = ticker_inverse_vols
                .iter()
                .map(|(k, v)| (k.clone(), *v / ticker_inverse_vols_sum))
                .collect();

            let ticker_weight_baseline = 1.0 / ticker_weights.len() as f64;
            let ticker_weight_min = ticker_weight_baseline * weight_scale_min;
            let ticker_weight_max = ticker_weight_baseline * weight_scale_max;

            let (tickers, weights): (Vec<Ticker>, Vec<f64>) =
                ticker_weights.iter().map(|(k, v)| (k.clone(), *v)).unzip();
            let weights_adj = constraint_array(&weights, ticker_weight_min, ticker_weight_max);
            let ticker_weights_adj: HashMap<Ticker, f64> = tickers
                .iter()
                .zip(weights_adj.iter())
                .map(|(k, v)| (k.clone(), *v))
                .collect();

            {
                let mut tickers_str = String::from("");
                for (ticker, weight) in &ticker_weights_adj {
                    let ticker_title = fetch_stock_detail(ticker).await?.title;
                    tickers_str.push_str(&format!("{ticker}({ticker_title})={weight:.4} "));
                }

                let _ = event_sender
                    .send(BacktestEvent::Info(format!("[{date_str}] {tickers_str}")))
                    .await;
            }

            let total_value = context.calc_total_value(date).await?;
            let position_value =
                total_value - context.portfolio.sideline_cash.values().sum::<f64>();

            for (ticker, weight) in ticker_weights_adj {
                let ticker_str = ticker.to_string();
                let ticker_value = position_value * weight;

                let kline = fetch_stock_kline(&ticker, StockDividendAdjust::ForwardProp).await?;
                if let Some(price) =
                    kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
                {
                    let holding_units = context.portfolio.positions.get(&ticker_str).unwrap_or(&0);
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
