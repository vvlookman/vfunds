use std::{collections::HashMap, str::FromStr};

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    backtest::{calc_buy_fee, calc_sell_fee},
    error::VfResult,
    financial::stock::{StockField, fetch_stock_daily_backward_adjusted_price, fetch_stock_detail},
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
    ticker::Ticker,
    utils,
};

pub struct Executor {
    #[allow(dead_code)]
    options: HashMap<String, serde_json::Value>,

    re_entry_cash: HashMap<String, f64>,
}

impl Executor {
    pub fn new(definition: &RuleDefinition) -> Self {
        Self {
            options: definition.options.clone(),

            re_entry_cash: HashMap::new(),
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
        let macd_period_fast = self
            .options
            .get("macd_period_fast")
            .and_then(|v| v.as_u64())
            .unwrap_or(12) as usize;
        let macd_period_slow = self
            .options
            .get("macd_period_slow")
            .and_then(|v| v.as_u64())
            .unwrap_or(26) as usize;
        let macd_period_signal = self
            .options
            .get("macd_period_signal")
            .and_then(|v| v.as_u64())
            .unwrap_or(9) as usize;
        let macd_slope_window = self
            .options
            .get("macd_slope_window")
            .and_then(|v| v.as_u64())
            .unwrap_or(5) as usize;
        let rsi_period = self
            .options
            .get("rsi_period")
            .and_then(|v| v.as_u64())
            .unwrap_or(14) as usize;
        let rsi_low = self
            .options
            .get("rsi_low")
            .and_then(|v| v.as_f64())
            .unwrap_or(30.0);
        let rsi_high = self
            .options
            .get("rsi_high")
            .and_then(|v| v.as_f64())
            .unwrap_or(70.0);

        let date_str = utils::datetime::date_to_str(date);

        for (ticker_str, units) in context.portfolio.positions.clone() {
            let ticker = Ticker::from_str(&ticker_str)?;

            let stock_daily = fetch_stock_daily_backward_adjusted_price(&ticker).await?;
            let latest_prices = stock_daily.get_latest_values::<f64>(
                date,
                &StockField::Price.to_string(),
                macd_period_slow + macd_period_signal + macd_slope_window,
            );
            let macds = utils::financial::calc_macd(
                &latest_prices,
                (macd_period_fast, macd_period_slow, macd_period_signal),
            );
            let rsis = utils::financial::calc_rsi(&latest_prices, rsi_period);

            if let (Some(macd_today), Some(macd_prev), Some(rsi)) =
                (macds.last(), macds.iter().rev().nth(1), rsis.last())
            {
                let macd_hists: Vec<f64> = macds
                    .iter()
                    .rev()
                    .take(macd_slope_window)
                    .rev()
                    .map(|v| v.2)
                    .collect();
                let macd_slope = utils::stats::slope(&macd_hists).unwrap_or(0.0);

                if macd_today.2 < 0.0 && macd_prev.2 > 0.0 && macd_slope < 0.0 && *rsi > rsi_low {
                    let ticker_title = fetch_stock_detail(&ticker).await?.title;

                    let _ = event_sender
                        .send(BacktestEvent::Info(format!(
                            "[{date_str}] {ticker}({ticker_title}) MACD Sell Signal"
                        )))
                        .await;

                    if let Some(price) = latest_prices.last() {
                        let value = price * units as f64;
                        let fee = calc_sell_fee(value, context.options);
                        let cash = value - fee;

                        context.portfolio.cash += cash;
                        context.portfolio.positions.remove(&ticker_str);

                        self.re_entry_cash
                            .entry(ticker_str.to_string())
                            .and_modify(|x| *x += cash)
                            .or_insert(cash);

                        let _ = event_sender
                                .send(BacktestEvent::Sell(format!(
                                    "[{date_str}] {ticker}({ticker_title}) {price:.2}x{units} -> +${cash:.2}"
                                )))
                                .await;
                    }
                }
            }
        }

        for (ticker_str, cash) in self.re_entry_cash.clone() {
            if context.portfolio.positions.contains_key(&ticker_str) {
                self.re_entry_cash.remove(&ticker_str);
                continue;
            }

            let ticker = Ticker::from_str(&ticker_str)?;

            let stock_daily = fetch_stock_daily_backward_adjusted_price(&ticker).await?;
            let latest_prices = stock_daily.get_latest_values::<f64>(
                date,
                &StockField::Price.to_string(),
                macd_period_slow + macd_period_signal + macd_slope_window,
            );
            let macds = utils::financial::calc_macd(
                &latest_prices,
                (macd_period_fast, macd_period_slow, macd_period_signal),
            );
            let rsis = utils::financial::calc_rsi(&latest_prices, rsi_period);

            if let (Some(macd_today), Some(macd_prev), Some(rsi)) =
                (macds.last(), macds.iter().rev().nth(1), rsis.last())
            {
                let macd_hists: Vec<f64> = macds
                    .iter()
                    .rev()
                    .take(macd_slope_window)
                    .rev()
                    .map(|v| v.2)
                    .collect();
                let macd_slope = utils::stats::slope(&macd_hists).unwrap_or(0.0);

                if macd_today.2 > 0.0 && macd_prev.2 < 0.0 && macd_slope > 0.0 && *rsi < rsi_high {
                    let ticker_title = fetch_stock_detail(&ticker).await?.title;

                    let _ = event_sender
                        .send(BacktestEvent::Info(format!(
                            "[{date_str}] {ticker}({ticker_title}) MACD Buy Signal"
                        )))
                        .await;

                    let buy_value = cash - calc_buy_fee(cash, context.options);

                    if let Some(price) = latest_prices.last() {
                        let buy_units = (buy_value / price).floor();
                        if buy_units > 0.0 {
                            let value = buy_units * price;
                            let fee = calc_buy_fee(value, context.options);
                            let cost = value + fee;

                            context.portfolio.cash -= cost;
                            context
                                .portfolio
                                .positions
                                .entry(ticker_str.to_string())
                                .and_modify(|x| *x += buy_units as u64)
                                .or_insert(buy_units as u64);

                            self.re_entry_cash.remove(&ticker_str);

                            let _ = event_sender
                                .send(BacktestEvent::Buy(format!(
                                    "[{date_str}] {ticker}({ticker_title}) {price:.2}x{buy_units} -> -${cost:.2}"
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
