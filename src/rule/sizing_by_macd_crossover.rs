use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    backtest::calc_buy_fee,
    error::VfResult,
    financial::stock::{
        StockDividendAdjust, StockKlineField, fetch_stock_detail, fetch_stock_kline,
    },
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
    utils::{
        datetime::date_to_str,
        financial::{calc_macd, calc_rsi},
        stats::slope,
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

        let date_str = date_to_str(date);

        for (ticker, _units) in context.portfolio.positions.clone() {
            let kline = fetch_stock_kline(&ticker, StockDividendAdjust::ForwardProp).await?;
            let latest_prices = kline.get_latest_values::<f64>(
                date,
                &StockKlineField::Close.to_string(),
                (macd_period_slow + macd_period_signal + macd_slope_window) as u32,
            );
            let macds = calc_macd(
                &latest_prices,
                (macd_period_fast, macd_period_slow, macd_period_signal),
            );
            let rsis = calc_rsi(&latest_prices, rsi_period);

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
                let macd_slope = slope(&macd_hists).unwrap_or(0.0);

                if macd_today.2 < 0.0 && macd_prev.2 > 0.0 && macd_slope < 0.0 && *rsi < rsi_low {
                    let ticker_title = fetch_stock_detail(&ticker).await?.title;

                    let _ = event_sender
                        .send(BacktestEvent::Info(format!(
                            "[{date_str}] [{rule_name}] Sell Signal {ticker}({ticker_title})"
                        )))
                        .await;

                    context.exit(&ticker, date, event_sender.clone()).await?;
                }
            }
        }

        for (ticker, cash) in context.portfolio.sideline_cash.clone() {
            if context.portfolio.positions.contains_key(&ticker) {
                context.portfolio.sideline_cash.remove(&ticker);
                continue;
            }

            let kline = fetch_stock_kline(&ticker, StockDividendAdjust::ForwardProp).await?;
            let latest_prices = kline.get_latest_values::<f64>(
                date,
                &StockKlineField::Close.to_string(),
                (macd_period_slow + macd_period_signal + macd_slope_window) as u32,
            );
            let macds = calc_macd(
                &latest_prices,
                (macd_period_fast, macd_period_slow, macd_period_signal),
            );
            let rsis = calc_rsi(&latest_prices, rsi_period);

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
                let macd_slope = slope(&macd_hists).unwrap_or(0.0);

                if macd_today.2 > 0.0 && macd_prev.2 < 0.0 && macd_slope > 0.0 && *rsi > rsi_high {
                    let ticker_title = fetch_stock_detail(&ticker).await?.title;

                    let _ = event_sender
                        .send(BacktestEvent::Info(format!(
                            "[{date_str}] [{rule_name}] Buy Signal {ticker}({ticker_title})"
                        )))
                        .await;

                    let ticker_value = cash - calc_buy_fee(cash, context.options);
                    context
                        .scale(&ticker, ticker_value, date, event_sender.clone())
                        .await?;
                }
            }
        }

        Ok(())
    }
}
