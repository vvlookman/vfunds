use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    error::VfResult,
    financial::{
        KlineField, get_ticker_title,
        stock::{StockDividendAdjust, fetch_stock_kline},
    },
    rule::{BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, rule_send_info},
    utils::{
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
        context: &mut FundBacktestContext,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let rule_name = mod_name!();

        let allow_short = self
            .options
            .get("allow_short")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
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

        for (ticker, _units) in context.portfolio.positions.clone() {
            let kline = fetch_stock_kline(&ticker, StockDividendAdjust::ForwardProp).await?;
            let latest_prices: Vec<f64> = kline
                .get_latest_values::<f64>(
                    date,
                    false,
                    &KlineField::Close.to_string(),
                    (macd_period_slow + macd_period_signal + macd_slope_window) as u32,
                )
                .iter()
                .map(|&(_, v)| v)
                .collect();
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
                    let ticker_title = get_ticker_title(&ticker).await;

                    rule_send_info(
                        rule_name,
                        &format!("[Sell Signal] {ticker_title}"),
                        date,
                        event_sender,
                    )
                    .await;

                    context
                        .position_close(&ticker, !allow_short, date, event_sender)
                        .await?;

                    if !allow_short {
                        context.cash_deploy_free(date, event_sender).await?;
                    }
                }
            }
        }

        for (ticker, _) in context.portfolio.reserved_cash.clone() {
            let kline = fetch_stock_kline(&ticker, StockDividendAdjust::ForwardProp).await?;
            let latest_prices: Vec<f64> = kline
                .get_latest_values::<f64>(
                    date,
                    false,
                    &KlineField::Close.to_string(),
                    (macd_period_slow + macd_period_signal + macd_slope_window) as u32,
                )
                .iter()
                .map(|&(_, v)| v)
                .collect();
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
                    let ticker_title = get_ticker_title(&ticker).await;

                    rule_send_info(
                        rule_name,
                        &format!("[Buy Signal] {ticker_title}"),
                        date,
                        event_sender,
                    )
                    .await;

                    context
                        .position_open_reserved(&ticker, date, event_sender)
                        .await?;
                }
            }
        }

        Ok(())
    }
}
