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
    spec::RuleOptions,
    utils::{
        financial::{calc_macd, calc_rsi},
        math::linear_regression,
    },
};

pub struct Executor {
    #[allow(dead_code)]
    options: RuleOptions,
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

        let allow_short = self.options.read_bool("allow_short", false);
        let macd_period_fast = self.options.read_u64_no_zero("macd_period_fast", 12);
        let macd_period_slow = self.options.read_u64_no_zero("macd_period_slow", 26);
        let macd_period_signal = self.options.read_u64_no_zero("macd_period_signal", 9);
        let macd_slope_window = self.options.read_u64_no_zero("macd_slope_window", 5);
        let rsi_period = self.options.read_u64_no_zero("rsi_period", 14);
        let rsi_low = self.options.read_f64_in_range("rsi_low", 30.0, 0.0..=100.0);
        let rsi_high = self
            .options
            .read_f64_in_range("rsi_high", 70.0, 0.0..=100.0);

        for (ticker, _units) in context.portfolio.positions.clone() {
            let kline = fetch_stock_kline(&ticker, StockDividendAdjust::Backward).await?;
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
                (
                    macd_period_fast as usize,
                    macd_period_slow as usize,
                    macd_period_signal as usize,
                ),
            );
            let rsis = calc_rsi(&latest_prices, rsi_period as usize);

            if let (Some(macd_today), Some(macd_prev), Some(rsi)) =
                (macds.last(), macds.iter().rev().nth(1), rsis.last())
            {
                let macd_hists: Vec<f64> = macds
                    .iter()
                    .rev()
                    .take(macd_slope_window as usize)
                    .rev()
                    .map(|v| v.2)
                    .collect();
                let (macd_slope, macd_slope_r2) =
                    linear_regression(&macd_hists).unwrap_or((0.0, 0.0));

                if macd_today.2 < 0.0
                    && macd_prev.2 > 0.0
                    && macd_slope * macd_slope_r2 < 0.0
                    && *rsi < rsi_low
                {
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
            let kline = fetch_stock_kline(&ticker, StockDividendAdjust::Backward).await?;
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
                (
                    macd_period_fast as usize,
                    macd_period_slow as usize,
                    macd_period_signal as usize,
                ),
            );
            let rsis = calc_rsi(&latest_prices, rsi_period as usize);

            if let (Some(macd_today), Some(macd_prev), Some(rsi)) =
                (macds.last(), macds.iter().rev().nth(1), rsis.last())
            {
                let macd_hists: Vec<f64> = macds
                    .iter()
                    .rev()
                    .take(macd_slope_window as usize)
                    .rev()
                    .map(|v| v.2)
                    .collect();
                let (macd_slope, macd_slope_r2) =
                    linear_regression(&macd_hists).unwrap_or((0.0, 0.0));

                if macd_today.2 > 0.0
                    && macd_prev.2 < 0.0
                    && macd_slope * macd_slope_r2 > 0.0
                    && *rsi > rsi_high
                {
                    let ticker_title = get_ticker_title(&ticker).await;

                    rule_send_info(
                        rule_name,
                        &format!("[Buy Signal] {ticker_title}"),
                        date,
                        event_sender,
                    )
                    .await;

                    context
                        .position_entry_reserved(&ticker, date, event_sender)
                        .await?;
                }
            }
        }

        Ok(())
    }
}
