use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    error::VfResult,
    financial::stock::{
        StockDividendAdjust, StockKlineField, fetch_stock_detail, fetch_stock_kline,
    },
    rule::{BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor},
    ticker::Ticker,
    utils::{datetime::date_to_str, financial::calc_annualized_volatility, math::constraint_array},
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
        {
            if lookback_trade_days == 0 {
                panic!("lookback_trade_days must > 0");
            }

            if weight_scale_max < 1.0 {
                panic!("weight_scale_max must >= 1.0");
            }

            if weight_scale_min > 1.0 {
                panic!("weight_scale_min must <= 1.0");
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let date_str = date_to_str(date);

            let mut tickers_weight_and_inverse_vols: HashMap<Ticker, (f64, f64)> = HashMap::new();
            for (ticker, (weight, _)) in &tickers_map {
                if context.portfolio.reserved_cash.contains_key(ticker) {
                    continue;
                }

                let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                let prices = kline.get_latest_values::<f64>(
                    date,
                    &StockKlineField::Close.to_string(),
                    lookback_trade_days as u32,
                );
                if let Some(vol) = calc_annualized_volatility(&prices) {
                    tickers_weight_and_inverse_vols.insert(
                        ticker.clone(),
                        (*weight, if vol > 0.0 { 1.0 / vol } else { 0.0 }),
                    );
                }
            }

            let tickers_inverse_vols_sum = tickers_weight_and_inverse_vols
                .iter()
                .map(|(_, (_, v))| *v)
                .sum::<f64>();
            let tickers_inverse_vol_weight: HashMap<Ticker, f64> = tickers_weight_and_inverse_vols
                .iter()
                .map(|(k, (_, v))| (k.clone(), *v / tickers_inverse_vols_sum))
                .collect();

            let inverse_vol_weight_baseline = 1.0 / tickers_inverse_vol_weight.len() as f64;
            let inverse_vol_weight_min = inverse_vol_weight_baseline * weight_scale_min;
            let inverse_vol_weight_max = inverse_vol_weight_baseline * weight_scale_max;

            let (tickers, inverse_vol_weights): (Vec<Ticker>, Vec<f64>) =
                tickers_inverse_vol_weight
                    .iter()
                    .map(|(k, v)| (k.clone(), *v))
                    .unzip();
            let inverse_vol_weights_scaled = constraint_array(
                &inverse_vol_weights,
                inverse_vol_weight_min,
                inverse_vol_weight_max,
            );
            let tickers_inverse_vol_weight_adj: HashMap<Ticker, f64> = tickers
                .iter()
                .zip(inverse_vol_weights_scaled.iter())
                .map(|(k, v)| (k.clone(), *v))
                .collect();

            let mut targets_weight: Vec<(Ticker, f64)> = vec![];
            for (ticker, inverse_vol_weight) in &tickers_inverse_vol_weight_adj {
                if let Some((origin_weight, _)) = tickers_weight_and_inverse_vols.get(ticker) {
                    targets_weight.push((ticker.clone(), (*origin_weight) * (*inverse_vol_weight)));
                }
            }

            {
                let mut tickers_strs: Vec<String> = vec![];
                for (ticker, weight) in &targets_weight {
                    let ticker_title = fetch_stock_detail(ticker).await?.title;
                    tickers_strs.push(format!("{ticker}({ticker_title})={weight:.4}"));
                }

                let tickers_str = tickers_strs.join(" ");
                let _ = event_sender
                    .send(BacktestEvent::Info(format!(
                        "[{date_str}] [{rule_name}] {tickers_str}"
                    )))
                    .await;
            }

            context
                .rebalance(&targets_weight, date, event_sender)
                .await?;
        }

        Ok(())
    }
}
