//! Close and re-enter position after price spike peak

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    STALE_DAYS_SHORT,
    error::VfResult,
    financial::{
        KlineField, PriceType, get_ticker_title,
        stock::{StockDividendAdjust, fetch_stock_kline},
    },
    rule::{BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, rule_send_info},
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

        let spike_days = self
            .options
            .get("spike_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as usize;
        let spike_fall_threshold = self
            .options
            .get("spike_fall_threshold")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.01);
        let spike_rise_threshold = self
            .options
            .get("spike_rise_threshold")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.03);

        for (ticker, _units) in context.portfolio.positions.clone() {
            let kline = fetch_stock_kline(&ticker, StockDividendAdjust::Backward).await?;
            let prices: Vec<f64> = kline
                .get_latest_values::<f64>(
                    date,
                    false,
                    &KlineField::Close.to_string(),
                    spike_days as u32 + 1,
                )
                .iter()
                .map(|&(_, v)| v)
                .collect();
            if let Some(&last_price) = prices.last() {
                'check_spike: for price in prices.iter().take(prices.len().saturating_sub(1)) {
                    let price_knock_out = price * (1.0 + spike_rise_threshold);
                    if last_price > price_knock_out {
                        let ticker_title = get_ticker_title(&ticker).await;

                        rule_send_info(
                            rule_name,
                            &format!("[Sell Signal] {ticker_title}"),
                            date,
                            event_sender,
                        )
                        .await;

                        context
                            .position_close_with_price_type(
                                &ticker,
                                true,
                                &PriceType::Mid,
                                date,
                                event_sender,
                            )
                            .await?;

                        break 'check_spike;
                    }
                }
            }
        }

        for (ticker, (_, reserved_date)) in context.portfolio.reserved_cash.clone() {
            let kline = fetch_stock_kline(&ticker, StockDividendAdjust::Backward).await?;
            if let Some((latest_trade_date, latest_price)) = kline.get_latest_value::<f64>(
                date,
                STALE_DAYS_SHORT,
                false,
                &KlineField::Close.to_string(),
            ) {
                if latest_trade_date > reserved_date {
                    if let Some((_, reserved_price)) =
                        kline.get_value::<f64>(&reserved_date, &KlineField::Close.to_string())
                    {
                        let price_knock_in = reserved_price * (1.0 - spike_fall_threshold);
                        if latest_price < price_knock_in {
                            let ticker_title = get_ticker_title(&ticker).await;

                            rule_send_info(
                                rule_name,
                                &format!("[Buy Signal] {ticker_title}"),
                                date,
                                event_sender,
                            )
                            .await;

                            context
                                .position_entry_reserved_with_price_type(
                                    &ticker,
                                    &PriceType::Mid,
                                    date,
                                    event_sender,
                                )
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
