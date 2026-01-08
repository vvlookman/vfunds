//! Close and re-enter position after price spike peak

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
            let kline = fetch_stock_kline(&ticker, StockDividendAdjust::ForwardProp).await?;
            let latest_prices: Vec<f64> = kline
                .get_latest_values::<f64>(
                    date,
                    false,
                    &KlineField::Close.to_string(),
                    spike_days as u32,
                )
                .iter()
                .map(|&(_, v)| v)
                .collect();
            if let Some((_, price)) = kline.get_value::<f64>(date, &KlineField::High.to_string()) {
                for latest_price in latest_prices {
                    let price_knock_out = latest_price * (1.0 + spike_rise_threshold);
                    if price > price_knock_out {
                        let ticker_title = get_ticker_title(&ticker).await;

                        rule_send_info(
                            rule_name,
                            &format!("[Sell Signal] {ticker_title}"),
                            date,
                            event_sender,
                        )
                        .await;

                        context
                            .position_close_with_price(
                                &ticker,
                                true,
                                price_knock_out,
                                date,
                                event_sender,
                            )
                            .await?;

                        break;
                    }
                }
            }
        }

        for (ticker, (_, reserved_date)) in context.portfolio.reserved_cash.clone() {
            if *date > reserved_date {
                let kline = fetch_stock_kline(&ticker, StockDividendAdjust::ForwardProp).await?;
                if let (Some((_, reserved_price)), Some((_, price))) = (
                    kline.get_value::<f64>(&reserved_date, &KlineField::Close.to_string()),
                    kline.get_value::<f64>(date, &KlineField::Low.to_string()),
                ) {
                    let price_knock_in = reserved_price * (1.0 - spike_fall_threshold);
                    if price < price_knock_in {
                        let ticker_title = get_ticker_title(&ticker).await;

                        rule_send_info(
                            rule_name,
                            &format!("[Buy Signal] {ticker_title}"),
                            date,
                            event_sender,
                        )
                        .await;

                        context
                            .position_entry_reserved_with_price(
                                &ticker,
                                price_knock_in,
                                date,
                                event_sender,
                            )
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }
}
