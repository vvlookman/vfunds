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
    utils,
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
            let date_str = utils::datetime::date_to_str(date);

            let total_value = context.calc_total_value(date).await?;
            let ticker_value = total_value / tickers.len() as f64;
            for ticker in tickers {
                let ticker_str = ticker.to_string();

                if context.portfolio.sideline_cash.contains_key(&ticker_str) {
                    context
                        .portfolio
                        .sideline_cash
                        .insert(ticker_str.to_string(), ticker_value);
                } else {
                    let kline =
                        fetch_stock_kline(&ticker, StockDividendAdjust::ForwardProp).await?;
                    if let Some(price) =
                        kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
                    {
                        let holding_units =
                            context.portfolio.positions.get(&ticker_str).unwrap_or(&0);
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
                                context.portfolio.positions.insert(
                                    ticker_str.to_string(),
                                    holding_units + buy_units as u64,
                                );

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
                                context.portfolio.positions.insert(
                                    ticker_str.to_string(),
                                    holding_units - sell_units as u64,
                                );

                                let ticker_title = fetch_stock_detail(&ticker).await?.title;
                                let _ = event_sender
                                .send(BacktestEvent::Sell(format!(
                                    "[{date_str}] {ticker}({ticker_title}) {price:.2}x{sell_units} -> +${cash:.2}"
                                )))
                                .await;
                            }
                        }
                    } else {
                        context
                            .portfolio
                            .sideline_cash
                            .insert(ticker_str.to_string(), ticker_value);
                    }
                }
            }
        }

        Ok(())
    }
}
