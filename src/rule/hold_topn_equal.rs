use std::{cmp::Ordering, collections::HashMap, str::FromStr};

use async_trait::async_trait;
use chrono::NaiveDate;
use log::debug;
use tokio::sync::mpsc::Sender;

use crate::{
    backtest::{calc_buy_fee, calc_sell_fee},
    error::VfResult,
    financial::stock::{
        StockField, fetch_stock_daily_backward_adjusted_price, fetch_stock_daily_indicators,
        fetch_stock_detail,
    },
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
    ticker::Ticker,
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
            let indicator = self
                .options
                .get("indicator")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let sort = self
                .options
                .get("sort")
                .and_then(|v| v.as_str())
                .unwrap_or("desc")
                .to_lowercase();
            let limit = self
                .options
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10);

            let date_str = utils::datetime::date_to_str(date);

            let indicator_field = StockField::from_str(indicator)?;
            let mut stocks_indicator: Vec<(String, f64)> = vec![];
            for ticker in tickers {
                let ticker_str = ticker.to_string();

                let stock_daily = fetch_stock_daily_indicators(&ticker).await?;
                if let Some(val) =
                    stock_daily.get_latest_value::<f64>(date, &indicator_field.to_string())
                {
                    stocks_indicator.push((ticker_str, val));
                    debug!("[{indicator}][{date_str}] {ticker} = {val}");
                }
            }

            match sort.as_str() {
                "desc" => stocks_indicator
                    .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal)),
                _ => stocks_indicator
                    .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal)),
            };

            {
                let mut top_tickers_str = String::from("");
                for (ticker_str, indicator) in stocks_indicator.iter().take(2 * limit as usize) {
                    let ticker = Ticker::from_str(ticker_str)?;
                    let ticker_title = fetch_stock_detail(&ticker).await?.title;

                    top_tickers_str.push_str(&format!("{ticker}({ticker_title})={indicator:.2} "));
                }

                let _ = event_sender
                    .send(BacktestEvent::Info(format!(
                        "[{date_str}] {top_tickers_str}"
                    )))
                    .await;
            }

            let selected_tickers: Vec<_> = stocks_indicator
                .iter()
                .take(limit as usize)
                .map(|x| x.0.to_string())
                .collect();

            let holding_tickers: Vec<_> = context.portfolio.positions.keys().cloned().collect();
            for ticker_str in &holding_tickers {
                if !selected_tickers.contains(ticker_str) {
                    let ticker = Ticker::from_str(ticker_str)?;

                    let stock_daily = fetch_stock_daily_backward_adjusted_price(&ticker).await?;
                    if let Some(price) =
                        stock_daily.get_latest_value::<f64>(date, &StockField::Price.to_string())
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

                let stock_daily = fetch_stock_daily_backward_adjusted_price(&ticker).await?;
                if let Some(price) =
                    stock_daily.get_latest_value::<f64>(date, &StockField::Price.to_string())
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
