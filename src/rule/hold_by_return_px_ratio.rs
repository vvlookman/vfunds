use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::{
        KlineField,
        stock::{StockDetail, StockDividendAdjust, fetch_stock_detail, fetch_stock_kline},
        tool::{calc_stock_pb, calc_stock_pe_ttm, calc_stock_ps_ttm},
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, notify_calc_progress,
        notify_tickers_indicator,
    },
    ticker::Ticker,
    utils::{
        financial::{TRADE_DAYS_PER_YEAR, calc_annualized_return_rate},
        math::signed_powf,
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
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let rule_name = mod_name!();

        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let lookback_years = self
            .options
            .get("lookback_years")
            .and_then(|v| v.as_u64())
            .unwrap_or(3);
        let min_trade_days = self
            .options
            .get("min_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(126);
        let px = self
            .options
            .get("px")
            .and_then(|v| v.as_str())
            .unwrap_or("pe");
        let skip_same_sector = self
            .options
            .get("skip_same_sector")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let weight_exp = self
            .options
            .get("weight_exp")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        {
            if limit == 0 {
                panic!("limit must > 0");
            }

            if lookback_years == 0 {
                panic!("lookback_years must > 0");
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let mut indicators: Vec<(Ticker, f64)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;

                for ticker in tickers_map.keys() {
                    calc_count += 1;

                    if context.portfolio.reserved_cash.contains_key(ticker) {
                        continue;
                    }

                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                    let prices: Vec<f64> = kline
                        .get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Close.to_string(),
                            (lookback_years as f64 * TRADE_DAYS_PER_YEAR) as u32,
                        )
                        .iter()
                        .map(|&(_, v)| v)
                        .collect();
                    if prices.len() < min_trade_days as usize {
                        continue;
                    }

                    if let Some(arr) = calc_annualized_return_rate(&prices) {
                        if arr > 0.0 {
                            if let Some(indicator) = match px {
                                "pb" => calc_stock_pb(ticker, date).await?,
                                "ps" => calc_stock_ps_ttm(ticker, date).await?,
                                _ => calc_stock_pe_ttm(ticker, date).await?,
                            } {
                                if indicator.is_finite() {
                                    indicators.push((ticker.clone(), indicator));
                                }
                            }
                        }
                    }

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        notify_calc_progress(
                            event_sender.clone(),
                            date,
                            rule_name,
                            calc_count as f64 / tickers_map.len() as f64 * 100.0,
                        )
                        .await;

                        last_time = Instant::now();
                    }
                }

                notify_calc_progress(event_sender.clone(), date, rule_name, 100.0).await;
            }

            indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

            let top_indicators = indicators
                .iter()
                .take(2 * limit as usize)
                .collect::<Vec<_>>();

            let mut tickers_detail: HashMap<Ticker, StockDetail> = HashMap::new();
            if skip_same_sector {
                for (ticker, _) in &top_indicators {
                    let detail = fetch_stock_detail(ticker).await?;
                    tickers_detail.insert(ticker.clone(), detail);
                }
            }

            let mut targets_indicator: Vec<(Ticker, f64)> = vec![];
            let mut candidates_indicator: Vec<(Ticker, f64)> = vec![];
            for (ticker, indicator) in &top_indicators {
                if targets_indicator.len() < limit as usize {
                    if skip_same_sector
                        && targets_indicator.iter().any(|(a, _)| {
                            if let (Some(Some(sector_a)), Some(Some(sector_b))) = (
                                tickers_detail.get(a).map(|v| &v.sector),
                                tickers_detail.get(ticker).map(|v| &v.sector),
                            ) {
                                sector_a == sector_b
                            } else {
                                false
                            }
                        })
                    {
                        candidates_indicator.push((ticker.clone(), *indicator));
                    } else {
                        targets_indicator.push((ticker.clone(), *indicator));
                    }
                } else {
                    candidates_indicator.push((ticker.clone(), *indicator));
                }
            }

            notify_tickers_indicator(
                event_sender.clone(),
                date,
                rule_name,
                &targets_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &candidates_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
            )
            .await;

            let mut targets_weight: Vec<(Ticker, f64)> = vec![];
            for (ticker, indicator) in &targets_indicator {
                if let Some((weight, _)) = tickers_map.get(ticker) {
                    targets_weight.push((
                        ticker.clone(),
                        (*weight) * signed_powf(*indicator, weight_exp),
                    ));
                }
            }

            context
                .rebalance(&targets_weight, date, event_sender)
                .await?;
        }

        Ok(())
    }
}
