use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS, STALE_DAYS_LONG, STALE_DAYS_SHORT,
    error::VfResult,
    filter::{
        filter_invalid::has_invalid_price, filter_market_cap::is_circulating_ratio_low,
        filter_st::is_st,
    },
    financial::stock::{StockIndicatorField, fetch_stock_indicators},
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_info, rule_send_warning,
        select_by_indicators,
    },
    ticker::Ticker,
    utils::stats::quantile,
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

        let circulating_ratio_lower = self
            .options
            .get("circulating_ratio_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let pb_quantile_upper = self
            .options
            .get("pb_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let skip_same_sector = self
            .options
            .get("skip_same_sector")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let weight_method = self
            .options
            .get("weight_method")
            .and_then(|v| v.as_str())
            .unwrap_or("equal");
        {
            if limit == 0 {
                panic!("limit must > 0");
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let mut tickers_factors: Vec<(Ticker, Factors)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;

                for ticker in tickers_map.keys() {
                    calc_count += 1;

                    if context.portfolio.reserved_cash.contains_key(ticker) {
                        continue;
                    }

                    if is_st(ticker, date, STALE_DAYS_LONG as u64).await? {
                        continue;
                    }

                    if has_invalid_price(ticker, date).await? {
                        rule_send_warning(
                            rule_name,
                            &format!("[Invalid Price] {ticker}"),
                            date,
                            event_sender,
                        )
                        .await;
                        continue;
                    }

                    if is_circulating_ratio_low(ticker, date, circulating_ratio_lower).await? {
                        continue;
                    }

                    let stock_indicators = fetch_stock_indicators(ticker).await?;
                    let market_cap_with_date = stock_indicators.get_latest_value::<f64>(
                        date,
                        STALE_DAYS_SHORT,
                        false,
                        &StockIndicatorField::MarketValueCirculating.to_string(),
                    );
                    let pb_with_date = stock_indicators.get_latest_value::<f64>(
                        date,
                        STALE_DAYS_SHORT,
                        false,
                        &StockIndicatorField::Pb.to_string(),
                    );

                    if let Some(fail_factor_name) = match (market_cap_with_date, pb_with_date) {
                        (None, _) => Some("market_cap"),
                        (_, None) => Some("pb"),
                        (Some((_, market_cap)), Some((_, pb))) => {
                            tickers_factors.push((ticker.clone(), Factors { market_cap, pb }));

                            None
                        }
                    } {
                        rule_send_warning(
                            rule_name,
                            &format!("[Σ '{fail_factor_name}' Failed] {ticker}"),
                            date,
                            event_sender,
                        )
                        .await;
                    }

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        rule_notify_calc_progress(
                            rule_name,
                            calc_count as f64 / tickers_map.len() as f64 * 100.0,
                            date,
                            event_sender,
                        )
                        .await;

                        last_time = Instant::now();
                    }
                }

                rule_notify_calc_progress(rule_name, 100.0, date, event_sender).await;
            }

            let factors_pb = tickers_factors
                .iter()
                .map(|(_, f)| f.pb)
                .collect::<Vec<f64>>();
            let pb_upper = quantile(&factors_pb, pb_quantile_upper);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            for (ticker, factors) in tickers_factors {
                if let Some(pb_upper) = pb_upper {
                    if factors.pb > pb_upper {
                        continue;
                    }
                }

                if factors.market_cap > 0.0 {
                    indicators.push((ticker, factors.market_cap / 1e4));
                }
            }
            indicators.sort_by(|a, b| a.1.total_cmp(&b.1));

            rule_send_info(
                rule_name,
                &format!("[Universe] {}({})", tickers_map.len(), indicators.len()),
                date,
                event_sender,
            )
            .await;

            let (targets_indicators, candidates_indicators) =
                select_by_indicators(&indicators, limit as usize, skip_same_sector).await?;

            rule_notify_indicators(
                rule_name,
                &targets_indicators
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &candidates_indicators
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                date,
                event_sender,
            )
            .await;

            let weights = calc_weights(&targets_indicators, weight_method)?;
            context.rebalance(&weights, date, event_sender).await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Factors {
    market_cap: f64,
    pb: f64,
}
