use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::NaiveDate;
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::{
        KlineField,
        stock::{
            StockDetail, StockDividendAdjust, StockReportCapitalField, fetch_stock_detail,
            fetch_stock_kline, fetch_stock_report_capital,
        },
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, notify_calc_progress,
        notify_tickers_indicator,
    },
    ticker::Ticker,
    utils::{
        datetime::date_to_str,
        financial::calc_annualized_volatility,
        math::{normalize_zscore, signed_powf},
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

        let factor_market_cap_weight = self
            .options
            .get("factor_market_cap_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let factor_turnover_ratio_weight = self
            .options
            .get("factor_turnover_ratio_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let factor_volatility_weight = self
            .options
            .get("factor_volatility_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let lookback_trade_days = self
            .options
            .get("lookback_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(21);
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

            if lookback_trade_days == 0 {
                panic!("lookback_trade_days must > 0");
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let date_str = date_to_str(date);

            let mut factors: Vec<(Ticker, f64, f64, f64)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;

                for ticker in tickers_map.keys() {
                    calc_count += 1;

                    if context.portfolio.reserved_cash.contains_key(ticker) {
                        continue;
                    }

                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
                    let volumes: Vec<f64> = kline
                        .get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Volume.to_string(),
                            lookback_trade_days as u32,
                        )
                        .iter()
                        .map(|&(_, v)| v)
                        .collect();
                    if volumes.len() < (lookback_trade_days as f64 * 0.95).round() as usize {
                        let _ = event_sender
                            .send(BacktestEvent::Info(format!(
                                "[{date_str}] [{rule_name}] [No Enough Data] {ticker}"
                            )))
                            .await;
                        continue;
                    }

                    let prices: Vec<f64> = kline
                        .get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Close.to_string(),
                            lookback_trade_days as u32,
                        )
                        .iter()
                        .map(|&(_, v)| v)
                        .collect();

                    let report_capital = fetch_stock_report_capital(ticker).await?;
                    if let (
                        Some((_, price)),
                        Some((_, total_capital)),
                        Some((_, circulating_capital)),
                        Some(volatility),
                    ) = (
                        kline.get_latest_value::<f64>(date, false, &KlineField::Close.to_string()),
                        report_capital.get_latest_value::<f64>(
                            date,
                            false,
                            &StockReportCapitalField::Total.to_string(),
                        ),
                        report_capital.get_latest_value::<f64>(
                            date,
                            false,
                            &StockReportCapitalField::Circulating.to_string(),
                        ),
                        calc_annualized_volatility(&prices),
                    ) {
                        let volumes_avg = volumes.iter().sum::<f64>() / volumes.len() as f64;
                        let turnover_ratio = 100.0 * volumes_avg / circulating_capital;

                        let market_cap = price * total_capital;

                        factors.push((ticker.clone(), turnover_ratio, market_cap, volatility));
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

            let normalized_turnover_ratio_values =
                normalize_zscore(&factors.iter().map(|x| x.1).collect::<Vec<f64>>());
            let normalized_market_cap_values =
                normalize_zscore(&factors.iter().map(|x| x.2).collect::<Vec<f64>>());
            let normalized_volatility_values =
                normalize_zscore(&factors.iter().map(|x| x.3).collect::<Vec<f64>>());

            let mut indicators: Vec<(Ticker, f64)> = factors
                .iter()
                .enumerate()
                .filter_map(|(i, x)| {
                    let ticker = &x.0;

                    let turnover_ratio = normalized_turnover_ratio_values[i];
                    let market_cap = normalized_market_cap_values[i];
                    let volatility = normalized_volatility_values[i];

                    let indicator = factor_turnover_ratio_weight * (1.0 - turnover_ratio.tanh())
                        + factor_market_cap_weight * (1.0 + market_cap.tanh())
                        + factor_volatility_weight * (1.0 - volatility.tanh());
                    debug!("[{date_str}] {ticker}={indicator:.4} (Turnover={turnover_ratio:.4} MarketCap={market_cap:.4} Vol={volatility:.4})");

                    if indicator.is_finite() {
                        Some((ticker.clone(), indicator))
                    } else {
                        None
                    }
                })
                .collect();

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
