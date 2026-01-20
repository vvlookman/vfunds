use std::{collections::HashMap, str::FromStr};

use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate};
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::{
        get_ticker_title,
        index::{IndexIndicatorField, fetch_index_indicators},
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor,
        rule_notify_calc_progress, rule_send_info,
    },
    spec::TickerSourceType,
    ticker::{Ticker, TickersIndex},
    utils::{datetime::date_to_str, stats::quantile},
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
        let lookback_years = self
            .options
            .get("lookback_years")
            .and_then(|v| v.as_u64())
            .unwrap_or(5);
        let pb_quantile_lower = self
            .options
            .get("pb_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.4);
        let pb_quantile_upper = self
            .options
            .get("pb_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.8);
        let pe_quantile_lower = self
            .options
            .get("pe_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.4);
        let pe_quantile_upper = self
            .options
            .get("pe_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.8);
        let ticker_watch_index = self
            .options
            .get("ticker_watch_index")
            .and_then(|v| v.as_object());
        let ticker_source_watch_index = self
            .options
            .get("ticker_source_watch_index")
            .and_then(|v| v.as_object());
        {
            if lookback_years == 0 {
                panic!("lookback_years must > 0");
            }

            if !(0.0..=1.0).contains(&pb_quantile_upper) {
                panic!("pb_quantile_upper must >= 0 and <= 1");
            }

            if !(0.0..=1.0).contains(&pb_quantile_lower) {
                panic!("pb_quantile_lower must >= 0 and <= 1");
            }

            if pb_quantile_upper < pb_quantile_lower {
                panic!("pb_quantile_upper must >= pb_quantile_lower");
            }

            if !(0.0..=1.0).contains(&pe_quantile_upper) {
                panic!("pe_quantile_upper must >= 0 and <= 1");
            }

            if !(0.0..=1.0).contains(&pe_quantile_lower) {
                panic!("pe_quantile_lower must >= 0 and <= 1");
            }

            if pe_quantile_upper < pe_quantile_lower {
                panic!("pe_quantile_upper must >= pe_quantile_lower");
            }
        }

        let mut ticker_watch_index_map: HashMap<Ticker, TickersIndex> = HashMap::new();
        if let Some(ticker_watch_index) = ticker_watch_index {
            for (k, v) in ticker_watch_index {
                if let Some(v_str) = v.as_str() {
                    if let (Ok(ticker), Ok(watch_index)) =
                        (Ticker::from_str(k), TickersIndex::from_str(v_str))
                    {
                        ticker_watch_index_map.insert(ticker, watch_index);
                    }
                }
            }
        }

        let mut ticker_source_watch_index_map: HashMap<TickersIndex, TickersIndex> = HashMap::new();
        if let Some(ticker_source_watch_index) = ticker_source_watch_index {
            for (k, v) in ticker_source_watch_index {
                if let Some(v_str) = v.as_str() {
                    if let (Ok(ticker_source), Ok(watch_index)) =
                        (TickersIndex::from_str(k), TickersIndex::from_str(v_str))
                    {
                        ticker_source_watch_index_map.insert(ticker_source, watch_index);
                    }
                }
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;

        let watching_tickers = context.watching_tickers();
        if !watching_tickers.is_empty() {
            let date_str = date_to_str(date);
            let date_from =
                date.with_year(date.year() - lookback_years as i32).unwrap() + Duration::days(1);
            let date_to = *date - Duration::days(1);

            let mut last_time = Instant::now();
            let mut calc_count: usize = 0;
            for ticker in &watching_tickers {
                calc_count += 1;

                let watch_index: Option<TickersIndex> =
                    if let Some(index) = ticker_watch_index_map.get(ticker) {
                        Some(index.clone())
                    } else {
                        if let Some((_, Some(ticker_source))) = tickers_map.get(ticker) {
                            match ticker_source.source_type {
                                TickerSourceType::Index => {
                                    if let Ok(tickers_index) =
                                        TickersIndex::from_str(&ticker_source.source)
                                    {
                                        ticker_source_watch_index_map.get(&tickers_index).cloned()
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            }
                        } else {
                            None
                        }
                    };

                if let Some(watch_index) = watch_index {
                    let index_indicators = fetch_index_indicators(&watch_index).await?;
                    let lookback_index_indicators =
                        index_indicators.slice_by_date_range(&date_from, &date_to)?;

                    let pb_values = lookback_index_indicators
                        .get_values::<f64>(&IndexIndicatorField::Pb.to_string())
                        .iter()
                        .map(|(_, v)| *v)
                        .collect::<Vec<f64>>();
                    let pe_values = lookback_index_indicators
                        .get_values::<f64>(&IndexIndicatorField::Pe.to_string())
                        .iter()
                        .map(|(_, v)| *v)
                        .collect::<Vec<f64>>();

                    if context.portfolio.positions.contains_key(ticker) {
                        if let (
                            Some(pb),
                            Some(pb_overvalued),
                            Some(pb_sell),
                            Some(pe),
                            Some(pe_overvalued),
                            Some(pe_sell),
                        ) = (
                            pb_values.last(),
                            quantile(&pb_values, (pb_quantile_upper - 0.1).max(0.0)),
                            quantile(&pb_values, pb_quantile_upper),
                            pe_values.last(),
                            quantile(&pe_values, (pe_quantile_upper - 0.1).max(0.0)),
                            quantile(&pe_values, pe_quantile_upper),
                        ) {
                            debug!(
                                "[{date_str}] {ticker} pb={pb:.2}  pb_overvalued={pb_overvalued:.2} pb_sell={pb_sell:.2} pe={pe:.2} pe_overvalued={pe_overvalued:.2} pe_sell={pe_sell:.2}"
                            );
                            if *pb > pb_overvalued || *pe > pe_overvalued {
                                let ticker_title = get_ticker_title(ticker).await;

                                if *pb > pb_sell || *pe > pe_sell {
                                    rule_send_info(
                                        rule_name,
                                        &format!("[Sell Signal] {ticker_title} PB:{pb:.2}>{pb_sell:.2} || PE:{pe:.2}>{pe_sell:.2}"),
                                        date,
                                        event_sender,
                                    )
                                    .await;

                                    context
                                        .position_close(ticker, true, date, event_sender)
                                        .await?;

                                    if !allow_short {
                                        context.cash_deploy_free(date, event_sender).await?;
                                    }
                                } else {
                                    rule_send_info(
                                        rule_name,
                                        &format!("[Overvalued Warn] {ticker_title} PB:{pb:.2}>{pb_overvalued:.2}~{pb_sell:.2} ||  PE:{pe:.2}>{pe_overvalued:.2}~{pe_sell:.2}"),
                                        date,
                                        event_sender,
                                        )
                                    .await;
                                }
                            }
                        }
                    } else {
                        if let (
                            Some(pb),
                            Some(pb_undervalued),
                            Some(pb_buy),
                            Some(pe),
                            Some(pe_undervalued),
                            Some(pe_buy),
                        ) = (
                            pb_values.last(),
                            quantile(&pb_values, (pb_quantile_lower + 0.1).min(1.0)),
                            quantile(&pb_values, pb_quantile_lower),
                            pe_values.last(),
                            quantile(&pe_values, (pe_quantile_lower + 0.1).min(1.0)),
                            quantile(&pe_values, pe_quantile_lower),
                        ) {
                            debug!(
                                "[{date_str}] {ticker} pb={pb:.2} pb_undervalued={pb_undervalued:.2} pb_buy={pb_buy:.2} pe={pe:.2} pe_undervalued={pe_undervalued:.2} pe_buy={pe_buy:.2}"
                            );
                            if *pb < pb_undervalued && *pe < pe_undervalued {
                                let ticker_title = get_ticker_title(ticker).await;

                                if *pb < pb_buy && *pe < pe_buy {
                                    rule_send_info(
                                        rule_name,
                                        &format!("[Buy Signal] {ticker_title} PB:{pb:.2}<{pb_buy:.2} && PE:{pe:.2}<{pe_buy:.2}"),
                                        date,
                                        event_sender,
                                    )
                                    .await;

                                    context
                                        .position_entry_reserved(ticker, date, event_sender)
                                        .await?;
                                } else {
                                    rule_send_info(
                                        rule_name,
                                        &format!("[Undervalued Warn] {ticker}({ticker_title}) PB:{pb:.2}<{pb_undervalued:.2}~{pb_buy:.2} && PE:{pe:.2}<{pe_undervalued:.2}~{pe_buy:.2}"),
                                        date,
                                        event_sender,
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                }

                if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                    rule_notify_calc_progress(
                        rule_name,
                        calc_count as f64 / watching_tickers.len() as f64 * 100.0,
                        date,
                        event_sender,
                    )
                    .await;

                    last_time = Instant::now();
                }
            }

            rule_notify_calc_progress(rule_name, 100.0, date, event_sender).await;
        }

        Ok(())
    }
}
