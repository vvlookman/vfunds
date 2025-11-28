use std::{collections::HashMap, str::FromStr};

use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate};
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::{
        KlineField, get_ticker_title,
        index::fetch_index_tickers,
        stock::{
            StockDividendAdjust, StockReportCapitalField, fetch_stock_kline,
            fetch_stock_report_capital,
        },
        tool::{calc_stock_pe_ttm, calc_stock_ps_ttm},
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

    valuation_indicators_cache: HashMap<(TickersIndex, i64), (f64, f64)>,
}

impl Executor {
    pub fn new(definition: &RuleDefinition) -> Self {
        Self {
            options: definition.options.clone(),

            valuation_indicators_cache: HashMap::new(),
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
        let ps_quantile_lower = self
            .options
            .get("ps_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.4);
        let ps_quantile_upper = self
            .options
            .get("ps_quantile_upper")
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
        let watch_period_days = self
            .options
            .get("watch_period_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(28);
        {
            if lookback_years == 0 {
                panic!("lookback_years must > 0");
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

            if !(0.0..=1.0).contains(&ps_quantile_upper) {
                panic!("ps_quantile_upper must >= 0 and <= 1");
            }

            if !(0.0..=1.0).contains(&ps_quantile_lower) {
                panic!("ps_quantile_lower must >= 0 and <= 1");
            }

            if ps_quantile_upper < ps_quantile_lower {
                panic!("ps_quantile_upper must >= ps_quantile_lower");
            }

            if watch_period_days == 0 {
                panic!("watch_period_days must > 0");
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
                    let valuation_indicators = self
                        .calc_valuation_indicators(
                            &watch_index,
                            &date_from,
                            date,
                            watch_period_days as i64,
                            event_sender,
                        )
                        .await?;
                    let pe_values: Vec<f64> =
                        valuation_indicators.iter().map(|(_, pe, _)| *pe).collect();
                    let ps_values: Vec<f64> =
                        valuation_indicators.iter().map(|(_, _, ps)| *ps).collect();

                    if context.portfolio.positions.contains_key(ticker) {
                        if let (
                            Some(pe),
                            Some(pe_overvalued),
                            Some(pe_sell),
                            Some(ps),
                            Some(ps_overvalued),
                            Some(ps_sell),
                        ) = (
                            pe_values.last(),
                            quantile(&pe_values, (pe_quantile_upper - 0.1).max(0.0)),
                            quantile(&pe_values, pe_quantile_upper),
                            ps_values.last(),
                            quantile(&ps_values, (ps_quantile_upper - 0.1).max(0.0)),
                            quantile(&ps_values, ps_quantile_upper),
                        ) {
                            debug!(
                                "[{date_str}] {ticker} pe={pe:.2} pe_overvalued={pe_overvalued:.2} pe_sell={pe_sell:.2} ps={ps:.2}  ps_overvalued={ps_overvalued:.2} ps_sell={ps_sell:.2}"
                            );
                            if *pe > pe_overvalued || *ps > ps_overvalued {
                                let ticker_title =
                                    get_ticker_title(ticker).await.unwrap_or_default();

                                if *pe > pe_sell || *ps > ps_sell {
                                    rule_send_info(
                                        rule_name,
                                        &format!("[Sell Signal] {ticker}({ticker_title}) PE:{pe:.2}>{pe_sell:.2} || PS:{ps:.2}>{ps_sell:.2}"),
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
                                        &format!("[Overvalued Warn] {ticker}({ticker_title}) PE:{pe:.2}>{pe_overvalued:.2}~{pe_sell:.2} || PS:{ps:.2}>{ps_overvalued:.2}~{ps_sell:.2}"),
                                        date,
                                        event_sender,
                                        )
                                    .await;
                                }
                            }
                        }
                    } else {
                        if let (
                            Some(pe),
                            Some(pe_undervalued),
                            Some(pe_buy),
                            Some(ps),
                            Some(ps_undervalued),
                            Some(ps_buy),
                        ) = (
                            pe_values.last(),
                            quantile(&pe_values, (pe_quantile_lower + 0.1).min(1.0)),
                            quantile(&pe_values, pe_quantile_lower),
                            ps_values.last(),
                            quantile(&ps_values, (ps_quantile_lower + 0.1).min(1.0)),
                            quantile(&ps_values, ps_quantile_lower),
                        ) {
                            debug!(
                                "[{date_str}] {ticker} pe={pe:.2} pe_undervalued={pe_undervalued:.2} pe_buy={pe_buy:.2} ps={ps:.2} ps_undervalued={ps_undervalued:.2} ps_buy={ps_buy:.2}"
                            );
                            if *pe < pe_undervalued && *ps < ps_undervalued {
                                let ticker_title =
                                    get_ticker_title(ticker).await.unwrap_or_default();

                                if *pe < pe_buy && *ps < ps_buy {
                                    rule_send_info(
                                        rule_name,
                                        &format!("[Buy Signal] {ticker}({ticker_title}) PE:{pe:.2}<{pe_buy:.2} && PS:{ps:.2}<{ps_buy:.2}"),
                                        date,
                                        event_sender,
                                    )
                                    .await;

                                    context
                                        .position_open_reserved(ticker, date, event_sender)
                                        .await?;
                                } else {
                                    rule_send_info(
                                        rule_name,
                                        &format!("[Undervalued Warn] {ticker}({ticker_title}) PE:{pe:.2}<{pe_undervalued:.2}~{pe_buy:.2} && PS:{ps:.2}<{ps_undervalued:.2}~{ps_buy:.2}"),
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

impl Executor {
    async fn calc_valuation_indicators(
        &mut self,
        index: &TickersIndex,
        date_from: &NaiveDate,
        date_to: &NaiveDate,
        watch_period_days: i64,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<Vec<(NaiveDate, f64, f64)>> {
        let mut valuation_indicators: Vec<(NaiveDate, f64, f64)> = vec![];

        let rule_name = mod_name!();

        let watch_days = date_to.signed_duration_since(*date_from).num_days();
        let period_count = (watch_days as f64 / watch_period_days as f64).ceil() as i64;
        for i in 0..period_count {
            let watch_date = *date_to - Duration::days((period_count - i) * watch_period_days);
            let watch_cache_idx =
                (watch_date.to_epoch_days() as f64 / watch_period_days as f64).round() as i64;

            if let Some((pe_ttm, ps_ttm)) = self
                .valuation_indicators_cache
                .get(&(index.clone(), watch_cache_idx))
            {
                valuation_indicators.push((watch_date, *pe_ttm, *ps_ttm));
                continue;
            }

            let tickers = fetch_index_tickers(index, &watch_date).await?;

            let mut last_time = Instant::now();
            let mut calc_count: usize = 0;

            let mut market_cap_sum = 0.0;
            let mut earning_ttm_sum = 0.0;
            let mut revenue_ttm_sum = 0.0;
            for ticker in &tickers {
                calc_count += 1;

                let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
                let report_capital = fetch_stock_report_capital(ticker).await?;

                if let (Some((_, price)), Some((_, total_captical))) = (
                    kline.get_latest_value::<f64>(
                        &watch_date,
                        false,
                        &KlineField::Close.to_string(),
                    ),
                    report_capital.get_latest_value::<f64>(
                        &watch_date,
                        false,
                        &StockReportCapitalField::Total.to_string(),
                    ),
                ) {
                    if let (Some(pe_ttm), Some(ps_ttm)) = (
                        calc_stock_pe_ttm(ticker, &watch_date).await?,
                        calc_stock_ps_ttm(ticker, &watch_date).await?,
                    ) {
                        let market_cap = price * total_captical;

                        market_cap_sum += market_cap;
                        earning_ttm_sum += market_cap / pe_ttm;
                        revenue_ttm_sum += market_cap / ps_ttm;
                    }
                }

                if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                    rule_notify_calc_progress(
                        rule_name,
                        calc_count as f64 / tickers.len() as f64 * 100.0,
                        &watch_date,
                        event_sender,
                    )
                    .await;

                    last_time = Instant::now();
                }
            }

            if earning_ttm_sum > 0.0 && revenue_ttm_sum > 0.0 {
                let pe_ttm = market_cap_sum / earning_ttm_sum;
                let ps_ttm = market_cap_sum / revenue_ttm_sum;

                valuation_indicators.push((watch_date, pe_ttm, ps_ttm));

                self.valuation_indicators_cache
                    .insert((index.clone(), watch_cache_idx), (pe_ttm, ps_ttm));
            }
        }

        Ok(valuation_indicators)
    }
}
