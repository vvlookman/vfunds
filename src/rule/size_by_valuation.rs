use std::{collections::HashMap, str::FromStr};

use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate};
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::stock::{
        StockDividendAdjust, StockKlineField, StockReportCapitalField, StockReportIncomeField,
        StockReportPershareField, fetch_stock_detail, fetch_stock_kline,
        fetch_stock_report_capital, fetch_stock_report_income, fetch_stock_report_pershare,
    },
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
    ticker::{Ticker, TickersIndex},
    utils::{
        datetime::{FiscalQuarter, date_from_str, date_to_fiscal_quarter, date_to_str},
        stats::quantile,
    },
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
        context: &mut BacktestContext,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
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
        let pe_quantile_ceil = self
            .options
            .get("pe_quantile_ceil")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.8);
        let pe_quantile_floor = self
            .options
            .get("pe_quantile_floor")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.4);
        let ps_quantile_ceil = self
            .options
            .get("ps_quantile_ceil")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.8);
        let ps_quantile_floor = self
            .options
            .get("ps_quantile_floor")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.4);
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

            if !(0.0..=1.0).contains(&pe_quantile_ceil) {
                panic!("pe_quantile_ceil must >= 0 and <= 1");
            }

            if !(0.0..=1.0).contains(&pe_quantile_floor) {
                panic!("pe_quantile_floor must >= 0 and <= 1");
            }

            if pe_quantile_ceil < pe_quantile_floor {
                panic!("pe_quantile_ceil must >= pe_quantile_floor");
            }

            if !(0.0..=1.0).contains(&ps_quantile_ceil) {
                panic!("ps_quantile_ceil must >= 0 and <= 1");
            }

            if !(0.0..=1.0).contains(&ps_quantile_floor) {
                panic!("ps_quantile_floor must >= 0 and <= 1");
            }

            if ps_quantile_ceil < ps_quantile_floor {
                panic!("ps_quantile_ceil must >= ps_quantile_floor");
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
                let watch_index: Option<TickersIndex> =
                    if let Some(index) = ticker_watch_index_map.get(ticker) {
                        Some(index.clone())
                    } else {
                        if let Some((_, Some(ticker_source))) = tickers_map.get(ticker) {
                            ticker_source_watch_index_map.get(ticker_source).cloned()
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
                            event_sender.clone(),
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
                            quantile(&pe_values, (pe_quantile_ceil - 0.1).max(0.0)),
                            quantile(&pe_values, pe_quantile_ceil),
                            ps_values.last(),
                            quantile(&ps_values, (ps_quantile_ceil - 0.1).max(0.0)),
                            quantile(&ps_values, ps_quantile_ceil),
                        ) {
                            debug!(
                                "[{date_str}] {ticker} pe={pe:.2} pe_overvalued={pe_overvalued:.2} pe_sell={pe_sell:.2} ps={ps:.2}  ps_overvalued={ps_overvalued:.2} ps_sell={ps_sell:.2}"
                            );
                            if *pe > pe_overvalued || *ps > ps_overvalued {
                                let ticker_title = fetch_stock_detail(ticker).await?.title;

                                if *pe > pe_sell || *ps > ps_sell {
                                    let _ = event_sender
                                    .send(BacktestEvent::Info(format!(
                                        "[{date_str}] [{rule_name}] [Sell Signal] {ticker}({ticker_title}) PE:{pe:.2}>{pe_sell:.2} || PS:{ps:.2}>{ps_sell:.2}"
                                    )))
                                    .await;

                                    context
                                        .position_exit(ticker, true, date, event_sender.clone())
                                        .await?;

                                    if !allow_short {
                                        context
                                            .cash_deploy_free(date, event_sender.clone())
                                            .await?;
                                    }
                                } else {
                                    let _ = event_sender
                                    .send(BacktestEvent::Info(format!(
                                        "[{date_str}] [{rule_name}] [Warn Overvalued] {ticker}({ticker_title}) PE:{pe:.2}>{pe_overvalued:.2}~{pe_sell:.2} || PS:{ps:.2}>{ps_overvalued:.2}~{ps_sell:.2}"
                                    )))
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
                            quantile(&pe_values, (pe_quantile_floor + 0.1).min(1.0)),
                            quantile(&pe_values, pe_quantile_floor),
                            ps_values.last(),
                            quantile(&ps_values, (ps_quantile_floor + 0.1).min(1.0)),
                            quantile(&ps_values, ps_quantile_floor),
                        ) {
                            debug!(
                                "[{date_str}] {ticker} pe={pe:.2} pe_undervalued={pe_undervalued:.2} pe_buy={pe_buy:.2} ps={ps:.2} ps_undervalued={ps_undervalued:.2} ps_buy={ps_buy:.2}"
                            );
                            if *pe < pe_undervalued && *ps < ps_undervalued {
                                let ticker_title = fetch_stock_detail(ticker).await?.title;

                                if *pe < pe_buy && *ps < ps_buy {
                                    let _ = event_sender
                                    .send(BacktestEvent::Info(format!(
                                        "[{date_str}] [{rule_name}] [Buy Signal] {ticker}({ticker_title}) PE:{pe:.2}<{pe_buy:.2} && PS:{ps:.2}<{ps_buy:.2}"
                                    )))
                                    .await;

                                    context
                                        .position_init_reserved(ticker, date, event_sender.clone())
                                        .await?;
                                } else {
                                    let _ = event_sender
                                    .send(BacktestEvent::Info(format!(
                                        "[{date_str}] [{rule_name}] [Warn Undervalued] {ticker}({ticker_title}) PE:{pe:.2}<{pe_undervalued:.2}~{pe_buy:.2} && PS:{ps:.2}<{ps_undervalued:.2}~{ps_buy:.2}"
                                    )))
                                    .await;
                                }
                            }
                        }
                    }
                }

                calc_count += 1;

                if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                    let calc_progress_pct =
                        calc_count as f64 / watching_tickers.len() as f64 * 100.0;
                    let _ = event_sender
                        .send(BacktestEvent::Toast(format!(
                            "[{date_str}] [{rule_name}] Σ {calc_progress_pct:.2}% ..."
                        )))
                        .await;

                    last_time = Instant::now();
                }
            }

            let _ = event_sender
                .send(BacktestEvent::Toast(format!(
                    "[{date_str}] [{rule_name}] Σ 100%"
                )))
                .await;
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
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<Vec<(NaiveDate, f64, f64)>> {
        let mut valuation_indicators: Vec<(NaiveDate, f64, f64)> = vec![];

        let rule_name = mod_name!();
        let date_str = date_to_str(date_to);

        let watch_days = date_to.signed_duration_since(*date_from).num_days() + 1;
        let period_count = (watch_days as f64 / watch_period_days as f64).ceil() as i64;
        for i in 0..period_count {
            let watch_date = *date_to - Duration::days((period_count - i - 1) * watch_period_days);
            let watch_cache_idx =
                (watch_date.to_epoch_days() as f64 / watch_period_days as f64).round() as i64;

            if let Some((pe_ttm, ps_ttm)) = self
                .valuation_indicators_cache
                .get(&(index.clone(), watch_cache_idx))
            {
                valuation_indicators.push((watch_date, *pe_ttm, *ps_ttm));
                continue;
            }

            let tickers = index.all_tickers(&watch_date).await?;
            let watch_date_str = date_to_str(&watch_date);

            let mut last_time = Instant::now();
            let mut calc_count: usize = 0;

            let mut market_cap_sum = 0.0;
            let mut earning_ttm_sum = 0.0;
            let mut revenue_ttm_sum = 0.0;
            for ticker in &tickers {
                let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
                let report_capital = fetch_stock_report_capital(ticker).await?;
                let report_income = fetch_stock_report_income(ticker).await?;
                let report_pershare = fetch_stock_report_pershare(ticker).await?;

                if let (Some(price), Some(total_captical), revenues, epss) = (
                    kline.get_latest_value::<f64>(&watch_date, &StockKlineField::Close.to_string()),
                    report_capital.get_latest_value::<f64>(
                        &watch_date,
                        &StockReportCapitalField::Total.to_string(),
                    ),
                    report_income.get_latest_values_with_label::<f64>(
                        &watch_date,
                        &StockReportIncomeField::Revenue.to_string(),
                        &StockReportIncomeField::ReportDate.to_string(),
                        5,
                    ),
                    report_pershare.get_latest_values_with_label::<f64>(
                        &watch_date,
                        &StockReportPershareField::Eps.to_string(),
                        &StockReportIncomeField::ReportDate.to_string(),
                        5,
                    ),
                ) {
                    let mut fiscal_epss: Vec<(FiscalQuarter, f64)> = vec![];
                    {
                        let mut current_fiscal_quarter: Option<FiscalQuarter> = None;
                        for (eps, date_label) in epss {
                            if let Some(date_label_str) = date_label {
                                if let Ok(date) = date_from_str(&date_label_str) {
                                    let fiscal_quarter = date_to_fiscal_quarter(&date);

                                    if let Some(current_fiscal_quarter) = current_fiscal_quarter {
                                        if fiscal_quarter.prev() != current_fiscal_quarter {
                                            // Reports must be consistent
                                            break;
                                        }
                                    }

                                    current_fiscal_quarter = Some(fiscal_quarter.clone());
                                    fiscal_epss.push((fiscal_quarter, eps));
                                }
                            }
                        }
                    }

                    let mut fiscal_revenues: Vec<(FiscalQuarter, f64)> = vec![];
                    {
                        let mut current_fiscal_quarter: Option<FiscalQuarter> = None;
                        for (revenue, date_label) in revenues {
                            if let Some(date_label_str) = date_label {
                                if let Ok(date) = date_from_str(&date_label_str) {
                                    let fiscal_quarter = date_to_fiscal_quarter(&date);

                                    if let Some(current_fiscal_quarter) = current_fiscal_quarter {
                                        if fiscal_quarter.prev() != current_fiscal_quarter {
                                            // Reports must be consistent
                                            break;
                                        }
                                    }

                                    current_fiscal_quarter = Some(fiscal_quarter.clone());
                                    fiscal_revenues.push((fiscal_quarter, revenue));
                                }
                            }
                        }
                    }

                    if fiscal_epss.len() == 5 && fiscal_revenues.len() == 5 {
                        let mut eps_ttm = 0.0;
                        for i in 1..5 {
                            let (_, prev_eps) = &fiscal_epss[i - 1];
                            let (fiscal_quarter, eps) = &fiscal_epss[i];

                            let quarter_eps: f64 = if fiscal_quarter.quarter == 1 {
                                *eps
                            } else {
                                eps - prev_eps
                            };

                            eps_ttm += quarter_eps;
                        }

                        let mut revenue_ttm = 0.0;
                        for i in 1..5 {
                            let (_, prev_revenue) = &fiscal_revenues[i - 1];
                            let (fiscal_quarter, revenue) = &fiscal_revenues[i];

                            let quarter_revenue: f64 = if fiscal_quarter.quarter == 1 {
                                *revenue
                            } else {
                                revenue - prev_revenue
                            };

                            revenue_ttm += quarter_revenue;
                        }

                        market_cap_sum += price * total_captical;
                        earning_ttm_sum += eps_ttm * total_captical;
                        revenue_ttm_sum += revenue_ttm;
                    }
                }

                calc_count += 1;

                if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                    let calc_progress_pct = calc_count as f64 / tickers.len() as f64 * 100.0;
                    let _ = event_sender
                    .send(BacktestEvent::Toast(format!(
                        "[{date_str}] [{rule_name}] [{index} {watch_date_str} {i}/{period_count}] Σ {calc_progress_pct:.2}% ..."
                    )))
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
