use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS, STALE_DAYS_LONG,
    error::VfResult,
    filter::{
        filter_invalid::has_invalid_price, filter_market_cap::is_circulating_ratio_low,
        filter_st::is_st,
    },
    financial::helper::{calc_stock_market_cap, calc_stock_pb, calc_stock_ps_ttm},
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_info, rule_send_warning,
        select_by_indicators,
    },
    spec::RuleOptions,
    ticker::Ticker,
    utils::stats::quantile_value,
};

pub struct Executor {
    #[allow(dead_code)]
    options: RuleOptions,
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

        let circulating_ratio_lower =
            self.options
                .read_f64_in_range("circulating_ratio_lower", 0.0, 0.0..=1.0);
        let limit = self.options.read_u64_no_zero("limit", 5);
        let pb_quantile_upper = self
            .options
            .read_f64_in_range("pb_quantile_upper", 1.0, 0.0..=1.0);
        let pre_select_ratio = self.options.read_u64_no_zero("pre_select_ratio", 20);
        let ps_quantile_upper = self
            .options
            .read_f64_in_range("ps_quantile_upper", 1.0, 0.0..=1.0);
        let skip_same_sector = self.options.read_bool("skip_same_sector", false);
        let weight_method = self.options.read_str("weight_method", "equal");

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let mut tickers_factors: Vec<(Ticker, Factors)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;

                for ticker in tickers_map.keys() {
                    calc_count += 1;

                    if let Ok(st) = is_st(ticker, date, STALE_DAYS_LONG as u64).await {
                        if st {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    if let Ok(invalid_price) = has_invalid_price(ticker, date).await {
                        if invalid_price {
                            rule_send_warning(
                                rule_name,
                                &format!("[Invalid Price] {ticker}"),
                                date,
                                event_sender,
                            )
                            .await;
                            continue;
                        }
                    } else {
                        continue;
                    }

                    if let Ok(circulating_ratio_low) =
                        is_circulating_ratio_low(ticker, date, circulating_ratio_lower).await
                    {
                        if circulating_ratio_low {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    if let Ok(market_cap) = calc_stock_market_cap(ticker, date, true).await {
                        tickers_factors.push((
                            ticker.clone(),
                            Factors {
                                market_cap,
                                pb: calc_stock_pb(ticker, date).await.ok(),
                                ps_ttm: calc_stock_ps_ttm(ticker, date).await.ok(),
                            },
                        ));
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
            tickers_factors.sort_by(|(_, f1), (_, f2)| f1.market_cap.total_cmp(&f2.market_cap));

            let pre_select_count = (pre_select_ratio * limit) as usize;
            let pre_select_tickers_factors = tickers_factors
                .into_iter()
                .take(pre_select_count)
                .collect::<Vec<_>>();

            let factors_pb = pre_select_tickers_factors
                .iter()
                .filter_map(|(_, f)| f.pb)
                .collect::<Vec<f64>>();
            let pb_upper = quantile_value(&factors_pb, pb_quantile_upper);

            let factors_ps = pre_select_tickers_factors
                .iter()
                .filter_map(|(_, f)| f.ps_ttm)
                .collect::<Vec<f64>>();
            let ps_upper = quantile_value(&factors_ps, ps_quantile_upper);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            for (ticker, factors) in pre_select_tickers_factors {
                if let Some(pb) = factors.pb
                    && let Some(pb_upper) = pb_upper
                {
                    if pb > pb_upper {
                        continue;
                    }
                }

                if let Some(ps_ttm) = factors.ps_ttm
                    && let Some(ps_upper) = ps_upper
                {
                    if ps_ttm > ps_upper {
                        continue;
                    }
                }

                if factors.market_cap > 0.0 {
                    indicators.push((ticker, factors.market_cap / 1e8));
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
    pb: Option<f64>,
    ps_ttm: Option<f64>,
}
