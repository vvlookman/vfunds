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
            StockDetail, StockDividendAdjust, StockReportCapitalField, StockReportIncomeField,
            StockReportPershareField, fetch_stock_detail, fetch_stock_kline,
            fetch_stock_report_capital, fetch_stock_report_income, fetch_stock_report_pershare,
        },
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, notify_tickers_indicator,
    },
    ticker::Ticker,
    utils::{
        datetime::{FiscalQuarter, date_from_str, date_to_fiscal_quarter, date_to_str},
        financial::calc_annualized_return_rate,
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
        let skip_same_sector = self
            .options
            .get("skip_same_sector")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
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
            let date_str = date_to_str(date);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;
                for ticker in tickers_map.keys() {
                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                    let prices: Vec<f64> = kline
                        .get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Close.to_string(),
                            lookback_years as u32 * 365,
                        )
                        .iter()
                        .map(|&(_, v)| v)
                        .collect();
                    if prices.len() < lookback_years as usize * 180 {
                        let _ = event_sender
                            .send(BacktestEvent::Info(format!(
                                "[{date_str}] [{rule_name}] [No Enough Data] {ticker}"
                            )))
                            .await;
                        continue;
                    }

                    if let Some(arr) = calc_annualized_return_rate(
                        prices[0],
                        prices[prices.len() - 1],
                        prices.len() as u64,
                    ) {
                        let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
                        let report_capital = fetch_stock_report_capital(ticker).await?;
                        let report_income = fetch_stock_report_income(ticker).await?;
                        let report_pershare = fetch_stock_report_pershare(ticker).await?;

                        if let (
                            Some((_, price)),
                            Some((_, total_captical)),
                            revenues,
                            epss,
                            Some((_, bps)),
                        ) = (
                            kline.get_latest_value::<f64>(
                                date,
                                false,
                                &KlineField::Close.to_string(),
                            ),
                            report_capital.get_latest_value::<f64>(
                                date,
                                false,
                                &StockReportCapitalField::Total.to_string(),
                            ),
                            report_income.get_latest_values_with_label::<f64>(
                                date,
                                false,
                                &StockReportIncomeField::Revenue.to_string(),
                                &StockReportIncomeField::ReportDate.to_string(),
                                5,
                            ),
                            report_pershare.get_latest_values_with_label::<f64>(
                                date,
                                false,
                                &StockReportPershareField::Eps.to_string(),
                                &StockReportIncomeField::ReportDate.to_string(),
                                5,
                            ),
                            report_pershare.get_latest_value::<f64>(
                                date,
                                false,
                                &StockReportPershareField::Bps.to_string(),
                            ),
                        ) {
                            let mut fiscal_epss: Vec<(FiscalQuarter, f64)> = vec![];
                            {
                                let mut current_fiscal_quarter: Option<FiscalQuarter> = None;
                                for (_, eps, report_date_label) in epss {
                                    if let Some(report_date_label_str) = report_date_label {
                                        if let Ok(report_date) =
                                            date_from_str(&report_date_label_str)
                                        {
                                            let fiscal_quarter =
                                                date_to_fiscal_quarter(&report_date);

                                            if let Some(current_fiscal_quarter) =
                                                current_fiscal_quarter
                                            {
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
                                for (_, revenue, report_date_label) in revenues {
                                    if let Some(report_date_label_str) = report_date_label {
                                        if let Ok(report_date) =
                                            date_from_str(&report_date_label_str)
                                        {
                                            let fiscal_quarter =
                                                date_to_fiscal_quarter(&report_date);

                                            if let Some(current_fiscal_quarter) =
                                                current_fiscal_quarter
                                            {
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

                                let _pe_ttm = price / eps_ttm;
                                let ps_ttm = price * total_captical / revenue_ttm;
                                let pb = price / bps;

                                let indicator = arr / pb;
                                debug!("[{date_str}] [{rule_name}] {ticker}={indicator:.4}");

                                indicators.push((ticker.clone(), indicator));
                            }
                        }
                    }

                    calc_count += 1;

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        let calc_progress_pct =
                            calc_count as f64 / tickers_map.len() as f64 * 100.0;
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
            .await?;

            let mut targets_weight: Vec<(Ticker, f64)> = vec![];
            for (ticker, _) in &targets_indicator {
                if let Some((weight, _)) = tickers_map.get(ticker) {
                    targets_weight.push((ticker.clone(), *weight));
                }
            }

            context
                .rebalance(&targets_weight, date, event_sender)
                .await?;
        }

        Ok(())
    }
}
