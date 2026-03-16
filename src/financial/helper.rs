use std::collections::HashMap;

use chrono::{Datelike, Days, Duration, NaiveDate};

use crate::{
    STALE_DAYS_LONG, STALE_DAYS_SHORT,
    error::{VfError, VfResult},
    financial::{
        KlineField,
        stock::{
            StockDividendAdjust, StockDividendField, StockIndicatorField, StockReportBalanceField,
            StockReportCapitalField, StockReportCashFlowField, StockReportIncomeField,
            StockReportPershareField, fetch_stock_dividends, fetch_stock_indicators,
            fetch_stock_kline, fetch_stock_report_balance, fetch_stock_report_capital,
            fetch_stock_report_cash_flow, fetch_stock_report_income, fetch_stock_report_pershare,
        },
    },
    ticker::Ticker,
    utils::{
        datetime::{FiscalQuarter, date_from_str, date_to_fiscal_quarter, date_to_str},
        stats::mean,
    },
};

pub async fn calc_stock_cash_ratio(ticker: &Ticker, date: &NaiveDate) -> VfResult<f64> {
    let report_balance = fetch_stock_report_balance(ticker).await?;

    if let Some((_, cash)) = report_balance.get_latest_value::<f64>(
        date,
        STALE_DAYS_LONG,
        false,
        &StockReportBalanceField::CashEquivalents.to_string(),
    ) && let Some((_, current_liability)) = report_balance.get_latest_value::<f64>(
        date,
        STALE_DAYS_LONG,
        false,
        &StockReportBalanceField::TotalCurrentLiability.to_string(),
    ) {
        return Ok(cash / current_liability);
    }

    Err(VfError::NoData {
        code: "NO_CASH_RATIO_DATA",
        message: format!("Cash ratio of '{ticker}' @{} not exists", date_to_str(date)),
    })
}

pub async fn calc_stock_current_ratio(ticker: &Ticker, date: &NaiveDate) -> VfResult<f64> {
    let report_balance = fetch_stock_report_balance(ticker).await?;

    if let Some((_, current_assets)) = report_balance.get_latest_value::<f64>(
        date,
        STALE_DAYS_LONG,
        false,
        &StockReportBalanceField::TotalCurrentAssets.to_string(),
    ) && let Some((_, current_liability)) = report_balance.get_latest_value::<f64>(
        date,
        STALE_DAYS_LONG,
        false,
        &StockReportBalanceField::TotalCurrentLiability.to_string(),
    ) {
        return Ok(current_assets / current_liability);
    }

    Err(VfError::NoData {
        code: "NO_CASH_RATIO_DATA",
        message: format!("Cash ratio of '{ticker}' @{} not exists", date_to_str(date)),
    })
}

pub async fn calc_stock_dividend_ratio_lt(
    ticker: &Ticker,
    date: &NaiveDate,
    lookback_years: u32,
) -> VfResult<f64> {
    let stock_dividends = fetch_stock_dividends(ticker).await?;

    let year_date_from = date.with_year(date.year() - lookback_years as i32).unwrap();
    let year_date_to = *date - Duration::days(1);
    if let Ok(year_dividends) = stock_dividends.slice_by_date_range(&year_date_from, &year_date_to)
    {
        let mut total_dividend = 0.0;

        for div_date in year_dividends.all_dates() {
            if let Some((_, interest)) = year_dividends
                .get_value::<f64>(&div_date, &StockDividendField::Interest.to_string())
            {
                let shares = calc_stock_shares(ticker, &div_date, false).await?;
                total_dividend += interest * shares;
            }
        }

        let market_cap = calc_stock_market_cap(ticker, date, false).await?;

        Ok(total_dividend / market_cap / lookback_years as f64)
    } else {
        Ok(0.0)
    }
}

pub async fn calc_stock_dividend_ratio_ttm(ticker: &Ticker, date: &NaiveDate) -> VfResult<f64> {
    // Try read from stock indicators
    {
        let stock_indicators = fetch_stock_indicators(ticker).await?;
        if let Some((_, dv_ttm_pct)) = stock_indicators.get_latest_value::<f64>(
            date,
            STALE_DAYS_SHORT,
            false,
            &StockIndicatorField::DividendRatioTtm.to_string(),
        ) {
            return Ok(dv_ttm_pct / 100.0);
        }
    }

    // Try calculate from stock dividends data
    {
        let stock_dividends = fetch_stock_dividends(ticker).await?;

        let year_date_from = date.with_year(date.year() - 1).unwrap();
        let year_date_to = *date - Duration::days(1);
        if let Ok(year_dividends) =
            stock_dividends.slice_by_date_range(&year_date_from, &year_date_to)
        {
            let mut total_dividend = 0.0;

            for div_date in year_dividends.all_dates() {
                if let Some((_, interest)) = year_dividends
                    .get_value::<f64>(&div_date, &StockDividendField::Interest.to_string())
                {
                    let shares = calc_stock_shares(ticker, &div_date, false).await?;
                    total_dividend += interest * shares;
                }
            }

            let market_cap = calc_stock_market_cap(ticker, date, false).await?;

            return Ok(total_dividend / market_cap);
        }
    }

    Err(VfError::NoData {
        code: "NO_DV_RATIO_TTM_DATA",
        message: format!(
            "DV Ratio TTM of '{ticker}' @{} not exists",
            date_to_str(date)
        ),
    })
}

pub async fn calc_stock_free_cash_ratio_ttm(ticker: &Ticker, date: &NaiveDate) -> VfResult<f64> {
    calc_stock_free_cash_ratio_lt(ticker, date, 1).await
}

pub async fn calc_stock_free_cash_ratio_lt(
    ticker: &Ticker,
    date: &NaiveDate,
    lookback_years: u32,
) -> VfResult<f64> {
    let report_cash_flow = fetch_stock_report_cash_flow(ticker).await?;

    let lookback_start_date = date
        .with_year(date.year() - lookback_years as i32)
        .unwrap_or(*date);
    let lookback_end_date = *date - Days::new(1);
    let lookback_report =
        report_cash_flow.slice_by_date_range(&lookback_start_date, &lookback_end_date)?;

    let net_cash_list_with_date = lookback_report
        .get_values::<f64>(&StockReportCashFlowField::NetCashByOperatingActivities.to_string());
    let cap_exp_list_with_date = lookback_report
        .get_values::<f64>(&StockReportCashFlowField::CapitalExpenditures.to_string());
    let cap_exp_map: HashMap<NaiveDate, f64> = cap_exp_list_with_date.into_iter().collect();

    let mut free_cash_list: Vec<(NaiveDate, f64)> = vec![];
    for (date, net_cash) in net_cash_list_with_date {
        if let Some(cap_exp) = cap_exp_map.get(&date) {
            free_cash_list.push((date, net_cash - cap_exp));
        }
    }

    let total_free_cash = free_cash_list.into_iter().map(|(_, v)| v).sum::<f64>();
    let market_cap = calc_stock_market_cap(ticker, date, false).await?;

    Ok(total_free_cash / market_cap / lookback_years as f64)
}

pub async fn calc_stock_market_cap(
    ticker: &Ticker,
    date: &NaiveDate,
    circulating: bool,
) -> VfResult<f64> {
    // Try read from stock indicators
    {
        let indicator_field = if circulating {
            StockIndicatorField::MarketValueCirculating.to_string()
        } else {
            StockIndicatorField::MarketValueTotal.to_string()
        };

        let stock_indicators = fetch_stock_indicators(ticker).await?;
        if let Some((_, market_cap_10k_unit)) = stock_indicators.get_latest_value::<f64>(
            date,
            STALE_DAYS_SHORT,
            false,
            &indicator_field,
        ) {
            return Ok(market_cap_10k_unit * 1e4);
        }
    }

    // Try calculate from stock report
    {
        let report_field = if circulating {
            StockReportCapitalField::Circulating.to_string()
        } else {
            StockReportCapitalField::Total.to_string()
        };

        let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
        let report_capital = fetch_stock_report_capital(ticker).await?;

        if let (Some((_, price)), Some((_, capital))) = (
            kline.get_latest_value::<f64>(
                date,
                STALE_DAYS_SHORT,
                false,
                &KlineField::Close.to_string(),
            ),
            report_capital.get_latest_value::<f64>(date, STALE_DAYS_LONG, false, &report_field),
        ) {
            let market_cap = price * capital;
            return Ok(market_cap);
        }
    }

    Err(VfError::NoData {
        code: "NO_MARKET_CAP_DATA",
        message: format!("Market cap of '{ticker}' @{} not exists", date_to_str(date)),
    })
}

pub async fn calc_stock_pb(ticker: &Ticker, date: &NaiveDate) -> VfResult<f64> {
    // Try read from stock indicators
    {
        let stock_indicators = fetch_stock_indicators(ticker).await?;
        if let Some((_, pb)) = stock_indicators.get_latest_value::<f64>(
            date,
            STALE_DAYS_SHORT,
            false,
            &StockIndicatorField::Pb.to_string(),
        ) {
            return Ok(pb);
        }
    }

    // Try calculate from stock report
    {
        let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
        let report_pershare = fetch_stock_report_pershare(ticker).await?;

        if let (Some((_, price)), Some((_, bps))) = (
            kline.get_latest_value::<f64>(
                date,
                STALE_DAYS_SHORT,
                false,
                &KlineField::Close.to_string(),
            ),
            report_pershare.get_latest_value::<f64>(
                date,
                STALE_DAYS_LONG,
                false,
                &StockReportPershareField::Bps.to_string(),
            ),
        ) {
            let pb = price / bps;
            return Ok(pb);
        }
    }

    Err(VfError::NoData {
        code: "NO_PB_DATA",
        message: format!("PB of '{ticker}' @{} not exists", date_to_str(date)),
    })
}

pub async fn calc_stock_pe_ttm(ticker: &Ticker, date: &NaiveDate) -> VfResult<f64> {
    let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
    let report_pershare = fetch_stock_report_pershare(ticker).await?;

    if let (Some((_, price)), eps_values) = (
        kline.get_latest_value::<f64>(
            date,
            STALE_DAYS_SHORT,
            false,
            &KlineField::Close.to_string(),
        ),
        report_pershare.get_latest_values_with_label::<f64>(
            date,
            false,
            &StockReportPershareField::Eps.to_string(),
            &StockReportIncomeField::ReportDate.to_string(),
            5,
        ),
    ) {
        let mut fiscal_eps_values: Vec<(FiscalQuarter, f64)> = vec![];
        {
            let mut current_fiscal_quarter: Option<FiscalQuarter> = None;
            for (_, eps, report_date_label) in eps_values {
                if let Some(report_date_label_str) = report_date_label {
                    if let Ok(report_date) = date_from_str(&report_date_label_str) {
                        let fiscal_quarter = date_to_fiscal_quarter(&report_date);

                        if let Some(current_fiscal_quarter) = current_fiscal_quarter {
                            if fiscal_quarter.prev() != current_fiscal_quarter {
                                // Reports must be consistent
                                break;
                            }
                        }

                        current_fiscal_quarter = Some(fiscal_quarter.clone());
                        fiscal_eps_values.push((fiscal_quarter, eps));
                    }
                }
            }
        }

        if fiscal_eps_values.len() == 5 {
            let mut eps_ttm = 0.0;
            for i in 1..5 {
                let (_, prev_eps) = &fiscal_eps_values[i - 1];
                let (fiscal_quarter, eps) = &fiscal_eps_values[i];

                let quarter_eps: f64 = if fiscal_quarter.quarter == 1 {
                    *eps
                } else {
                    eps - prev_eps
                };

                eps_ttm += quarter_eps;
            }

            let pe_ttm = price / eps_ttm;
            return Ok(pe_ttm);
        }
    }

    Err(VfError::NoData {
        code: "NO_PE_TTM_DATA",
        message: format!("PE TTM of '{ticker}' @{} not exists", date_to_str(date)),
    })
}

pub async fn calc_stock_ps_ttm(ticker: &Ticker, date: &NaiveDate) -> VfResult<f64> {
    // Try read from stock indicators
    {
        let stock_indicators = fetch_stock_indicators(ticker).await?;
        if let Some((_, ps_ttm)) = stock_indicators.get_latest_value::<f64>(
            date,
            STALE_DAYS_SHORT,
            false,
            &StockIndicatorField::PsTtm.to_string(),
        ) {
            return Ok(ps_ttm);
        }
    }

    // Try calculate from stock report
    {
        let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
        let report_capital = fetch_stock_report_capital(ticker).await?;
        let report_income = fetch_stock_report_income(ticker).await?;

        if let (Some((_, price)), Some((_, total_captical)), revenues) = (
            kline.get_latest_value::<f64>(
                date,
                STALE_DAYS_SHORT,
                false,
                &KlineField::Close.to_string(),
            ),
            report_capital.get_latest_value::<f64>(
                date,
                STALE_DAYS_LONG,
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
        ) {
            let mut fiscal_revenues: Vec<(FiscalQuarter, f64)> = vec![];
            {
                let mut current_fiscal_quarter: Option<FiscalQuarter> = None;
                for (_, revenue, report_date_label) in revenues {
                    if let Some(report_date_label_str) = report_date_label {
                        if let Ok(report_date) = date_from_str(&report_date_label_str) {
                            let fiscal_quarter = date_to_fiscal_quarter(&report_date);

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

            if fiscal_revenues.len() == 5 {
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

                let ps_ttm = price * total_captical / revenue_ttm;
                return Ok(ps_ttm);
            }
        }
    }

    Err(VfError::NoData {
        code: "NO_PS_TTM_DATA",
        message: format!("PS TTM of '{ticker}' @{} not exists", date_to_str(date)),
    })
}

pub async fn calc_stock_roe_lt(
    ticker: &Ticker,
    date: &NaiveDate,
    lookback_years: u32,
) -> VfResult<Option<f64>> {
    let report_pershare = fetch_stock_report_pershare(ticker).await?;

    let lookback_start_date = date
        .with_year(date.year() - lookback_years as i32)
        .unwrap_or(*date);
    let lookback_end_date = date.checked_sub_days(Days::new(1)).unwrap_or(*date);
    let lookback_report =
        report_pershare.slice_by_date_range(&lookback_start_date, &lookback_end_date)?;

    let roe_list_with_date =
        lookback_report.get_values::<f64>(&StockReportPershareField::EquityRoe.to_string());

    let roe_list = roe_list_with_date
        .into_iter()
        .filter(|&(date, _)| date >= lookback_start_date)
        .map(|(_, v)| v)
        .collect::<Vec<_>>();

    Ok(mean(&roe_list))
}

pub async fn calc_stock_shares(
    ticker: &Ticker,
    date: &NaiveDate,
    circulating: bool,
) -> VfResult<f64> {
    // Try read from stock indicators
    {
        let indicator_field = if circulating {
            StockIndicatorField::SharesCirculating.to_string()
        } else {
            StockIndicatorField::SharesTotal.to_string()
        };

        let stock_indicators = fetch_stock_indicators(ticker).await?;
        if let Some((_, shares)) = stock_indicators.get_latest_value::<f64>(
            date,
            STALE_DAYS_SHORT,
            false,
            &indicator_field,
        ) {
            return Ok(shares);
        }
    }

    // Try calculate from stock report
    {
        let report_field = if circulating {
            StockReportCapitalField::Circulating.to_string()
        } else {
            StockReportCapitalField::Total.to_string()
        };

        let report_capital = fetch_stock_report_capital(ticker).await?;

        if let Some((_, capital)) =
            report_capital.get_latest_value::<f64>(date, STALE_DAYS_LONG, false, &report_field)
        {
            return Ok(capital);
        }
    }

    Err(VfError::NoData {
        code: "NO_SHARES_DATA",
        message: format!("Shares of '{ticker}' @{} not exists", date_to_str(date)),
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_calc_stock_free_cash_ratio_lt() {
        let free_cash_ratio = calc_stock_free_cash_ratio_lt(
            &Ticker::from_str("600383").unwrap(),
            &date_from_str("2021-01-01").unwrap(),
            3,
        )
        .await
        .unwrap();

        assert!(free_cash_ratio < 0.0);
    }

    #[tokio::test]
    async fn test_calc_stock_roe_lt() {
        let roe_mean = calc_stock_roe_lt(
            &Ticker::from_str("600383").unwrap(),
            &date_from_str("2021-01-01").unwrap(),
            3,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(roe_mean > 0.0);
    }

    #[tokio::test]
    async fn test_calc_stock_market_cap() {
        assert!(
            calc_stock_market_cap(
                &Ticker::from_str("600383").unwrap(),
                &date_from_str("2021-01-01").unwrap(),
                false,
            )
            .await
            .unwrap()
                > 1e8
        );
    }
}
