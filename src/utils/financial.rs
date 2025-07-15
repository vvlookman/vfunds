use crate::utils::stats;

const DAYS_PER_YEAR: f64 = 365.2425;
const TRADE_DAYS_PER_YEAR: f64 = 252.0;

pub fn calc_annual_return_rate(start_value: f64, end_value: f64, days: u64) -> Option<f64> {
    if start_value > 0.0 && end_value > 0.0 && days > 0 {
        return Some((end_value / start_value).powf(DAYS_PER_YEAR / days as f64) - 1.0);
    }

    None
}

pub fn calc_sharpe_ratio(daily_values: &[f64], risk_free_rate: f64) -> Option<f64> {
    if daily_values.len() > 1 {
        let daily_return = stats::pct_change(&daily_values);

        let return_mean = stats::mean(&daily_return);
        let return_std = stats::std(&daily_return);

        if let (Some(return_mean), Some(return_std)) = (return_mean, return_std) {
            let annual_return = (1.0 + return_mean).powf(TRADE_DAYS_PER_YEAR) - 1.0;
            let annual_volatility = return_std * (TRADE_DAYS_PER_YEAR).sqrt();
            let sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility;

            return Some(sharpe_ratio);
        }
    }

    None
}

pub fn calc_sortino_ratio(daily_values: &[f64], min_acceptable_return: f64) -> Option<f64> {
    if daily_values.len() > 1 {
        let daily_return = stats::pct_change(&daily_values);

        if let Some(return_mean) = stats::mean(&daily_return) {
            let daily_return_downside: Vec<_> = daily_return
                .iter()
                .filter(|&value| *value < return_mean)
                .map(|x| *x as f64)
                .collect();
            if daily_return_downside.len() > 1 {
                if let Some(return_std_downside) = stats::std(&daily_return_downside) {
                    let annual_return = (1.0 + return_mean).powf(TRADE_DAYS_PER_YEAR) - 1.0;
                    let annual_volatility = return_std_downside * (TRADE_DAYS_PER_YEAR).sqrt();
                    let sortino_ratio = (annual_return - min_acceptable_return) / annual_volatility;

                    return Some(sortino_ratio);
                }
            }
        }
    }

    None
}
