use ta::{
    Next,
    indicators::{
        ExponentialMovingAverage, MovingAverageConvergenceDivergence, RelativeStrengthIndex,
    },
};

use crate::utils::stats;

const DAYS_PER_YEAR: f64 = 365.2425;
const TRADE_DAYS_PER_YEAR: f64 = 252.0;

pub fn calc_annualized_return_rate(start_value: f64, end_value: f64, days: u64) -> Option<f64> {
    if start_value > 0.0 && end_value > 0.0 && days > 0 {
        return Some((end_value / start_value).powf(DAYS_PER_YEAR / days as f64) - 1.0);
    }

    None
}

pub fn calc_annualized_volatility(daily_values: &[f64]) -> Option<f64> {
    if daily_values.len() > 1 {
        let daily_return = stats::pct_change(daily_values);

        if let Some(return_std) = stats::std(&daily_return) {
            return Some(return_std * (TRADE_DAYS_PER_YEAR).sqrt());
        }
    }

    None
}

pub fn calc_ema(daily_values: &[f64], period: usize) -> Vec<f64> {
    let mut results: Vec<f64> = vec![];

    if daily_values.len() > 1 {
        if let Ok(mut ema) = ExponentialMovingAverage::new(period) {
            for value in daily_values {
                results.push(ema.next(*value));
            }
        }
    }

    results
}

pub fn calc_macd(daily_values: &[f64], periods: (usize, usize, usize)) -> Vec<(f64, f64, f64)> {
    let mut results: Vec<(f64, f64, f64)> = vec![];

    if daily_values.len() > 1 {
        let (fast_period, slow_period, signal_period) = periods;
        if let Ok(mut macd) =
            MovingAverageConvergenceDivergence::new(fast_period, slow_period, signal_period)
        {
            for value in daily_values {
                results.push(macd.next(*value).into());
            }
        }
    }

    results
}

pub fn calc_max_drawdown(values: &[f64]) -> Option<f64> {
    if values.len() > 1 {
        let mut peak = 0.0;
        let mut max_dd = 0.0;

        for &p in values.iter() {
            if p > peak {
                peak = p;
            }

            let dd = (peak - p) / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }

        return Some(max_dd);
    }

    None
}

pub fn calc_momentum(daily_values: &[f64]) -> Option<f64> {
    if daily_values.len() > 1 {
        let daily_return = stats::pct_change(daily_values);

        if let Some(slope) = stats::slope(&daily_return) {
            return Some(slope);
        }
    }

    None
}

pub fn calc_profit_factor(daily_values: &[f64]) -> Option<f64> {
    if daily_values.len() > 1 {
        let daily_return = stats::pct_change(daily_values);

        let profit = daily_return.iter().filter(|&v| *v > 0.0).sum::<f64>();
        let loss = daily_return
            .iter()
            .filter(|&v| *v < 0.0)
            .map(|&v| v.abs())
            .sum::<f64>();

        if loss > 0.0 {
            return Some(profit / loss);
        }
    }

    None
}

pub fn calc_rsi(daily_values: &[f64], period: usize) -> Vec<f64> {
    let mut results: Vec<f64> = vec![];

    if daily_values.len() > 1 {
        if let Ok(mut rsi) = RelativeStrengthIndex::new(period) {
            for value in daily_values {
                results.push(rsi.next(*value));
            }
        }
    }

    results
}

pub fn calc_sharpe_ratio(daily_values: &[f64], risk_free_rate: f64) -> Option<f64> {
    if daily_values.len() > 1 {
        let daily_return = stats::pct_change(daily_values);

        if let (Some(return_mean), Some(return_std)) =
            (stats::mean(&daily_return), stats::std(&daily_return))
        {
            let annualized_volatility = return_std * (TRADE_DAYS_PER_YEAR).sqrt();
            if annualized_volatility > 0.0 {
                let annualized_return = (1.0 + return_mean).powf(TRADE_DAYS_PER_YEAR) - 1.0;
                let sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility;

                return Some(sharpe_ratio);
            }
        }
    }

    None
}

pub fn calc_sortino_ratio(daily_values: &[f64], min_acceptable_return: f64) -> Option<f64> {
    if daily_values.len() > 1 {
        let daily_return = stats::pct_change(daily_values);

        if let Some(return_mean) = stats::mean(&daily_return) {
            let daily_return_downside: Vec<_> = daily_return
                .iter()
                .filter(|&v| *v < return_mean)
                .copied()
                .collect();
            if daily_return_downside.len() > 1 {
                if let Some(return_std_downside) = stats::std(&daily_return_downside) {
                    let annualized_volatility = return_std_downside * (TRADE_DAYS_PER_YEAR).sqrt();
                    if annualized_volatility > 0.0 {
                        let annualized_return = (1.0 + return_mean).powf(TRADE_DAYS_PER_YEAR) - 1.0;
                        let sortino_ratio =
                            (annualized_return - min_acceptable_return) / annualized_volatility;

                        return Some(sortino_ratio);
                    }
                }
            }
        }
    }

    None
}

pub fn calc_win_rate(daily_values: &[f64]) -> Option<f64> {
    if daily_values.len() > 1 {
        let daily_return = stats::pct_change(daily_values);
        let wins = daily_return.iter().filter(|&v| *v > 0.0).count();

        return Some(wins as f64 / daily_return.len() as f64);
    }

    None
}
