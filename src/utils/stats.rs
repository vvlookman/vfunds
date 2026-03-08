/// Mean Absolute Deviation
pub fn mad(values: &[f64]) -> Option<f64> {
    if let Some(mean) = mean(values) {
        let count = values.len();
        if count > 0 {
            return Some(values.iter().map(|&x| (x - mean).abs()).sum::<f64>() / count as f64);
        }
    }

    None
}

pub fn mean(values: &[f64]) -> Option<f64> {
    let sum = values.iter().sum::<f64>();
    let count = values.len();

    if count > 0 {
        Some(sum / count as f64)
    } else {
        None
    }
}

pub fn pct_change(values: &[f64]) -> Vec<f64> {
    let mut pct_changes = Vec::new();

    let count = values.len();
    if count > 1 {
        for i in 1..count {
            let pct_change = (values[i] - values[i - 1]) / values[i - 1];
            pct_changes.push(pct_change);
        }
    }

    pct_changes
}

pub fn quantile_rank(values: &[f64], value: f64) -> Option<f64> {
    let mut sorted = values.to_vec();
    sorted.retain(|&x| x.is_finite());

    if sorted.is_empty() {
        return None;
    }

    sorted.sort_by(|a, b| a.total_cmp(b));

    let n = sorted.len();
    match sorted.binary_search_by(|probe| probe.total_cmp(&value)) {
        Ok(index) => {
            if n == 1 {
                Some(0.5)
            } else {
                Some(index as f64 / (n - 1) as f64)
            }
        }
        Err(insert_idx) => {
            if insert_idx == 0 {
                return Some(0.0);
            }
            if insert_idx >= n {
                return Some(1.0);
            }

            let lower_val = sorted[insert_idx - 1];
            let upper_val = sorted[insert_idx];
            let range = upper_val - lower_val;
            if range == 0.0 {
                return Some(insert_idx as f64 / (n - 1) as f64);
            }

            let fraction = (value - lower_val) / range;
            let lower_rank = (insert_idx - 1) as f64 / (n - 1) as f64;
            let rank_step = 1.0 / (n - 1) as f64;

            Some(lower_rank + fraction * rank_step)
        }
    }
}

pub fn quantile_value(values: &[f64], rank: f64) -> Option<f64> {
    if !(0.0..=1.0).contains(&rank) {
        return None;
    }

    let mut sorted = values.to_vec();
    sorted.retain(|&x| x.is_finite());

    if sorted.is_empty() {
        return None;
    }

    sorted.sort_by(|a, b| a.total_cmp(b));

    let n = sorted.len();
    let pos = (n as f64 - 1.0) * rank;
    let lower = pos.floor() as usize;
    let upper = pos.ceil() as usize;
    let weight = pos - lower as f64;

    if upper >= n {
        Some(sorted[lower])
    } else {
        Some(sorted[lower] * (1.0 - weight) + sorted[upper] * weight)
    }
}

/// Standard Deviation
pub fn std(values: &[f64]) -> Option<f64> {
    if let Some(mean) = mean(values) {
        let count = values.len();
        if count > 0 {
            let variance = values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / count as f64;

            return Some(variance.sqrt());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mad() {
        assert_eq!(mad(&vec![1.0, -1.0, 1.0, -1.0]).unwrap(), 1.0);
    }

    #[test]
    fn test_mean() {
        assert_eq!(mean(&vec![0.0, 1.0]).unwrap(), 0.5);
    }

    #[test]
    fn test_pct_change() {
        assert_eq!(pct_change(&vec![1.0, 1.0, 2.0, 3.0]), [0.0, 1.0, 0.5]);
    }

    #[test]
    fn test_quantile_rank() {
        let data = [3.0, 4.0, 5.0, 1.0, 2.0];

        assert!((quantile_rank(&data, 1.0).unwrap() - 0.0).abs() < 1e-6);
        assert!((quantile_rank(&data, 1.4).unwrap() - 0.1).abs() < 1e-6);
        assert!((quantile_rank(&data, 2.0).unwrap() - 0.25).abs() < 1e-6);
        assert!((quantile_rank(&data, 3.0).unwrap() - 0.5).abs() < 1e-6);
        assert!((quantile_rank(&data, 4.0).unwrap() - 0.75).abs() < 1e-6);
        assert!((quantile_rank(&data, 4.6).unwrap() - 0.9).abs() < 1e-6);
        assert!((quantile_rank(&data, 5.0).unwrap() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_quantile_value() {
        let data = [3.0, 4.0, 5.0, 1.0, 2.0];

        assert!((quantile_value(&data, 0.0).unwrap() - 1.0).abs() < 1e-6);
        assert!((quantile_value(&data, 0.1).unwrap() - 1.4).abs() < 1e-6);
        assert!((quantile_value(&data, 0.25).unwrap() - 2.0).abs() < 1e-6);
        assert!((quantile_value(&data, 0.5).unwrap() - 3.0).abs() < 1e-6);
        assert!((quantile_value(&data, 0.75).unwrap() - 4.0).abs() < 1e-6);
        assert!((quantile_value(&data, 0.9).unwrap() - 4.6).abs() < 1e-6);
        assert!((quantile_value(&data, 1.0).unwrap() - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_std() {
        assert_eq!(std(&vec![1.0, 1.0]).unwrap(), 0.0);
    }
}
