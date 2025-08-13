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

pub fn slope(values: &[f64]) -> Option<f64> {
    let count = values.len();
    if count > 1 {
        let n = count as f64;
        let sum_x = (n - 1.0) * n / 2.0;
        let sum_x2 = (n - 1.0) * n * (2.0 * n - 1.0) / 6.0;
        let sum_y = values.iter().sum::<f64>();
        let sum_xy = values
            .iter()
            .enumerate()
            .map(|(i, &v)| i as f64 * v)
            .sum::<f64>();

        return Some((n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x));
    }

    None
}

pub fn std(values: &[f64]) -> Option<f64> {
    if let Some(mean) = mean(values) {
        let count = values.len();
        if count > 0 {
            let variance = values
                .iter()
                .map(|value| {
                    let diff = *value - mean;

                    diff * diff
                })
                .sum::<f64>()
                / count as f64;

            return Some(variance.sqrt());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean() {
        assert_eq!(mean(&vec![0.0, 1.0]).unwrap(), 0.5);
    }

    #[test]
    fn test_pct_change() {
        assert_eq!(pct_change(&vec![1.0, 1.0, 2.0, 3.0]), [0.0, 1.0, 0.5]);
    }

    #[test]
    fn test_slope() {
        assert_eq!(slope(&vec![1.0, 2.0, 3.0]).unwrap(), 1.0);
    }

    #[test]
    fn test_std() {
        assert_eq!(std(&vec![1.0, 1.0]).unwrap(), 0.0);
    }
}
