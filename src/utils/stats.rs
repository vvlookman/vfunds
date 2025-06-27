pub fn mean(values: &[f64]) -> Option<f64> {
    let sum = values.iter().sum::<f64>();
    let count = values.len();

    if count > 0 {
        Some(sum / count as f64)
    } else {
        None
    }
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
    fn test_std() {
        assert_eq!(std(&vec![1.0, 1.0]).unwrap(), 0.0);
    }
}
