use smartcore::{
    linalg::basic::{arrays::Array, matrix::DenseMatrix},
    linear::linear_regression::{
        LinearRegression, LinearRegressionParameters, LinearRegressionSolverName,
    },
    metrics::r2,
};

pub fn constraint_array(values: &[f64], min: f64, max: f64) -> Vec<f64> {
    let n = values.len();
    let sum: f64 = values.iter().sum();

    if n == 0 || sum < min * n as f64 || sum > max * n as f64 {
        return values.to_vec();
    }

    let mut indexed_values: Vec<(usize, f64)> =
        values.iter().enumerate().map(|(i, &v)| (i, v)).collect();
    indexed_values.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut value_groups = Vec::new();
    let mut current_group = Vec::new();
    let mut current_value = indexed_values[0].1;

    for &(idx, value) in &indexed_values {
        if (value - current_value).abs() < 1e-10 {
            current_group.push(idx);
        } else {
            value_groups.push((current_value, current_group));
            current_group = vec![idx];
            current_value = value;
        }
    }
    value_groups.push((current_value, current_group));

    let total_remaining = sum - min * n as f64;
    let mut result = vec![min; n];
    let mut remaining = total_remaining;

    for &(value, ref indices) in &value_groups {
        let group_size = indices.len();
        let group_proportion = if sum > 0.0 {
            (value * group_size as f64) / sum
        } else {
            group_size as f64 / n as f64
        };

        let group_allocation = (max - min) * group_size as f64;
        let allocation_per_item =
            (group_allocation.min(remaining * group_proportion)) / group_size as f64;

        for &idx in indices {
            result[idx] += allocation_per_item;
        }
        remaining -= allocation_per_item * group_size as f64;
    }

    if remaining > 0.0 {
        let mut available_indices: Vec<usize> =
            (0..n).filter(|&i| result[i] < max - 1e-10).collect();

        while remaining > 1e-10 && !available_indices.is_empty() {
            let share = remaining / available_indices.len() as f64;
            let mut new_available = Vec::new();

            for &idx in &available_indices {
                let available_space = max - result[idx];
                let add = available_space.min(share);
                result[idx] += add;
                remaining -= add;

                if result[idx] < max - 1e-10 {
                    new_available.push(idx);
                }
            }
            available_indices = new_available;
        }
    }

    result
}

pub fn linear_regression(values: &[f64]) -> Option<(f64, f64)> {
    let features: Vec<Vec<f64>> = (0..values.len()).map(|i| vec![i as f64]).collect();
    if let Ok(x) = DenseMatrix::from_2d_array(
        &features
            .iter()
            .map(|v| v.as_slice())
            .collect::<Vec<&[f64]>>(),
    ) {
        let y: Vec<f64> = values.to_vec();
        let parameters =
            LinearRegressionParameters::default().with_solver(LinearRegressionSolverName::QR);
        if let Ok(model) = LinearRegression::fit(&x, &y, parameters) {
            let s = model.coefficients().get((0, 0));

            if let Ok(y_pred) = model.predict(&x) {
                let r2_score = r2(&y, &y_pred);
                return Some((*s, r2_score));
            }
        }
    }

    None
}

pub fn normalize_zscore(values: &[f64]) -> Vec<f64> {
    let computed_values: Vec<f64> = values.iter().filter(|v| v.is_finite()).copied().collect();
    if computed_values.is_empty() {
        return values.to_vec();
    }

    let n = computed_values.len() as f64;
    let mean = computed_values.iter().sum::<f64>() / n;
    let std = (computed_values
        .iter()
        .map(|&v| (v - mean).powi(2))
        .sum::<f64>()
        / n)
        .sqrt();

    if std == 0.0 {
        values.to_vec()
    } else {
        values
            .iter()
            .map(|&v| if v.is_finite() { (v - mean) / std } else { v })
            .collect()
    }
}

pub fn transpose(mat: &[Vec<f64>]) -> Vec<Vec<f64>> {
    if mat.is_empty() || mat[0].is_empty() {
        return vec![];
    }

    (0..mat[0].len())
        .map(|c| mat.iter().map(|row| row[c]).collect())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constraint_array() {
        let values1 = vec![0.1, 0.2, 0.3, 0.4];
        let values2 = vec![0.4, 0.3, 0.2, 0.1];

        let result1 = constraint_array(&values1, 0.2, 0.3);
        let result2 = constraint_array(&values2, 0.2, 0.3);

        assert!((result1.iter().sum::<f64>() - values1.iter().sum::<f64>()).abs() < 1e-10);
        assert!((result2.iter().sum::<f64>() - values2.iter().sum::<f64>()).abs() < 1e-10);

        let mut sorted1 = result1.clone();
        let mut sorted2 = result2.clone();
        sorted1.sort_by(|a, b| a.partial_cmp(b).unwrap());
        sorted2.sort_by(|a, b| a.partial_cmp(b).unwrap());

        for (a, b) in sorted1.iter().zip(sorted2.iter()) {
            assert!((a - b).abs() < 1e-10);
        }
    }

    #[test]
    fn test_linear_regression() {
        assert!((linear_regression(&vec![1.0, 2.0, 3.0]).unwrap().0 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_normalize_zscore() {
        let values = vec![f64::NAN, f64::NEG_INFINITY, 0.0, 1.0, 2.0, f64::INFINITY];
        let result = normalize_zscore(&values);

        assert!(result[0].is_nan());
        assert_eq!(result[1], f64::NEG_INFINITY);
        assert!((result[2] + 1.5_f64.sqrt()).abs() < 1e-10);
        assert!(result[3].abs() < 1e-10);
        assert!((result[4] - 1.5_f64.sqrt()).abs() < 1e-10);
        assert_eq!(result[5], f64::INFINITY);
    }

    #[test]
    fn test_transpose() {
        let mat = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        let result = transpose(&mat);

        assert_eq!(result[0][0], 1.0);
        assert_eq!(result[1][0], 2.0);
        assert_eq!(result[2][0], 3.0);
        assert_eq!(result[0][1], 4.0);
        assert_eq!(result[1][1], 5.0);
        assert_eq!(result[2][1], 6.0);
    }
}
