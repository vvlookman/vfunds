use std::cmp::Ordering;

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

pub fn normalize_min_max(values: &[f64]) -> Vec<f64> {
    let max = values
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
        .unwrap_or(&1.0);

    let min = values
        .iter()
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
        .unwrap_or(&0.0);

    let range = max - min;
    let is_constant = range.abs() < f64::EPSILON;

    values
        .iter()
        .map(|x| {
            if is_constant {
                1.0
            } else {
                (*x - *min) / range
            }
        })
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
}
