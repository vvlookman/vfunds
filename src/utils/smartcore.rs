use smartcore::linalg::basic::{arrays::Array, matrix::DenseMatrix};

use crate::error::{VfError, VfResult};

pub fn normalize_zscore_matrix(mat: &DenseMatrix<f64>) -> Option<DenseMatrix<f64>> {
    let (n_rows, n_cols) = mat.shape();

    let mut means = Vec::with_capacity(n_cols);
    let mut stds = Vec::with_capacity(n_cols);
    {
        for col in 0..n_cols {
            let values: Vec<f64> = (0..n_rows).map(|row| *mat.get((row, col))).collect();

            let mean = values.iter().sum::<f64>() / n_rows as f64;
            means.push(mean);

            let std =
                (values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / n_rows as f64).sqrt();
            stds.push(std.max(1e-8));
        }
    }

    let mut data = Vec::with_capacity(n_rows * n_cols);
    for row in 0..n_rows {
        for col in 0..n_cols {
            let normalized = (mat.get((row, col)) - means[col]) / stds[col];
            data.push(normalized);
        }
    }

    DenseMatrix::new(n_rows, n_cols, data, true).ok()
}

pub fn validate_array(array: &[f64]) -> VfResult<()> {
    if array.is_empty() {
        return Err(VfError::Invalid {
            code: "INVALID_ARRAY_SHAPE",
            message: "Array is empty".to_string(),
        });
    }

    for v in array {
        if !v.is_finite() {
            return Err(VfError::Invalid {
                code: "INVALID_ARRAY_VALUE",
                message: "Value is not finite".to_string(),
            });
        }
    }

    Ok(())
}

pub fn validate_matrix(mat: &DenseMatrix<f64>) -> VfResult<()> {
    let (n_rows, n_cols) = mat.shape();

    if n_rows == 0 || n_cols == 0 {
        return Err(VfError::Invalid {
            code: "INVALID_MATRIX_SHAPE",
            message: "Rows or columns is zero".to_string(),
        });
    }

    for i in 0..n_rows {
        for j in 0..n_cols {
            if !mat.get((i, j)).is_finite() {
                return Err(VfError::Invalid {
                    code: "INVALID_MATRIX_VALUE",
                    message: "Value is not finite".to_string(),
                });
            }
        }
    }

    Ok(())
}
