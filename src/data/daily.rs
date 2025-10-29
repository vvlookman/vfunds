use std::collections::HashMap;

use chrono::NaiveDate;
use num_traits::NumCast;
use polars::prelude::*;
use serde::Serialize;
use serde_json::Value;

use crate::error::{VfError, VfResult};

#[derive(Clone, Debug, Serialize)]
pub struct DailyDataset {
    df: DataFrame,

    date_field_name: String,
    value_field_names: HashMap<String, String>,
}

impl DailyDataset {
    pub fn from_json(
        json: &Value,
        date_field_name: &str,
        value_field_names: &HashMap<String, String>,
    ) -> VfResult<Self> {
        if let Some(array) = json.as_array() {
            let column_names: Vec<String> = [
                vec![date_field_name.to_string()],
                value_field_names.values().map(|v| v.to_string()).collect(),
            ]
            .concat();

            let mut series: Vec<Column> = Vec::with_capacity(column_names.len());
            for column_name in column_names {
                let is_date_column = column_name == date_field_name;
                let mut values: Vec<AnyValue> = vec![];

                for item in array {
                    if let Some(obj) = item.as_object() {
                        if let Some(val) = obj.get(&column_name) {
                            match val {
                                Value::Null => {
                                    values.push(AnyValue::Null);
                                    continue;
                                }
                                Value::Bool(b) => {
                                    values.push(AnyValue::Boolean(*b));
                                    continue;
                                }
                                Value::Number(n) => {
                                    if let Some(i) = n.as_i64() {
                                        values.push(AnyValue::Int64(i));
                                        continue;
                                    } else if let Some(f) = n.as_f64() {
                                        values.push(AnyValue::Float64(f));
                                        continue;
                                    } else if let Some(u) = n.as_u64() {
                                        values.push(AnyValue::UInt64(u));
                                        continue;
                                    }
                                }
                                Value::String(s) => {
                                    if is_date_column {
                                        let days_after_epoch: i32 = if let Ok((date, _)) =
                                            NaiveDate::parse_and_remainder(s, "%Y-%m-%d")
                                        {
                                            date.to_epoch_days()
                                        } else {
                                            0
                                        };
                                        values.push(AnyValue::Date(days_after_epoch));
                                    } else {
                                        values.push(AnyValue::String(s));
                                    }

                                    continue;
                                }
                                _ => {}
                            }
                        }
                    }

                    values.push(AnyValue::Null);
                }

                series.push(Column::new(column_name.into(), values));
            }

            let df = DataFrame::new(series)?;

            Ok(Self {
                df,
                date_field_name: date_field_name.to_string(),
                value_field_names: value_field_names.clone(),
            })
        } else {
            Err(VfError::Invalid {
                code: "JSON_IS_NOT_ARRAY",
                message: "Json is not a valid array".to_string(),
            })
        }
    }

    pub fn get_dates(&self) -> Vec<NaiveDate> {
        let mut dates = vec![];

        if let Ok(col_date) = self.df.column(&self.date_field_name) {
            for i in 0..col_date.len() {
                if let Ok(cell_date) = col_date.get(i) {
                    if let Some(date_days_after_epoch) = cell_date.extract::<i32>() {
                        if let Some(date) = NaiveDate::from_epoch_days(date_days_after_epoch) {
                            dates.push(date);
                        }
                    }
                }
            }
        }

        dates
    }

    pub fn get_latest_value<T: NumCast>(
        &self,
        date: &NaiveDate,
        include_today: bool,
        field_name: &str,
    ) -> Option<(NaiveDate, T)> {
        if let Some(origin_field_name) = self.value_field_names.get(field_name) {
            let filter = if include_today {
                col(&self.date_field_name)
                    .lt_eq(lit(*date))
                    .and(col(origin_field_name).is_not_null())
            } else {
                col(&self.date_field_name)
                    .lt(lit(*date))
                    .and(col(origin_field_name).is_not_null())
            };

            if let Ok(df) = self
                .df
                .clone()
                .lazy()
                .filter(filter)
                .sort(
                    [&self.date_field_name],
                    SortMultipleOptions::default().with_order_descending(true),
                )
                .collect()
            {
                if let (Ok(col_date), Ok(col_val)) = (
                    df.column(&self.date_field_name),
                    df.column(origin_field_name),
                ) {
                    if let (Ok(cell_date), Ok(cell_val)) = (col_date.get(0), col_val.get(0)) {
                        if let (Some(date_days_after_epoch), Some(val)) =
                            (cell_date.extract::<i32>(), cell_val.extract::<T>())
                        {
                            if let Some(date) = NaiveDate::from_epoch_days(date_days_after_epoch) {
                                return Some((date, val));
                            }
                        }
                    }
                }
            }
        }

        None
    }

    pub fn get_latest_values<T: NumCast>(
        &self,
        date: &NaiveDate,
        include_today: bool,
        field_name: &str,
        count: u32,
    ) -> Vec<(NaiveDate, T)> {
        if let Some(origin_field_name) = self.value_field_names.get(field_name) {
            let filter = if include_today {
                col(&self.date_field_name)
                    .lt_eq(lit(*date))
                    .and(col(origin_field_name).is_not_null())
            } else {
                col(&self.date_field_name)
                    .lt(lit(*date))
                    .and(col(origin_field_name).is_not_null())
            };

            if let Ok(df) = self
                .df
                .clone()
                .lazy()
                .filter(filter)
                .sort(
                    [&self.date_field_name],
                    SortMultipleOptions::default().with_order_descending(false),
                )
                .tail(count)
                .collect()
            {
                if let (Ok(col_date), Ok(col_val)) = (
                    df.column(&self.date_field_name),
                    df.column(origin_field_name),
                ) {
                    let mut vals = vec![];

                    for i in 0..col_date.len() {
                        if let (Ok(cell_date), Ok(cell_val)) = (col_date.get(i), col_val.get(i)) {
                            if let (Some(date_days_after_epoch), Some(val)) =
                                (cell_date.extract::<i32>(), cell_val.extract::<T>())
                            {
                                if let Some(date) =
                                    NaiveDate::from_epoch_days(date_days_after_epoch)
                                {
                                    vals.push((date, val));
                                }
                            }
                        }
                    }

                    return vals;
                }
            }
        }

        vec![]
    }

    pub fn get_latest_values_with_label<T: NumCast>(
        &self,
        date: &NaiveDate,
        include_today: bool,
        field_name: &str,
        label_field_name: &str,
        count: u32,
    ) -> Vec<(NaiveDate, T, Option<String>)> {
        if let (Some(origin_field_name), Some(origin_label_field_name)) = (
            self.value_field_names.get(field_name),
            self.value_field_names.get(label_field_name),
        ) {
            let filter = if include_today {
                col(&self.date_field_name)
                    .lt_eq(lit(*date))
                    .and(col(origin_field_name).is_not_null())
            } else {
                col(&self.date_field_name)
                    .lt(lit(*date))
                    .and(col(origin_field_name).is_not_null())
            };

            if let Ok(df) = self
                .df
                .clone()
                .lazy()
                .filter(filter)
                .sort(
                    [&self.date_field_name],
                    SortMultipleOptions::default().with_order_descending(false),
                )
                .tail(count)
                .collect()
            {
                let mut vals = vec![];

                if let (Ok(col_date), Ok(col_val), Ok(col_label)) = (
                    df.column(&self.date_field_name),
                    df.column(origin_field_name),
                    df.column(origin_label_field_name),
                ) {
                    for i in 0..col_date.len() {
                        if let (Ok(cell_date), Ok(cell_val), Ok(cell_label)) =
                            (col_date.get(i), col_val.get(i), col_label.get(i))
                        {
                            if let (Some(date_days_after_epoch), Some(val), label) = (
                                cell_date.extract::<i32>(),
                                cell_val.extract::<T>(),
                                cell_label.get_str(),
                            ) {
                                if let Some(date) =
                                    NaiveDate::from_epoch_days(date_days_after_epoch)
                                {
                                    vals.push((date, val, label.map(|s| s.to_string())));
                                }
                            }
                        }
                    }
                }

                return vals;
            }
        }

        vec![]
    }

    pub fn get_values<T: NumCast>(
        &self,
        date_from: &NaiveDate,
        date_to: &NaiveDate,
        field_name: &str,
    ) -> Vec<(NaiveDate, T)> {
        if let Some(origin_field_name) = self.value_field_names.get(field_name) {
            if let Ok(df) = self
                .df
                .clone()
                .lazy()
                .filter(
                    col(&self.date_field_name)
                        .gt_eq(lit(*date_from))
                        .and(col(&self.date_field_name).lt_eq(lit(*date_to)))
                        .and(col(origin_field_name).is_not_null()),
                )
                .collect()
            {
                let mut vals = vec![];

                if let (Ok(col_date), Ok(col_val)) = (
                    df.column(&self.date_field_name),
                    df.column(origin_field_name),
                ) {
                    for i in 0..col_date.len() {
                        if let (Ok(cell_date), Ok(cell_val)) = (col_date.get(i), col_val.get(i)) {
                            if let (Some(date_days_after_epoch), Some(val)) =
                                (cell_date.extract::<i32>(), cell_val.extract::<T>())
                            {
                                if let Some(date) =
                                    NaiveDate::from_epoch_days(date_days_after_epoch)
                                {
                                    vals.push((date, val));
                                }
                            }
                        }
                    }
                }

                return vals;
            }
        }

        vec![]
    }
}
