use std::{collections::HashMap, fs::read_dir};

use chrono::NaiveDate;
use log::debug;
use rayon::prelude::*;

use crate::{FundDefinition, WORKSPACE, error::*, utils};

pub struct BacktestOptions {
    pub init_cash: f64,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
}

pub struct FundBacktestResult {}

pub async fn backtest(
    funds: &[String],
    options: &BacktestOptions,
) -> VfResult<HashMap<String, FundBacktestResult>> {
    debug!("{funds:?}");

    let mut result_map: HashMap<String, FundBacktestResult> = HashMap::new();

    for fund in funds {
        result_map.insert(fund.to_string(), FundBacktestResult {});
    }

    Ok(result_map)
}

pub async fn funds() -> VfResult<Vec<(String, FundDefinition)>> {
    let mut funds: Vec<(String, FundDefinition)> = vec![];

    let workspace = WORKSPACE.read()?;
    if let Ok(entries) = read_dir(&*workspace) {
        let mut entries: Vec<_> = entries.filter_map(|entry| entry.ok()).collect();
        entries.par_sort_by(|a, b| {
            utils::text::compare_phonetic(
                &a.file_name().to_string_lossy(),
                &b.file_name().to_string_lossy(),
            )
        });

        for entry in entries {
            let entry_path = entry.path();
            if let Some(name) = entry_path.file_stem() {
                let fund_name = name.to_string_lossy().to_string();
                let fund_definition = FundDefinition::from_file(&entry_path)?;
                funds.push((fund_name, fund_definition));
            }
        }
    }

    Ok(funds)
}
