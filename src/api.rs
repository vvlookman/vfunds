use std::{fs::read_dir, path::PathBuf};

use log::debug;
use rayon::prelude::*;

use crate::{WORKSPACE, backtest, error::*, spec::FundDefinition, utils};

pub type BacktestOptions = backtest::BacktestOptions;
pub type BacktestResult = backtest::BacktestResult;

pub async fn backtest(
    fund_names: &[String],
    options: &BacktestOptions,
) -> VfResult<Vec<(String, BacktestResult)>> {
    let mut results: Vec<(String, BacktestResult)> = vec![];

    let mut funds = funds().await?;
    if !fund_names.is_empty() {
        funds.retain(|(name, _)| fund_names.contains(name));
    }

    debug!(
        "Backtest funds: {:?}",
        funds.iter().map(|(name, _)| name).collect::<Vec<_>>()
    );

    for (fund_name, fund_definition) in funds {
        let result = backtest::run_fund(&fund_definition, options).await?;
        results.push((fund_name, result));
    }

    Ok(results)
}

pub async fn funds() -> VfResult<Vec<(String, FundDefinition)>> {
    let mut funds: Vec<(String, FundDefinition)> = vec![];

    let workspace = { WORKSPACE.read()? };
    if let Ok(entries) = read_dir(&*workspace) {
        let mut entries: Vec<_> = entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let entry_path = entry.path();
                !entry_path.is_dir()
                    && entry_path.extension().map(|s| s.to_ascii_lowercase()) == Some("toml".into())
            })
            .collect();
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

pub fn get_workspace() -> VfResult<PathBuf> {
    let workspace = { WORKSPACE.read()? };
    Ok(workspace.clone())
}
