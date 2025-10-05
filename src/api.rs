use std::{fs::read_dir, path::PathBuf};

use rayon::prelude::*;

use crate::{
    WORKSPACE, backtest,
    error::*,
    spec::{FundDefinition, RuleDefinition},
    utils,
};

pub type BacktestEvent = backtest::BacktestEvent;
pub type BacktestOptions = backtest::BacktestOptions;
pub type BacktestStream = backtest::BacktestStream;

pub async fn backtest(
    fund_names: &[String],
    options: &BacktestOptions,
) -> VfResult<Vec<(String, BacktestStream)>> {
    let mut streams: Vec<(String, BacktestStream)> = vec![];

    let mut funds = funds().await?;
    if !fund_names.is_empty() {
        funds.retain(|(name, _)| fund_names.contains(name));
    }

    for fund_name in fund_names {
        if funds.iter().filter(|(name, _)| name == fund_name).count() == 0 {
            return Err(VfError::NotExists(
                "FUND_NOT_EXISTS",
                format!("Fund '{fund_name}' not exists"),
            ));
        }
    }

    if let Some(benchmark_str) = &options.benchmark {
        let fund_definition = FundDefinition {
            title: format!("Benchmark: {benchmark_str}"),
            tickers: vec![benchmark_str.to_string()],
            rules: vec![RuleDefinition {
                name: "hold_equal".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let stream = backtest::backtest_fund(&fund_definition, options).await?;
        streams.push((format!("# {benchmark_str} #"), stream));
    }

    for (fund_name, fund_definition) in funds {
        let stream = backtest::backtest_fund(&fund_definition, options).await?;
        streams.push((fund_name, stream));
    }

    Ok(streams)
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
