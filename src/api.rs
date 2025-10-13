use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    WORKSPACE, backtest,
    error::*,
    spec::{FundDefinition, RuleDefinition, TickersDefinition},
    utils,
};

pub type BacktestEvent = backtest::BacktestEvent;
pub type BacktestMetrics = backtest::BacktestMetrics;
pub type BacktestOptions = backtest::BacktestOptions;
pub type BacktestResult = backtest::BacktestResult;
pub type BacktestStream = backtest::BacktestStream;

#[derive(Serialize, Deserialize)]
pub struct BacktestOutputResult {
    pub options: BacktestOptions,
    pub portfolio: BacktestOutputPortfolio,
    pub metrics: BacktestMetrics,
}

#[derive(Serialize, Deserialize)]
pub struct BacktestOutputPortfolio {
    pub cash: f64,
    pub positions_value: HashMap<String, f64>,
}

pub async fn backtest(
    fund_names: &[String],
    options: &BacktestOptions,
) -> VfResult<Vec<(String, BacktestStream)>> {
    let mut streams: Vec<(String, BacktestStream)> = vec![];

    let mut fund_definitions = fund_definitions().await?;
    if !fund_names.is_empty() {
        fund_definitions.retain(|(name, _)| fund_names.contains(name));
    }

    for fund_name in fund_names {
        if fund_definitions
            .iter()
            .filter(|(name, _)| name == fund_name)
            .count()
            == 0
        {
            return Err(VfError::NotExists(
                "FUND_NOT_EXISTS",
                format!("Fund '{fund_name}' not exists"),
            ));
        }
    }

    if let Some(benchmark_str) = &options.benchmark {
        let fund_definition = FundDefinition {
            title: format!("Benchmark: {benchmark_str}"),
            tickers: TickersDefinition::Array(vec![benchmark_str.to_string()]),
            rules: vec![RuleDefinition {
                name: "hold".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let stream = backtest::backtest_fund(&fund_definition, options).await?;
        streams.push((format!("*{benchmark_str}*"), stream));
    }

    for (fund_name, fund_definition) in fund_definitions {
        let stream = backtest::backtest_fund(&fund_definition, options).await?;
        streams.push((fund_name, stream));
    }

    Ok(streams)
}

pub async fn backtest_results(
    fund_names: &[String],
    output_dir: &Path,
) -> VfResult<Vec<(String, BacktestOutputResult)>> {
    let mut results: Vec<(String, BacktestOutputResult)> = vec![];

    if let Ok(entries) = fs::read_dir(output_dir) {
        let mut entries: Vec<_> = entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let entry_path = entry.path();
                !entry_path.is_dir()
                    && entry_path.extension().map(|s| s.to_ascii_lowercase()) == Some("json".into())
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

                if !fund_names.is_empty() && !fund_names.contains(&fund_name) {
                    continue;
                }

                let content = fs::read_to_string(&entry_path)?;
                if let Ok(result) = serde_json::from_str(&content) {
                    results.push((fund_name, result));
                }
            }
        }
    }

    Ok(results)
}

pub async fn fund_definitions() -> VfResult<Vec<(String, FundDefinition)>> {
    let mut fund_definitions: Vec<(String, FundDefinition)> = vec![];

    let workspace = { WORKSPACE.read()? };
    if let Ok(entries) = fs::read_dir(&*workspace) {
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
                fund_definitions.push((fund_name, fund_definition));
            }
        }
    }

    Ok(fund_definitions)
}

pub fn get_workspace() -> VfResult<PathBuf> {
    let workspace = { WORKSPACE.read()? };
    Ok(workspace.clone())
}
