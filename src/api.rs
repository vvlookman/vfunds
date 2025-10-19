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
    spec::{FofDefinition, FundDefinition, RuleDefinition, TickersDefinition},
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
    names: &[String],
    options: &BacktestOptions,
) -> VfResult<Vec<(String, BacktestStream)>> {
    let mut streams: Vec<(String, BacktestStream)> = vec![];

    let mut fof_definitions = load_fof_definitions().await?;
    let mut fund_definitions = load_fund_definitions().await?;
    if !names.is_empty() {
        fof_definitions.retain(|(name, _)| names.contains(name));
        fund_definitions.retain(|(name, _)| names.contains(name));
    }

    for name in names {
        if fof_definitions.iter().filter(|(v, _)| v == name).count() == 0
            && fund_definitions.iter().filter(|(v, _)| v == name).count() == 0
        {
            return Err(VfError::NotExists {
                code: "FOF_OR_FUND_NOT_EXISTS",
                message: format!("FOF/Fund '{name}' not exists"),
            });
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

    for (fof_name, fof_definition) in fof_definitions {
        let stream = backtest::backtest_fof(&fof_definition, options).await?;
        streams.push((fof_name, stream));
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
                !entry_path.is_dir() && entry_path.to_string_lossy().ends_with(".backtest.json")
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
            if let Some(file_stem) = entry_path.file_stem() {
                let fund_name = file_stem
                    .to_string_lossy()
                    .strip_suffix(".backtest")
                    .unwrap_or(&file_stem.to_string_lossy())
                    .to_string();

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

pub async fn get_workspace() -> VfResult<PathBuf> {
    let workspace = { WORKSPACE.read().await.clone() };
    Ok(workspace)
}

pub async fn load_fof_definitions() -> VfResult<Vec<(String, FofDefinition)>> {
    let mut fof_definitions: Vec<(String, FofDefinition)> = vec![];

    let workspace = { WORKSPACE.read().await.clone() };
    if let Ok(entries) = fs::read_dir(&*workspace) {
        let mut entries: Vec<_> = entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let entry_path = entry.path();
                !entry_path.is_dir() && entry_path.to_string_lossy().ends_with(".fof.toml")
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
            if let Some(file_stem) = entry_path.file_stem() {
                let fund_name = file_stem
                    .to_string_lossy()
                    .strip_suffix(".fof")
                    .unwrap_or(&file_stem.to_string_lossy())
                    .to_string();

                let fof_definition = FofDefinition::from_file(&entry_path)?;
                fof_definitions.push((fund_name, fof_definition));
            }
        }
    }

    Ok(fof_definitions)
}

pub async fn load_fund_definitions() -> VfResult<Vec<(String, FundDefinition)>> {
    let mut fund_definitions: Vec<(String, FundDefinition)> = vec![];

    let workspace = { WORKSPACE.read().await.clone() };
    if let Ok(entries) = fs::read_dir(&*workspace) {
        let mut entries: Vec<_> = entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let entry_path = entry.path();
                !entry_path.is_dir() && entry_path.to_string_lossy().ends_with(".fund.toml")
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
            if let Some(file_stem) = entry_path.file_stem() {
                let fund_name = file_stem
                    .to_string_lossy()
                    .strip_suffix(".fund")
                    .unwrap_or(&file_stem.to_string_lossy())
                    .to_string();

                let fund_definition = FundDefinition::from_file(&entry_path)?;
                fund_definitions.push((fund_name, fund_definition));
            }
        }
    }

    Ok(fund_definitions)
}
