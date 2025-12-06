use std::{
    collections::HashMap,
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};

use chrono::NaiveDate;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    CONFIG, CONFIG_PATH, Config, VERSION, WORKSPACE, backtest,
    ds::*,
    error::*,
    spec::{FofDefinition, FundDefinition},
    utils,
    utils::datetime::{date_from_str, date_to_str},
};

pub type BacktestCvOptions = backtest::BacktestCvOptions;
pub type BacktestEvent = backtest::BacktestEvent;
pub type BacktestMetrics = backtest::BacktestMetrics;
pub type BacktestOptions = backtest::BacktestOptions;
pub type BacktestResult = backtest::BacktestResult;
pub type BacktestStream = backtest::BacktestStream;

#[derive(Serialize, Deserialize)]
pub struct BacktestOutputResult {
    pub title: Option<String>,
    pub options: BacktestOptions,
    pub portfolio: BacktestOutputPortfolio,
    pub metrics: BacktestMetrics,

    #[serde(default)]
    pub order_dates: Vec<NaiveDate>,

    #[serde(default)]
    pub version: String,
}

#[derive(Serialize, Deserialize)]
pub struct BacktestOutputPortfolio {
    pub cash: f64,
    pub positions_value: HashMap<String, f64>,
}

pub enum Vfund {
    Fof(FofDefinition),
    Fund(FundDefinition),
}

pub async fn backtest(
    vfund_names: &[String],
    options: &BacktestOptions,
) -> VfResult<Vec<(String, BacktestStream)>> {
    let mut vfunds = load_vfunds().await?;
    if !vfund_names.is_empty() {
        vfunds.retain(|(name, _)| vfund_names.contains(name));
    }

    for name in vfund_names {
        if vfunds.iter().filter(|(v, _)| v == name).count() == 0 {
            return Err(VfError::NotExists {
                code: "VFUND_NOT_EXISTS",
                message: format!("Vfund '{name}' not exists"),
            });
        }
    }

    let mut streams: Vec<(String, BacktestStream)> = vec![];

    for (vfund_name, vfund) in vfunds {
        let stream = match vfund {
            Vfund::Fof(fof_definition) => backtest::backtest_fof(&fof_definition, options).await?,
            Vfund::Fund(fund_definition) => {
                backtest::backtest_fund(&fund_definition, options).await?
            }
        };
        streams.push((vfund_name, stream));
    }

    Ok(streams)
}

pub async fn backtest_cv(
    vfund_names: &[String],
    cv_options: &BacktestCvOptions,
) -> VfResult<Vec<(String, BacktestStream)>> {
    let mut vfunds = load_vfunds().await?;
    if !vfund_names.is_empty() {
        vfunds.retain(|(name, _)| vfund_names.contains(name));
    }

    for name in vfund_names {
        if vfunds.iter().filter(|(v, _)| v == name).count() == 0 {
            return Err(VfError::NotExists {
                code: "VFUND_NOT_EXISTS",
                message: format!("Vfund '{name}' not exists"),
            });
        }
    }

    let mut streams: Vec<(String, BacktestStream)> = vec![];

    for (vfund_name, vfund) in vfunds {
        let stream = match vfund {
            Vfund::Fof(fof_definition) => {
                backtest::backtest_fof_cv(&fof_definition, cv_options).await?
            }
            Vfund::Fund(fund_definition) => {
                backtest::backtest_fund_cv(&fund_definition, cv_options).await?
            }
        };
        streams.push((vfund_name, stream));
    }

    Ok(streams)
}

pub async fn check() -> VfResult<Vec<(&'static str, Option<VfError>)>> {
    let (qmt_result, tushare_result) = tokio::join!(qmt::check_api(), tushare::check_api());

    Ok(vec![
        ("QMT", qmt_result.err()),
        ("Tushare", tushare_result.err()),
    ])
}

pub async fn get_config() -> VfResult<Config> {
    let config = { CONFIG.read().await.clone() };
    Ok(config)
}

pub async fn get_workspace() -> VfResult<PathBuf> {
    let workspace = { WORKSPACE.read().await.clone() };
    Ok(workspace)
}

pub async fn load_backtest_results(
    output_dir: &Path,
    vfund_names: &[String],
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
                let vfund_name = file_stem
                    .to_string_lossy()
                    .strip_suffix(".backtest")
                    .unwrap_or(&file_stem.to_string_lossy())
                    .to_string();

                if !vfund_names.is_empty() && !vfund_names.contains(&vfund_name) {
                    continue;
                }

                let content = fs::read_to_string(&entry_path)?;
                let result = serde_json::from_str(&content)?;
                results.push((vfund_name, result));
            }
        }
    }

    Ok(results)
}

pub async fn load_backtest_values(
    output_dir: &Path,
    vfund_name: &str,
) -> VfResult<Vec<(NaiveDate, f64)>> {
    let mut result: Vec<(NaiveDate, f64)> = vec![];

    let path = output_dir.join(format!("{vfund_name}.values.csv"));
    let mut csv_reader = csv::Reader::from_path(&path)?;
    for record in csv_reader.records() {
        let row = record?;

        let date_str = &row[0];
        let value_str = &row[1];

        if let (Ok(date), Ok(value)) = (date_from_str(date_str), value_str.parse::<f64>()) {
            result.push((date, value));
        }
    }

    Ok(result)
}

pub async fn load_vfunds() -> VfResult<Vec<(String, Vfund)>> {
    let mut vfunds: Vec<(String, Vfund)> = vec![];

    let workspace = { WORKSPACE.read().await.clone() };
    if let Ok(entries) = fs::read_dir(&*workspace) {
        let mut entries: Vec<_> = entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let entry_path = entry.path();
                let entry_path_str = entry_path.to_string_lossy();
                !entry_path.is_dir()
                    && (entry_path_str.ends_with(".fof.toml")
                        || entry_path_str.ends_with(".fund.toml"))
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
                let file_stem_str = file_stem.to_string_lossy();
                if file_stem_str.ends_with(".fof") {
                    let fof_name = file_stem_str
                        .strip_suffix(".fof")
                        .unwrap_or(&file_stem.to_string_lossy())
                        .to_string();

                    let fof_definition =
                        FofDefinition::from_file(&entry_path).map_err(|_| VfError::Invalid {
                            code: "INVALID_FOF_DEFINITION",
                            message: format!("FOF definition '{}' invalid", entry_path.display()),
                        })?;
                    vfunds.push((fof_name, Vfund::Fof(fof_definition)));
                } else if file_stem_str.ends_with(".fund") {
                    let fund_name = file_stem_str
                        .strip_suffix(".fund")
                        .unwrap_or(&file_stem.to_string_lossy())
                        .to_string();

                    let fund_definition =
                        FundDefinition::from_file(&entry_path).map_err(|_| VfError::Invalid {
                            code: "INVALID_FUND_DEFINITION",
                            message: format!("Fund definition '{}' invalid", entry_path.display()),
                        })?;
                    vfunds.push((fund_name, Vfund::Fund(fund_definition)));
                }
            }
        }
    }

    Ok(vfunds)
}

pub async fn set_config(key: &str, value: &str) -> VfResult<Config> {
    let mut config = { CONFIG.read().await.clone() };

    match key.to_lowercase().as_str() {
        "qmt_api" => {
            config.qmt_api = value.to_string();
        }
        "tushare_api" => {
            config.tushare_api = value.to_string();
        }
        "tushare_token" => {
            config.tushare_token = value.to_string();
        }
        _ => {
            return Err(VfError::Invalid {
                code: "INVALID_CONFIG_KEY",
                message: format!("Invalid config key '{key}'"),
            });
        }
    }

    {
        *CONFIG.write().await = config.clone();
    }

    confy::store_path(&*CONFIG_PATH, &config)?;

    Ok(config)
}

pub async fn output_backtest(
    output_dir: &Path,
    output_name: &str,
    backtest_result: &BacktestResult,
    backtest_logs: &[String],
) -> VfResult<()> {
    {
        let result = BacktestOutputResult {
            title: backtest_result.title.clone(),
            options: backtest_result.options.clone(),
            portfolio: BacktestOutputPortfolio {
                cash: backtest_result.final_cash,
                positions_value: backtest_result
                    .final_positions_value
                    .iter()
                    .map(|(k, v)| (k.to_string(), *v))
                    .collect(),
            },
            metrics: backtest_result.metrics.clone(),
            order_dates: backtest_result.order_dates.clone(),
            version: VERSION.to_string(),
        };

        let path = output_dir.join(format!("{output_name}.backtest.json"));
        let file = fs::File::create(path)?;
        serde_json::to_writer_pretty(file, &result)?;
    }

    {
        let path = output_dir.join(format!("{output_name}.values.csv"));

        let mut csv_writer = csv::Writer::from_path(&path)?;
        csv_writer.write_record(["date", "value"])?;
        for (date, value) in &backtest_result.trade_dates_value {
            csv_writer.write_record(&[date_to_str(date), format!("{value:.2}")])?;
        }
        csv_writer.flush()?;
    }

    {
        let path = output_dir.join(format!("{output_name}.log"));

        if backtest_logs.is_empty() {
            if let Err(err) = fs::remove_file(path) {
                if err.kind() != ErrorKind::NotFound {
                    return Err(VfError::from(err));
                }
            }
        } else {
            let mut file = fs::File::create(path)?;

            for log in backtest_logs.iter().rev() {
                writeln!(file, "{log}")?;
            }

            file.flush()?;
        }
    }

    Ok(())
}
