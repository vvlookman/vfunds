use std::{collections::HashMap, num::ParseIntError, path::Path, str::FromStr};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::{
    error::VfResult,
    financial::{index::fetch_index_tickers, sector::fetch_sector_tickers},
    ticker::{Ticker, TickersIndex},
};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct FofDefinition {
    pub title: String,
    pub description: Option<String>,

    #[serde(default)]
    pub funds: HashMap<String, f64>,

    #[serde(default)]
    pub search: HashMap<String, Vec<f64>>,
}

impl FofDefinition {
    pub fn from_file(path: &Path) -> VfResult<Self> {
        confy::load_path(path).map_err(Into::into)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct FundDefinition {
    pub title: String,
    pub description: Option<String>,

    #[serde(default)]
    pub options: FundOptions,

    #[serde(default)]
    pub tickers: TickersDefinition,

    #[serde(default)]
    pub ticker_sources: Vec<TickerSourceDefinition>,

    #[serde(default)]
    pub rules: Vec<RuleDefinition>,
}

impl FundDefinition {
    pub fn from_file(path: &Path) -> VfResult<Self> {
        confy::load_path(path).map_err(Into::into)
    }

    pub async fn all_tickers_map(
        &self,
        date: &NaiveDate,
    ) -> VfResult<HashMap<Ticker, (f64, Option<TickerSourceDefinition>)>> {
        let mut all_tickers_map = HashMap::new();

        match &self.tickers {
            TickersDefinition::Array(array) => {
                for ticker_str in array {
                    let ticker = Ticker::from_str(ticker_str)?;
                    all_tickers_map.insert(ticker, (1.0, None));
                }
            }
            TickersDefinition::Map(map) => {
                for (ticker_str, weight) in map {
                    if *weight > 0.0 {
                        let ticker = Ticker::from_str(ticker_str)?;
                        all_tickers_map.insert(ticker, (*weight, None));
                    }
                }
            }
        };

        for ticker_source in &self.ticker_sources {
            match ticker_source.source_type {
                TickerSourceType::Index => {
                    let index = TickersIndex::from_str(&ticker_source.source)?;
                    let index_tickers = fetch_index_tickers(&index, date).await?;
                    for ticker in index_tickers {
                        all_tickers_map.insert(ticker, (1.0, Some(ticker_source.clone())));
                    }
                }
                TickerSourceType::Sector => {
                    let tickers_sector_map = fetch_sector_tickers(&ticker_source.source).await?;
                    for ticker in tickers_sector_map.keys() {
                        all_tickers_map.insert(ticker.clone(), (1.0, Some(ticker_source.clone())));
                    }
                }
            }
        }

        Ok(all_tickers_map)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Frequency {
    pub days: u64,
}

impl FromStr for Frequency {
    type Err = ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let days: u64 = if let Some(stripped) = s.strip_suffix("d") {
            stripped.parse()?
        } else if let Some(stripped) = s.strip_suffix("w") {
            let weeks: u64 = stripped.parse()?;
            7 * weeks
        } else if let Some(stripped) = s.strip_suffix("m") {
            let months: u64 = stripped.parse()?;
            (30.4375 * months as f64).round() as u64
        } else if let Some(stripped) = s.strip_suffix("y") {
            let years: u64 = stripped.parse()?;
            (365.25 * years as f64).round() as u64
        } else {
            0
        };

        Ok(Frequency { days })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct FundOptions {
    pub suspend_months: Vec<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RuleDefinition {
    pub name: String,

    #[serde(default)]
    #[serde(deserialize_with = "deserialize_frequency")]
    pub frequency: Frequency,

    #[serde(default)]
    pub frequency_take_profit_pct: u32,

    #[serde(default)]
    pub options: HashMap<String, serde_json::Value>,

    #[serde(default)]
    pub search: HashMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum TickersDefinition {
    Array(Vec<String>),
    Map(HashMap<String, f64>),
}

impl Default for TickersDefinition {
    fn default() -> Self {
        TickersDefinition::Array(Vec::new())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TickerSourceDefinition {
    pub source: String,
    pub source_type: TickerSourceType,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "lowercase")]
pub enum TickerSourceType {
    Index,
    Sector,
}

fn deserialize_frequency<'de, D>(deserializer: D) -> Result<Frequency, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?.to_lowercase();

    Frequency::from_str(&s).map_err(serde::de::Error::custom)
}
