use std::{
    collections::HashMap, num::ParseIntError, ops::RangeInclusive, panic, path::Path, str::FromStr,
};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    DAYS_PER_YEAR,
    error::VfResult,
    financial::{index::fetch_index_tickers, sector::fetch_sector_tickers},
    ticker::{Ticker, TickersIndex},
};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct FofDefinition {
    pub title: String,
    pub description: Option<String>,

    #[serde(default)]
    #[serde(deserialize_with = "deserialize_frequency")]
    pub frequency: Frequency,

    #[serde(default)]
    pub funds: HashMap<String, f64>,

    #[serde(default)]
    pub search: FofSearch,
}

impl FofDefinition {
    pub fn from_file(path: &Path) -> VfResult<Self> {
        confy::load_path(path).map_err(Into::into)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct FofSearch {
    #[serde(default)]
    pub frequency: Vec<String>,

    #[serde(default)]
    pub funds: HashMap<String, Vec<f64>>,
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
    days: u64,
    string: String,
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
            (30.43685 * months as f64).round() as u64
        } else if let Some(stripped) = s.strip_suffix("y") {
            let years: u64 = stripped.parse()?;
            (DAYS_PER_YEAR * years as f64).round() as u64
        } else {
            s.parse()?
        };

        Ok(Frequency {
            days,
            string: s.to_string(),
        })
    }
}

impl Frequency {
    pub fn to_days(&self) -> u64 {
        self.days
    }

    pub fn to_str(&self) -> &str {
        &self.string
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
    pub options: RuleOptions,

    #[serde(default)]
    pub search: RuleSearch,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RuleOptions(HashMap<String, serde_json::Value>);

impl RuleOptions {
    pub fn read_array(&self, key: &str) -> Option<&Vec<Value>> {
        self.0.get(key).and_then(|v| v.as_array())
    }

    pub fn read_bool(&self, key: &str, default_value: bool) -> bool {
        self.0
            .get(key)
            .and_then(|v| v.as_bool())
            .unwrap_or(default_value)
    }

    pub fn read_f64(&self, key: &str, default_value: f64) -> f64 {
        self.0
            .get(key)
            .and_then(|v| v.as_f64())
            .unwrap_or(default_value)
    }

    pub fn read_f64_gt(&self, key: &str, default_value: f64, min_value: f64) -> f64 {
        let val = self
            .0
            .get(key)
            .and_then(|v| v.as_f64())
            .unwrap_or(default_value);

        if val <= min_value {
            panic!("Option '{key}' must > {min_value}");
        }

        val
    }

    pub fn read_f64_gte(&self, key: &str, default_value: f64, min_value: f64) -> f64 {
        let val = self
            .0
            .get(key)
            .and_then(|v| v.as_f64())
            .unwrap_or(default_value);

        if val < min_value {
            panic!("Option '{key}' must >= {min_value}");
        }

        val
    }

    pub fn read_f64_in_range(
        &self,
        key: &str,
        default_value: f64,
        range: RangeInclusive<f64>,
    ) -> f64 {
        let val = self
            .0
            .get(key)
            .and_then(|v| v.as_f64())
            .unwrap_or(default_value);

        if !range.contains(&val) {
            panic!(
                "Option '{key}' must >= {} and <= {}",
                range.start(),
                range.end()
            );
        }

        val
    }

    pub fn read_object(&self, key: &str) -> Option<&Map<String, Value>> {
        self.0.get(key).and_then(|v| v.as_object())
    }

    pub fn read_str(&self, key: &str, default_value: &'static str) -> &str {
        self.0
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or(default_value)
    }

    pub fn read_u64(&self, key: &str, default_value: u64) -> u64 {
        self.0
            .get(key)
            .and_then(|v| v.as_u64())
            .unwrap_or(default_value)
    }

    pub fn read_u64_no_zero(&self, key: &str, default_value: u64) -> u64 {
        let val = self
            .0
            .get(key)
            .and_then(|v| v.as_u64())
            .unwrap_or(default_value);

        if val == 0 {
            panic!("Option '{key}' must > 0");
        }

        val
    }

    pub fn set(&mut self, key: &str, value: Value) {
        self.0.insert(key.to_string(), value);
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RuleSearch {
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_frequency_vec")]
    pub frequency: Vec<Frequency>,

    #[serde(default)]
    pub options: HashMap<String, Vec<serde_json::Value>>,
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

fn deserialize_frequency_vec<'de, D>(deserializer: D) -> Result<Vec<Frequency>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let strs: Vec<String> = Vec::deserialize(deserializer)?;

    strs.into_iter()
        .map(|s| Frequency::from_str(&s).map_err(serde::de::Error::custom))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[tokio::test]
    async fn test_fof_definition() {
        assert!(FofDefinition::from_file(&PathBuf::from("example/afof.fof.toml")).is_ok());
    }

    #[tokio::test]
    async fn test_fund_definition() {
        assert!(FundDefinition::from_file(&PathBuf::from("example/conv-bond.fund.toml")).is_ok());
    }
}
