use std::{collections::HashMap, path::Path, str::FromStr};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::{
    error::VfResult,
    ticker::{Ticker, TickersIndex},
};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct FofDefinition {
    pub title: String,

    #[serde(default)]
    pub funds: HashMap<String, f64>,

    #[serde(default)]
    pub search: HashMap<String, Vec<f64>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct FundDefinition {
    pub title: String,

    #[serde(default)]
    pub tickers: TickersDefinition,

    #[serde(default)]
    pub ticker_sources: Vec<String>,

    #[serde(default)]
    pub rules: Vec<RuleDefinition>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Frequency {
    pub days: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RuleDefinition {
    pub name: String,

    #[serde(default)]
    #[serde(deserialize_with = "deserialize_frequency")]
    pub frequency: Frequency,

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

impl FofDefinition {
    pub fn from_file(path: &Path) -> VfResult<Self> {
        confy::load_path(path).map_err(Into::into)
    }
}

impl FundDefinition {
    pub fn from_file(path: &Path) -> VfResult<Self> {
        confy::load_path(path).map_err(Into::into)
    }

    pub async fn all_tickers_map(
        &self,
        date: &NaiveDate,
    ) -> VfResult<HashMap<Ticker, (f64, Option<TickersIndex>)>> {
        let mut all_tickers_map: HashMap<Ticker, (f64, Option<TickersIndex>)> = HashMap::new();

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

        for ticker_source_str in &self.ticker_sources {
            let ticker_source = TickersIndex::from_str(ticker_source_str)?;
            let index_tickers = ticker_source.all_tickers(date).await?;
            for ticker in index_tickers {
                all_tickers_map.insert(ticker, (1.0, Some(ticker_source.clone())));
            }
        }

        Ok(all_tickers_map)
    }
}

impl Default for TickersDefinition {
    fn default() -> Self {
        TickersDefinition::Array(Vec::new())
    }
}

fn deserialize_frequency<'de, D>(deserializer: D) -> Result<Frequency, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?.to_lowercase();

    let days: i64 = if s.ends_with("d") {
        s[..s.len() - 1].parse().map_err(serde::de::Error::custom)?
    } else if s.ends_with("w") {
        let weeks: i64 = s[..s.len() - 1].parse().map_err(serde::de::Error::custom)?;
        7 * weeks
    } else if s.ends_with("m") {
        let months: i64 = s[..s.len() - 1].parse().map_err(serde::de::Error::custom)?;
        30 * months
    } else if s.ends_with("y") {
        let years: i64 = s[..s.len() - 1].parse().map_err(serde::de::Error::custom)?;
        365 * years
    } else {
        0
    };

    Ok(Frequency { days })
}
