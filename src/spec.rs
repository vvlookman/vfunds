use std::{
    collections::{HashMap, HashSet},
    path::Path,
    str::FromStr,
};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::{
    error::VfResult,
    ticker::{Ticker, TickersSource},
};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct FundDefinition {
    pub title: String,

    #[serde(default)]
    pub tickers: Vec<String>,

    #[serde(default)]
    pub tickers_sources: Vec<String>,

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
}

impl FundDefinition {
    pub fn from_file(path: &Path) -> VfResult<Self> {
        confy::load_path(path).map_err(Into::into)
    }

    pub async fn all_tickers(&self, date: &NaiveDate) -> VfResult<Vec<Ticker>> {
        let mut tickers: HashSet<Ticker> = HashSet::new();

        for ticker_str in &self.tickers {
            let ticker = Ticker::from_str(ticker_str)?;
            tickers.insert(ticker);
        }

        for tickers_source_str in &self.tickers_sources {
            let tickers_source = TickersSource::from_str(tickers_source_str)?;
            let tickers_from_source = tickers_source.extract_tickers(date).await?;

            for ticker in tickers_from_source {
                tickers.insert(ticker);
            }
        }

        Ok(tickers.into_iter().collect())
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
