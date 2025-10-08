use std::{collections::HashMap, path::Path, str::FromStr};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::{
    error::VfResult,
    ticker::{Ticker, TickersIndex},
};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct FundDefinition {
    pub title: String,

    #[serde(default)]
    pub tickers: Vec<String>,

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

impl FundDefinition {
    pub fn from_file(path: &Path) -> VfResult<Self> {
        confy::load_path(path).map_err(Into::into)
    }

    pub async fn all_tickers(
        &self,
        date: &NaiveDate,
    ) -> VfResult<HashMap<Ticker, Option<TickersIndex>>> {
        let mut all_tickers: HashMap<Ticker, Option<TickersIndex>> = HashMap::new();

        for ticker_str in &self.tickers {
            let ticker = Ticker::from_str(ticker_str)?;
            all_tickers.insert(ticker, None);
        }

        for ticker_source_str in &self.ticker_sources {
            let ticker_source = TickersIndex::from_str(ticker_source_str)?;
            let index_tickers = ticker_source.all_tickers(date).await?;
            for ticker in index_tickers {
                all_tickers.insert(ticker, Some(ticker_source.clone()));
            }
        }

        Ok(all_tickers)
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
