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

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct FundDefinition {
    pub title: String,

    #[serde(default)]
    pub tickers: Vec<String>,

    #[serde(default)]
    pub tickers_sources: Vec<String>,

    #[serde(default)]
    pub rules: Vec<RuleDefinition>,
}

#[derive(
    Serialize, Deserialize, Clone, Debug, Default, PartialEq, strum::Display, strum::EnumString,
)]
#[strum(ascii_case_insensitive)]
pub enum Frequency {
    #[default]
    Once,

    #[strum(serialize = "1d")]
    Daily,

    #[strum(serialize = "1w", serialize = "7d")]
    Weekly,

    #[strum(serialize = "2w", serialize = "14d")]
    Biweekly,

    #[strum(serialize = "1m")]
    Monthly,

    #[strum(serialize = "3m")]
    Quarterly,

    #[strum(serialize = "6m")]
    Semiannually,

    #[strum(serialize = "12m", serialize = "1y")]
    Annually,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RuleDefinition {
    pub name: String,

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
    let s = String::deserialize(deserializer)?;
    Frequency::from_str(&s).map_err(serde::de::Error::custom)
}
