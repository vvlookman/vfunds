use std::{path::Path, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::error::VfResult;

#[derive(Serialize, Deserialize, Default)]
pub struct FundDefinition {
    pub title: String,
    pub tickers: Vec<String>,
    pub rules: Vec<RuleDefinition>,
}

#[derive(Serialize, Deserialize, Clone, Default, PartialEq, strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum Frequency {
    #[default]
    Once,
    Daily,
    Weekly,
    Biweekly,
    Monthly,
    Quarterly,
    Yearly,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct RuleDefinition {
    pub name: String,

    #[serde(deserialize_with = "deserialize_frequency")]
    pub frequency: Frequency,
}

impl FundDefinition {
    pub fn from_file(path: &Path) -> VfResult<Self> {
        confy::load_path(path).map_err(Into::into)
    }
}

fn deserialize_frequency<'de, D>(deserializer: D) -> Result<Frequency, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Frequency::from_str(&s).map_err(serde::de::Error::custom)
}
