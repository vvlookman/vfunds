use std::{fmt::Display, str::FromStr};

use chrono::NaiveDate;

use crate::{
    error::{VfError, VfResult},
    financial::index::{fetch_cnindex_tickers, fetch_csindex_tickers},
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Ticker {
    pub exchange: String,
    pub symbol: String,
}

#[derive(Clone, Debug)]
pub struct TickersSource {
    pub name: String,
    pub symbol: String,
}

impl FromStr for Ticker {
    type Err = VfError;
    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let s = s.trim();

        if let Some((symbol, exchange)) = s.rsplit_once('.') {
            Ok(Self {
                exchange: exchange.trim().to_uppercase().to_string(),
                symbol: symbol.trim().to_uppercase().to_string(),
            })
        } else {
            let exchange = if s.len() == 6 {
                if s.starts_with("600")
                    || s.starts_with("601")
                    || s.starts_with("603")
                    || s.starts_with("688")
                    || s.starts_with("51")
                    || s.starts_with("588")
                {
                    Some("SH")
                } else if s.starts_with("000")
                    || s.starts_with("002")
                    || s.starts_with("300")
                    || s.starts_with("15")
                    || s.starts_with("16")
                {
                    Some("SZ")
                } else {
                    None
                }
            } else if s.len() == 5 {
                Some("HK")
            } else {
                None
            };

            if let Some(exchange) = exchange {
                Ok(Self {
                    exchange: exchange.to_string(),
                    symbol: s.to_uppercase().to_string(),
                })
            } else {
                Err(VfError::Invalid(
                    "UNSUPPORTED_EXCHANGE",
                    format!("Unsupported exchange '{s}'"),
                ))
            }
        }
    }
}

impl Display for Ticker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.symbol, self.exchange)
    }
}

impl Ticker {
    pub fn to_qmt_code(&self) -> String {
        format!("{}.{}", self.symbol, self.exchange)
    }
}

impl FromStr for TickersSource {
    type Err = VfError;
    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let s = s.trim();

        let parts: Vec<_> = s.splitn(2, ':').collect();
        if parts.len() == 2 {
            Ok(Self {
                name: parts[0].trim().to_uppercase().to_string(),
                symbol: parts[1].trim().to_uppercase().to_string(),
            })
        } else {
            Err(VfError::Invalid(
                "UNSUPPORTED_TICKERS_SOURCE",
                format!("Unsupported tickers source '{s}'"),
            ))
        }
    }
}

impl Display for TickersSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.name, self.symbol)
    }
}

impl TickersSource {
    pub async fn extract_tickers(&self, date: &NaiveDate) -> VfResult<Vec<Ticker>> {
        let tickers = match self.name.to_lowercase().as_str() {
            "cnindex" => fetch_cnindex_tickers(&self.symbol, date).await?,
            "csindex" => fetch_csindex_tickers(&self.symbol).await?,
            _ => {
                return Err(VfError::Invalid(
                    "UNSUPPORTED_TICKERS_SOURCE",
                    format!("Unsupported tickers source '{self}'"),
                ));
            }
        };

        Ok(tickers)
    }
}
