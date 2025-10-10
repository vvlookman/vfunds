use std::{fmt::Display, str::FromStr};

use chrono::NaiveDate;

use crate::{
    error::{VfError, VfResult},
    financial::index::{fetch_cnindex_tickers, fetch_csindex_tickers},
    utils::text::is_ascii_digits,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Ticker {
    pub exchange: String,
    pub symbol: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TickersIndex {
    pub provider: String,
    pub symbol: String,
}

impl FromStr for Ticker {
    type Err = VfError;
    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let s = s.trim();

        let ticker = if is_ascii_digits(s) {
            let exchange = if s.len() == 6 {
                if s.starts_with("60")
                    || s.starts_with("68")
                    || s.starts_with("51")
                    || s.starts_with("58")
                {
                    Some("SH")
                } else if s.starts_with("00")
                    || s.starts_with("30")
                    || s.starts_with("15")
                    || s.starts_with("16")
                {
                    Some("SZ")
                } else if s.starts_with("92")
                    || s.starts_with("83")
                    || s.starts_with("43")
                    || s.starts_with("87")
                {
                    Some("BJ")
                } else {
                    None
                }
            } else if s.len() == 5 {
                Some("HK")
            } else {
                None
            };

            if let Some(exchange) = exchange {
                Some(Self {
                    exchange: exchange.to_string(),
                    symbol: s.to_uppercase().to_string(),
                })
            } else {
                None
            }
        } else {
            if let Some((symbol, exchange)) = s.rsplit_once('.') {
                Some(Self {
                    exchange: exchange.trim().to_uppercase().to_string(),
                    symbol: symbol.trim().to_uppercase().to_string(),
                })
            } else {
                None
            }
        };

        if let Some(ticker) = ticker {
            Ok(ticker)
        } else {
            Err(VfError::Invalid(
                "INVALID_TICKER",
                format!("Invalid ticker '{s}'"),
            ))
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

impl FromStr for TickersIndex {
    type Err = VfError;
    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let s = s.trim();

        if let Some((symbol, provider)) = s.rsplit_once('.') {
            Ok(Self {
                provider: provider.trim().to_uppercase().to_string(),
                symbol: symbol.trim().to_uppercase().to_string(),
            })
        } else {
            Err(VfError::Invalid(
                "INVALID_TICKERS_INDEX",
                format!("Invalid tickers index '{s}'"),
            ))
        }
    }
}

impl Display for TickersIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.symbol, self.provider)
    }
}

impl TickersIndex {
    pub async fn all_tickers(&self, date: &NaiveDate) -> VfResult<Vec<Ticker>> {
        let tickers = match self.provider.as_str() {
            "CNI" | "CNINDEX" => fetch_cnindex_tickers(&self.symbol, date).await?,
            "CSI" | "CSINDEX" => fetch_csindex_tickers(&self.symbol).await?,
            _ => {
                return Err(VfError::Invalid(
                    "UNSUPPORTED_TICKERS_INDEX",
                    format!("Unsupported tickers index '{self}'"),
                ));
            }
        };

        Ok(tickers)
    }
}
