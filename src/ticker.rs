use std::{fmt::Display, str::FromStr};

use serde::Serialize;

use crate::{error::VfError, utils::text::is_ascii_digits};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct Ticker {
    pub exchange: String,
    pub symbol: String,
    pub r#type: TickerType,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct TickersIndex {
    pub provider: String,
    pub symbol: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub enum TickerType {
    ConvBond,
    Stock,
}

impl FromStr for Ticker {
    type Err = VfError;
    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let s = s.trim();

        let ticker = if is_ascii_digits(s) {
            let exchange = detect_ticker_exchange(s);
            exchange.map(|exchange| Self {
                exchange: exchange.to_string(),
                symbol: s.to_uppercase().to_string(),
                r#type: detect_ticker_type(s),
            })
        } else {
            if let Some((symbol, exchange)) = s.rsplit_once('.') {
                Some(Self {
                    exchange: exchange.trim().to_uppercase().to_string(),
                    symbol: symbol.trim().to_uppercase().to_string(),
                    r#type: detect_ticker_type(symbol),
                })
            } else {
                None
            }
        };

        if let Some(ticker) = ticker {
            Ok(ticker)
        } else {
            Err(VfError::Invalid {
                code: "INVALID_TICKER",
                message: format!("Invalid ticker '{s}'"),
            })
        }
    }
}

impl Display for Ticker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.symbol, self.exchange)
    }
}

impl Ticker {
    pub fn from_qmt_str(s: &str) -> Option<Self> {
        let s = s.trim();

        if is_ascii_digits(s) {
            let exchange = detect_ticker_exchange(s);
            exchange.map(|exchange| Self {
                exchange: exchange.to_string(),
                symbol: s.to_uppercase().to_string(),
                r#type: detect_ticker_type(s),
            })
        } else {
            if let Some((symbol, qmt_exchange)) = s.rsplit_once('.') {
                let exchange = match qmt_exchange {
                    "SH" => "XSHG",
                    "SZ" => "XSHE",
                    "BJ" => "BSE",
                    "HK" => "XHKG",
                    _ => qmt_exchange,
                };

                Some(Self {
                    exchange: exchange.trim().to_uppercase().to_string(),
                    symbol: symbol.trim().to_uppercase().to_string(),
                    r#type: detect_ticker_type(symbol),
                })
            } else {
                None
            }
        }
    }

    pub fn from_tushare_str(s: &str) -> Option<Self> {
        Self::from_qmt_str(s)
    }

    pub fn to_qmt_code(&self) -> String {
        let suffix = match self.exchange.as_str() {
            "XSHG" => "SH",
            "XSHE" => "SZ",
            "BSE" => "BJ",
            "XHKG" => "HK",
            _ => &self.exchange,
        };

        format!("{}.{suffix}", self.symbol)
    }

    pub fn to_tushare_code(&self) -> String {
        self.to_qmt_code()
    }

    pub fn to_sina_code(&self) -> String {
        let prefix = match self.exchange.as_str() {
            "XSHG" => "sh",
            "XSHE" => "sz",
            _ => "",
        };

        format!("{prefix}{}", self.symbol)
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
            Err(VfError::Invalid {
                code: "INVALID_TICKERS_INDEX",
                message: format!("Invalid tickers index '{s}'"),
            })
        }
    }
}

impl Display for TickersIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.symbol, self.provider)
    }
}

impl TickersIndex {
    pub fn to_tushare_code(&self) -> String {
        match self.provider.as_str() {
            "CNI" | "CNINDEX" | "CSI" | "CSINDEX" => {
                if self.symbol.len() == 6 {
                    if self.symbol.starts_with("000") {
                        return format!("{}.SH", self.symbol);
                    } else if self.symbol.starts_with("399") {
                        return format!("{}.SZ", self.symbol);
                    }
                }
            }
            _ => {}
        }

        self.to_string()
    }
}

fn detect_ticker_exchange(symbol: &str) -> Option<String> {
    if symbol.len() == 6 {
        if symbol.starts_with("11")
            || symbol.starts_with("13")
            || symbol.starts_with("50")
            || symbol.starts_with("51")
            || symbol.starts_with("58")
            || symbol.starts_with("60")
            || symbol.starts_with("68")
        {
            return Some("XSHG".to_string());
        } else if symbol.starts_with("00")
            || symbol.starts_with("12")
            || symbol.starts_with("15")
            || symbol.starts_with("16")
            || symbol.starts_with("30")
        {
            return Some("XSHE".to_string());
        } else if symbol.starts_with("43")
            || symbol.starts_with("83")
            || symbol.starts_with("87")
            || symbol.starts_with("92")
        {
            return Some("BSE".to_string());
        }
    } else if symbol.len() == 5 {
        return Some("XHKG".to_string());
    }

    None
}

fn detect_ticker_type(symbol: &str) -> TickerType {
    if symbol.len() == 6 {
        if symbol.starts_with("11") || symbol.starts_with("12") || symbol.starts_with("13") {
            return TickerType::ConvBond;
        }
    }

    TickerType::Stock
}
