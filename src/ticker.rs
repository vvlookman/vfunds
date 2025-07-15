use std::{fmt::Display, str::FromStr};

use crate::error::VfError;

#[derive(Clone, Debug)]
pub struct Ticker {
    pub exchange: String,
    pub symbol: String,
}

impl FromStr for Ticker {
    type Err = VfError;
    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let s = s.trim();

        let parts: Vec<_> = s.splitn(2, ':').collect();
        if parts.len() == 2 {
            Ok(Self {
                exchange: parts[0].trim().to_uppercase().to_string(),
                symbol: parts[1].trim().to_uppercase().to_string(),
            })
        } else {
            let exchange = if s.len() == 6 {
                if s.starts_with("600")
                    || s.starts_with("601")
                    || s.starts_with("603")
                    || s.starts_with("688")
                {
                    Some("SSE")
                } else if s.starts_with("000") || s.starts_with("002") || s.starts_with("300") {
                    Some("SZSE")
                } else {
                    None
                }
            } else if s.len() == 5 {
                Some("HKEX")
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
                    "NO_EXCHANGE",
                    format!("Unable to determine exchange of '{s}'"),
                ))
            }
        }
    }
}

impl Display for Ticker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.exchange, self.symbol)
    }
}
