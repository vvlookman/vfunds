use std::fmt::Display;

use chrono::{DateTime, Datelike, NaiveDate};

use crate::error::{VfError, VfResult};

#[derive(Clone, Debug, PartialEq)]
pub struct FiscalQuarter {
    pub year: i32,
    pub quarter: u8, // 1, 2, 3, 4
}

pub fn date_from_str(s: &str) -> VfResult<NaiveDate> {
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y%m%d") {
        Ok(date)
    } else if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        Ok(date)
    } else if let Ok(date) = NaiveDate::parse_from_str(s, "%Y%m%dT%H%M%S") {
        // ISO 8601 Basic
        Ok(date)
    } else if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        // ISO 8601 Extended
        Ok(date)
    } else if let Ok(datetime) = DateTime::parse_from_rfc3339(s) {
        // RFC 3339
        Ok(datetime.date_naive())
    } else {
        Err(VfError::Invalid {
            code: "INVALID_DATE",
            message: format!("Unable to parse date '{s}'"),
        })
    }
}

pub fn date_to_fiscal_quarter(date: &NaiveDate) -> FiscalQuarter {
    let year = date.year();

    let quarter = if date.month() < 4 {
        1
    } else if date.month() < 7 {
        2
    } else if date.month() < 10 {
        3
    } else {
        4
    };

    FiscalQuarter { year, quarter }
}

pub fn date_to_str(date: &NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}

impl FiscalQuarter {
    pub fn new(year: i32, quarter: u8) -> Self {
        Self {
            year,
            quarter: quarter.clamp(1, 4),
        }
    }

    pub fn prev(&self) -> Self {
        Self {
            year: if self.quarter == 1 {
                self.year - 1
            } else {
                self.year
            },
            quarter: match self.quarter {
                1 => 4,
                2 => 1,
                3 => 2,
                _ => 3,
            },
        }
    }
}

impl Display for FiscalQuarter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}Q{}", self.year, self.quarter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fiscal_quarter_to_string() {
        assert_eq!(FiscalQuarter::new(2025, 1).to_string().as_str(), "2025Q1");
    }
}
