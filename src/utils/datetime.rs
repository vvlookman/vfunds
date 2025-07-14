use std::fmt::Display;

use chrono::{DateTime, Datelike, Local, NaiveDate};

use crate::error::{VfError, VfResult};

#[derive(Clone, Debug, PartialEq, strum::Display)]
pub enum Quarter {
    Q1,
    Q2,
    Q3,
    Q4,
}

#[derive(Clone, Debug)]
pub struct FiscalQuarter {
    pub year: i32,
    pub quarter: Quarter,
}

pub fn date_from_days_after_epoch(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(719163 + days)
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
        Err(VfError::Invalid(
            "INVALID_DATE",
            format!("Unable to parse date '{s}'"),
        ))
    }
}

pub fn date_to_str(date: &NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}

pub fn days_after_epoch(date: &NaiveDate) -> Option<i32> {
    let num_days = date.signed_duration_since(EPOCH).num_days();
    let days: i32 = num_days.try_into().ok()?;

    Some(days)
}

pub fn prev_fiscal_quarter(date: Option<&NaiveDate>) -> FiscalQuarter {
    let date = date.copied().unwrap_or(Local::now().date_naive());
    if date.month() < 4 {
        FiscalQuarter::new(date.year() - 1, Quarter::Q4)
    } else if date.month() < 7 {
        FiscalQuarter::new(date.year(), Quarter::Q1)
    } else if date.month() < 10 {
        FiscalQuarter::new(date.year(), Quarter::Q2)
    } else {
        FiscalQuarter::new(date.year(), Quarter::Q3)
    }
}

static EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

impl FiscalQuarter {
    pub fn new(year: i32, quarter: Quarter) -> Self {
        Self { year, quarter }
    }

    pub fn prev(&self) -> Self {
        Self {
            year: if self.quarter == Quarter::Q1 {
                self.year - 1
            } else {
                self.year
            },
            quarter: match self.quarter {
                Quarter::Q1 => Quarter::Q4,
                Quarter::Q2 => Quarter::Q1,
                Quarter::Q3 => Quarter::Q2,
                Quarter::Q4 => Quarter::Q3,
            },
        }
    }
}

impl Display for FiscalQuarter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.year, self.quarter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quarter_to_string() {
        assert_eq!(Quarter::Q1.to_string().as_str(), "Q1");
        assert_eq!(Quarter::Q2.to_string().as_str(), "Q2");
        assert_eq!(Quarter::Q3.to_string().as_str(), "Q3");
        assert_eq!(Quarter::Q4.to_string().as_str(), "Q4");
    }

    #[test]
    fn test_fiscal_quarter_to_string() {
        assert_eq!(
            FiscalQuarter::new(2025, Quarter::Q1).to_string().as_str(),
            "2025Q1"
        );
    }
}
