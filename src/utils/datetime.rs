use std::fmt::Display;

use chrono::{DateTime, Datelike, NaiveDate};

use crate::error::{VfError, VfResult};

#[derive(Clone, Debug, PartialEq)]
pub struct FiscalQuarter {
    pub year: i32,
    pub quarter: u8, // 1, 2, 3, 4
}

pub fn date_from_str(s: &str) -> VfResult<NaiveDate> {
    const FORMATS: &[&str] = &[
        "%Y%m%d",
        "%Y-%m-%d",
        "%Y%m%dT%H%M%S",        // ISO 8601 Basic
        "%Y-%m-%dT%H:%M:%S%.f", // ISO 8601 Extended
    ];

    for format in FORMATS {
        if let Ok(date) = NaiveDate::parse_from_str(s, format) {
            return Ok(date);
        }
    }

    if let Ok(datetime) = DateTime::parse_from_rfc3339(s) {
        // RFC 3339
        return Ok(datetime.date_naive());
    }

    Err(VfError::Invalid {
        code: "INVALID_DATE",
        message: format!("Unable to parse date '{s}'"),
    })
}

pub fn date_to_fiscal_quarter(date: &NaiveDate) -> FiscalQuarter {
    let year = date.year();
    let quarter: u8 = (((date.month() - 1) / 3) + 1) as u8;

    FiscalQuarter { year, quarter }
}

pub fn date_to_str(date: &NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}

pub fn secs_to_human_str(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;

    if h > 0 {
        format!("{h}h{m}m{s}s")
    } else if m > 0 {
        format!("{m}m{s}s")
    } else {
        format!("{s}s")
    }
}

impl FiscalQuarter {
    pub fn new(year: i32, quarter: u8) -> Self {
        Self {
            year,
            quarter: quarter.clamp(1, 4),
        }
    }

    pub fn prev(&self) -> Self {
        let (year, quarter) = if self.quarter == 1 {
            (self.year - 1, 4)
        } else {
            (self.year, self.quarter - 1)
        };

        Self { year, quarter }
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
    fn test_date_from_str() {
        assert_eq!(
            date_to_str(&date_from_str("20231231").unwrap()),
            "2023-12-31"
        );
        assert_eq!(
            date_to_str(&date_from_str("20231231T235959").unwrap()),
            "2023-12-31"
        );
        assert_eq!(
            date_to_str(&date_from_str("2023-12-31").unwrap()),
            "2023-12-31"
        );
        assert_eq!(
            date_to_str(&date_from_str("2023-12-31T23:59:59").unwrap()),
            "2023-12-31"
        );
        assert_eq!(
            date_to_str(&date_from_str("2023-12-31T23:59:59Z").unwrap()),
            "2023-12-31"
        );
        assert_eq!(
            date_to_str(&date_from_str("2023-12-31T23:59:59+08:00").unwrap()),
            "2023-12-31"
        );
        assert_eq!(
            date_to_str(&date_from_str("2023-12-31T23:59:59.123456").unwrap()),
            "2023-12-31"
        );
        assert!(date_from_str("invalid-date").is_err());
    }

    #[test]
    fn test_date_to_fiscal_quarter() {
        assert_eq!(
            date_to_fiscal_quarter(&NaiveDate::from_ymd_opt(2023, 1, 15).unwrap()).year,
            2023
        );
        assert_eq!(
            date_to_fiscal_quarter(&NaiveDate::from_ymd_opt(2023, 1, 15).unwrap()).quarter,
            1
        );
        assert_eq!(
            date_to_fiscal_quarter(&NaiveDate::from_ymd_opt(2023, 4, 15).unwrap()).year,
            2023
        );
        assert_eq!(
            date_to_fiscal_quarter(&NaiveDate::from_ymd_opt(2023, 4, 15).unwrap()).quarter,
            2
        );
        assert_eq!(
            date_to_fiscal_quarter(&NaiveDate::from_ymd_opt(2023, 7, 15).unwrap()).year,
            2023
        );
        assert_eq!(
            date_to_fiscal_quarter(&NaiveDate::from_ymd_opt(2023, 7, 15).unwrap()).quarter,
            3
        );
        assert_eq!(
            date_to_fiscal_quarter(&NaiveDate::from_ymd_opt(2023, 10, 15).unwrap()).year,
            2023
        );
        assert_eq!(
            date_to_fiscal_quarter(&NaiveDate::from_ymd_opt(2023, 10, 15).unwrap()).quarter,
            4
        );
    }

    #[test]
    fn test_date_to_str() {
        assert_eq!(
            date_to_str(&NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
            "2023-01-01"
        );
        assert_eq!(
            date_to_str(&NaiveDate::from_ymd_opt(2023, 12, 31).unwrap()),
            "2023-12-31"
        );
    }

    #[test]
    fn test_secs_to_human_str() {
        assert_eq!(secs_to_human_str(0), "0s");
        assert_eq!(secs_to_human_str(30), "30s");
        assert_eq!(secs_to_human_str(59), "59s");
        assert_eq!(secs_to_human_str(60), "1m0s");
        assert_eq!(secs_to_human_str(90), "1m30s");
        assert_eq!(secs_to_human_str(3599), "59m59s");
        assert_eq!(secs_to_human_str(3600), "1h0m0s");
        assert_eq!(secs_to_human_str(3661), "1h1m1s");
        assert_eq!(secs_to_human_str(7325), "2h2m5s");
    }

    #[test]
    fn test_fiscal_quarter_new() {
        assert_eq!(FiscalQuarter::new(2023, 2).year, 2023);
        assert_eq!(FiscalQuarter::new(2023, 2).quarter, 2);
        assert_eq!(FiscalQuarter::new(2023, 0).quarter, 1); // clamped to 1
        assert_eq!(FiscalQuarter::new(2023, 5).quarter, 4); // clamped to 4
    }

    #[test]
    fn test_fiscal_quarter_prev() {
        assert_eq!(FiscalQuarter::new(2023, 1).prev().year, 2022);
        assert_eq!(FiscalQuarter::new(2023, 1).prev().quarter, 4);
        assert_eq!(FiscalQuarter::new(2023, 2).prev().quarter, 1);
        assert_eq!(FiscalQuarter::new(2023, 3).prev().quarter, 2);
        assert_eq!(FiscalQuarter::new(2023, 4).prev().quarter, 3);
    }
}
