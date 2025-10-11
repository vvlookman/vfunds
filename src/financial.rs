use std::collections::HashMap;

use crate::ticker::Ticker;

pub mod index;
pub mod stock;
pub mod tool;

#[derive(Debug)]
pub struct Portfolio {
    pub free_cash: f64,
    pub reserved_cash: HashMap<Ticker, f64>,
    pub positions: HashMap<Ticker, u64>,
}

#[derive(Debug, PartialEq, strum::Display, strum::EnumIter, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum Prospect {
    Bullish,
    Bearish,
    Neutral,
}

impl Portfolio {
    pub fn new(cash: f64) -> Self {
        Self {
            free_cash: cash,
            reserved_cash: HashMap::new(),
            positions: HashMap::new(),
        }
    }
}
