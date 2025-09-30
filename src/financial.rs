use std::collections::HashMap;

use crate::ticker::Ticker;

pub mod index;
pub mod stock;
pub mod tool;

#[derive(Debug)]
pub struct Portfolio {
    pub cash: f64,
    pub positions: HashMap<Ticker, u64>,
    pub sideline_cash: HashMap<Ticker, f64>,
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
            cash,
            positions: HashMap::new(),
            sideline_cash: HashMap::new(),
        }
    }
}
