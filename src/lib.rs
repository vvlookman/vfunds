//! # vfunds lib

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{LazyLock, RwLock},
};

use rayon::iter::*;
use serde::{Deserialize, Serialize};

use crate::error::VfResult;

pub mod api;
pub mod error;
pub mod utils;

#[derive(Serialize, Deserialize, Default)]
pub struct FundDefinition {
    pub title: String,
}

/// Options that each item is String in <key>:<value> format
pub struct VecOptions<'a>(pub &'a [String]);

pub fn init(workspace: Option<PathBuf>) {
    env_logger::Builder::new()
        .parse_filters(std::env::var("LOG").as_deref().unwrap_or("off"))
        .init();

    if let Some(workspace) = workspace {
        if workspace.is_dir() {
            if let Ok(mut w) = WORKSPACE.write() {
                *w = workspace;
            }
        }
    }
}

static WORKSPACE: LazyLock<RwLock<PathBuf>> = LazyLock::new(|| {
    RwLock::new(std::env::current_dir().expect("Unable to get current directory!"))
});

mod data;
mod ds;
mod financial;
mod ticker;

impl FundDefinition {
    pub fn from_file(path: &Path) -> VfResult<Self> {
        confy::load_path(path).map_err(Into::into)
    }
}

impl VecOptions<'_> {
    pub fn get(&self, name: &str) -> Option<String> {
        if let Some(option_text) = self.0.par_iter().find_any(|s| {
            s.to_lowercase()
                .starts_with(&format!("{}:", name.to_lowercase()))
        }) {
            let parts: Vec<_> = option_text.splitn(2, ':').collect();
            parts.get(1).map(|s| s.trim().to_string())
        } else {
            None
        }
    }

    pub fn into_map(self) -> HashMap<String, String> {
        let mut map: HashMap<String, String> = HashMap::new();

        for option_text in self.0 {
            let parts: Vec<_> = option_text.splitn(2, ':').collect();
            if parts.len() == 2 {
                map.insert(parts[0].to_string(), parts[1].trim().to_string());
            }
        }

        map
    }

    pub fn into_tuples(self) -> Vec<(String, String)> {
        let mut tuples: Vec<(String, String)> = vec![];

        for option_text in self.0 {
            let parts: Vec<_> = option_text.splitn(2, ':').collect();
            if parts.len() == 2 {
                tuples.push((parts[0].to_string(), parts[1].trim().to_string()));
            }
        }

        tuples
    }
}
