//! # vfunds lib

use std::{collections::HashMap, env, path::PathBuf, sync::LazyLock};

use directories::ProjectDirs;
use rayon::iter::*;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod api;
pub mod error;
pub mod gui;
pub mod spec;
pub mod utils;

pub static CHANNEL_BUFFER_DEFAULT: usize = 64;

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub qmt_api: String,
    pub tushare_api: String,
    pub tushare_token: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            qmt_api: "http://127.0.0.1:9000".to_string(),
            tushare_api: "http://api.tushare.pro".to_string(),
            tushare_token: "".to_string(),
        }
    }
}

/// Options that each item is String in <key>:<value> format
pub struct VecOptions<'a>(pub &'a [String]);

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

pub async fn init(workspace: Option<PathBuf>) {
    env_logger::Builder::new()
        .parse_filters(env::var("LOG").as_deref().unwrap_or("off"))
        .init();

    if let Err(err) = cache::init().await {
        panic!("Initialize cache error: {err}");
    }

    if let Ok(config) = confy::load_path::<Config>(&*CONFIG_PATH) {
        *CONFIG.write().await = config;
    }

    if let Some(workspace) = workspace {
        if workspace.is_dir() {
            *WORKSPACE.write().await = workspace;
        }
    }
}

#[macro_export]
macro_rules! mod_name {
    () => {
        std::module_path!()
            .rsplit("::")
            .next()
            .unwrap_or(std::module_path!())
    };
}

const CANDIDATE_TICKER_RATIO: usize = 2;
const POSITION_TOLERANCE: f64 = 0.02;
const REQUIRED_DATA_COMPLETENESS: f64 = 0.9;

mod backtest;
mod cache;
mod data;
mod ds;
mod financial;
mod market;
mod rule;
mod ticker;

static CACHE_NO_EXPIRE: LazyLock<bool> = LazyLock::new(|| {
    let v = env::var("CACHE_NO_EXPIRE")
        .as_deref()
        .unwrap_or_default()
        .to_lowercase();
    v == "true" || v == "t" || v == "yes" || v == "y"
});

static CACHE_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
    match ProjectDirs::from("", "", env!("CARGO_PKG_NAME")) {
        Some(proj_dirs) => proj_dirs.data_dir().to_path_buf(),
        None => env::current_dir().expect("Unable to get current directory!"),
    }
    .join("cache.db")
});

static CONFIG_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
    match ProjectDirs::from("", "", env!("CARGO_PKG_NAME")) {
        Some(proj_dirs) => proj_dirs.data_dir().to_path_buf(),
        None => env::current_dir().expect("Unable to get current directory!"),
    }
    .join("config.toml")
});

static CONFIG: LazyLock<RwLock<Config>> = LazyLock::new(|| RwLock::new(Config::default()));

static PROGRESS_INTERVAL_SECS: u64 = 1;

static WORKSPACE: LazyLock<RwLock<PathBuf>> =
    LazyLock::new(|| RwLock::new(env::current_dir().expect("Unable to get current directory!")));

#[cfg(test)]
use ctor::ctor;

#[cfg(test)]
#[ctor]
fn global_test_setup() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(init(None));
}
