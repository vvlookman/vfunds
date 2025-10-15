//! # vfunds lib

use std::{
    collections::HashMap,
    env,
    path::PathBuf,
    sync::{LazyLock, RwLock},
};

use directories::ProjectDirs;
use rayon::iter::*;

#[macro_export]
macro_rules! mod_name {
    () => {
        std::module_path!()
            .rsplit("::")
            .next()
            .unwrap_or(std::module_path!())
    };
}

pub mod api;
pub mod error;
pub mod spec;
pub mod utils;

pub static CHANNEL_BUFFER_DEFAULT: usize = 64;

/// Options that each item is String in <key>:<value> format
pub struct VecOptions<'a>(pub &'a [String]);

pub async fn init(workspace: Option<PathBuf>) {
    env_logger::Builder::new()
        .parse_filters(env::var("LOG").as_deref().unwrap_or("off"))
        .init();

    if let Err(err) = cache::init().await {
        panic!("Initialize cache error: {err}");
    }

    if let Some(workspace) = workspace {
        if workspace.is_dir() {
            if let Ok(mut w) = WORKSPACE.write() {
                *w = workspace;
            }
        }
    }
}

mod backtest;
mod cache;
mod data;
mod ds;
mod financial;
mod rule;
mod ticker;

static CACHE_ONLY: LazyLock<bool> = LazyLock::new(|| {
    let v = env::var("CACHE_ONLY")
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

static PROGRESS_INTERVAL_SECS: u64 = 1;

static WORKSPACE: LazyLock<RwLock<PathBuf>> =
    LazyLock::new(|| RwLock::new(env::current_dir().expect("Unable to get current directory!")));

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
