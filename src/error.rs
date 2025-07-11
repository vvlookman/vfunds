use std::{
    path::PathBuf,
    sync::{PoisonError, RwLockReadGuard},
};

pub type VfResult<T> = Result<T, VfError>;

#[derive(Debug, thiserror::Error)]
pub enum VfError {
    #[error("[Concurrent Error] {0}")]
    ConcurrentError(#[from] ::tokio::task::JoinError),

    #[error("[Dataframe Error] {0}")]
    DataframeError(#[from] ::polars::error::PolarsError),

    #[error("[HTTP Request Error] {0}")]
    HttpRequestError(#[from] ::reqwest::Error),

    #[error("[HTTP Status Error] {0}")]
    HttpStatusError(String),

    #[error("[Invalid] {1}")]
    Invalid(&'static str, String),

    #[error("[IO Error] {0}")]
    IoError(#[from] std::io::Error),

    #[error("[Lock Error] {0}")]
    LockError(String),

    #[error("[No Data] {1}")]
    NoData(&'static str, String),

    #[error("[Not Exists] {1}")]
    NotExists(&'static str, String),

    #[error("[Parse Config Error] {0}")]
    ParseConfigError(#[from] ::confy::ConfyError),

    #[error("[Parse Enum Error] {0}")]
    ParseEnumError(#[from] ::strum::ParseError),

    #[error("[Parse URL Error] {0}")]
    ParseUrlError(#[from] url::ParseError),

    #[error("[Required] {1}")]
    Required(&'static str, String),

    #[error("[Serde JSON Error] {0}")]
    SerdeJsonError(#[from] ::serde_json::Error),
}

impl From<PoisonError<RwLockReadGuard<'_, PathBuf>>> for VfError {
    fn from(err: PoisonError<std::sync::RwLockReadGuard<'_, PathBuf>>) -> Self {
        Self::LockError(err.to_string())
    }
}
