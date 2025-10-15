use std::path::PathBuf;

pub type VfResult<T> = Result<T, VfError>;

#[derive(Debug, thiserror::Error)]
pub enum VfError {
    #[error("[Concurrent Error] {0}")]
    ConcurrentError(#[from] ::tokio::task::JoinError),

    #[error("[CSV Error] {0}")]
    CsvError(#[from] ::csv::Error),

    #[error("[Dataframe Error] {0}")]
    DataframeError(#[from] ::polars::error::PolarsError),

    #[error("[HTTP Request Error] {0}")]
    HttpRequestError(#[from] ::reqwest::Error),

    #[error("[HTTP Middleware Error] {0}")]
    HttpMiddlewareError(#[from] ::reqwest_middleware::Error),

    #[error("[HTTP Status Error] [{request}] {status}")]
    HttpStatusError { status: String, request: String },

    #[error("[Invalid] {message}")]
    Invalid { code: &'static str, message: String },

    #[error("[IO Error] {0}")]
    IoError(#[from] std::io::Error),

    #[error("[Lock Error] {0}")]
    LockError(String),

    #[error("[Machine Learning Error] {0}")]
    MachineLearningError(String),

    #[error("[No Data] {message}")]
    NoData { code: &'static str, message: String },

    #[error("[Not Exists] {message}")]
    NotExists { code: &'static str, message: String },

    #[error("[Parse Config Error] {0}")]
    ParseConfigError(#[from] ::confy::ConfyError),

    #[error("[Parse DataTime Error] {0}")]
    ParseDataTimeError(#[from] chrono::ParseError),

    #[error("[Parse Enum Error] {0}")]
    ParseEnumError(#[from] ::strum::ParseError),

    #[error("[Parse URL Error] {0}")]
    ParseUrlError(#[from] url::ParseError),

    #[error("[Serde JSON Error] {0}")]
    SerdeJsonError(#[from] ::serde_json::Error),

    #[error("[SQL Error] {0}")]
    SqlError(#[from] ::libsql::Error),
}

impl From<std::sync::PoisonError<std::sync::RwLockReadGuard<'_, PathBuf>>> for VfError {
    fn from(err: std::sync::PoisonError<std::sync::RwLockReadGuard<'_, PathBuf>>) -> Self {
        Self::LockError(err.to_string())
    }
}
