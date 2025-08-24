use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteError {
    #[error("SQLite database error: {0}")]
    Rusqlite(#[from] rusqlite::Error),

    #[error("Input/Output error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Environment variable error: {0}")]
    EnvVar(#[from] std::env::VarError),

    #[error("Value conversion to SQL type failed: {0}")]
    ValueConversionError(String),

    #[error("Query `{query}` should have {expected}, but instead {result}")]
    UnexpectedQueryResult {
        query: String,
        result: String,
        expected: String,
    },

    #[error("Failed query {query_info}: {source}")]
    FailedSqliteQuery {
        query_info: String,
        #[source]
        source: rusqlite::Error,
    },

    #[error("Unsupported sync operation: {0}")]
    UnsupportedSyncOperation(String),

    #[error("Data Integrity Error: {0}")]
    DataIntegrityError(String),
}
