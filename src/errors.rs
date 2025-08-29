use serde_json::Value;
use thiserror::Error;

#[cfg(feature = "sqlite")]
use crate::data_stores::sqlite::errors::SqliteError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),

    #[error("Invalid input: {message}")]
    InputParseError { message: String },

    #[error("Invalid select sync addition: {message}")]
    DuplicateSyncAddition { message: String },

    #[error("Failed to parse input {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error(transparent)]
    DataStoreError(#[from] DataStoreError),

    #[error("Record with invalid schema {message}: {values}\n{record}", values=invalid_values.join(", "))]
    InvalidSchemaValues {
        message: String,
        invalid_values: Vec<String>,
        record: Value,
    },
}

#[derive(Error, Debug)]
pub enum DataStoreError {
    #[error("Sqlite Error: {0}")]
    #[cfg(feature = "sqlite")]
    SqliteError(#[from] SqliteError),
}
