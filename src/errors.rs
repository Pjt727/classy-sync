// Error handling will be looked at more
use std::backtrace::{Backtrace, BacktraceStatus};
use std::fmt;

#[derive(Debug)]
pub enum Error {
    SyncError {
        message: String,
        backtrace: Backtrace,
    },
    DataStoreError {
        message: String,
        backtrace: Backtrace,
    },
}

impl Error {
    /// errors in the logic of how to use the data store
    pub fn sync_error<T: ToString>(message: T) -> Self {
        Error::SyncError {
            message: message.to_string(),
            backtrace: Backtrace::capture(),
        }
    }

    /// errors in performing actions for the datastore
    pub fn data_store_error<T: ToString>(message: T) -> Self {
        Error::DataStoreError {
            message: message.to_string(),
            backtrace: Backtrace::capture(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::SyncError { message, backtrace } => {
                writeln!(f, "Sync Error: {message}")?;
                if backtrace.status() != BacktraceStatus::Disabled {
                    write!(f, "Backtrace:\n{backtrace}")?;
                }
                Ok(())
            }
            Error::DataStoreError { message, backtrace } => {
                writeln!(f, "Data Store Error: {message}")?;
                if backtrace.status() != BacktraceStatus::Disabled {
                    write!(f, "Backtrace:\n{backtrace}")?;
                }
                Ok(())
            }
        }
    }
}
