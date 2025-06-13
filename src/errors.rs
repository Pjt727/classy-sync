// Error handling will be looked at more
use std::fmt;

pub type Error = Box<dyn std::error::Error>;

#[derive(Debug)]
pub struct SyncError {
    message: String,
}

impl SyncError {
    pub fn new<T: ToString>(message: T) -> Box<SyncError> {
        Box::new(SyncError {
            message: message.to_string(),
        })
    }
}

impl std::error::Error for SyncError {}

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Oh no, something bad went down")
    }
}
