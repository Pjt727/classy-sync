use crate::argument_parser::SyncResources;

use super::sync_requests::{AllSyncResult, SelectSync, SyncOptions, TermSyncResult};
use crate::errors::DataStoreError;

/// Datastores may choose to make it possible to have all syncs / schools syncs /term syncs work
/// with each other, but they may also choose to make some of them mutaully exclusive
pub trait Datastore {
    fn set_request_sync_resources(
        &mut self,
        resources: SyncResources,
    ) -> Result<(), DataStoreError>;

    fn unset_request_sync_resources(
        &mut self,
        resources: SyncResources,
    ) -> Result<(), DataStoreError>;

    fn generate_sync_options(&mut self) -> Result<SyncOptions, DataStoreError>;

    fn execute_all_request_sync(
        &mut self,
        all_sync_response: AllSyncResult,
    ) -> Result<(), DataStoreError>;

    fn execute_select_request_sync(
        &mut self,
        select_sync_request: SelectSync,
        select_sync_response: TermSyncResult,
    ) -> Result<(), DataStoreError>;
}

/// gets the datastore that is selected as per the first feature
pub fn get_datastore() -> Result<Box<dyn Datastore>, DataStoreError> {
    #[cfg(feature = "sqlite")]
    {
        use log::warn;
        use std::env;

        let db_path = env::var("SQLITE_DB_PATH").ok();

        if db_path.is_none() {
            warn!("Using an in memory database because env varible SQLITE_DB_PATH is not found")
        }

        let config = super::sqlite::storage::SqliteConfig {
            db_path,
            // TODO: add this to config
            is_strict: false,
            ..Default::default()
        };

        return Ok(Box::new(super::sqlite::Sqlite::new(config)?));
    }

    #[allow(unreachable_code)]
    {
        unreachable!("A data store backend feature must be enabled at compile time.")
    }
}
