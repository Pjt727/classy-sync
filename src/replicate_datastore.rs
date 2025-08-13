use crate::argument_parser::SyncResources;
use crate::sync_requests::SyncOptions;

use super::errors::Error;
use super::sync_requests::{AllSyncResult, SelectSync, TermSyncResult};

/// Datastores may choose to make it possible to have all syncs / schools syncs /term syncs work
/// with each other, but they may also choose to make some of them mutaully exclusive
pub trait Datastore {
    fn set_request_sync_resources(&mut self, resources: SyncResources) -> Result<(), Error>;
    fn unset_request_sync_resources(&mut self, resources: SyncResources) -> Result<(), Error>;
    fn generate_sync_options(&mut self) -> Result<SyncOptions, Error>;

    fn execute_all_request_sync(&mut self, all_sync_response: AllSyncResult) -> Result<(), Error>;
    fn execute_select_request_sync(
        &mut self,
        select_sync_request: SelectSync,
        select_sync_response: TermSyncResult,
    ) -> Result<(), Error>;
}
