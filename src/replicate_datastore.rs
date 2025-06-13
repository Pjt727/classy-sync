use super::argument_parser::SelectSyncOptions;
use super::errors::Error;
use super::sync_requests::{AllSync, AllSyncResult, SelectSync, SelectSyncResult};

pub trait Datastore {
    fn get_all_request_options(&mut self) -> Result<AllSync, Error>;
    fn get_select_request_options(
        &mut self,
        arguments: SelectSyncOptions,
    ) -> Result<SelectSync, Error>;

    fn execute_all_request_sync(&mut self, all_sync_response: AllSyncResult) -> Result<(), Error>;
    fn execute_select_request_sync(
        &mut self,
        select_sync_response: SelectSyncResult,
    ) -> Result<(), Error>;
}
