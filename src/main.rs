#![allow(dead_code)]
use classy_sync::argument_parser::SelectSyncOptions;
use classy_sync::argument_parser::SyncResources;
use classy_sync::data_stores::{
    replicate_datastore, replicate_datastore::Datastore, sync_requests,
};
use dotenv::dotenv;
use lazy_static::lazy_static;
use reqwest::blocking::Client;
use std::env;

const SYNC_DOMAIN: &str = "http://localhost:3000";

lazy_static! {
    static ref SYNC_ALL_ROUTE: String = format!("{}/sync/all", SYNC_DOMAIN);
    static ref SYNC_SELECT_ROUTE: String = format!("{}/sync/schools", SYNC_DOMAIN);
}

// TODO: eventually this file will also be responsible for
//   - authentication?
//   - pagination

pub struct SyncConfig {
    pub sync_all: String,
    pub sync_select: String,
}

impl Default for SyncConfig {
    fn default() -> Self {
        SyncConfig {
            sync_all: SYNC_ALL_ROUTE.clone(),
            sync_select: SYNC_SELECT_ROUTE.clone(),
        }
    }
}

fn main() {
    dotenv().ok();
    env_logger::init();

    // for now just taking the first argument as
    let args: Vec<String> = env::args().collect();
    let mut data_store = replicate_datastore::get_datastore().unwrap();
    if args.len() >= 2 {
        let sync_instructions = args.get(1).unwrap();
        if sync_instructions == "all" {
            data_store
                .set_request_sync_resources(SyncResources::Everything)
                .unwrap();
        } else {
            let sync_options = SelectSyncOptions::from_input(sync_instructions.to_string());
            data_store
                .set_request_sync_resources(SyncResources::Select(sync_options))
                .unwrap();
        }
    }

    let client = Client::new();
    sync(client, SyncConfig::default(), &mut *data_store);
}

fn sync(client: Client, routes: SyncConfig, data_store: &mut dyn Datastore) {
    let request_options = data_store.generate_sync_options().unwrap();
    match request_options {
        sync_requests::SyncOptions::All(all_sync) => {
            let response: sync_requests::AllSyncResult = client
                .get(routes.sync_all)
                .query(&all_sync)
                .send()
                .unwrap()
                .json()
                .unwrap();
            data_store.execute_all_request_sync(response).unwrap()
        }

        sync_requests::SyncOptions::Select(select_sync) => {
            let response: sync_requests::TermSyncResult = client
                .post(routes.sync_select)
                .json(&select_sync)
                .send()
                .unwrap()
                .json()
                .unwrap();
            data_store
                .execute_select_request_sync(select_sync, response)
                .unwrap()
        }
    }
}

#[cfg(test)]
mod sync_tests {
    use super::*;
    use classy_sync::data_stores::replicate_datastore::get_datastore;
    use dotenv::dotenv;

    // TODO: add mock tests
    #[test]
    fn full_sync() {
        dotenv().ok();
        env_logger::init();
        let mut sqlite_datastore = get_datastore().expect("Could not get sqlite data store");

        sqlite_datastore
            .set_request_sync_resources(SyncResources::Everything)
            .unwrap()
    }
}
