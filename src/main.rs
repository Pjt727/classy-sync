#![allow(dead_code)]
mod argument_parser;
mod data_stores;
mod errors;
mod replicate_datastore;
mod sync_requests;
use env_logger;
use lazy_static::lazy_static;
use replicate_datastore::Datastore;
use reqwest::blocking::Client;
use std::env;

const SYNC_DOMAIN: &str = "localhost:3000";

lazy_static! {
    static ref SYNC_ALL_ROUTE: String = format!("{}/sync/all", SYNC_DOMAIN);
    static ref SYNC_SELECT_ROUTE: String = format!("{}/sync/select", SYNC_DOMAIN);
}

// TODO: eventually this file will also be responsible for
//   - authentication
//   - pagination

fn main() {
    env_logger::init();

    // for now just taking the first argument as
    let args: Vec<String> = env::args().collect();
    let sync_instructions = args.get(1).expect("Provide `sync_instructions` argument!");
    let mut data_store = data_stores::sqlite::Sqlite::new().unwrap();
    let client = Client::new();
    if sync_instructions == "all" {
        sync_all(client, &mut data_store)
    } else {
        let sync_options =
            argument_parser::SelectSyncOptions::from_input(sync_instructions.to_string());
        sync_select(client, &mut data_store, sync_options)
    }
}

fn sync_all(client: Client, data_store: &mut dyn Datastore) {
    let request_options = data_store.get_all_request_options().unwrap();
    let response: sync_requests::AllSyncResult = client
        .get(SYNC_ALL_ROUTE.to_string())
        .query(&request_options)
        .send()
        .unwrap()
        .json()
        .unwrap();
    data_store.execute_all_request_sync(response).unwrap()
}

fn sync_select(
    client: Client,
    data_store: &mut dyn Datastore,
    sync_options: argument_parser::SelectSyncOptions,
) {
    let request_options = data_store.get_select_request_options(sync_options).unwrap();
    let response: sync_requests::SelectSyncResult = client
        .get(SYNC_SELECT_ROUTE.to_string())
        .json(&request_options)
        .send()
        .unwrap()
        .json()
        .unwrap();
    data_store.execute_select_request_sync(response).unwrap()
}
