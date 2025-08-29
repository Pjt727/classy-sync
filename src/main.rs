#![allow(dead_code)]
use clap::Parser;
use clap::Subcommand;
use clap::command;
use classy_sync::argument_parser::SelectSyncOptions;
use classy_sync::argument_parser::SyncResources;
use classy_sync::data_stores::{
    replicate_datastore, replicate_datastore::Datastore, sync_requests,
};
use classy_sync::errors::DataStoreError;
use dotenv::dotenv;
use reqwest::blocking::Client;

const CLASSY_URI: &str = "http://localhost:3000";

// TODO: eventually this file will also be responsible for
//   - authentication?
//   - pagination

pub struct SyncConfig {
    pub uri: String,
}

impl SyncConfig {
    fn get_sync_all(self) -> String {
        format!("{}/sync/all", self.uri)
    }

    fn get_sync_select(self) -> String {
        format!("{}/sync/schools", self.uri)
    }
}

impl Default for SyncConfig {
    fn default() -> Self {
        SyncConfig {
            uri: CLASSY_URI.to_string(),
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

// Define the subcommands
#[derive(Subcommand, Debug)]
enum Commands {
    Set { sync_instructions: String },
    Unset { sync_instructions: String },
}

fn main() {
    dotenv().ok();
    env_logger::init();

    // for now just taking the first argument as
    let cli = Cli::parse();
    let mut data_store = replicate_datastore::get_datastore().unwrap();
    match &cli.command {
        Some(Commands::Set { sync_instructions }) => {
            if sync_instructions == "all" {
                data_store
                    .set_request_sync_resources(SyncResources::Everything)
                    .unwrap();
            } else {
                let sync_options = SelectSyncOptions::from_input(sync_instructions);
                data_store
                    .set_request_sync_resources(SyncResources::Select(sync_options))
                    .unwrap();
            }
        }
        Some(Commands::Unset { sync_instructions }) => {
            let sync_options = SelectSyncOptions::from_input(sync_instructions);
            data_store
                .unset_request_sync_resources(SyncResources::Select(sync_options))
                .unwrap();
        }
        None => {}
    }

    sync(SyncConfig::default(), &mut *data_store).expect("Failed to sync");
}

pub fn sync(config: SyncConfig, data_store: &mut dyn Datastore) -> Result<(), DataStoreError> {
    let client = Client::new();
    let request_options = data_store.generate_sync_options().unwrap();
    match request_options {
        sync_requests::SyncOptions::All(all_sync) => {
            let response: sync_requests::AllSyncResult = client
                .get(config.get_sync_all())
                .query(&all_sync)
                .send()
                .unwrap()
                .json()
                .unwrap();
            data_store.execute_all_request_sync(response)?;
        }

        sync_requests::SyncOptions::Select(select_sync) => {
            let response: sync_requests::TermSyncResult = client
                .post(config.get_sync_select())
                .json(&select_sync)
                .send()
                .unwrap()
                .json()
                .unwrap();
            data_store.execute_select_request_sync(select_sync, response)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod sync_tests {
    use std::fs;

    use super::*;
    use classy_sync::data_stores::{
        replicate_datastore::get_datastore,
        sync_requests::{AllSyncResult, SelectSync, SyncOptions, TermSyncResult},
    };
    use serde_json::from_str;

    fn load_all_sync_data(path: &str) -> String {
        let updates_text = fs::read_to_string(path).expect("Could not access test json");
        let _: AllSyncResult = from_str(&updates_text)
            .expect("Test json is not the correct shape for all sync result");
        updates_text
    }

    fn load_select_sync_data(path: &str) -> String {
        let updates_text = fs::read_to_string(path).expect("Could not access test json");
        let _: TermSyncResult = from_str(&updates_text)
            .expect("Test json is not the correct shape for term sync result");
        updates_text
    }

    #[test]
    #[cfg(feature = "sqlite")]
    fn sqlite_full_sync() {
        let mut server = mockito::Server::new();

        let updates_text = load_all_sync_data("test-syncs/maristfall2024/01.json");
        server
            .mock("GET", "/sync/all")
            .match_query(mockito::Matcher::UrlEncoded(
                "last_sync".to_string(),
                "0".to_string(),
            ))
            .with_header("content-type", "application/json")
            .with_body(updates_text)
            .create();

        let mut sqlite_datastore = get_datastore().expect("Could not get sqlite data store");

        sqlite_datastore
            .set_request_sync_resources(SyncResources::Everything)
            .unwrap();
        match sqlite_datastore.generate_sync_options().unwrap() {
            SyncOptions::All(all_sync) => {
                assert_eq!(all_sync.last_sync, 0, "Expected sequence 0");
            }
            SyncOptions::Select(_) => panic!("Expected all sync"),
        }
        sync(SyncConfig { uri: server.url() }, &mut *sqlite_datastore).expect("Sync failed");
        match sqlite_datastore.generate_sync_options().unwrap() {
            SyncOptions::All(all_sync) => {
                assert_eq!(all_sync.last_sync, 6303, "Expected sequence 6303")
            }
            SyncOptions::Select(_) => panic!("Expected all sync"),
        }
    }

    #[test]
    #[cfg(feature = "sqlite")]
    fn sqlite_term_sync() {
        let mut server = mockito::Server::new();
        // first call with a single term
        let mut select_sync = SelectSync::new();
        select_sync
            .add_term_sync("marist".to_string(), "202440".to_string(), 0)
            .unwrap();

        server
            .mock("POST", "/sync/schools")
            .match_body(serde_json::to_string(&select_sync).unwrap().as_str())
            .with_header("content-type", "application/json")
            .with_body(load_select_sync_data("test-syncs/maristterms/202440.json"))
            .create();

        // second call with the other term
        let mut select_sync = SelectSync::new();
        select_sync
            .add_term_sync("marist".to_string(), "202440".to_string(), 6929)
            .unwrap();
        select_sync
            .add_term_sync("marist".to_string(), "202540".to_string(), 0)
            .unwrap();

        server
            .mock("POST", "/sync/schools")
            .match_body(serde_json::to_string(&select_sync).unwrap().as_str())
            .with_header("content-type", "application/json")
            .with_body(load_select_sync_data("test-syncs/maristterms/202540.json"))
            .create();

        let mut sqlite_datastore = get_datastore().expect("Could not get sqlite data store");

        sqlite_datastore
            .set_request_sync_resources(SyncResources::Select(SelectSyncOptions::from_input(
                "marist,202440",
            )))
            .unwrap();
        let expected_sync_options: SelectSync = serde_json::from_str(
            r#"
            {
              "exclude": {},
              "max_records_per_request": 10000,
              "schools": {
                "marist": {
                  "202440": 0
                }
              }
            }
            "#,
        )
        .unwrap();
        match sqlite_datastore.generate_sync_options().unwrap() {
            SyncOptions::All(_) => {
                panic!("Expected select sync")
            }
            SyncOptions::Select(options) => {
                assert_eq!(options, expected_sync_options)
            }
        }
        sync(SyncConfig { uri: server.url() }, &mut *sqlite_datastore).expect("Sync failed");
        sqlite_datastore
            .set_request_sync_resources(SyncResources::Select(SelectSyncOptions::from_input(
                "marist,202540",
            )))
            .unwrap();

        let expected_sync_options: SelectSync = serde_json::from_str(
            r#"
            {
              "exclude": {},
              "max_records_per_request": 10000,
              "schools": {
                "marist": {
                  "202440": 6929,
                  "202540": 0
                }
              }
            }
            "#,
        )
        .unwrap();
        match sqlite_datastore.generate_sync_options().unwrap() {
            SyncOptions::All(_) => {
                panic!("Expected select sync")
            }
            SyncOptions::Select(options) => {
                assert_eq!(options, expected_sync_options)
            }
        }
        sync(SyncConfig { uri: server.url() }, &mut *sqlite_datastore).expect("Sync failed");
    }
}
