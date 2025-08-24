#![allow(dead_code)]
use clap::Parser;
use clap::Subcommand;
use clap::command;
use classy_sync::argument_parser::SelectSyncOptions;
use classy_sync::argument_parser::SyncResources;
use classy_sync::data_stores::{
    replicate_datastore, replicate_datastore::Datastore, sync_requests,
};
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
                let sync_options = SelectSyncOptions::from_input(sync_instructions.to_string());
                data_store
                    .set_request_sync_resources(SyncResources::Select(sync_options))
                    .unwrap();
            }
        }
        Some(Commands::Unset { sync_instructions }) => {
            let sync_options = SelectSyncOptions::from_input(sync_instructions.to_string());
            data_store
                .unset_request_sync_resources(SyncResources::Select(sync_options))
                .unwrap();
        }
        None => {}
    }

    sync(SyncConfig::default(), &mut *data_store);
}

fn sync(config: SyncConfig, data_store: &mut dyn Datastore) {
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
            data_store.execute_all_request_sync(response).unwrap()
        }

        sync_requests::SyncOptions::Select(select_sync) => {
            let response: sync_requests::TermSyncResult = client
                .post(config.get_sync_select())
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
    use std::fs;

    use super::*;
    use classy_sync::data_stores::{
        replicate_datastore::get_datastore, sync_requests::AllSyncResult,
    };
    use serde_json::from_str;

    #[cfg(feature = "sqlite")]
    #[test]
    fn full_sync() {
        env_logger::init();
        let mut server = mockito::Server::new();

        let test_path = "test-syncs/maristfall2024/01.json";
        let updates_text = fs::read_to_string(test_path).unwrap();
        let _: AllSyncResult = from_str(&updates_text).unwrap();
        server
            .mock("GET", "/sync/all")
            .with_header("content-type", "application/json")
            .with_body(updates_text)
            .create();

        let mut sqlite_datastore = get_datastore().expect("Could not get sqlite data store");

        sqlite_datastore
            .set_request_sync_resources(SyncResources::Everything)
            .unwrap()
    }
}
