#![allow(dead_code)]
mod argument_parser;
mod data_stores;
mod errors;
mod replicate_datastore;
mod sync_classes;
mod sync_requests;
use env_logger;
use log::{error, info, warn};
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::env;
use sync_classes::*;

const SYNC_ALL_ROUTE: &str = "sync/all";
const SYNC_TERM_ROUTE: &str = "sync/select";
fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    let db_path = env::var("SQLITE_DB").expect("sqlite database env var not found");
    let con = Connection::open(&db_path).expect(&format!("Could not open path `{}`", db_path));
}
