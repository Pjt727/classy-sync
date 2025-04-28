#![allow(dead_code)]
mod sync_classes;
use env_logger;
use log::{error, info, warn};
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::env;
use structopt::StructOpt;
use sync_classes::*;

#[derive(Debug, StructOpt)]
struct Cli {
    /// Sets the schools/ terms relevant for the action
    ///
    /// Comma separated pairs of schoolid,termcollectionid deliminated by semicolons
    ///   or just the school itself
    /// ex: "marist;temple,202422"
    #[structopt()]
    schools_or_terms: Option<String>,
}

fn main() {
    env_logger::init();
    let args = Cli::from_args();
    let db_path = env::var("SQLITE_DB").expect("sqlite database env var not found");
    let con = Connection::open(&db_path).expect(&format!("Could not open path `{}`", db_path));
    if let Some(input) = args.schools_or_terms {
        let mut sync_options = SelectSyncOptions::new();
        let schools_or_terms: Vec<String> = input.split(";").map(|s| s.to_string()).collect();
        for schoool_or_term in schools_or_terms.into_iter() {
            let mut school_and_maybe_term = schoool_or_term.split(",").into_iter();
            let school = school_and_maybe_term.next().expect("No School given?");
            if let Some(term) = school_and_maybe_term.next() {
                sync_options.add_term(school, term)
            } else {
                sync_options.add_school(school)
            };
        }
    }
}
