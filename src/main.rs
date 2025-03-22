#![allow(dead_code)]
mod sync_classes;
use chrono::{DateTime, Utc};
use env_logger;
use log::{error, info, warn};
use rusqlite::Connection;
use std::env;
use std::io::{BufRead, stdin};
use std::sync::{Arc, Mutex};
use std::{thread, time};

struct SyncInfo {
    last_sync: Option<DateTime<Utc>>,
    is_syncing: bool,
}

fn main() {
    env_logger::init();
    const DEBOUNCE_TIME: time::Duration = time::Duration::from_millis(500);
    const REFRESH_RATE: time::Duration = time::Duration::from_millis(5_000);
    let db_path = env::var("SQLITE_DB").expect("sqlite database env var not found");
    let last_info = Arc::new(Mutex::new(SyncInfo {
        last_sync: None,
        is_syncing: false,
    }));
    let con = Arc::new(Mutex::new(
        Connection::open(&db_path).expect(&format!("Could not open path `{}`", db_path)),
    ));

    let last_info_clone = Arc::clone(&last_info);
    let con_clone = Arc::clone(&con);
    thread::spawn(move || {
        loop {
            info!("Automatic sync starting");
            let end = match do_sync(&last_info_clone, &con_clone) {
                SyncResult::CompletedIn(t) => t,
                SyncResult::InProgress => {
                    info!("Syncing already in progress...");
                    thread::sleep(REFRESH_RATE);
                    continue;
                }
                SyncResult::Failure(e) => {
                    error!("Syncing failed {:?}", e);
                    continue;
                }
            };
            info!("Syncing finished in {:?}", end);
            thread::sleep(REFRESH_RATE - end)
        }
    });

    info!("Listening to syncs");
    let handle = stdin().lock();
    for line in handle.lines() {
        let input = match line {
            Ok(input) => input,
            Err(e) => {
                error!("Error reading input: {}", e);
                break;
            }
        };

        // Handle invalid input early
        if input.trim().to_lowercase() != "update" {
            warn!("Invalid piped program input of `{}`", input);
            continue;
        }

        // Process the "update" command
        info!("Starting sync request");
        let end = match do_sync(&last_info, &con) {
            SyncResult::CompletedIn(t) => t,
            SyncResult::InProgress => {
                info!("Syncing already in progress...");
                continue;
            }
            SyncResult::Failure(e) => {
                error!("Syncing failed {:?}", e);
                continue;
            }
        };
        info!("Syncing finished in {:?}", end);
    }
}

enum SyncResult {
    CompletedIn(time::Duration),
    InProgress,
    Failure(sync_classes::SyncError),
}

fn do_sync(
    last_info_clone: &Arc<Mutex<SyncInfo>>,
    con_clone: &Arc<Mutex<Connection>>,
) -> SyncResult {
    let start_time = time::Instant::now();

    let last_time;
    {
        let mut sync_info = last_info_clone.lock().unwrap();
        if sync_info.is_syncing {
            drop(sync_info);
            return SyncResult::InProgress;
        }
        sync_info.is_syncing = true;
    }
    {
        let last_update_time;
        {
            let sync_info = last_info_clone.lock().unwrap();
            last_update_time = sync_info.last_sync.clone()
        }
        let mut con = con_clone.lock().unwrap();
        last_time = match sync_classes::sync_all(&mut con, last_update_time) {
            Ok(t) => t,
            Err(e) => return SyncResult::Failure(e),
        }
    }
    {
        let mut sync_info = last_info_clone.lock().unwrap();
        sync_info.last_sync = Some(last_time);
        sync_info.is_syncing = false;
        return SyncResult::CompletedIn(start_time.elapsed());
    }
}
