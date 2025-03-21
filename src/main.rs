#![allow(dead_code)]
mod sync_classes;
use env_logger;
use log::info;
use rusqlite::Connection;
use std::env;
use std::sync::{Arc, Mutex};
use std::{thread, time};

struct SyncInfo {
    last_sync: time::Instant,
    is_syncing: bool,
}

fn main() {
    env_logger::init();
    const DEBOUNCE_TIME: time::Duration = time::Duration::from_millis(500);
    const REFRESH_RATE: time::Duration = time::Duration::from_millis(5_000);
    let db_path = env::var("SQLITE_DB").expect("sqlite database env var not found");
    let last_info = Arc::new(Mutex::new(SyncInfo {
        last_sync: time::Instant::now(),
        is_syncing: false,
    }));
    let con = Arc::new(Mutex::new(
        Connection::open(&db_path).expect(&format!("Could not open path `{}`", db_path)),
    ));

    thread::spawn(move || {
        let last_info_clone = Arc::clone(&last_info);
        let con_clone = Arc::clone(&con);
        loop {
            info!("Syncing started");
            let start_time = time::Instant::now();
            {
                let mut inf = last_info_clone.lock().unwrap();
                inf.is_syncing = true;
            }
            {
                let mut con = con_clone.lock().unwrap();
                sync_classes::sync_all(&mut con).unwrap();
            }
            {
                let mut inf = last_info_clone.lock().unwrap();
                inf.last_sync = start_time;
                inf.is_syncing = false;
            }
            let end = start_time.elapsed();
            info!("Syncing finished in {:?}", end);
            thread::sleep(REFRESH_RATE)
        }
    });

    info!("Listening to syncs");
    loop {}
}
