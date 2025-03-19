#![allow(dead_code)]
mod sync_classes;
use env_logger;
use log::info;
use std::{thread, time};

fn main() {
    env_logger::init();
    loop {
        info!("Syncing started");
        let start_time = time::Instant::now();
        // TODO: connect to the actaul server
        let end = time::Instant::now();
        info!("Syncing finished in {:?}", end.duration_since(start_time));
        thread::sleep(time::Duration::from_millis(500))
    }
}
