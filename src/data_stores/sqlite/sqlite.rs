use crate::argument_parser::SelectSyncOptions;
use crate::errors::{Error, SyncError};
use crate::replicate_datastore::Datastore;
use crate::sync_requests::{
    AllSync, AllSyncResult, ClassDataSync, SelectSync, SelectSyncResult, SyncAction,
};
use log::{trace, warn};
use rusqlite::{Connection, Transaction, params_from_iter};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::result::Result;
use std::{env, fs};

pub struct Sqlite {
    conn: Connection,
}

impl Sqlite {
    pub fn new() -> Result<Sqlite, Error> {
        let db_path = env::var("SQLITE_DB_PATH")?;
        let file_path = Path::new(&db_path);
        Ok(Sqlite {
            conn: Sqlite::get_db_connection(file_path)?,
        })
    }

    fn get_db_connection(file_path: &Path) -> Result<Connection, Error> {
        if !file_path.exists() {
            if let Some(parent_dir) = file_path.parent() {
                fs::create_dir_all(parent_dir)?;
            }
            fs::File::create(file_path)?;
            // TODO: embed the migrations into the build process and run up migrations
            let up_migration =
                fs::read_to_string("src/data_stores/sqlite/migrations/001.up.sql").unwrap();
            let conn = Connection::open(file_path)?;
            conn.execute_batch(&up_migration)?;
            Ok(conn)
        } else {
            // TODO: check to see if the migrations are up to date
            Ok(Connection::open(file_path)?)
        }
    }
    // this is the crux of the sqlite data store... being able to convert a `ClassDataSync` into a
    // sqlite query
    fn execute_sync(conn: &Transaction, sync: ClassDataSync) -> Result<(), Error> {
        sync.verify_columns()?;
        let result = match sync.sync_action {
            SyncAction::Update => {
                if sync.relevant_fields.is_none()
                    || sync.relevant_fields.as_ref().unwrap().len() == 0
                {
                    warn!("Update sync with no changes: `{:?}`", sync);
                    return Ok(());
                }
                let mut arg_counter: usize = 0;
                let mut param_args: Vec<rusqlite::types::Value> = vec![];
                let mut set_values = vec![];
                for (col, val) in sync
                    .relevant_fields
                    .as_ref()
                    .unwrap_or(&HashMap::new())
                    .iter()
                {
                    match convert_to_sql_value(&val) {
                        Ok(v) => param_args.push(v),
                        Err(e) => return Err(e),
                    }
                    arg_counter += 1;
                    set_values.push(format!("{} = ?{}", col, arg_counter))
                }
                let set_values = set_values.join(", ");
                let mut where_values = vec![];
                for (col, val) in sync.pk_fields.iter() {
                    match convert_to_sql_value(&val) {
                        Ok(v) => param_args.push(v),
                        Err(e) => return Err(e),
                    }
                    arg_counter += 1;
                    where_values.push(format!("{} = ?{}", col, arg_counter))
                }

                let where_values = where_values.join(" AND ");
                let sql_string = format!(
                    "UPDATE {} SET {} WHERE {};",
                    sync.table_name, set_values, where_values
                );
                let maybe_statement = conn.prepare_cached(&sql_string);

                maybe_statement.map(|mut s| s.execute(params_from_iter(param_args)))
            }
            SyncAction::Delete => {
                let mut arg_counter: usize = 0;
                let mut param_args: Vec<rusqlite::types::Value> = vec![];
                let mut where_values = vec![];
                for (col, val) in sync.pk_fields.iter() {
                    match convert_to_sql_value(&val) {
                        Ok(v) => param_args.push(v),
                        Err(e) => return Err(e),
                    }
                    arg_counter += 1;
                    where_values.push(format!("{} = ?{}", col, arg_counter))
                }
                let where_values = where_values.join(" AND ");

                let sql_string = format!("DELETE FROM {} WHERE {};", sync.table_name, where_values);
                let maybe_statement = conn.prepare_cached(&sql_string);
                maybe_statement.map(|mut s| s.execute(params_from_iter(param_args)))
            }
            SyncAction::Insert => {
                let mut arg_counter: usize = 0;
                let mut param_args: Vec<rusqlite::types::Value> = vec![];
                let mut columns = vec![];
                let mut values = vec![];
                for (col, val) in sync.pk_fields.iter() {
                    match convert_to_sql_value(&val) {
                        Ok(v) => param_args.push(v),
                        Err(e) => return Err(e),
                    }
                    arg_counter += 1;
                    columns.push(col.to_string());
                    values.push(format!("?{}", arg_counter))
                }
                for (col, val) in sync
                    .relevant_fields
                    .as_ref()
                    .unwrap_or(&HashMap::new())
                    .iter()
                {
                    match convert_to_sql_value(&val) {
                        Ok(v) => param_args.push(v),
                        Err(e) => return Err(e),
                    }
                    arg_counter += 1;
                    columns.push(col.to_string());
                    values.push(format!("?{}", arg_counter))
                }
                let columns = columns.join(", ");
                let values = values.join(", ");

                let sql_string = format!(
                    "INSERT INTO {} ({}) VALUES ({});",
                    sync.table_name, columns, values
                );
                let maybe_statement = conn.prepare_cached(&sql_string);

                maybe_statement.map(|mut s| s.execute(params_from_iter(param_args)))
            }
        };
        match result {
            Ok(statement) => match statement {
                Ok(num) => {
                    if num != 1 {
                        warn!("Query affected {} rows expected 1", num)
                    }
                    return Ok(());
                }
                Err(err) => {
                    return Err(SyncError::new(&format!("Error executing query {:?}", err)));
                }
            },
            Err(err) => Err(SyncError::new(&format!(
                "Error preparing statement {:?}",
                err,
            ))),
        }
    }
}

impl Datastore for Sqlite {
    fn get_all_request_options(&mut self) -> Result<AllSync, Error> {
        let last_sync: u64 = self.conn.query_row(
            r#" SELECT COALESCE(
                            MAX(synced_at),
                            0
                        ) FROM previous_all_collections;
                    "#,
            (),
            |row| row.get(0),
        )?;
        return Ok(AllSync { last_sync });
    }

    fn get_select_request_options(
        &mut self,
        arguments: SelectSyncOptions,
    ) -> Result<SelectSync, Error> {
        todo!()
    }

    fn execute_all_request_sync(&mut self, all_sync_response: AllSyncResult) -> Result<(), Error> {
        let tx = self.conn.transaction()?;
        tx.execute(
            r#" INSERT INTO previous_all_collections (synced_at) 
            VALUES ($1);
        "#,
            (all_sync_response.new_latest_sync,),
        )?;
        for sync in all_sync_response.data.into_iter() {
            Sqlite::execute_sync(&tx, sync)?
        }
        tx.commit()?;
        return Ok(());
    }

    fn execute_select_request_sync(
        &mut self,
        select_sync_response: SelectSyncResult,
    ) -> Result<(), Error> {
        todo!()
    }
}

fn convert_to_sql_value(v: &Value) -> Result<rusqlite::types::Value, Error> {
    match v {
        Value::String(s) => Ok(rusqlite::types::Value::Text(s.to_string())),
        Value::Null => Ok(rusqlite::types::Value::Null),
        Value::Bool(b) => Ok(rusqlite::types::Value::Integer(*b as i64)),
        Value::Number(n) => {
            if let Some(n) = n.as_i64() {
                Ok(rusqlite::types::Value::Integer(n))
            } else if let Some(n) = n.as_f64() {
                Ok(rusqlite::types::Value::Real(n))
            } else {
                Ok(rusqlite::types::Value::Null)
            }
        }
        _ => Err(SyncError::new(format!("Unsupported type {:?}", v))),
    }
}

#[cfg(test)]
mod sync_tests {
    use super::*;
    use dotenv::dotenv;
    use log::info;
    use serde_json::from_str;
    use std::fs;

    // note if not using an in-memory database only run a single test or use --test-threads=1
    //   which will leave your database with the last sqlite test data in the db
    #[test]
    fn full_sync() {
        dotenv().ok();
        env_logger::init();
        let mut conn;
        if let Ok(path_to_sqlite_db) = env::var("TEST_SQLITE_DB_PATH") {
            let file_path = Path::new(&path_to_sqlite_db);
            conn = Sqlite::get_db_connection(file_path).unwrap();
        } else {
            conn = Connection::open_in_memory().unwrap();
        }

        let directory_of_test_syncs = "test-syncs/maristfall2024";
        let mut stored_syncs = Vec::new();
        for entry in fs::read_dir(directory_of_test_syncs).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                if let Some(extension) = path.extension() {
                    if extension != "json" {
                        continue;
                    }
                    if let Some(file_name) = path.file_name() {
                        if let Some(file_name_str) = file_name.to_str() {
                            stored_syncs
                                .push(directory_of_test_syncs.to_string() + "/" + file_name_str);
                        }
                    }
                }
            }
        }

        for test_sync in stored_syncs {
            info!("Starting sync: {}", test_sync);
            let tx = conn.transaction().unwrap();
            let updates_text = fs::read_to_string(&test_sync).unwrap();
            let response: AllSyncResult = from_str(&updates_text).unwrap();
            for update in response.data {
                Sqlite::execute_sync(&tx, update).unwrap()
            }
            tx.commit().unwrap();
            info!("Finished sync: {}", test_sync);
        }
    }
}
