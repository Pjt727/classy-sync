use crate::argument_parser::{CollectionType, SyncResources};
use crate::data_stores::replicate_datastore::Datastore;
use crate::data_stores::sync_requests::{
    self, AllSync, AllSyncResult, ClassDataSync, SelectSync, SyncAction, SyncOptions,
    TermSyncResult,
};
use crate::errors::{Error, SyncError};
use log::{trace, warn};
use rusqlite::{Connection, Transaction, params_from_iter};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::result::Result;
use std::{env, fs};

const DEFAULT_MAX_RECORDS: u16 = 10_000;

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
            let up_migration_classy =
                fs::read_to_string("src/data_stores/sqlite/migrations/001.up.sql").unwrap();
            let up_migration_sync =
                fs::read_to_string("src/data_stores/sqlite/migrations/002.up.sql").unwrap();
            let conn = Connection::open(file_path)?;
            conn.execute_batch(&up_migration_classy)?;
            conn.execute_batch(&up_migration_sync)?;
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
                trace!("update: {}", &sql_string);
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
                trace!("delete: {}", &sql_string);
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
                trace!("insert: {} {:?}", &sql_string, param_args);
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

    fn is_all_sync(&mut self) -> Result<bool, Error> {
        Ok(self.conn.query_row(
            r#" 
            SELECT EXISTS (
                SELECT 1 FROM _previous_all_collections
            );
            "#,
            (),
            |row| row.get(0),
        )?)
    }

    fn is_select_sync(&mut self) -> Result<bool, Error> {
        Ok(self.conn.query_row(
            r#" 
        SELECT (
            EXISTS (SELECT 1 FROM _school_strategies) 
        );
        "#,
            (),
            |row| row.get(0),
        )?)
    }
    fn get_all_request_options(&mut self) -> Result<AllSync, Error> {
        if self.is_select_sync()? {
            return Err(SyncError::new(
                "Cannot sync all because term sync and or school sync was ran before",
            ));
        }
        let last_sync: u64 = self.conn.query_row(
            r#" 
                SELECT COALESCE(MAX(synced_at), 0)
                FROM _previous_all_collections;
            "#,
            (),
            |row| row.get(0),
        )?;
        return Ok(AllSync {
            last_sync,
            max_records_count: Some(DEFAULT_MAX_RECORDS),
        });
    }

    fn get_select_request_options(&mut self) -> Result<SelectSync, Error> {
        if self.is_all_sync()? {
            return Err(SyncError::new(
                "Cannot sync select because sync all has been run previously",
            ));
        }
        let mut all_school_query = self.conn.prepare(
            r#" 
                SELECT s.school_id, COALESCE(MAX(p.synced_at), 0) AS sequence
                FROM _school_strategies s
                LEFT JOIN _previous_school_collections p ON s.school_id = p.school_id
                WHERE s.term_collection_id IS NULL
                GROUP BY s.school_id
                ;
            "#,
        )?;
        let school_to_last_sequence = all_school_query
            .query_map((), |r| {
                let res: (String, u64) = (r.get(0)?, r.get(1)?);
                Ok(res)
            })?
            .collect::<Result<HashMap<_, _>, _>>()?;

        let mut term_school_query = self.conn.prepare(
            r#" 
                SELECT s.school_id, s.term_collection_id, COALESCE(MAX(p.synced_at), 0) AS sequence
                FROM _school_strategies s
                LEFT JOIN _previous_term_collections p 
                    ON s.school_id = p.school_id AND s.term_collection_id = p.term_collection_id
                WHERE s.term_collection_id IS NOT NULL
                GROUP BY s.school_id, s.term_collection_id
                ;
            "#,
        )?;

        let term_to_last_sequence = term_school_query
            .query_map((), |r| {
                let res: ((String, String), u64) = ((r.get(0)?, r.get(1)?), r.get(2)?);
                Ok(res)
            })?
            .collect::<Result<HashMap<_, _>, _>>()?;

        let mut term_sync = SelectSync::new();
        for ((school_id, term_collection_id), sequence) in term_to_last_sequence {
            if school_to_last_sequence.contains_key(&school_id) {
                // this situation happens when an the scope of syncing goes from term to the whole
                // school
                // this exclusion is just for the next sync operation and then it should no longer
                // be needed so long as the school's sync is >= the excluded sequence
                term_sync.add_exclusion(school_id, term_collection_id, sequence)?;
            } else {
                term_sync.add_term_sync(school_id, term_collection_id, sequence)?;
            }
        }

        for (school_id, sequence) in school_to_last_sequence {
            term_sync.add_school_sync(school_id, sequence)?;
        }
        Ok(term_sync)
    }
}

impl Datastore for Sqlite {
    fn execute_all_request_sync(&mut self, all_sync_response: AllSyncResult) -> Result<(), Error> {
        let tx = self.conn.transaction()?;
        tx.execute(
            r#" INSERT INTO _previous_all_collections (synced_at) 
            VALUES ($1);
        "#,
            (all_sync_response.new_latest_sync,),
        )?;
        for sync in all_sync_response.sync_data.into_iter() {
            Sqlite::execute_sync(&tx, sync)?
        }
        tx.commit()?;
        return Ok(());
    }

    fn execute_select_request_sync(
        &mut self,
        select_sync_request: SelectSync,
        select_sync_response: TermSyncResult,
    ) -> Result<(), Error> {
        let _ = select_sync_request;
        let tx = self.conn.transaction()?;
        for (school_id, entry) in &select_sync_response.new_sync_term_sequences {
            match entry {
                sync_requests::SchoolEntry::TermToSequence(term_sequence) => {
                    for (term, sequence) in term_sequence {
                        tx.execute(
                            r#"
                            INSERT INTO _previous_term_collections (synced_at, school_id, term_collection_id) 
                            VALUES ($1, $2, $3);
                            "#,
                            (sequence, school_id, term),
                        )?;
                    }
                }
                sync_requests::SchoolEntry::Sequence(sequence) => {
                    tx.execute(
                        r#"
                        INSERT INTO _previous_school_collections (synced_at, school_id) 
                        VALUES ($1, $2);
                        "#,
                        (sequence, school_id),
                    )?;
                }
            }
        }
        for sync in select_sync_response.sync_data.into_iter() {
            Sqlite::execute_sync(&tx, sync)?
        }
        tx.commit()?;
        return Ok(());
    }

    fn generate_sync_options(&mut self) -> Result<SyncOptions, Error> {
        match (self.is_select_sync()?, self.is_all_sync()?) {
            (true, true) => Err(SyncError::new(
                "Dirty db state cannot be both select and all sync",
            )),
            (true, false) => Ok(SyncOptions::Select(self.get_select_request_options()?)),
            (false, true) => Ok(SyncOptions::All(self.get_all_request_options()?)),
            (false, false) => Err(SyncError::new(
                "Sync stratgey not set! Set the resources to sync.",
            )),
        }
    }

    fn set_request_sync_resources(&mut self, resources: SyncResources) -> Result<(), Error> {
        match resources {
            SyncResources::Everything => {
                if self.is_select_sync()? {
                    return Err(SyncError::new(
                        "Cannot set sync all because select syncs have already been done",
                    ));
                }
                // is already set to sync all so do nothing
                if self.is_all_sync()? {
                    return Ok(());
                }
                self.conn.execute(
                    r#"
                    INSERT INTO _previous_all_collections (synced_at)
                    VALUES (0);
                    "#,
                    (),
                )?;
            }
            SyncResources::Select(select_sync_options) => {
                if self.is_all_sync()? {
                    return Err(SyncError::new(
                        "Cannot set sync select because sync all has already been done",
                    ));
                }
                let mut get_full_schools = self.conn.prepare(
                    r#"
                    SELECT school_id, term_collection_id
                    FROM _school_strategies
                    "#,
                )?;
                let mut full_school_collections: HashSet<(String, Option<String>)> = HashSet::new();
                let full_school_collections_rows =
                    get_full_schools.query_map((), |r| Ok((r.get(0)?, r.get(1)?)))?;
                for f in full_school_collections_rows {
                    full_school_collections.insert(f?);
                }
                for (school_id, collection_type) in select_sync_options.school_to_collection {
                    match collection_type {
                        CollectionType::AllSchoolData => {
                            if !full_school_collections.contains(&(school_id.clone(), None)) {
                                self.conn.execute(
                                    r#"
                                    INSERT INTO _school_strategies 
                                    (school_id, term_collection_id) 
                                    VALUES (?, NULL)
                                    "#,
                                    [school_id],
                                )?;
                            }
                        }
                        CollectionType::SelectTermData(terms) => {
                            if full_school_collections.contains(&(school_id.clone(), None)) {
                                return Err(SyncError::new(format!(
                                    "Cannot do select term sync for school `{}` because the whole school as been synced",
                                    school_id
                                )));
                            }
                            for term in terms {
                                if !full_school_collections
                                    .contains(&(school_id.clone(), Some(term.clone())))
                                {
                                    self.conn.execute(
                                        r#"
                                        INSERT INTO _school_strategies 
                                        (school_id, term_collection_id) 
                                        VALUES (?, ?)
                                        "#,
                                        [school_id.clone(), term],
                                    )?;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn unset_request_sync_resources(&mut self, resources: SyncResources) -> Result<(), Error> {
        let _ = resources;
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
    use std::{fs, path::PathBuf};

    // note if not using an in-memory database only run a single test or use --test-threads=1
    //   which will leave your database with the last sqlite test data in the db
    #[test]
    fn full_sync() {
        dotenv().ok();
        env_logger::init();
        let mut conn;
        if let Ok(path_to_sqlite_db) = env::var("TEST_SQLITE_DB_PATH") {
            let file_path = Path::new(&path_to_sqlite_db);
            if file_path.exists() {
                fs::remove_file(file_path).unwrap()
            }
            conn = Sqlite::get_db_connection(file_path).unwrap();
        } else {
            conn = Connection::open_in_memory().unwrap();
        }

        let mut stored_syncs = Vec::new();
        let directory_of_test_syncs = "test-syncs/maristfall2024";
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
                            stored_syncs.push(file_name_str.to_string());
                        }
                    }
                }
            }
        }
        stored_syncs.sort();

        let mut base_path = PathBuf::new();
        base_path.push(directory_of_test_syncs);
        for test_sync in &stored_syncs {
            let mut full_path = base_path.clone();
            full_path.push(test_sync);
            info!("Starting sync: {}", test_sync);
            let tx = conn.transaction().unwrap();
            let updates_text = fs::read_to_string(&full_path).unwrap();
            let response: AllSyncResult = from_str(&updates_text).unwrap();
            for update in response.sync_data {
                Sqlite::execute_sync(&tx, update).unwrap()
            }
            tx.commit().unwrap();
            info!("Finished sync: {}", test_sync);
        }
    }
}
