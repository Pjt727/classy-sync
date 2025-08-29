use crate::argument_parser::{CollectionType, SyncResources};
use crate::data_stores::replicate_datastore::Datastore;
use crate::data_stores::sqlite::errors::SqliteError;
use crate::data_stores::sync_requests::{
    self, AllSync, AllSyncResult, ClassDataSync, SelectSync, SyncAction, SyncOptions,
    TermSyncResult,
};
use crate::errors::DataStoreError; // Keep this import for the Datastore trait
use log::{trace, warn};
use rusqlite::{Connection, Transaction, params_from_iter};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::result::Result;

const DEFAULT_MAX_RECORDS: u16 = 10_000;

pub struct Sqlite {
    conn: Connection,
    is_strict: bool,
}

pub struct SqliteConfig {
    pub db_path: Option<String>,
    pub is_strict: bool,
    pub max_records_for_syncs: u16,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            db_path: None,
            is_strict: true,
            max_records_for_syncs: DEFAULT_MAX_RECORDS,
        }
    }
}

impl Sqlite {
    pub fn new(config: SqliteConfig) -> Result<Sqlite, SqliteError> {
        let conn = if let Some(db_path) = config.db_path {
            let file_path = Path::new(&db_path);
            Sqlite::get_db_connection(file_path)?
        } else {
            let conn = Connection::open_in_memory()?;
            Sqlite::run_migrations(&conn)?;
            conn
        };
        Ok(Sqlite {
            conn,
            is_strict: false,
        })
    }

    fn get_db_connection(file_path: &Path) -> Result<Connection, SqliteError> {
        // Return SqliteError
        if !file_path.exists() {
            if let Some(parent_dir) = file_path.parent() {
                fs::create_dir_all(parent_dir)?;
            }
            fs::File::create(file_path)?;
            let conn = Connection::open(file_path)?;
            Sqlite::run_migrations(&conn)?;
            Ok(conn)
        } else {
            // TODO: check to see if the migrations are up to date
            Ok(Connection::open(file_path)?)
        }
    }

    fn run_migrations(conn: &Connection) -> Result<(), SqliteError> {
        // TODO: embed the migrations into the build process and run up migrations
        let up_migration_classy =
            fs::read_to_string("src/data_stores/sqlite/migrations/001.up.sql")?;
        let up_migration_sync = fs::read_to_string("src/data_stores/sqlite/migrations/002.up.sql")?;
        conn.execute_batch(&up_migration_classy)?;
        conn.execute_batch(&up_migration_sync)?;
        Ok(())
    }

    // This is the crux of the sqlite data store... being able to convert a `ClassDataSync` into a
    // sqlite query
    fn execute_sync(
        conn: &Transaction,
        sync: ClassDataSync,
        is_strict: bool,
    ) -> Result<(), SqliteError> {
        sync.verify_record()
            .map_err(|e| SqliteError::ValueConversionError(e.to_string()))?;
        let sql_string: String;
        let result = match sync.sync_action {
            SyncAction::Update => {
                if sync.relevant_fields.is_none()
                    || sync.relevant_fields.as_ref().unwrap().is_empty()
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
                    param_args.push(convert_to_sql_value(val)?);
                    arg_counter += 1;
                    set_values.push(format!("{col} = ?{arg_counter}"))
                }
                let set_values = set_values.join(", ");
                let mut where_values = vec![];
                for (col, val) in sync.pk_fields.iter() {
                    param_args.push(convert_to_sql_value(val)?);
                    arg_counter += 1;
                    where_values.push(format!("{col} = ?{arg_counter}"))
                }

                let where_values = where_values.join(" AND ");
                sql_string = format!(
                    "UPDATE {} SET {} WHERE {};",
                    sync.table_name, set_values, where_values
                );
                trace!("update: {}", &sql_string);
                let mut maybe_statement = conn.prepare_cached(&sql_string)?;

                maybe_statement.execute(params_from_iter(param_args))
            }
            SyncAction::Delete => {
                let mut arg_counter: usize = 0;
                let mut param_args: Vec<rusqlite::types::Value> = vec![];
                let mut where_values = vec![];
                for (col, val) in sync.pk_fields.iter() {
                    param_args.push(convert_to_sql_value(val)?);
                    arg_counter += 1;
                    where_values.push(format!("{col} = ?{arg_counter}"))
                }
                let where_values = where_values.join(" AND ");

                sql_string = format!("DELETE FROM {} WHERE {};", sync.table_name, where_values);
                trace!("delete: {}", &sql_string);
                let mut maybe_statement = conn.prepare_cached(&sql_string)?;
                maybe_statement.execute(params_from_iter(param_args))
            }
            SyncAction::Insert => {
                let mut arg_counter: usize = 0;
                let mut param_args: Vec<rusqlite::types::Value> = vec![];
                let mut columns = vec![];
                let mut values = vec![];
                for (col, val) in sync.pk_fields.iter() {
                    param_args.push(convert_to_sql_value(val)?);
                    arg_counter += 1;
                    columns.push(col.to_string());
                    values.push(format!("?{arg_counter}"))
                }
                for (col, val) in sync
                    .relevant_fields
                    .as_ref()
                    .unwrap_or(&HashMap::new())
                    .iter()
                {
                    param_args.push(convert_to_sql_value(val)?);
                    arg_counter += 1;
                    columns.push(col.to_string());
                    values.push(format!("?{arg_counter}"))
                }
                let columns = columns.join(", ");
                let values = values.join(", ");

                sql_string = format!(
                    "INSERT INTO {} ({}) VALUES ({});",
                    sync.table_name, columns, values
                );
                trace!("insert: {} {:?}", &sql_string, param_args);
                let mut maybe_statement = conn.prepare_cached(&sql_string)?;

                maybe_statement.execute(params_from_iter(param_args))
            }
        };

        let query_output = result.map_err(|err| SqliteError::FailedSqliteQuery {
            query_info: format!("sync query `{}`", sql_string),
            source: err,
        })?;

        match (query_output, is_strict) {
            (n, false) if n != 1 => {
                warn!("Query affected {} rows expected 1", n);
                Ok(())
            }
            (n, true) if n != 1 => Err(SqliteError::UnexpectedQueryResult {
                query: sql_string.to_string(),
                result: n.to_string(),
                expected: "1".to_string(),
            }),
            (_, _) => Ok(()),
        }
    }

    fn is_all_sync(&mut self) -> Result<bool, SqliteError> {
        // Return SqliteError
        self.conn
            .query_row(
                r#"
            SELECT EXISTS (
                SELECT 1 FROM _previous_all_collections
            );
            "#,
                (),
                |row| row.get(0),
            )
            .map_err(|e| SqliteError::FailedSqliteQuery {
                query_info: "getting all sync".to_string(),
                source: e,
            })
    }

    fn is_select_sync(&mut self) -> Result<bool, SqliteError> {
        // Return SqliteError
        self.conn
            .query_row(
                r#"
        SELECT (
            EXISTS (SELECT 1 FROM _school_strategies)
        );
        "#,
                (),
                |row| row.get(0),
            )
            .map_err(|e| SqliteError::FailedSqliteQuery {
                query_info: "does select sync".to_string(),
                source: e,
            })
    }

    fn get_all_request_options(&mut self) -> Result<AllSync, SqliteError> {
        if self.is_select_sync()? {
            return Err(SqliteError::UnsupportedSyncOperation(
                "Cannot sync all because term sync and or school sync was ran before".to_string(),
            ));
        }
        let last_sync: u64 = self
            .conn
            .query_row(
                r#"
                SELECT COALESCE(MAX(synced_at), 0)
                FROM _previous_all_collections;
            "#,
                (),
                |row| row.get(0),
            )
            .map_err(|e| SqliteError::FailedSqliteQuery {
                query_info: "getting lastest all sync".to_string(),
                source: e,
            })?;
        Ok(AllSync {
            last_sync,
            max_records_count: Some(DEFAULT_MAX_RECORDS),
        })
    }

    fn get_select_request_options(&mut self) -> Result<SelectSync, SqliteError> {
        if self.is_all_sync()? {
            return Err(SqliteError::UnsupportedSyncOperation(
                "Cannot sync select because sync all has been run previously".to_string(),
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
            })? // #[from] RusqliteError
            .collect::<Result<HashMap<_, _>, _>>() // Inner map error can be from rusqlite::Error::FromSqlError
            .map_err(|e| SqliteError::FailedSqliteQuery {
                query_info: "collecting school last sequence".to_string(),
                source: e,
            })?;

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
        )?; // #[from] RusqliteError

        let term_to_last_sequence = term_school_query
            .query_map((), |r| {
                let res: ((String, String), u64) = ((r.get(0)?, r.get(1)?), r.get(2)?);
                Ok(res)
            })
            .map_err(|e| SqliteError::FailedSqliteQuery {
                query_info: "getting term last sequence".to_string(),
                source: e,
            })?
            .collect::<Result<HashMap<_, _>, _>>()
            .map_err(|_| {
                SqliteError::DataIntegrityError(
                    "could not collect term and last sequence into hashmap".to_string(),
                )
            })?;

        let mut term_sync = SelectSync::new();
        for ((school_id, term_collection_id), sequence) in term_to_last_sequence {
            if school_to_last_sequence.contains_key(&school_id) {
                // this situation happens when an the scope of syncing goes from term to the whole
                // school
                // this exclusion is just for the next sync operation and then it should no longer
                // be needed so long as the school's sync is >= the excluded sequence
                term_sync
                    .add_exclusion(school_id.clone(), term_collection_id.clone(), sequence)
                    .map_err(|_| {
                        SqliteError::DataIntegrityError(format!(
                            "({school_id}, {term_collection_id}) could not be added to select sync exclusion"
                        ))
                    })?;
            } else {
                term_sync
                    .add_term_sync(school_id.clone(), term_collection_id.clone(), sequence)
                    .map_err(|_| {
                        SqliteError::DataIntegrityError(format!(
                            "({school_id}, {term_collection_id}) could not be added to select syncs"
                        ))
                    })?
            }
        }

        for (school_id, sequence) in school_to_last_sequence {
            term_sync
                .add_school_sync(school_id.clone(), sequence)
                .map_err(|_| {
                    SqliteError::DataIntegrityError(format!(
                        "`{school_id}` could not be added to select syncs"
                    ))
                })?
        }
        Ok(term_sync)
    }
}

impl Datastore for Sqlite {
    fn execute_all_request_sync(
        &mut self,
        all_sync_response: AllSyncResult,
    ) -> Result<(), DataStoreError> {
        let tx = self.conn.transaction().map_err(SqliteError::from)?;
        tx.execute(
            r#" INSERT INTO _previous_all_collections (synced_at)
            VALUES ($1);
        "#,
            (all_sync_response.new_latest_sync,),
        )
        .map_err(|e| SqliteError::FailedSqliteQuery {
            query_info: "inserting previous all collections".to_string(),
            source: e,
        })?;
        for sync in all_sync_response.sync_data.into_iter() {
            Self::execute_sync(&tx, sync, self.is_strict)?
        }
        tx.commit().map_err(SqliteError::from)?;
        Ok(())
    }

    fn execute_select_request_sync(
        &mut self,
        select_sync_request: SelectSync,
        select_sync_response: TermSyncResult,
    ) -> Result<(), DataStoreError> {
        let _ = select_sync_request;
        let tx = self.conn.transaction().map_err(SqliteError::from)?;
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
                        )
                        .map_err(|e| SqliteError::FailedSqliteQuery { query_info: "insert previous term collelctions".to_string(), source: e })?;
                    }
                }
                sync_requests::SchoolEntry::Sequence(sequence) => {
                    tx.execute(
                        r#"
                        INSERT INTO _previous_school_collections (synced_at, school_id)
                        VALUES ($1, $2);
                        "#,
                        (sequence, school_id),
                    )
                    .map_err(|e| SqliteError::FailedSqliteQuery {
                        query_info: "insert previous school collelctions".to_string(),
                        source: e,
                    })?;
                }
            }
        }
        for sync in select_sync_response.sync_data.into_iter() {
            Self::execute_sync(&tx, sync, self.is_strict)?
        }
        tx.commit().map_err(SqliteError::from)?;
        Ok(())
    }

    fn generate_sync_options(&mut self) -> Result<SyncOptions, DataStoreError> {
        match (self.is_select_sync()?, self.is_all_sync()?) {
            (true, true) => Err(SqliteError::DataIntegrityError(
                "dirty db state cannot be both select and all sync".to_string(),
            ))?,
            (true, false) => Ok(SyncOptions::Select(self.get_select_request_options()?)),
            (false, true) => Ok(SyncOptions::All(self.get_all_request_options()?)),
            (false, false) => Err(SqliteError::DataIntegrityError(
                "sync stratgey not set, Set the resources to sync".to_string(),
            ))?,
        }
    }

    fn set_request_sync_resources(
        &mut self,
        resources: SyncResources,
    ) -> Result<(), DataStoreError> {
        match resources {
            SyncResources::Everything => {
                if self.is_select_sync()? {
                    Err(SqliteError::DataIntegrityError(
                        "Cannot set sync all because select syncs have already been done"
                            .to_string(),
                    ))?
                }
                // is already set to sync all so do nothing
                if self.is_all_sync()? {
                    return Ok(());
                }
                self.conn
                    .execute(
                        r#"
                    INSERT INTO _previous_all_collections (synced_at)
                    VALUES (0);
                    "#,
                        (),
                    )
                    .map_err(|e| SqliteError::FailedSqliteQuery {
                        query_info: "insert previous all collections".to_string(),
                        source: e,
                    })?;
            }
            SyncResources::Select(select_sync_options) => {
                if self.is_all_sync()? {
                    Err(SqliteError::DataIntegrityError(
                        "Cannot set sync select because sync all has already been done".to_string(),
                    ))?
                }
                let mut get_full_schools = self
                    .conn
                    .prepare(
                        r#"
                    SELECT school_id, term_collection_id
                    FROM _school_strategies
                    "#,
                    )
                    .map_err(SqliteError::from)?;
                let mut full_school_collections: HashSet<(String, Option<String>)> = HashSet::new();
                let full_school_collections_rows = get_full_schools
                    .query_map((), |r| Ok((r.get(0)?, r.get(1)?)))
                    .map_err(|e| SqliteError::FailedSqliteQuery {
                        query_info: "get school_id, term_collection_id".to_string(),
                        source: e,
                    })?
                    .collect::<Result<Vec<_>, _>>() // Collect to Vec first to handle inner errors
                    .map_err(|e| SqliteError::FailedSqliteQuery {
                        query_info: "get and collect school_id, term_collection_id".to_string(),
                        source: e,
                    })?;

                for f in full_school_collections_rows {
                    full_school_collections.insert(f);
                }

                for (school_id, collection_type) in select_sync_options.get_collections() {
                    match collection_type {
                        CollectionType::AllSchoolData => {
                            if !full_school_collections.contains(&(school_id.clone(), None)) {
                                self.conn
                                    .execute(
                                        r#"
                                    INSERT INTO _school_strategies
                                    (school_id, term_collection_id)
                                    VALUES (?, NULL)
                                    "#,
                                        [school_id],
                                    )
                                    .map_err(|e| SqliteError::FailedSqliteQuery {
                                        query_info: "insert all school strategies".to_string(),
                                        source: e,
                                    })?;
                            }
                        }
                        CollectionType::SelectTermData(terms) => {
                            if full_school_collections.contains(&(school_id.clone(), None)) {
                                Err(SqliteError::DataIntegrityError(format!(
                                    "Cannot do select term sync for school `{school_id}` because the whole school as been synced"
                                )))?
                            }
                            for term in terms {
                                if !full_school_collections
                                    .contains(&(school_id.clone(), Some(term.clone())))
                                {
                                    self.conn
                                        .execute(
                                            r#"
                                        INSERT INTO _school_strategies
                                        (school_id, term_collection_id)
                                        VALUES (?, ?)
                                        "#,
                                            [school_id, term],
                                        )
                                        .map_err(|e| SqliteError::FailedSqliteQuery {
                                            query_info: "insert select school strategies"
                                                .to_string(),
                                            source: e,
                                        })?;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn unset_request_sync_resources(
        &mut self,
        resources: SyncResources,
    ) -> Result<(), DataStoreError> {
        let _ = resources;
        todo!()
    }

    fn add_schools(&mut self, schools: Vec<sync_requests::School>) -> Result<(), DataStoreError> {
        let tx = self.conn.transaction().map_err(SqliteError::from)?;
        for school in schools {
            tx.execute(
                r#"
            INSERT INTO schools (id, name)
            VALUES ($1, $2);
            "#,
                (school.id, school.name),
            )
            .map_err(|e| SqliteError::FailedSqliteQuery {
                query_info: "insert schools".to_string(),
                source: e,
            })?;
        }
        tx.commit().map_err(SqliteError::from)?;
        Ok(())
    }

    fn add_terms(&mut self, terms: Vec<sync_requests::Term>) -> Result<(), DataStoreError> {
        let tx = self.conn.transaction().map_err(SqliteError::from)?;
        for term in terms {
            tx.execute(
                r#"
            INSERT INTO terms (id, school_id, year, season, name, still_collecting)
            VALUES ($1, $2, $3, $4, $5, $6);
            "#,
                (
                    term.id,
                    term.school_id,
                    term.year,
                    term.season,
                    term.name,
                    term.still_collecting,
                ),
            )
            .map_err(|e| SqliteError::FailedSqliteQuery {
                query_info: "insert schools".to_string(),
                source: e,
            })?;
        }
        tx.commit().map_err(SqliteError::from)?;
        Ok(())
    }
}

// This helper function also needs to return SqliteError
fn convert_to_sql_value(v: &Value) -> Result<rusqlite::types::Value, SqliteError> {
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
                // If the number cannot be represented as i64 or f64, treat as null or error
                // Changed to an error for explicit handling
                Err(SqliteError::ValueConversionError(format!(
                    "Unsupported number format: {n:?}"
                )))
            }
        }
        _ => Err(SqliteError::ValueConversionError(format!(
            "Unsupported type {v:?}"
        ))),
    }
}

#[cfg(test)]
mod sync_tests {
    use super::*;
    use log::info;
    use serde_json::from_str;
    use std::{fs, path::PathBuf};

    // note if not using an in-memory database only run a single test or use --test-threads=1
    //   which will leave your database with the last sqlite test data in the db
    #[test]
    fn full_sync() {
        // dotenv().ok();
        // env_logger::init();
        let mut sqlite = Sqlite::new(SqliteConfig {
            ..Default::default()
        })
        .unwrap();

        let mut stored_syncs = Vec::new();
        let directory_of_test_syncs = "test-syncs/maristfall2024";
        for entry in fs::read_dir(directory_of_test_syncs).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file()
                && let Some(extension) = path.extension()
            {
                if extension != "json" {
                    continue;
                }
                if let Some(file_name) = path.file_name()
                    && let Some(file_name_str) = file_name.to_str()
                {
                    stored_syncs.push(file_name_str.to_string());
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
            let tx = sqlite.conn.transaction().unwrap();
            let updates_text = fs::read_to_string(&full_path).unwrap();
            let response: AllSyncResult = from_str(&updates_text).unwrap();
            for update in response.sync_data {
                let res = Sqlite::execute_sync(&tx, update, true);
                if let Err(err) = res {
                    panic!("could not do sync {test_sync} {err}")
                }
            }
            tx.commit().unwrap();
            info!("Finished sync: {}", test_sync);
        }
    }
}
