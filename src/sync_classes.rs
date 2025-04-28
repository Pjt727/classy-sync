use chrono::{DateTime, Utc};
use log::{trace, warn};
use regex::Regex;
use reqwest::blocking::get;
use rusqlite::{Connection, Result, Transaction, params_from_iter};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::env;

const VALID_TABLES: [&str; 6] = [
    "meeting_times",
    "sections",
    "professors",
    "courses",
    "previous_section_collections",
    "term_collections",
];

const SYNC_ALL_ROUTE: &str = "sync/all";
const SYNC_TERM_ROUTE: &str = "sync/select";

#[derive(Deserialize, Debug)]
struct SyncResponse {
    sync_data: Vec<ClassDataUpdate>,
    #[serde(deserialize_with = "deserialize_datetime_utc")]
    last_update: DateTime<Utc>,
}

fn deserialize_datetime_utc<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%:z")
        .map_err(serde::de::Error::custom)
        .map(|dt| dt.with_timezone(&Utc))
}

#[derive(Deserialize, Debug)]
struct ClassDataUpdate {
    table_name: String,
    sync_action: SyncAction,
    pk_fields: HashMap<String, Value>,
    relevant_fields: Option<HashMap<String, Value>>,
}

#[derive(Deserialize, Debug)]
enum SyncAction {
    #[serde(rename = "update")]
    Update,
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "insert")]
    Insert,
}

#[derive(Debug)]
pub struct SyncError {
    message: String,
}

impl SyncError {
    fn new(message: &str) -> SyncError {
        SyncError {
            message: message.to_string(),
        }
    }
}

pub fn sync_all(conn: &mut Connection) -> Result<String, SyncError> {
    let url = env::var("CLASSY_API_HOST")
        .map_err(|_| SyncError::new("classy api env var not set"))?
        + SYNC_ALL_ROUTE;
    let last_sync: i32 = conn
        .query_row(
            r#" SELECT COALESCE(
                            MAX(synced_at),
                            '1970-01-01 00:00:00'
                        ) FROM previous_all_collections;
                    "#,
            (),
            |row| row.get(0),
        )
        .map_err(|e| SyncError::new(&format!("could not get last collection time {}", e)))?;
    let mut query_params = HashMap::new();
    query_params.insert("lastSyncTimeStamp", last_sync);
    let result = get(url).map_err(|e| SyncError::new(&format!("request error {:?}", e)))?;
    let response: SyncResponse = result
        .json()
        .map_err(|e| SyncError::new(&format!("response to json error {:?}", e)))?;
    let tx = conn
        .transaction()
        .map_err(|e| SyncError::new(&format!("transaction failed {:?}", e)))?;
    for update in response.sync_data {
        if let Some(err) = execute_update(&tx, update) {
            return Err(err);
        }
    }
    tx.commit()
        .map_err(|e| SyncError::new(&format!("sync failed {:?}", e)))?;
    return Ok(response.last_update);
}

#[derive(Serialize, Deserialize)]
struct PerTerm {
    school_id: String,
    term_collection_id: String,
    last_sequence: i32,
}

#[derive(Serialize, Deserialize)]
struct PerSchool {
    school_id: String,
    last_sequence: i32,
}

#[derive(Serialize, Deserialize)]
struct SyncDataPerTermParams {
    term_sequences: Vec<PerTerm>,
    school_sequences: Vec<PerSchool>,
}

// previous_term_collections
//
pub fn create_sync_params<S, T>(
    conn: &mut Connection,
    schools: S,
    terms: T,
) -> Result<SyncDataPerTermParams, SyncError>
where
    S: Iterator<Item = String>,
    T: Iterator<Item = (String, String)>,
{
    let mut params = SyncDataPerTermParams {
        term_sequences: vec![],
        school_sequences: vec![],
    };
    for school_id in schools {
        let last_sequence: i32 = conn
            .query_row(
                r#" SELECT COALESCE(
                            MAX(common_sequence),
                            0
                    ) 
                    FROM previous_term_collections
                    WHERE school_id = ?1
                    ;
                    "#,
                (&school_id,),
                |row| row.get(0),
            )
            .map_err(|e| SyncError::new(&format!("could not get last collection time {}", e)))?;
        params.school_sequences.push(PerSchool {
            school_id,
            last_sequence,
        })
    }
    Ok(params)
}

pub struct SelectSyncOptions {
    schools: HashSet<String>,
    terms: HashMap<String, HashSet<String>>,
}

impl SelectSyncOptions {
    pub fn new() -> SelectSyncOptions {
        return SelectSyncOptions {
            schools: HashSet::new(),
            terms: HashMap::new(),
        };
    }

    pub fn add_school(&mut self, school_id: &str) {
        if self.terms.contains_key(school_id) {
            warn!(
                "School {} already added from a term sync - the common sync will be ignored",
                school_id
            )
        }
        let did_add = self.schools.insert(school_id.to_owned());
        if did_add {
            warn!("School {} already added", school_id)
        }
    }

    pub fn add_term(&mut self, school_id: &str, term_id: &str) {
        if self.schools.contains(school_id) {
            warn!(
                "School {} already added as a common sync - the common sync will be ignored",
                school_id
            );
            self.schools.remove(school_id);
        }
        let did_exist = self
            .terms
            .entry(school_id.to_string())
            .or_insert(HashSet::new())
            .insert(term_id.to_string());
        if did_exist {
            warn!(
                "Term {},{} was already added - ignoring duplicate",
                school_id, term_id
            );
        }
    }
}

pub fn sync_select(
    conn: &mut Connection,
    common_only_syncs: Vec<String>,
    term_syncs: Vec<Term>,
) -> Result<DateTime<Utc>, SyncError> {
    let url = env::var("CLASSY_API_HOST")
        .map_err(|_| SyncError::new("classy api env var not set"))?
        + SYNC_ALL_ROUTE;
    let last_update = match maybe_last_update {
        Some(u) => u,
        None => {
            let timestamp: String = conn
                .query_row(
                    r#" SELECT COALESCE(
                            MAX(synced_at),
                            '1970-01-01 00:00:00'
                        ) FROM previous_all_collections;
                    "#,
                    (),
                    |row| row.get(0),
                )
                .map_err(|e| {
                    SyncError::new(&format!("could not get last collection time {}", e))
                })?;
            DateTime::parse_from_rfc3339(&timestamp)
                .map_err(|e| SyncError::new(&format!("could not parse date time {}", e)))?
                .with_timezone(&Utc)
        }
    };
    let mut query_params = HashMap::new();
    query_params.insert("lastSyncTimeStamp", last_update.to_rfc3339());
    let result = get(url).map_err(|e| SyncError::new(&format!("request error {:?}", e)))?;
    let response: SyncResponse = result
        .json()
        .map_err(|e| SyncError::new(&format!("response to json error {:?}", e)))?;
    let tx = conn
        .transaction()
        .map_err(|e| SyncError::new(&format!("transaction failed {:?}", e)))?;
    for update in response.sync_data {
        if let Some(err) = execute_update(&tx, update) {
            return Err(err);
        }
    }
    tx.commit()
        .map_err(|e| SyncError::new(&format!("sync failed {:?}", e)))?;
    return Ok(response.last_update);
}

fn execute_update(conn: &Transaction, update: ClassDataUpdate) -> Option<SyncError> {
    // to prvent possible sql injection attacks if the sync api was
    //   ever compromised
    if !VALID_TABLES.contains(&update.table_name.as_str()) {
        return Some(SyncError::new(&format!(
            "`{}` is not a valid table name",
            update.table_name
        )));
    }
    let is_column = Regex::new(r"\b[a-zA-Z_]\b").unwrap();
    let invalid_cols: Vec<_> = update
        .relevant_fields
        .as_ref()
        .unwrap_or(&HashMap::new())
        .iter()
        .filter_map(|(col, _)| {
            if is_column.is_match(col) {
                Some(col.to_string())
            } else {
                None
            }
        })
        .collect();
    if !invalid_cols.is_empty() {
        return Some(SyncError::new(&format!(
            "`{:?}` There is are invalid column(s) in relevant fields: {}",
            update.relevant_fields,
            invalid_cols.join(", ")
        )));
    }

    let invalid_cols: Vec<_> = update
        .pk_fields
        .iter()
        .filter_map(|(col, _)| {
            if is_column.is_match(col) {
                Some(col.to_string())
            } else {
                None
            }
        })
        .collect();
    if !invalid_cols.is_empty() {
        return Some(SyncError::new(&format!(
            "`{:?}` There is an invalid column in pk fields",
            update.pk_fields
        )));
    }

    let result = match update.sync_action {
        SyncAction::Update => {
            let mut arg_counter: usize = 0;
            let mut param_args: Vec<rusqlite::types::Value> = vec![];
            let mut set_values = vec![];
            for (col, val) in update
                .relevant_fields
                .as_ref()
                .unwrap_or(&HashMap::new())
                .iter()
            {
                match convert_to_sql_value(&val) {
                    Ok(v) => param_args.push(v),
                    Err(e) => return Some(e),
                }
                arg_counter += 1;
                set_values.push(format!("{} = ?{}", col, arg_counter))
            }
            let set_values = set_values.join(", ");
            let mut where_values = vec![];
            for (col, val) in update.pk_fields.iter() {
                match convert_to_sql_value(&val) {
                    Ok(v) => param_args.push(v),
                    Err(e) => return Some(e),
                }
                arg_counter += 1;
                where_values.push(format!("{} = ?{}", col, arg_counter))
            }

            let where_values = where_values.join(" AND ");
            let sql_string = format!(
                "UPDATE {} SET {} WHERE {};",
                update.table_name, set_values, where_values
            );
            println!("{}", sql_string);
            let maybe_statement = conn.prepare_cached(&sql_string);

            maybe_statement.map(|mut s| s.execute(params_from_iter(param_args)))
        }
        SyncAction::Delete => {
            let mut arg_counter: usize = 0;
            let mut param_args: Vec<rusqlite::types::Value> = vec![];
            let mut where_values = vec![];
            for (col, val) in update.pk_fields.iter() {
                match convert_to_sql_value(&val) {
                    Ok(v) => param_args.push(v),
                    Err(e) => return Some(e),
                }
                arg_counter += 1;
                where_values.push(format!("{} = ?{}", col, arg_counter))
            }
            let where_values = where_values.join(" AND ");

            let sql_string = format!("DELETE FROM {} WHERE {};", update.table_name, where_values);
            trace!("EXECUTING SQL:\n{} {:?}", sql_string, param_args);
            let maybe_statement = conn.prepare_cached(&sql_string);
            maybe_statement.map(|mut s| s.execute(params_from_iter(param_args)))
        }
        SyncAction::Insert => {
            let mut arg_counter: usize = 0;
            let mut param_args: Vec<rusqlite::types::Value> = vec![];
            let mut columns = vec![];
            let mut values = vec![];
            for (col, val) in update.pk_fields.iter() {
                match convert_to_sql_value(&val) {
                    Ok(v) => param_args.push(v),
                    Err(e) => return Some(e),
                }
                arg_counter += 1;
                columns.push(col.to_string());
                values.push(format!("?{}", arg_counter))
            }
            for (col, val) in update
                .relevant_fields
                .as_ref()
                .unwrap_or(&HashMap::new())
                .iter()
            {
                match convert_to_sql_value(&val) {
                    Ok(v) => param_args.push(v),
                    Err(e) => return Some(e),
                }
                arg_counter += 1;
                columns.push(col.to_string());
                values.push(format!("?{}", arg_counter))
            }
            let columns = columns.join(", ");
            let values = values.join(", ");

            let sql_string = format!(
                "INSERT INTO {} ({}) VALUES ({});",
                update.table_name, columns, values
            );
            println!("{}", sql_string);
            let maybe_statement = conn.prepare_cached(&sql_string);

            maybe_statement.map(|mut s| s.execute(params_from_iter(param_args)))
        }
    };
    match result {
        Ok(statement) => match statement {
            Ok(num) => {
                if num != 1 {
                    warn!("Query affected {} rows extected 1", num)
                }
                return None;

                // return Some(SyncError::new(&format!(
                //     "Query affected {} rows expected 1",
                //     num
                // )));
            }
            Err(err) => return Some(SyncError::new(&format!("Error executing query {:?}", err))),
        },
        Err(err) => Some(SyncError::new(&format!(
            "Error preparing statement {:?}",
            err,
        ))),
    }
}

fn convert_to_sql_value(v: &Value) -> Result<rusqlite::types::Value, SyncError> {
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
        _ => Err(SyncError::new(&format!("Unsupported type {:?}", v))),
    }
}

#[cfg(test)]
mod sync_tests {
    use super::*;
    use serde_json::from_str;
    use std::fs;

    #[test]
    fn full_sync() {
        env_logger::init();
        let mut conn = Connection::open_in_memory().unwrap();
        let up_migration = fs::read_to_string("migrations/001.up.sql").unwrap();
        conn.execute_batch(&up_migration).unwrap();

        let updates_text = fs::read_to_string("test-syncs/maristfall2024.json").unwrap();
        let response: SyncResponse = from_str(&updates_text).unwrap();
        let tx = conn.transaction().unwrap();
        for update in response.sync_data {
            if let Some(error) = execute_update(&tx, update) {
                panic!("{:?}", error);
            }
        }

        if env::var("SAVE_DB").is_ok() {
            conn.backup(rusqlite::backup::Backup::new(
                conn.clone(),
                rusqlite::Connection::open("test.db").unwrap(),
            ))
            .unwrap();
        }

        tx.commit().unwrap();
    }
}
