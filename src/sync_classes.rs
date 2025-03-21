use log::{trace, warn};
use regex::Regex;
use reqwest::blocking::get;
use rusqlite::{Connection, Result, Transaction, params_from_iter};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::env;

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

const VALID_TABLES: [&str; 6] = [
    "meeting_times",
    "sections",
    "professors",
    "courses",
    "previous_section_collections",
    "term_collections",
];
const SYNC_ALL_ROUTE: &str = "sync/all";

pub fn sync_all(conn: &mut Connection, /* last_update: time::Instant */) -> Result<i32, SyncError> {
    let url = env::var("CLASSY_API_HOST")
        .map_err(|_| SyncError::new("classy api env var not set"))?
        + SYNC_ALL_ROUTE;
    // let mut query_params = HashMap::new();
    // let as_date: DateTime<UTC> = last_update.into();
    // query_params.insert("lastSyncTimeStamp");
    let result = get(url).map_err(|e| SyncError::new(&format!("request error {:?}", e)))?;
    let updates: Vec<ClassDataUpdate> = result
        .json()
        .map_err(|e| SyncError::new(&format!("response to json error {:?}", e)))?;
    let tx = conn
        .transaction()
        .map_err(|e| SyncError::new(&format!("transaction failed {:?}", e)))?;
    for update in updates {
        if let Some(err) = execute_update(&tx, update) {
            return Err(err);
        }
    }
    tx.commit()
        .map_err(|e| SyncError::new(&format!("sync failed {:?}", e)))?;
    return Ok(1);
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
        let updates: Vec<ClassDataUpdate> = from_str(&updates_text).unwrap();
        let tx = conn.transaction().unwrap();
        for update in updates {
            if let Some(error) = execute_update(&tx, update) {
                panic!("{:?}", error);
            }
        }
        tx.commit().unwrap();
    }
}
