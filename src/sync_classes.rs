use regex::Regex;
use reqwest;
use rusqlite::{CachedStatement, Connection, Result, params_from_iter};
use serde::Deserialize;
use serde_json::{Value, from_str};
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
struct ClassDataUpdate {
    table_name: String,
    sync_action: SyncAction,
    #[serde(rename = "updated_pk_fields")]
    pk_fields: HashMap<String, Value>,
    relevant_fields: HashMap<String, Value>,
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
struct SyncError {
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

fn execute_update(conn: &Connection, update: &ClassDataUpdate) -> Option<SyncError> {
    // to prvent possible sql injection attacks if the sync api was
    //   ever compromised
    if !VALID_TABLES.contains(&update.table_name.as_str()) {
        return Some(SyncError::new(&format!(
            "`{}` is not a valid table name",
            update.table_name
        )));
    }
    let is_column = Regex::new(r"\b[a-zA-Z_]\b").unwrap();
    if update
        .relevant_fields
        .iter()
        .any(|(col, _)| !is_column.is_match(col))
    {
        return Some(SyncError::new(&format!(
            "`{:?}` There is an invalid column in relevant fields",
            update.relevant_fields
        )));
    }

    if update
        .pk_fields
        .iter()
        .any(|(col, _)| !is_column.is_match(col))
    {
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
            for (col, val) in update.relevant_fields.iter() {
                match convert_to_sql_value(&val) {
                    Ok(v) => param_args.push(v),
                    Err(e) => return Some(e),
                }
                arg_counter += 1;
                set_values.push(format!("{} = {}?", col, arg_counter))
            }
            let set_values = set_values.join(", ");
            let mut where_values = vec![];
            for (col, val) in update.pk_fields.iter() {
                match convert_to_sql_value(&val) {
                    Ok(v) => param_args.push(v),
                    Err(e) => return Some(e),
                }
                arg_counter += 1;
                where_values.push(format!("{} = {}?", col, arg_counter))
            }

            let where_values = where_values.join(" AND ");
            let sql_string = format!(
                "UPDATE {} SET {} WHERE {};",
                update.table_name, set_values, where_values
            );
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
                where_values.push(format!("{} = {}?", col, arg_counter))
            }
            let where_values = where_values.join(" AND ");

            let sql_string = format!("DELETE FROM {} WHERE {};", update.table_name, where_values);
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
                values.push(format!("{}?", arg_counter))
            }
            for (col, val) in update.relevant_fields.iter() {
                match convert_to_sql_value(&val) {
                    Ok(v) => param_args.push(v),
                    Err(e) => return Some(e),
                }
                arg_counter += 1;
                columns.push(col.to_string());
                values.push(format!("{}?", arg_counter))
            }
            let columns = columns.join(", ");
            let values = values.join(", ");

            let sql_string = format!(
                "INSERT INTO {} ({}) VALUES ({});",
                update.table_name, columns, values
            );
            let maybe_statement = conn.prepare_cached(&sql_string);

            maybe_statement.map(|mut s| s.execute(params_from_iter(param_args)))
        }
    };
    match result {
        Ok(statement) => match statement {
            Ok(num) => {
                if num == 1 {
                    return None;
                }
                return Some(SyncError::new(&format!(
                    "Query affect {} rows expected 1",
                    num
                )));
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
