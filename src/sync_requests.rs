use super::errors::{Error, SyncError};
use regex::Regex;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use strum_macros::Display;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum SyncAction {
    Update,
    Delete,
    Insert,
}

#[derive(Serialize, Display, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableName {
    #[strum(serialize = "sections")]
    Sections,
    #[strum(serialize = "professors")]
    Professors,
    #[strum(serialize = "courses")]
    Courses,
    #[strum(serialize = "term_collections")]
    TermCollections,
}

#[derive(Serialize, Display, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommonTable {
    #[strum(serialize = "professors")]
    Professors,
    #[strum(serialize = "courses")]
    Courses,
    #[strum(serialize = "term_collections")]
    TermCollections,
}

#[derive(Serialize, Debug, Deserialize)]
pub struct ClassDataSync {
    pub table_name: TableName,
    pub sync_action: SyncAction,
    /// column names are not sanitized by default so it is recommended to use the `verify_columns` method
    /// when using column names in sql expressions
    pub pk_fields: HashMap<String, Value>,
    /// column names are not sanitized by default so it is recommended to use the `verify_columns` method
    /// when using column names in sql expressions
    pub relevant_fields: Option<HashMap<String, Value>>,
}

impl ClassDataSync {
    pub fn verify_columns(&self) -> Result<(), Error> {
        let is_column = Regex::new(r"\b[a-zA-Z_]\b").unwrap();
        let invalid_cols: Vec<_> = self
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
            return Err(SyncError::new(&format!(
                "`{:?}` There is are invalid column(s) in relevant fields: {}",
                self.relevant_fields,
                invalid_cols.join(", ")
            )));
        }

        let invalid_cols: Vec<_> = self
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
            return Err(SyncError::new(&format!(
                "`{:?}` There is an invalid column in pk fields: {}",
                self.pk_fields,
                invalid_cols.join(", ")
            )));
        }

        Ok(())
    }
}

// SELECT SYNCS - for getting bits of info from classy
#[derive(Debug, Serialize, Deserialize)]
pub struct CommonTableSyncEntry {
    pub table_name: CommonTable,
    pub last_sync: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectTermEntry {
    term_collection_id: String,
    last_sync: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectSchoolEntry {
    pub common_tables: Vec<CommonTableSyncEntry>,
    pub select_terms: Vec<SelectTermEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectSync {
    /// mapping from school_id to last sync entries
    pub select_schools: HashMap<String, SelectSchoolEntry>,
}

#[derive(Serialize, Deserialize)]
pub struct SelectSyncResult {
    pub select_schools: HashMap<String, SelectSchoolEntry>,
    pub data: Vec<ClassDataSync>,
}

// ALL SYNCS - for getting all information from class

#[derive(Debug, Serialize, Deserialize)]
pub struct AllSync {
    pub last_sync: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AllSyncResult {
    pub new_latest_sync: u64,
    pub data: Vec<ClassDataSync>,
}

fn sync_all(
    client: &Client,
    sync_all_route: &String,
    sync_options: AllSync,
) -> Result<AllSyncResult, Error> {
    let sync_response = client.get(sync_all_route).query(&sync_options).send()?;

    if sync_response.status().is_success() {
        let my_data: AllSyncResult = sync_response.json()?;
        Ok(my_data)
    } else {
        Err(SyncError::new(sync_response.status()))
    }
}
