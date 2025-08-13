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
    #[strum(serialize = "meeting_times")]
    MeetingTimes,
    #[strum(serialize = "sections")]
    Sections,
    #[strum(serialize = "professors")]
    Professors,
    #[strum(serialize = "courses")]
    Courses,
    #[strum(serialize = "term_collections")]
    TermCollections,
    #[strum(serialize = "schools")]
    Schools,
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
    /// This funciton should be used to verify columns in case of sql injection
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

#[derive(Debug)]
pub enum SyncOptions {
    All(AllSync),
    Select(SelectSync),
}

// TERM SYNCS - for getting information about specfic terms from classy
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SchoolEntry {
    TermToSequence(HashMap<String, u64>),
    Sequence(u64),
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SelectSync {
    exclude: HashMap<String, HashMap<String, u64>>,
    max_records_per_request: Option<u16>,
    schools: HashMap<String, SchoolEntry>,
}

impl SelectSync {
    pub fn new() -> SelectSync {
        SelectSync {
            ..Default::default()
        }
    }

    pub fn get_exclusions(&self) -> &HashMap<String, HashMap<String, u64>> {
        return &self.exclude;
    }

    pub fn get_max_records(&self) -> Option<u16> {
        return self.max_records_per_request;
    }

    pub fn get_schools(&self) -> &HashMap<String, SchoolEntry> {
        return &self.schools;
    }

    // all of these setter methods are pretty picky so maybe just make they less so

    pub fn add_school_sync(&mut self, school_id: String, synced_at: u64) -> Result<(), Error> {
        if self.schools.contains_key(&school_id) {
            return Err(SyncError::new(format!(
                "school_id `{}` is already set",
                school_id
            )));
        }
        self.schools
            .insert(school_id, SchoolEntry::Sequence(synced_at));
        Ok(())
    }

    pub fn add_term_sync(
        &mut self,
        school_id: String,
        term_collection_id: String,
        synced_at: u64,
    ) -> Result<(), Error> {
        let school_entry = self
            .schools
            .entry(school_id)
            .or_insert(SchoolEntry::TermToSequence(HashMap::new()));
        match school_entry {
            SchoolEntry::TermToSequence(terms) => {
                if let Some(old_sync) = terms.insert(term_collection_id, synced_at) {
                    return Err(SyncError::new(format!(
                        "This term already was set to sync with {}",
                        old_sync
                    )));
                }
            }
            SchoolEntry::Sequence(sequence) => {
                return Err(SyncError::new(format!(
                    "school id already being synced with {}",
                    sequence
                )));
            }
        };
        Ok(())
    }

    pub fn add_exclusion(
        &mut self,
        school_id: String,
        term_collection_id: String,
        synced_at: u64,
    ) -> Result<(), Error> {
        let terms = self.exclude.entry(school_id).or_insert_with(HashMap::new);
        if let Some(old_sync) = terms.insert(term_collection_id, synced_at) {
            return Err(SyncError::new(format!(
                "This term already was set as an exclusion with {}",
                old_sync
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TermSyncResult {
    pub schools: HashMap<String, SchoolEntry>,
    pub data: Vec<ClassDataSync>,
}

// ALL SYNCS - for getting all information from class

#[derive(Debug, Serialize, Deserialize)]
pub struct AllSync {
    pub last_sync: u64,
    pub max_records_count: Option<u16>,
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
