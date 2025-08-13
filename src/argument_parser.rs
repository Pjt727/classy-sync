use std::collections::{HashMap, HashSet};

///
/// Sets the schools/ terms relevant for the action
///
/// Comma separated pairs of schoolid,termcollectionid deliminated by semicolons
///   or just the school itself
/// ex: "marist;temple,202422"
///
///

pub enum SyncResources {
    Everything,
    Select(SelectSyncOptions),
}

pub enum CollectionType {
    AllSchoolData,
    SelectTermData(HashSet<String>),
}

pub struct SelectSyncOptions {
    pub school_to_collection: HashMap<String, CollectionType>,
}

impl SelectSyncOptions {
    pub fn from_input(input: String) -> SelectSyncOptions {
        let schools_or_terms: Vec<String> = input.split(";").map(|s| s.to_string()).collect();
        let mut school_to_collection: HashMap<String, CollectionType> = HashMap::new();

        for schoool_or_term in schools_or_terms.into_iter() {
            let school_and_maybe_term: Vec<&str> =
                schoool_or_term.split(",").map(|s| s.trim()).collect();
            assert_eq!(school_and_maybe_term.len(), 1, "No school given?");
            let school = school_and_maybe_term[0].to_string();

            if schoool_or_term.len() == 1 {
                // only get the terms of the school
                todo!();
            }
            if school_and_maybe_term[1] == "all" {
                school_to_collection.insert(school.to_string(), CollectionType::AllSchoolData);
                continue;
            }

            school_to_collection.insert(
                school.to_string(),
                CollectionType::SelectTermData(
                    school_and_maybe_term[1..]
                        .iter()
                        .map(|t| t.to_string())
                        .collect(),
                ),
            );
        }
        return SelectSyncOptions {
            school_to_collection: HashMap::new(),
        };
    }
}
