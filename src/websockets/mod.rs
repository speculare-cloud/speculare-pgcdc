use crate::utils::specific_filter::SpecificFilter;

pub mod cdc_transmitter;
pub mod client;
pub mod server;

/// Contains info for what does the Ws is listening to
#[derive(Clone)]
pub struct WsWatchFor {
    pub change_table: String,
    pub change_type: ChangeType,
    pub specific: Option<SpecificFilter>,
}

/// Representation of SQL Change for CDC
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum ChangeType {
    Insert = 0,
    Update = 1,
    Delete = 2,
    Unknown = 3,
    AllTypes = 4,
}

impl PartialEq for ChangeType {
    fn eq(&self, other: &ChangeType) -> bool {
        *self as u8 == *other as u8
    }
}

/// Convert str typed SQL change to ChangeType
pub fn str_to_change_type(change_type: &str) -> ChangeType {
    match change_type {
        "insert" => ChangeType::Insert,
        "update" => ChangeType::Update,
        "delete" => ChangeType::Delete,
        "*" => ChangeType::AllTypes,
        _ => ChangeType::Unknown,
    }
}
