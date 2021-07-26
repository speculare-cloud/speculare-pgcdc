use crate::utils::specific_filter::SpecificFilter;

pub mod cdc_transmitter;
pub mod client;
pub mod server;

pub const INSERT: u8 = 1 << 1;
pub const UPDATE: u8 = 1 << 2;
pub const DELETE: u8 = 1 << 3;

/// Contains info for what does the Ws is listening to
#[derive(Clone)]
pub struct WsWatchFor {
    pub change_table: String,
    pub change_flag: u8,
    pub specific: Option<SpecificFilter>,
}

pub fn apply_flag(flag: &mut u8, ctype: &str) {
    match ctype {
        "insert" => {
            *flag |= INSERT;
        }
        "update" => {
            *flag |= UPDATE;
        }
        "delete" => {
            *flag |= DELETE;
        }
        "*" => {
            *flag |= INSERT;
            *flag |= UPDATE;
            *flag |= DELETE;
        }
        _ => {
            error!("parts[0] (change_type) don't match any of the available types.")
        }
    }
}
