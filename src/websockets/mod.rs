pub mod ws_dispatcher;
pub mod ws_index;
pub mod ws_server;
pub mod ws_session;
pub mod ws_specific_filter;

/// Contains info for what does the Ws is listening to
#[derive(Clone)]
pub struct WsWatchFor {
    pub change_table: String,
    pub change_type: ChangeType,
    pub specific: Option<ws_specific_filter::SpecificFilter>,
}

/// Representation of SQL Change for CDC
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum ChangeType {
    INSERT = 0,
    UPDATE = 1,
    DELETE = 2,
    UNKNOWN = 3,
    ALL = 4,
}

impl PartialEq for ChangeType {
    fn eq(&self, other: &ChangeType) -> bool {
        *self as u8 == *other as u8
    }
}
