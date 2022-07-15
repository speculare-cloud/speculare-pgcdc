use super::ws_utils::{self, WsWatchFor};

use crate::{
    utils::specific_filter::{DataType, SpecificFilter},
    TABLES,
};

use sproot::apierrors::ApiError;

pub fn parse_ws_query(query: &str) -> Result<WsWatchFor, ApiError> {
    let mut parts = query.split(':');
    let mut change_flag = 0;

    // Apply bit operation to the change_flag based on the query type
    match parts.next() {
        Some(val) => val
            .split(',')
            .for_each(|ctype| ws_utils::apply_flag(&mut change_flag, ctype)),
        None => {
            return Err(ApiError::ExplicitError(String::from(
                "the change_type params is not present",
            )));
        }
    }

    // If change_flag is 0, we have an error because we don't listen to any known event types
    if change_flag == 0 {
        return Err(ApiError::ExplicitError(String::from(
            "the change_type params does not match requirements",
        )));
    }

    // Get the change_table and check if the table is valid
    let change_table = match parts.next() {
        Some(table) => {
            // Check if the table exists inside TABLES
            if !TABLES.read().unwrap().iter().any(|v| v == table) {
                return Err(ApiError::ExplicitError(String::from(
                    "the table asked for does not exists",
                )));
            }
            table.to_owned()
        }
        None => {
            return Err(ApiError::ExplicitError(String::from(
                "the change_table params is not present",
            )));
        }
    };

    // Construct the SpecificFilter from the request
    let specific: Option<SpecificFilter> = if let Some(filter) = parts.next() {
        let mut fparts = filter.splitn(3, '.');
        // let mut fparts = filter.splitn(2, ".eq.");
        match (fparts.next(), fparts.next(), fparts.next()) {
            (Some(col), Some(eq), Some(val)) => match eq {
                "eq" => Some(SpecificFilter {
                    column: serde_json::Value::String(col.to_owned()),
                    value: DataType::String(val.to_owned()),
                }),
                "in" => {
                    let items = val
                        .split(',')
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>();
                    Some(SpecificFilter {
                        column: serde_json::Value::String(col.to_owned()),
                        value: DataType::Array(items),
                    })
                }
                _ => None,
            },
            _ => None,
        }
    } else {
        None
    };

    // Construct what the client is listening to
    Ok(WsWatchFor {
        change_table,
        change_flag,
        specific,
    })
}
