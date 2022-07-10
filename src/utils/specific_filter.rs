/// List of supported data type
#[derive(Debug, Clone)]
pub enum DataType {
    String(String),
    Array(Vec<String>),
}

/// Contains the specific filter applied to the Ws
#[derive(Debug, Clone)]
pub struct SpecificFilter {
    pub column: serde_json::Value,
    pub value: DataType,
}

impl SpecificFilter {
    /// Determine if the filter match the message passed as parameter
    pub fn match_filter(&self, message: &serde_json::Value) -> bool {
        // Determine if the column is present in this change
        let columns = match message["columnnames"].as_array() {
            Some(val) => val,
            None => {
                error!("Specific: the message does not contains `columnnames`");
                return false;
            }
        };
        // Check if the cloumns we asked for exist in this data change
        let value_index = columns.iter().position(|c| c == &self.column);
        if value_index.is_none() {
            return false;
        }
        // Basically it just match, filter and sort around the criteria of the column value.
        if let Some(col_vals) = message["columnvalues"].as_array() {
            let targeted_value = &col_vals[value_index.unwrap()];
            // If the value we asked for is a String or a Number
            return match &self.value {
                DataType::String(val) => {
                    // Check if is_string, and if so, convert it then check
                    match targeted_value.as_str() {
                        Some(t) => t == val,
                        None => false,
                    }
                }
                DataType::Array(val) => match targeted_value.as_str() {
                    Some(t) => val.iter().any(|x| x == t),
                    None => false,
                },
            };
        }
        false
    }
}
