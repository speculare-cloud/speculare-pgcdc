// TODO - Handle more Type
/// List of supported data type
#[derive(Clone)]
pub enum DataType {
    Number(i64),
    String(String),
}

// TODO - Handle more OP
/// List of supported type
#[derive(Clone)]
pub enum Op {
    Eq,
    Lower,
    Higher,
}

/// Contains the specific filter applied to the Ws
#[derive(Clone)]
pub struct SpecificFilter {
    pub column: String,
    pub value: DataType,
    pub op: Op,
}

impl SpecificFilter {
    /// Determine if the filter match the message passed as parameter
    pub fn match_specific_filter(&self, message: &serde_json::Value) -> bool {
        // Get the SpecificFilter object, firstly by a ref and then unwrap
        let filter = self;
        // Define easy to use variable
        let column = &filter.column;
        let value = &filter.value;
        // TODO - Implement OP (operation)
        // - Default to Eq
        //let op = &specific.op;

        if message["columnnames"].is_array() {
            // Determine if the column is present in this change
            let columns = message["columnnames"].as_array().unwrap();
            // Check if the cloumns we asked for exist in this data change
            let value_index = match (*columns)
                .iter()
                // Iterate and get the position back if it match the condition
                .position(|r| r == &serde_json::Value::String(column.to_owned()))
            {
                Some(index) => index,
                None => return false,
            };
            // This part is to optimize and rewrite, but basically it just
            // match, filter and sort around the criteria of the column value.
            if message["columnvalues"].is_array() {
                let values = message["columnvalues"].as_array().unwrap();
                let targeted_value = &values[value_index];
                // If the value we asked for is a String or a Number
                match value {
                    DataType::String(val) => {
                        // Check if is_string, and if so, convert it then check
                        if targeted_value.is_string() && targeted_value.as_str().unwrap() == val {
                            return true;
                        }
                    }
                    DataType::Number(val) => {
                        // Check if is_number and convert to i64 (might place a TODO for latter handling more type)
                        // For number we currently only support i64
                        if targeted_value.is_number() && targeted_value.as_i64().unwrap() == *val {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }
}
