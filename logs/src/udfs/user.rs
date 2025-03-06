use datafusion::arrow::array::{MapBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::cast::as_string_array;
use datafusion::common::plan_err;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::sql::sqlparser::parser::ParserError::ParserError;
use std::any::Any;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug)]
pub struct Json {
    signature: Signature,
}

impl Json {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Json {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        if arg_types.len() != 1 {
            return plan_err!("json only accepts 1 argument");
        }

        if !matches!(arg_types.first(), Some(&DataType::Utf8)) {
            return plan_err!("json only accepts String arguments");
        }

        let field_ref = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ])),
            false,
        ));

        Ok(DataType::Map(field_ref, false))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let strings = as_string_array(&args[0])?;
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

        for s in strings.into_iter().flatten() {
            let json = serde_json::Value::from_str(s).map_err(|e| {
                DataFusionError::SQL(
                    ParserError(format!("failed to parse string as JSON: {}", e)),
                    None,
                )
            })?;

            if let Some(object) = json.as_object() {
                for (k, v) in object {
                    builder.keys().append_value(k);
                    let value_str = match v {
                        serde_json::Value::Null => "".to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Array(a) => format!("{:?}", a),
                        serde_json::Value::Object(o) => format!("{:?}", o),
                    };
                    builder.values().append_value(&value_str);
                }
                builder.append(true).unwrap();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
