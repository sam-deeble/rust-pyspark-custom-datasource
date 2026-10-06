use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

/// Schema of the batches returned to Python.
///
/// Must match `SCHEMA` in `python/arinc429_datasource/schema.py`.
pub fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("label_octal", DataType::Utf8, false),
        Field::new("sdi", DataType::Int32, false),
        Field::new("data_field", DataType::Int64, false),
        Field::new("ssm", DataType::Utf8, false),
        Field::new("parity_ok", DataType::Boolean, false),
        Field::new("parameter", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("unit", DataType::Utf8, true),
        Field::new("file_path", DataType::Utf8, false),
    ]))
}
