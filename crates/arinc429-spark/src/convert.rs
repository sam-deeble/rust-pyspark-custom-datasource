use std::sync::Arc;

use arinc429_sdk::{Arinc429Word, Ssm, params};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use crate::schema::arrow_schema;

fn ssm_name(ssm: Ssm) -> &'static str {
    match ssm {
        Ssm::FailureWarning => "FailureWarning",
        Ssm::NoComputedData => "NoComputedData",
        Ssm::FunctionalTest => "FunctionalTest",
        Ssm::NormalOperation => "NormalOperation",
    }
}

/// Builds a record batch matching [`arrow_schema`], one row per word.
pub fn words_to_batch(words: &[Arinc429Word], file_path: &str) -> Result<RecordBatch, ArrowError> {
    let n = words.len();
    let mut label_octal = Vec::with_capacity(n);
    let mut sdi = Vec::with_capacity(n);
    let mut data_field = Vec::with_capacity(n);
    let mut ssm = Vec::with_capacity(n);
    let mut parity_ok = Vec::with_capacity(n);
    let mut parameter: Vec<Option<String>> = Vec::with_capacity(n);
    let mut value: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut unit: Vec<Option<String>> = Vec::with_capacity(n);

    for w in words {
        label_octal.push(format!("{:03o}", w.label_octal()));
        sdi.push(i32::from(w.sdi()));
        data_field.push(i64::from(w.data_field()));
        ssm.push(ssm_name(w.ssm()));
        parity_ok.push(w.has_valid_parity());

        match params::lookup(w.label_octal()) {
            Some(p) => {
                parameter.push(Some(p.name.to_string()));
                value.push(Some(p.decode(w)));
                unit.push(Some(p.unit.to_string()));
            }
            None => {
                parameter.push(None);
                value.push(None);
                unit.push(None);
            }
        }
    }

    let file_paths = vec![file_path; n];

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(label_octal)),
        Arc::new(Int32Array::from(sdi)),
        Arc::new(Int64Array::from(data_field)),
        Arc::new(StringArray::from(ssm)),
        Arc::new(BooleanArray::from(parity_ok)),
        Arc::new(StringArray::from(parameter)),
        Arc::new(Float64Array::from(value)),
        Arc::new(StringArray::from(unit)),
        Arc::new(StringArray::from(file_paths)),
    ];

    RecordBatch::try_new(arrow_schema(), arrays)
}
