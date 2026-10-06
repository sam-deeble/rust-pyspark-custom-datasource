//! Python bindings for `arinc429-sdk`, imported as `arinc429_datasource._arinc429`.
//!
//! Decoded words are returned to Python as `pyarrow.RecordBatch`es through the
//! Arrow C Data Interface, so no per-row Python objects are created.

mod batch_reader;
mod convert;
mod schema;

use arinc429_sdk::synth;
use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

/// Writes `count` synthetic words to `path`. The same `seed` gives the same file.
#[pyfunction]
fn generate_demo_file(path: String, count: usize, seed: u64) -> PyResult<()> {
    let bytes = synth::to_bytes(&synth::generate_words(count, seed));
    std::fs::write(&path, bytes).map_err(|e| PyIOError::new_err(format!("{path}: {e}")))
}

/// Returns the schema of the batches `PyBatchReader` yields, as a `pyarrow.Schema`.
#[pyfunction]
fn arrow_schema(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    schema::arrow_schema().as_ref().to_pyarrow(py)
}

#[pymodule]
fn _arinc429(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<batch_reader::PyBatchReader>()?;
    m.add_function(wrap_pyfunction!(generate_demo_file, m)?)?;
    m.add_function(wrap_pyfunction!(arrow_schema, m)?)?;
    Ok(())
}
