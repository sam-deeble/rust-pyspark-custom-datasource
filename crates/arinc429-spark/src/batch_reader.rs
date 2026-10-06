use std::fs;

use arinc429_sdk::{Arinc429Word, decode_all};
use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;

use crate::convert::words_to_batch;

/// Decodes a whole file, then yields it as `pyarrow.RecordBatch`es of at most
/// `max_batch_rows` rows.
#[pyclass(module = "arinc429_datasource._arinc429")]
pub struct PyBatchReader {
    words: Vec<Arinc429Word>,
    file_path: String,
    max_batch_rows: usize,
    offset: usize,
}

#[pymethods]
impl PyBatchReader {
    #[new]
    fn new(py: Python<'_>, path: String, max_batch_rows: usize) -> PyResult<Self> {
        if max_batch_rows == 0 {
            return Err(PyValueError::new_err("max_batch_rows must be at least 1"));
        }
        // Reading and decoding touch no Python objects, so release the GIL.
        let words = py.detach(|| {
            let bytes = fs::read(&path).map_err(|e| PyIOError::new_err(format!("{path}: {e}")))?;
            decode_all(&bytes).map_err(|e| PyValueError::new_err(format!("{path}: {e}")))
        })?;
        Ok(Self {
            words,
            file_path: path,
            max_batch_rows,
            offset: 0,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(mut slf: PyRefMut<'py, Self>) -> PyResult<Option<Bound<'py, PyAny>>> {
        if slf.offset >= slf.words.len() {
            return Ok(None);
        }
        let py = slf.py();
        let end = (slf.offset + slf.max_batch_rows).min(slf.words.len());
        let chunk = slf.words[slf.offset..end].to_vec();
        let file_path = slf.file_path.clone();
        slf.offset = end;

        let batch = py
            .detach(|| words_to_batch(&chunk, &file_path))
            .map_err(|e| PyValueError::new_err(format!("failed to build Arrow batch: {e}")))?;

        // Passes the buffers to pyarrow through the C Data Interface without copying.
        batch.to_pyarrow(py).map(Some)
    }
}
