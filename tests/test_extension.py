"""Tests for the compiled extension, without Spark."""

import pyarrow as pa
import pytest
from pyspark.sql.pandas.types import to_arrow_schema

from arinc429_datasource import generate_demo_file
from arinc429_datasource._arinc429 import PyBatchReader
from arinc429_datasource.schema import SCHEMA

pytestmark = pytest.mark.rust


def read_table(path, max_batch_rows=10_000) -> pa.Table:
    return pa.Table.from_batches(list(PyBatchReader(str(path), max_batch_rows)))


def test_generate_demo_file_writes_four_bytes_per_word(tmp_path):
    path = tmp_path / "demo.arinc429"
    generate_demo_file(str(path), 1234, seed=1)
    assert path.stat().st_size == 1234 * 4


def test_reader_decodes_every_word(demo_file, demo_words):
    assert read_table(demo_file).num_rows == demo_words


def test_batches_match_the_spark_schema(demo_file):
    # Spark only checks column names, so a type mismatch would surface as a
    # confusing JVM error at read time.
    batch = next(iter(PyBatchReader(str(demo_file), 100)))
    assert batch.schema.equals(to_arrow_schema(SCHEMA))


def test_reader_respects_max_batch_rows(demo_file):
    batches = list(PyBatchReader(str(demo_file), 500))
    assert [b.num_rows for b in batches] == [500] * 6


def test_rejects_zero_max_batch_rows(demo_file):
    with pytest.raises(ValueError, match="at least 1"):
        PyBatchReader(str(demo_file), 0)


def test_rejects_truncated_file(tmp_path):
    path = tmp_path / "bad.arinc429"
    path.write_bytes(b"\x00" * 5)
    with pytest.raises(ValueError, match="multiple of 4"):
        PyBatchReader(str(path), 100)


def test_known_parameters_have_value_and_unit(demo_file):
    rows = read_table(demo_file).to_pylist()
    assert {r["parameter"] for r in rows} == {"altitude", "airspeed", None}
    for r in rows:
        has_parameter = r["parameter"] is not None
        assert (r["value"] is not None) == has_parameter
        assert (r["unit"] is not None) == has_parameter


def test_synthetic_words_have_valid_parity(demo_file):
    assert all(read_table(demo_file).column("parity_ok").to_pylist())
