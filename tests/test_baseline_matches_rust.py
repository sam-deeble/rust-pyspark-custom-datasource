"""The benchmark is only fair if all three decoders produce the same rows."""

import pyarrow as pa
import pytest

from arinc429_datasource._arinc429 import PyBatchReader
from arinc429_datasource.examples.baselines.pure_python import decode_file
from arinc429_datasource.examples.baselines.pyarrow import decode_file_batches

pytestmark = pytest.mark.rust


def rust_table(path) -> pa.Table:
    return pa.Table.from_batches(list(PyBatchReader(str(path), 10_000)))


def test_pure_python_matches_rust(demo_file):
    expected = [tuple(row.values()) for row in rust_table(demo_file).to_pylist()]
    assert list(decode_file(str(demo_file))) == expected


def test_pyarrow_matches_rust(demo_file):
    table = pa.Table.from_batches(list(decode_file_batches(str(demo_file), 10_000)))
    assert table.equals(rust_table(demo_file))
