"""NumPy and PyArrow version of the ``arinc429`` source.

Decodes the whole file with numpy and yields Arrow record batches, the same
output the Rust reader produces. Comparing it with ``pure_python`` shows the
gain from batching; comparing it with the Rust reader shows the gain from
compiled decoding.
"""

from collections.abc import Iterator

import numpy as np
import pyarrow as pa
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.pandas.types import to_arrow_schema
from pyspark.sql.types import StructType

from arinc429_datasource._files import FilePartition, check_schema, input_files, max_batch_rows
from arinc429_datasource.schema import SCHEMA

# As in crates/arinc429-sdk/src/params.rs
ALTITUDE_LABEL, ALTITUDE_RESOLUTION, ALTITUDE_UNIT = 0o203, 1.0, "ft"
AIRSPEED_LABEL, AIRSPEED_RESOLUTION, AIRSPEED_UNIT = 0o206, 0.125, "kt"

SIGN_BIT = 0x40000
FIELD_MASK = 0x7FFFF

_REVERSE_BYTE = np.array([int(f"{i:08b}"[::-1], 2) for i in range(256)], dtype=np.uint8)
_LABEL_OCTAL_STRINGS = np.array([f"{i:03o}" for i in range(256)], dtype=object)
_POPCOUNT = np.array([i.bit_count() for i in range(256)], dtype=np.uint8)
_SSM_NAMES = np.array(
    ["FailureWarning", "NoComputedData", "FunctionalTest", "NormalOperation"], dtype=object
)

ARROW_SCHEMA = to_arrow_schema(SCHEMA)


def decode_arrays(raw_bytes: bytes) -> dict:
    """Decode a buffer of 32-bit little-endian words into one numpy array per column."""
    raw = np.frombuffer(raw_bytes, dtype="<u4")

    byte0 = (raw & 0xFF).astype(np.uint8)
    label_octal_value = _REVERSE_BYTE[byte0]  # labels are sent bit-reversed
    label_octal = _LABEL_OCTAL_STRINGS[label_octal_value]

    sdi = ((raw >> 8) & 0b11).astype(np.int32)
    data_field = ((raw >> 10) & FIELD_MASK).astype(np.int64)
    ssm = _SSM_NAMES[(raw >> 29) & 0b11]

    byte1 = ((raw >> 8) & 0xFF).astype(np.uint8)
    byte2 = ((raw >> 16) & 0xFF).astype(np.uint8)
    byte3 = ((raw >> 24) & 0xFF).astype(np.uint8)
    bit_count = (
        _POPCOUNT[byte0].astype(np.uint16) + _POPCOUNT[byte1] + _POPCOUNT[byte2] + _POPCOUNT[byte3]
    )
    parity_ok = (bit_count % 2 == 1).astype(bool)

    signed = np.where(data_field & SIGN_BIT, data_field - (FIELD_MASK + 1), data_field)

    matched_altitude = label_octal_value == ALTITUDE_LABEL
    matched_airspeed = label_octal_value == AIRSPEED_LABEL
    matched = matched_altitude | matched_airspeed

    parameter = np.where(
        matched_altitude, "altitude", np.where(matched_airspeed, "airspeed", "")
    ).astype(object)
    unit = np.where(
        matched_altitude, ALTITUDE_UNIT, np.where(matched_airspeed, AIRSPEED_UNIT, "")
    ).astype(object)
    value = np.where(
        matched_altitude,
        signed.astype(np.float64) * ALTITUDE_RESOLUTION,
        np.where(matched_airspeed, signed.astype(np.float64) * AIRSPEED_RESOLUTION, 0.0),
    )

    return {
        "label_octal": label_octal,
        "sdi": sdi,
        "data_field": data_field,
        "ssm": ssm,
        "parity_ok": parity_ok,
        "parameter": parameter,
        "value": value,
        "unit": unit,
        "null_mask": ~matched,
    }


def decode_file_batches(path: str, max_batch_rows: int) -> Iterator[pa.RecordBatch]:
    with open(path, "rb") as f:
        raw_bytes = f.read()
    cols = decode_arrays(raw_bytes)
    n = len(cols["label_octal"])
    null_mask = cols["null_mask"]

    for start in range(0, n, max_batch_rows):
        end = min(start + max_batch_rows, n)
        chunk_mask = null_mask[start:end]
        file_paths = np.full(end - start, path, dtype=object)
        yield pa.RecordBatch.from_arrays(
            [
                pa.array(cols["label_octal"][start:end], type=pa.string()),
                pa.array(cols["sdi"][start:end], type=pa.int32()),
                pa.array(cols["data_field"][start:end], type=pa.int64()),
                pa.array(cols["ssm"][start:end], type=pa.string()),
                pa.array(cols["parity_ok"][start:end], type=pa.bool_()),
                pa.array(cols["parameter"][start:end], type=pa.string(), mask=chunk_mask),
                pa.array(cols["value"][start:end], type=pa.float64(), mask=chunk_mask),
                pa.array(cols["unit"][start:end], type=pa.string(), mask=chunk_mask),
                pa.array(file_paths, type=pa.string()),
            ],
            schema=ARROW_SCHEMA,
        )


class Arinc429PyArrowDataSource(DataSource):
    """Benchmark baseline: decodes with NumPy and yields Arrow record batches."""

    @classmethod
    def name(cls) -> str:
        return "arinc429_pyarrow"

    def schema(self) -> StructType:
        return SCHEMA

    def reader(self, schema: StructType) -> "Arinc429PyArrowReader":
        check_schema(schema, self.name())
        return Arinc429PyArrowReader(self.options, self.name())


class Arinc429PyArrowReader(DataSourceReader):
    def __init__(self, options: dict[str, str], source_name: str):
        self.files = input_files(options, source_name)
        self.max_batch_rows = max_batch_rows(options, source_name)

    def partitions(self) -> list[FilePartition]:
        return [FilePartition(path) for path in self.files]

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        assert isinstance(partition, FilePartition)
        return decode_file_batches(partition.path, self.max_batch_rows)
