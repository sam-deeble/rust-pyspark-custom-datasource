"""Pure-Python version of the ``arinc429`` source.

Decodes one word at a time and yields one tuple per row. This is the slowest
of the three implementations compared in the benchmark.
"""

import struct
from collections.abc import Iterator

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from arinc429_datasource._files import FilePartition, check_schema, input_files
from arinc429_datasource.schema import SCHEMA

# label -> (parameter, resolution, unit), as in crates/arinc429-sdk/src/params.rs
KNOWN_PARAMETERS = {
    0o203: ("altitude", 1.0, "ft"),
    0o206: ("airspeed", 0.125, "kt"),
}

SSM_NAMES = ("FailureWarning", "NoComputedData", "FunctionalTest", "NormalOperation")


def decode_word(raw: int) -> tuple:
    """Decode one 32-bit word into a row of ``SCHEMA``, without ``file_path``."""
    label_bits = raw & 0xFF
    label_octal_value = int(f"{label_bits:08b}"[::-1], 2)  # labels are sent bit-reversed
    sdi = (raw >> 8) & 0b11
    data_field = (raw >> 10) & 0x7FFFF
    ssm = SSM_NAMES[(raw >> 29) & 0b11]
    parity_ok = raw.bit_count() % 2 == 1

    parameter = value = unit = None
    known = KNOWN_PARAMETERS.get(label_octal_value)
    if known is not None:
        name, resolution, unit_name = known
        signed = data_field - 0x80000 if data_field & 0x40000 else data_field
        parameter, value, unit = name, signed * resolution, unit_name

    return (f"{label_octal_value:03o}", sdi, data_field, ssm, parity_ok, parameter, value, unit)


def decode_file(path: str) -> Iterator[tuple]:
    with open(path, "rb") as f:
        data = f.read()
    for offset in range(0, len(data), 4):
        (raw,) = struct.unpack_from("<I", data, offset)
        yield decode_word(raw) + (path,)


class Arinc429PythonDataSource(DataSource):
    """Benchmark baseline: decodes one word at a time and yields tuples."""

    @classmethod
    def name(cls) -> str:
        return "arinc429_python"

    def schema(self) -> StructType:
        return SCHEMA

    def reader(self, schema: StructType) -> "Arinc429PythonReader":
        check_schema(schema, self.name())
        return Arinc429PythonReader(self.options, self.name())


class Arinc429PythonReader(DataSourceReader):
    def __init__(self, options: dict[str, str], source_name: str):
        self.files = input_files(options, source_name)

    def partitions(self) -> list[FilePartition]:
        return [FilePartition(path) for path in self.files]

    def read(self, partition: InputPartition) -> Iterator[tuple]:
        assert isinstance(partition, FilePartition)
        yield from decode_file(partition.path)
