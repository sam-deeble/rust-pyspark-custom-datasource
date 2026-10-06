"""The Rust-backed ``arinc429`` data source."""

from collections.abc import Iterator

import pyarrow as pa
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from arinc429_datasource._arinc429 import PyBatchReader
from arinc429_datasource._files import FilePartition, check_schema, input_files, max_batch_rows
from arinc429_datasource.schema import SCHEMA


class Arinc429DataSource(DataSource):
    """Reads files of ARINC 429 words, decoded in Rust.

    Options
    -------
    path : str
        Required. Comma-separated local or ``/Volumes/...`` files and
        directories. Directories are searched (non-recursively) for
        ``*.arinc429`` files.
    maxBatchRows : int, default 10000
        Maximum rows per Arrow record batch.

    Examples
    --------
    >>> spark.dataSource.register(Arinc429DataSource)
    >>> df = spark.read.format("arinc429").option("path", "/Volumes/main/default/raw").load()
    """

    @classmethod
    def name(cls) -> str:
        return "arinc429"

    def schema(self) -> StructType:
        return SCHEMA

    def reader(self, schema: StructType) -> "Arinc429Reader":
        check_schema(schema, self.name())
        return Arinc429Reader(self.options, self.name())


class Arinc429Reader(DataSourceReader):
    """Plans one partition per file and decodes each one with ``PyBatchReader``."""

    def __init__(self, options: dict[str, str], source_name: str):
        # Spark pickles the reader to send it to executors, so it only holds
        # plain Python values.
        self.files = input_files(options, source_name)
        self.max_batch_rows = max_batch_rows(options, source_name)

    def partitions(self) -> list[FilePartition]:
        return [FilePartition(path) for path in self.files]

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        assert isinstance(partition, FilePartition)
        return PyBatchReader(partition.path, self.max_batch_rows)
