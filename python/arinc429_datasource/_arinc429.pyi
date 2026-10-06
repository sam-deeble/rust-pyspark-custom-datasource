from collections.abc import Iterator

import pyarrow as pa

class PyBatchReader:
    """Decode a ``.arinc429`` file into Arrow record batches."""

    def __init__(self, path: str, max_batch_rows: int) -> None: ...
    def __iter__(self) -> Iterator[pa.RecordBatch]: ...
    def __next__(self) -> pa.RecordBatch: ...

def generate_demo_file(path: str, count: int, seed: int) -> None:
    """Write ``count`` synthetic words to ``path``. The same seed gives the same file."""

def arrow_schema() -> pa.Schema:
    """Schema of the batches ``PyBatchReader`` yields."""
