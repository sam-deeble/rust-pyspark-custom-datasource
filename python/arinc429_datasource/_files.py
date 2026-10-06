"""Option parsing and partitioning shared by the Rust source and the Python baselines."""

import glob
import json
import os

from pyspark.sql.datasource import InputPartition
from pyspark.sql.types import StructType

from arinc429_datasource.schema import SCHEMA

DEFAULT_MAX_BATCH_ROWS = 10_000


class FilePartition(InputPartition):
    """One input file, read by a single Spark task."""

    def __init__(self, path: str):
        super().__init__(path)
        self.path = path


def check_schema(schema: StructType, source_name: str) -> None:
    """Reject a schema passed with ``.schema(...)``.

    Spark only checks that the batch column names exist, so a reordered or
    retyped schema would silently put values in the wrong columns.
    """
    if schema != SCHEMA:
        raise ValueError(f"{source_name}: a user-specified schema is not supported")


def input_files(options: dict[str, str], source_name: str) -> list[str]:
    """Expand the ``path`` option into a sorted list of files.

    Parameters
    ----------
    options : dict of str
        Data source options. ``path`` is a comma-separated list of files and
        directories; ``.load([...])`` passes a list as ``paths`` instead.
        Directories are searched (non-recursively) for ``*.arinc429`` files.
    source_name : str
        Format name, used in error messages.
    """
    if "paths" in options:
        entries = json.loads(options["paths"])
    elif options.get("path"):
        entries = options["path"].split(",")
    else:
        raise ValueError(f"{source_name}: the 'path' option is required")

    files = []
    for entry in (e.strip() for e in entries):
        if not entry:
            continue
        if os.path.isdir(entry):
            files.extend(glob.glob(os.path.join(glob.escape(entry), "*.arinc429")))
        elif os.path.isfile(entry):
            files.append(entry)
        else:
            raise FileNotFoundError(
                f"{source_name}: {entry} does not exist. Paths must be local or "
                "Unity Catalog volume paths (/Volumes/...), not cloud URIs."
            )

    if not files:
        raise ValueError(f"{source_name}: no .arinc429 files found in {entries}")
    return sorted(files)


def max_batch_rows(options: dict[str, str], source_name: str) -> int:
    """Parse the ``maxBatchRows`` option, which must be a positive integer."""
    value = options.get("maxBatchRows", str(DEFAULT_MAX_BATCH_ROWS))
    try:
        rows = int(value)
    except ValueError:
        rows = 0
    if rows < 1:
        raise ValueError(f"{source_name}: maxBatchRows must be a positive integer, got {value!r}")
    return rows
