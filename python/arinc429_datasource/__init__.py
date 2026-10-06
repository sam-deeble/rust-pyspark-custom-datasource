"""A PySpark data source for ARINC 429 files, backed by a Rust decoder."""

from arinc429_datasource._arinc429 import generate_demo_file
from arinc429_datasource.datasource import Arinc429DataSource

__all__ = ["Arinc429DataSource", "generate_demo_file"]
