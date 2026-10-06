from pyspark.sql.pandas.types import from_arrow_schema

from arinc429_datasource._arinc429 import arrow_schema

# Defined once in Rust, next to the decoder (crates/arinc429-spark/src/schema.rs).
SCHEMA = from_arrow_schema(arrow_schema())
