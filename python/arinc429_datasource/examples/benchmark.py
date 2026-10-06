"""Benchmark the Rust reader against the two Python baselines.

Each implementation decodes the same synthetic file, first directly and then
through ``spark.read.format(...).count()``.
"""

import argparse
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource

from arinc429_datasource import Arinc429DataSource, generate_demo_file
from arinc429_datasource._arinc429 import PyBatchReader
from arinc429_datasource.examples._common import add_scratch_dir_arg, get_spark, scratch_dir
from arinc429_datasource.examples.baselines.pure_python import (
    Arinc429PythonDataSource,
    decode_file,
)
from arinc429_datasource.examples.baselines.pyarrow import (
    Arinc429PyArrowDataSource,
    decode_file_batches,
)

WORD_SIZE_BYTES = 4
BATCH_ROWS = 50_000


@dataclass(frozen=True)
class Contender:
    source: type[DataSource]
    decode: Callable[[str], int]  # decodes a file without Spark, returns the row count


CONTENDERS = [
    Contender(Arinc429PythonDataSource, lambda path: sum(1 for _ in decode_file(path))),
    Contender(
        Arinc429PyArrowDataSource,
        lambda path: sum(b.num_rows for b in decode_file_batches(path, BATCH_ROWS)),
    ),
    Contender(
        Arinc429DataSource,
        lambda path: sum(b.num_rows for b in PyBatchReader(path, BATCH_ROWS)),
    ),
]


def timed(fn: Callable[[], int], expected_rows: int) -> float:
    start = time.perf_counter()
    rows = fn()
    elapsed = time.perf_counter() - start
    # A decoder that silently dropped rows would look faster than it is.
    if rows != expected_rows:
        raise RuntimeError(f"expected {expected_rows} rows, got {rows}")
    return elapsed


def report(title: str, timings: dict[str, float], n_words: int) -> None:
    print(f"\n== {title} ==")
    megabytes = n_words * WORD_SIZE_BYTES / 1024**2
    for name, seconds in timings.items():
        print(
            f"{name:<20} {seconds:7.3f}s  {n_words / seconds:>12,.0f} words/s"
            f"  {megabytes / seconds:8.2f} MB/s"
        )

    baseline_name, baseline_seconds = next(iter(timings.items()))
    print(f"\nSpeed-up over {baseline_name}:")
    for name, seconds in list(timings.items())[1:]:
        print(f"  {name:<18} {baseline_seconds / seconds:.1f}x")


def run(spark: SparkSession, path: str, n_words: int) -> None:
    raw = {c.source.name(): timed(lambda c=c: c.decode(path), n_words) for c in CONTENDERS}
    report("Decode only, no Spark", raw, n_words)

    via_spark = {}
    for c in CONTENDERS:
        spark.dataSource.register(c.source)
        df = spark.read.format(c.source.name()).option("path", path).load()
        via_spark[c.source.name()] = timed(df.count, n_words)
    report("spark.read.format(...).count()", via_spark, n_words)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("word_count", type=int, nargs="?", default=500_000)
    add_scratch_dir_arg(parser)
    args = parser.parse_args()

    spark = get_spark("arinc429-benchmark")
    with scratch_dir(args.scratch_dir) as tmp:
        path = str(Path(tmp) / "benchmark.arinc429")
        generate_demo_file(path, args.word_count, seed=42)
        print(f"Generated {args.word_count:,} words at {path}")
        run(spark, path, args.word_count)


if __name__ == "__main__":
    main()
