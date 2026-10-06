import argparse
import contextlib
import os
import shutil
import tempfile
import uuid
from collections.abc import Iterator

from pyspark.sql import SparkSession


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str) -> SparkSession:
    if on_databricks():
        return SparkSession.builder.getOrCreate()
    return SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()


def add_scratch_dir_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--scratch-dir",
        metavar="PATH",
        help=(
            "where to write the generated data. Required on Databricks, where it must be "
            "a Unity Catalog volume path such as /Volumes/main/default/scratch. "
            "Defaults to a temporary directory when running locally."
        ),
    )


@contextlib.contextmanager
def scratch_dir(base: str | None) -> Iterator[str]:
    """Yield an empty directory that is deleted on exit.

    On Databricks the directory must be on a volume, because executors cannot
    see the driver's local disk.
    """
    if base is None:
        if on_databricks():
            raise SystemExit("--scratch-dir is required on Databricks")
        with tempfile.TemporaryDirectory() as tmp:
            yield tmp
        return

    run_dir = os.path.join(base, f"arinc429-{uuid.uuid4().hex[:8]}")
    os.makedirs(run_dir)
    try:
        yield run_dir
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)
