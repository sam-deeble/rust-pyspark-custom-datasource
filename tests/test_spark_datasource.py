"""End-to-end tests through a local Spark session."""

import pickle

import pytest

from arinc429_datasource import generate_demo_file
from arinc429_datasource.datasource import Arinc429Reader
from arinc429_datasource.examples.baselines.pure_python import Arinc429PythonDataSource
from arinc429_datasource.examples.baselines.pyarrow import Arinc429PyArrowDataSource
from arinc429_datasource.schema import SCHEMA

pytestmark = pytest.mark.spark_local


def read(spark, path, source="arinc429", **options):
    return spark.read.format(source).option("path", str(path)).options(**options).load()


def test_reads_a_file(spark, demo_file, demo_words):
    df = read(spark, demo_file)
    assert df.schema == SCHEMA
    assert df.count() == demo_words


def test_reads_every_file_in_a_directory(spark, tmp_path):
    generate_demo_file(str(tmp_path / "a.arinc429"), 1_000, seed=1)
    generate_demo_file(str(tmp_path / "b.arinc429"), 2_000, seed=2)
    df = read(spark, tmp_path)
    assert df.rdd.getNumPartitions() == 2
    assert df.count() == 3_000


def test_load_accepts_a_list_of_paths(spark, tmp_path):
    paths = [str(tmp_path / name) for name in ("a.arinc429", "b.arinc429")]
    for seed, path in enumerate(paths):
        generate_demo_file(path, 1_000, seed=seed)
    assert spark.read.format("arinc429").load(paths).count() == 2_000


def test_max_batch_rows_does_not_change_the_result(spark, demo_file, demo_words):
    assert read(spark, demo_file, maxBatchRows="100").count() == demo_words


@pytest.mark.parametrize("value", ["0", "-1", "abc"])
def test_rejects_invalid_max_batch_rows(spark, demo_file, value):
    with pytest.raises(Exception, match="maxBatchRows must be a positive integer"):
        read(spark, demo_file, maxBatchRows=value).count()


def test_missing_path_option_raises(spark):
    with pytest.raises(Exception, match="'path' option is required"):
        spark.read.format("arinc429").load().count()


def test_missing_file_raises_before_reading(spark, tmp_path):
    with pytest.raises(Exception, match="does not exist"):
        read(spark, tmp_path / "missing.arinc429").count()


def test_rejects_a_user_specified_schema(spark, demo_file):
    reordered = SCHEMA.fields[3:4] + SCHEMA.fields[:3] + SCHEMA.fields[4:]
    df = spark.read.format("arinc429").schema(SCHEMA.__class__(reordered))
    with pytest.raises(Exception, match="user-specified schema is not supported"):
        df.option("path", str(demo_file)).load().count()


def test_reader_pickles_without_native_objects(demo_file):
    # Spark ships the reader to executors; the Rust reader is only created in read().
    reader = Arinc429Reader({"path": str(demo_file)}, "arinc429")
    restored = pickle.loads(pickle.dumps(reader))
    assert restored.files == [str(demo_file)]


def test_counts_rows_per_parameter(spark, demo_file, demo_words):
    counts = dict(read(spark, demo_file).groupBy("parameter").count().collect())
    assert set(counts) == {"altitude", "airspeed", None}
    assert sum(counts.values()) == demo_words


@pytest.mark.parametrize("source", [Arinc429PythonDataSource, Arinc429PyArrowDataSource])
def test_baselines_read_the_same_rows(spark, demo_file, source):
    spark.dataSource.register(source)
    expected = read(spark, demo_file).collect()
    assert read(spark, demo_file, source=source.name()).collect() == expected
