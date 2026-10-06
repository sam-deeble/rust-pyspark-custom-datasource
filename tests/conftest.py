import pytest
from pyspark.sql import SparkSession

from arinc429_datasource import Arinc429DataSource, generate_demo_file


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("arinc429-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.dataSource.register(Arinc429DataSource)
    yield session
    session.stop()


@pytest.fixture
def demo_words() -> int:
    return 3_000


@pytest.fixture
def demo_file(tmp_path, demo_words):
    path = tmp_path / "demo.arinc429"
    generate_demo_file(str(path), demo_words, seed=7)
    return path
