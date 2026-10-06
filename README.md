# rust-pyspark-custom-datasource

This repo is a working example of a PySpark custom data source ([Python Data Source API](https://docs.databricks.com/aws/en/pyspark/datasources)) whose decoding is done in Rust. Each read yields `pyarrow.RecordBatch`es built in Rust and handed to pyarrow through the Arrow C Data Interface, without copying. The example format is [ARINC 429](https://en.wikipedia.org/wiki/ARINC_429).

The blog explains why this design was chosen and what it achieved. This README covers how to run it, where to find each piece, and how to adapt it to your own format.

> [!IMPORTANT]
> **Educational example only.** The decoder implements a simplified subset of the public ARINC 429 word layout (label, SDI, data, SSM, parity) plus two made-up parameters (`altitude`, `airspeed`). All data is synthetic. It is not a complete implementation of the standard, is not taken from any real aircraft interface control document, and **must not be used in avionics systems**.

## Where to look

Each idea from the blog, and the file that implements it:

| Topic in the blog | Code |
|---|---|
| The decoder doesn't know Spark exists | [`crates/arinc429-sdk/`](crates/arinc429-sdk/): a plain Rust crate that turns bytes into `Vec<Arinc429Word>`, with no PyO3, Arrow or Spark dependency. |
| Arrow is the only thing that crosses the FFI boundary | [`crates/arinc429-spark/src/batch_reader.rs`](crates/arinc429-spark/src/batch_reader.rs) (`to_pyarrow`, GIL released while reading and decoding) and [`convert.rs`](crates/arinc429-spark/src/convert.rs) (builds the Arrow `RecordBatch`) |
| The PySpark layer is intentionally lightweight | [`python/arinc429_datasource/datasource.py`](python/arinc429_datasource/datasource.py): options, one partition per file, schema. The Rust reader is created inside `read()`, so only plain Python state is pickled to workers. |
| Schema defined once, next to the decoder | [`crates/arinc429-spark/src/schema.rs`](crates/arinc429-spark/src/schema.rs). [`schema.py`](python/arinc429_datasource/schema.py) builds the Spark schema from it through `arrow_schema()`. |
| Benchmarks against pure Python and PyArrow | [`python/arinc429_datasource/examples/benchmark.py`](python/arinc429_datasource/examples/benchmark.py) and [`examples/baselines/`](python/arinc429_datasource/examples/baselines/) |
| Shipped as a wheel built with Maturin | [`pyproject.toml`](pyproject.toml), [`databricks.yml`](databricks.yml) |
| Version and architecture compatibility handled in CI | [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs the tests and builds both Linux wheels on every push (nothing is published). [`rust-toolchain.toml`](rust-toolchain.toml) pins Rust, and `pyproject.toml` pins the serverless package versions. |

## Prerequisites

| Tool | Needed for | Notes |
|---|---|---|
| macOS or Linux | everything | On Windows, use [WSL2](https://learn.microsoft.com/windows/wsl/install). Native Windows is untested. |
| A C compiler and linker | building the extension | macOS: `xcode-select --install`. Debian/Ubuntu: `sudo apt install build-essential`. |
| [rustup](https://rustup.rs) | building the extension | [`rust-toolchain.toml`](rust-toolchain.toml) selects the toolchain and the Linux targets automatically. |
| [uv](https://docs.astral.sh/uv/getting-started/installation/) | Python environment | Installs Python 3.12 for you. |
| Java 17 or 21 | running Spark locally | `java -version` should work, or set `JAVA_HOME`. |
| [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) | deploying only | Tested with v1.16.0. |
| A Databricks workspace with serverless jobs and an existing Unity Catalog volume you can write to | deploying only | |

## Quickstart (local)

```sh
git clone https://github.com/sam-deeble/rust-pyspark-custom-datasource.git
cd rust-pyspark-custom-datasource
uv sync # creates .venv and compiles the Rust extension
uv run arinc429-quickstart
```

This generates 5,000 synthetic words, reads them with `spark.read.format("arinc429")` on a local Spark session, and prints the results (first rows shown):

```text
Decoded 5,000 words. Sample rows:
+-----------+---+----------+---------------+---------+---------+---------+----+---------+
|label_octal|sdi|data_field|ssm            |parity_ok|parameter|value    |unit|file_path|
+-----------+---+----------+---------------+---------+---------+---------+----+---------+
|203        |0  |84650     |NormalOperation|true     |altitude |84650.0  |ft  |...      |
|206        |0  |131775    |NormalOperation|true     |airspeed |16471.875|kt  |...      |
|377        |2  |84346     |FunctionalTest |true     |NULL     |NULL     |NULL|...      |
```

### Matching Databricks Serverless

The Python version and dependency pins in [`pyproject.toml`](pyproject.toml) match **Databricks serverless environment 5** (Python 3.12). They were generated with [`databricks environments setup-local`](https://docs.databricks.com/aws/en/dev-tools/cli/reference/environments-commands#databricks-environments-setup-local):

```sh
databricks environments setup-local --serverless-version 5 --no-dbconnect
```

This writes a managed `[tool.uv] constraint-dependencies` block, so `uv sync` installs the same versions of pyarrow, numpy and the other packages that serverless jobs use. `--no-dbconnect` is needed because Databricks Connect ships its own copy of PySpark, which conflicts with the standalone `pyspark` used for local Spark sessions.

To target another serverless version, rerun the command with a different `--serverless-version` and update `environment_version` in [`resources/*.yml`](resources/) to match. Environments 3 and later use Python 3.12 and need nothing else. Environment 2 and Databricks Runtime 15.4 LTS use Python 3.11, so the wheels would also need rebuilding for it: change `-i python3.12` in `databricks.yml` and `ci.yml`, `requires-python`, and `.python-version`.

### Use it in your own code

```python
from pyspark.sql import SparkSession
from arinc429_datasource import Arinc429DataSource

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.dataSource.register(Arinc429DataSource)

df = (
    spark.read.format("arinc429")
    .option("path", "/path/to/data")  # file, directory, or comma-separated list
    .load()
)
```

To generate test data:

```python
from arinc429_datasource import generate_demo_file

generate_demo_file("demo.arinc429", 10_000, seed=42)  # 4 bytes per word, deterministic per seed
```

**Options**

| Option | Default | Description |
|---|---|---|
| `path` | required | A file, a directory (scanned non-recursively for `*.arinc429`), or a comma-separated mix. Paths must be local or Unity Catalog volume paths (`/Volumes/...`), not `s3://` or `abfss://` URIs. `.load([path, ...])` also works. Each file becomes one Spark partition. |
| `maxBatchRows` | `10000` | Maximum rows per Arrow `RecordBatch` passed to Spark. Must be a positive integer. |

The schema is fixed, so `.schema(...)` is rejected. A file whose length is not a multiple of 4 bytes fails its task; words with bad parity are kept, with `parity_ok` set to `false`.

**Schema**

| Column | Type | Description |
|---|---|---|
| `label_octal` | string | Word label in octal, e.g. `"203"` |
| `sdi` | int | Source/Destination Identifier (0–3) |
| `data_field` | long | Raw 19-bit data field |
| `ssm` | string | Sign/Status Matrix, e.g. `"NormalOperation"` |
| `parity_ok` | boolean | Whether the word has valid odd parity |
| `parameter` | string, nullable | `"altitude"`, `"airspeed"`, or `null` for other labels |
| `value` | double, nullable | Decoded engineering value |
| `unit` | string, nullable | Unit of `value` |
| `file_path` | string | Source file |

## Develop

```sh
cargo test                            # Rust SDK unit tests
uv run pytest                         # extension + local Spark end-to-end tests
uv run pytest -m "not spark_local"    # the same without Spark, if Java isn't installed
uv run ruff check python tests        # lint
uv run ruff format --check python tests
PYO3_PYTHON=$PWD/.venv/bin/python cargo clippy --workspace --all-targets -- -D warnings

uv run maturin develop --release      # rebuild the extension after editing Rust
cargo run -p arinc429-sdk --example decode_file   # the SDK on its own, no Python
```

## Benchmark

```sh
uv run arinc429-benchmark            # 500,000 synthetic words
uv run arinc429-benchmark 2000000    # or choose a word count
```

The benchmark decodes the same file with three implementations, first without Spark and then through `spark.read.format(...).count()`:

| Format | Implementation | Shows |
|---|---|---|
| `arinc429_python` | [`baselines/pure_python.py`](python/arinc429_datasource/examples/baselines/pure_python.py): one `struct.unpack` per word, one tuple per row | A straightforward first version |
| `arinc429_pyarrow` | [`baselines/pyarrow.py`](python/arinc429_datasource/examples/baselines/pyarrow.py): numpy-vectorized decode, yields Arrow batches | The gain from returning Arrow batches, still in Python |
| `arinc429` | Rust, yields Arrow batches | The further gain from compiled decoding |

The PyArrow version separates the two effects: comparing Python with PyArrow shows the cost of building one row at a time, and comparing PyArrow with Rust shows the cost of decoding in Python. [`tests/test_baseline_matches_rust.py`](tests/test_baseline_matches_rust.py) checks that all three produce identical rows.

The benchmark reads a single file, so it measures decode speed within one partition rather than scaling across a cluster. Results on a laptop differ from Databricks; the chart in the blog comes from the `spark.read.format(...).count()` section of the `arinc429_benchmark` job described below, run with the default 500,000 words.

## Run on Databricks

The bundle in [`databricks.yml`](databricks.yml) ([Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/), formerly Databricks Asset Bundles) builds the wheel, uploads it, and creates two serverless jobs. The jobs run the same entry points as the local commands above.

```sh
databricks auth login --host https://<your-workspace>.cloud.databricks.com
export BUNDLE_VAR_scratch_dir=/Volumes/<catalog>/<schema>/<volume>   # an existing volume

databricks bundle deploy
databricks bundle run arinc429_quickstart
databricks bundle run arinc429_benchmark   # add --params word_count=2000000 for a bigger file
```

Serverless compute runs on both x86_64 and aarch64 (arm64), so `deploy` cross-compiles a `manylinux` wheel for each with `maturin build --zig` (zig is installed by `uv sync` through the `ziglang` package). The jobs install the package with `--find-links`, and pip picks the wheel that matches the node. The wheels are built for Python 3.12, so they install on serverless environment 3 or later; see [Matching Databricks Serverless](#matching-databricks-serverless) for older runtimes.

The examples generate their own data, which has to be on storage every executor can read. Each run writes to a new subdirectory of `scratch_dir` and deletes it when it finishes.

To use the connector in your own job or notebook, add these two lines to its environment's dependencies and call `spark.dataSource.register(Arinc429DataSource)`:

```text
--find-links /Workspace/Users/<you>/.bundle/arinc429-datasource/dev/artifacts/.internal/
arinc429-datasource
```

`databricks bundle validate -o json` prints the exact folder as `workspace.artifact_path`; add `/.internal/` to it.

Remove everything with `databricks bundle destroy`.

## Adapt this to your own format

1. Replace `crates/arinc429-sdk` with your decoder. It should take bytes and return plain Rust structs, without depending on PyO3 or Arrow.
2. Update `convert.rs`, `schema.rs` and `schema.py` together. A mismatch between the Arrow and Spark schemas only shows up at read time.
3. Rename together: the format (`Arinc429DataSource.name()`), the Python package, `module-name` in `pyproject.toml`, `[lib] name` in `crates/arinc429-spark/Cargo.toml` and the `#[pymodule]` function in `lib.rs` (these last three must match), and the `*.arinc429` pattern in `_files.py`.
4. Decide how to partition. This example uses one `FilePartition` per file (`_files.py`); for very large files, add a byte range to the partition and split in `partitions()`.
5. Keep the reader picklable: store paths and options on the `DataSourceReader` and create the native reader in `read()`.
6. Keep a simple Python decoder like [`baselines/pure_python.py`](python/arinc429_datasource/examples/baselines/pure_python.py) and test the Rust output against it.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `JAVA_GATEWAY_EXITED` or "Unable to locate a Java Runtime" | Install Java 17 or 21 and set `JAVA_HOME`. |
| A Homebrew `rust` takes precedence over rustup, so the Linux targets are missing | `brew uninstall rust`, or put `~/.cargo/bin` first on `PATH`. |
| Rust changes have no effect | Run `uv run maturin develop --release` (or `uv sync --reinstall-package arinc429-datasource`). |
| The job fails with `No matching distribution found for arinc429-datasource` | Check the compute uses Python 3.12 (serverless environment 3 or later). Then run `databricks bundle deploy` again and check that both wheels appear under the bundle's `artifacts/.internal/` folder. |
| The job runs old code after you change `version` in `pyproject.toml` | Delete `target/wheels/`. The artifact globs upload every matching wheel, and pip installs the highest version. |
| Spark fails to start with `BindException ... Service 'sparkDriver'` | `export SPARK_LOCAL_IP=127.0.0.1`. This is common on macOS with a VPN. |
| `no value assigned to required variable scratch_dir` | Set `BUNDLE_VAR_scratch_dir` (or pass `--var scratch_dir=/Volumes/...`). |

## Project layout

```text
crates/
  arinc429-sdk/        Standalone Rust decoder (no Python/Arrow/Spark)
  arinc429-spark/      PyO3 + Arrow bindings: PyBatchReader, generate_demo_file
python/arinc429_datasource/
  datasource.py        The PySpark DataSource and DataSourceReader
  _files.py            Option parsing and FilePartition, shared with the baselines
  schema.py            Spark schema, built from crates/arinc429-spark/src/schema.rs
  examples/            Quickstart, benchmark and Python baselines (packaged in the wheel)
tests/                 pytest: extension, baselines and local-Spark end-to-end
databricks.yml         Bundle: wheel artifacts (x86_64 + aarch64)
resources/             Bundle: the two serverless jobs
.github/workflows/     CI: lint, tests and both wheel builds
```

## License

MIT. See [LICENSE](LICENSE).
