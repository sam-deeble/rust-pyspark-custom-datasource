"""Write a small synthetic file and read it back with spark.read.format("arinc429")."""

import argparse
from pathlib import Path

from arinc429_datasource import Arinc429DataSource, generate_demo_file
from arinc429_datasource.examples._common import add_scratch_dir_arg, get_spark, scratch_dir


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_scratch_dir_arg(parser)
    args = parser.parse_args()

    spark = get_spark("arinc429-quickstart")
    spark.dataSource.register(Arinc429DataSource)

    with scratch_dir(args.scratch_dir) as tmp:
        path = str(Path(tmp) / "demo.arinc429")
        generate_demo_file(path, 5_000, seed=42)

        df = spark.read.format("arinc429").option("path", path).load()
        print(f"Decoded {df.count():,} words. Sample rows:")
        df.show(10, truncate=False)

        print("Rows per parameter (unknown labels are NULL):")
        df.groupBy("parameter").count().show()


if __name__ == "__main__":
    main()
