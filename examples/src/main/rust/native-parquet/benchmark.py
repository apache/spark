#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compares the queries of rust_sdk_parquet with those of Spark's Parquet reader.

Run with Java 22+ and spark-submit from a build that includes PR #59424, with the package of a
release build: a debug build of the library is many times slower.
"""

import argparse
from pathlib import Path
import statistics
import time

from pyspark.sql import SparkSession

# `k` selects 1% of the rows for each value, and `payload` is a 64-character string.
COLUMNS = ["id", "cast(id % 100 as int) as k", "repeat(md5(cast(id as string)), 2) as payload"]

QUERIES = {
    "one column": "SELECT sum(id) FROM {table}",
    "all columns": "SELECT sum(id), sum(k), max(length(payload)) FROM {table}",
    "10% of the rows": "SELECT sum(id), max(length(payload)) FROM {table} WHERE k < 10",
    "1% of the rows": "SELECT sum(id), max(length(payload)) FROM {table} WHERE k = 7",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True, help="Written if it does not exist")
    parser.add_argument("--rows", type=int, default=20_000_000)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Rust native Parquet source benchmark").getOrCreate()
    try:
        spark.addArtifact(str(args.package.resolve(strict=True)))
        data = str(args.data.resolve())
        if not args.data.exists():
            spark.range(args.rows).selectExpr(*COLUMNS).write.parquet(data)
        spark.read.parquet(data).createOrReplaceTempView("spark_reader")
        native = spark.read.format("rust_sdk_parquet")
        native.load(data).createOrReplaceTempView("rust_row_filter")
        native.option("rowFilter", "false").load(data).createOrReplaceTempView("rust_no_filter")
        tables = ["spark_reader", "rust_row_filter", "rust_no_filter"]

        print(f"{'query':16} " + " ".join(f"{table:>16}" for table in tables))
        for name, query in QUERIES.items():
            results, times = set(), []
            for table in tables:
                # A new DataFrame for each run: one that ran already reuses its shuffle files.
                def run():
                    results.add(tuple(spark.sql(query.format(table=table)).collect()[0]))

                # The first runs warm up the JIT compiler and the page cache.
                for _ in range(2):
                    run()
                runs = []
                for _ in range(args.runs):
                    start = time.perf_counter()
                    run()
                    runs.append(time.perf_counter() - start)
                times.append(statistics.median(runs))
            assert len(results) == 1, (name, results)
            print(f"{name:16} " + " ".join(f"{seconds:15.3f}s" for seconds in times))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
