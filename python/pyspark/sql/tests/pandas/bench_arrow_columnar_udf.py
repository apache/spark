#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Benchmark: Arrow columnar vs row-based input for scalar Arrow Python UDFs.

Compares end-to-end execution time of applying a scalar pandas_udf to data
from two sources:

1. ArrowBackedDataSourceV2 -- produces ColumnarBatch with ArrowColumnVector.
   ArrowEvalPythonExec extracts Arrow FieldVectors directly via the columnar
   path (no ColumnarToRow conversion).

2. spark.range() -- produces row-based data.
   ArrowEvalPythonExec uses the standard path: InternalRow -> ArrowWriter.

The UDF does minimal computation (addition) so the benchmark isolates
the data transfer overhead between JVM and Python.

Usage:
    cd $SPARK_HOME
    python python/pyspark/sql/tests/pandas/bench_arrow_columnar_udf.py \
        [--rows N] [--iterations N] [--partitions N]
"""

import argparse
import sys
import os
import time

# Allow running from the Spark root directory.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../.."))

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col


ARROW_SOURCE = "org.apache.spark.sql.execution.python.ArrowBackedDataSourceV2"


def create_spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("ArrowColumnarUDFBenchmark")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.python.worker.reuse", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def timed_run(df, warmup=2, iterations=5):
    """Run a DataFrame with noop sink, returning per-iteration timings."""
    for _ in range(warmup):
        df.write.format("noop").mode("overwrite").save()

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        df.write.format("noop").mode("overwrite").save()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    return times


def print_stats(label, times):
    avg = sum(times) / len(times)
    mn = min(times)
    mx = max(times)
    print(f"  {label}")
    print(
        f"    avg = {avg * 1000:8.1f} ms   "
        f"min = {mn * 1000:8.1f} ms   "
        f"max = {mx * 1000:8.1f} ms   "
        f"({len(times)} iterations)"
    )
    return avg


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Arrow columnar vs row-based Python UDF input"
    )
    parser.add_argument(
        "--rows", type=int, default=500_000, help="Number of rows (default: 500000)"
    )
    parser.add_argument("--iterations", type=int, default=5, help="Timed iterations (default: 5)")
    parser.add_argument(
        "--partitions", type=int, default=1, help="Number of partitions (default: 1)"
    )
    args = parser.parse_args()

    spark = create_spark()

    print("=" * 70)
    print("Arrow Columnar vs Row-Based Input for Scalar Arrow Python UDF")
    print("=" * 70)
    print(f"  rows={args.rows}  partitions={args.partitions}  iterations={args.iterations}")
    print()

    # Identity UDF -- returns input as-is to minimize Python-side cost
    # and isolate JVM data transfer overhead.
    @pandas_udf("string")
    def identity_udf(data: pd.Series) -> pd.Series:
        return data

    conf_key = "spark.sql.execution.arrow.pythonUDF.columnarInput.enabled"

    def make_df():
        return (
            spark.read.format(ARROW_SOURCE)
            .option("numRows", str(args.rows))
            .option("numPartitions", str(args.partitions))
            .load()
            .select(col("id"), col("name"), col("value"), col("data"), identity_udf(col("data")))
        )

    # ----- Benchmark 1: Arrow columnar (config=true) -----
    spark.conf.set(conf_key, "true")
    arrow_df = make_df()
    print("--- Physical Plan: Arrow columnar source ---")
    arrow_df.explain()
    print()
    print("--- Results ---")
    print()
    arrow_times = timed_run(arrow_df, iterations=args.iterations)
    arrow_avg = print_stats("Arrow columnar (direct FieldVector extraction)", arrow_times)
    print()

    # ----- Benchmark 2: Row-based with ColumnarToRow (config=false) -----
    spark.conf.set(conf_key, "false")
    row_df = make_df()
    print("--- Physical Plan: Row-based (ColumnarToRow) source ---")
    row_df.explain()
    print()
    row_times = timed_run(row_df, iterations=args.iterations)
    row_avg = print_stats("Row-based (ColumnarToRow + ArrowWriter)", row_times)
    spark.conf.set(conf_key, "true")
    print()

    if arrow_avg > 0:
        speedup = row_avg / arrow_avg
        faster = "faster" if speedup > 1.0 else "slower"
        print(f"  Speedup: {speedup:.2f}x ({faster} with Arrow columnar)")
    print()

    spark.stop()


if __name__ == "__main__":
    main()
