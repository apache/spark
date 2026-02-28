#!/usr/bin/env python3
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

"""
Benchmark: out-of-process pandas UDF vs in-process UDF.

Measures wall-clock time (driver perspective, including collect()) for a
simple arithmetic operation (x * 2) on a LongType column of varying sizes.

Both UDF types use the same Arrow batch size (spark.sql.execution.arrow.maxRecordsPerBatch)
so the comparison reflects IPC overhead, not batch-size differences.

Usage (via run_inprocess_udf_benchmark.sh):
    bash python/integration/run_inprocess_udf_benchmark.sh

Direct usage (environment must already be set up):
    INPROCESS_TESTS=1 python python/integration/benchmark_inprocess_udf.py
"""

import os
import statistics
import time

import pyarrow.compute as pc
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import LongType

from pyspark.inprocess.udf import inprocess_udf

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ROW_COUNTS   = [100_000, 1_000_000, 5_000_000]
ITERATIONS   = 5    # timed runs per (udf_type, row_count) combination
WARMUP_RUNS  = 2    # untimed warmup runs to let the JIT and interpreter settle
BATCH_SIZE   = 10_000  # spark.sql.execution.arrow.maxRecordsPerBatch

# ---------------------------------------------------------------------------
# UDF definitions
# ---------------------------------------------------------------------------

@inprocess_udf(return_type=LongType())
def inprocess_double(x):
    return pc.multiply(x, 2)


@pandas_udf(LongType())
def pandas_double(x):
    return x * 2


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------

def _time_once(df, udf_col) -> float:
    """Return wall-clock seconds for df.select(udf_col).count()."""
    t0 = time.perf_counter()
    df.select(udf_col).count()
    return time.perf_counter() - t0


def benchmark(df, udf_col, label: str) -> dict:
    """Warm up then time ITERATIONS runs; return stats in milliseconds."""
    col = df[df.columns[0]]
    for _ in range(WARMUP_RUNS):
        df.select(udf_col(col)).count()

    samples_ms = [_time_once(df, udf_col(col)) * 1_000 for _ in range(ITERATIONS)]
    return {
        "label":  label,
        "n":      df.count(),
        "median": statistics.median(samples_ms),
        "mean":   statistics.mean(samples_ms),
        "min":    min(samples_ms),
        "max":    max(samples_ms),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if os.environ.get("INPROCESS_TESTS") != "1":
        raise SystemExit(
            "Set INPROCESS_TESTS=1 and run via "
            "python/integration/run_inprocess_udf_benchmark.sh"
        )

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("InProcessUDFBenchmark")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", BATCH_SIZE)
        # Suppress noisy Spark logs
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    results = []
    for n_rows in ROW_COUNTS:
        df = spark.range(n_rows).toDF("v")
        df.cache()
        df.count()  # materialise cache

        print(f"\n[{n_rows:>9,} rows]  warming up and timing …", flush=True)
        results.append(benchmark(df, inprocess_double, "inprocess_udf"))
        results.append(benchmark(df, pandas_double,    "pandas_udf   "))

        df.unpersist()

    spark.stop()

    # ------------------------------------------------------------------
    # Print results table
    # ------------------------------------------------------------------
    header = f"{'UDF type':<14}  {'rows':>9}  {'median ms':>10}  {'mean ms':>9}  {'min ms':>8}  {'max ms':>8}"
    print("\n" + "=" * len(header))
    print("  In-process UDF vs pandas UDF — wall-clock time (ms)")
    print("  Operation : x * 2  (LongType)")
    print(f"  Batch size: {BATCH_SIZE:,} rows  |  {ITERATIONS} timed runs + {WARMUP_RUNS} warmup  |  local[1]")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    prev_n = None
    for r in results:
        if prev_n is not None and r["n"] != prev_n:
            print()
        print(
            f"{r['label']:<14}  {r['n']:>9,}  "
            f"{r['median']:>10.1f}  {r['mean']:>9.1f}  "
            f"{r['min']:>8.1f}  {r['max']:>8.1f}"
        )
        prev_n = r["n"]

    print("=" * len(header))

    # Speedup summary
    print("\n  Speedup (pandas_udf median / inprocess_udf median):")
    inprocess = [r for r in results if "inprocess" in r["label"]]
    pandas    = [r for r in results if "pandas"    in r["label"]]
    for ip, pd in zip(inprocess, pandas):
        speedup = pd["median"] / ip["median"]
        print(f"    {ip['n']:>9,} rows : {speedup:.2f}x")
    print()


if __name__ == "__main__":
    main()
