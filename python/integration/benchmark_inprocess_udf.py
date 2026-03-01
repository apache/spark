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
Benchmark: out-of-process pandas UDF vs in-process UDF, five scenarios.

Scenario A (baseline)  -- 1 column LongType, small batch (10K rows)
    IPC overhead per batch is small (80 KB), so jep dispatch overhead
    dominates: inprocess is slightly slower than pandas UDF.

Scenario B (wide/large) -- 10 columns LongType, large batch (1M rows)
    IPC must serialize ~80 MB of input per batch over a loopback socket;
    in-process zero-copies the same data via native addresses, so
    in-process is expected to win.

Scenario C (very wide) -- 100 columns LongType, large batch (100K rows)
    Even more IPC serialization overhead.

Scenario D (short strings) -- 1 column StringType, medium batch (100K rows)
    Variable-length strings were unsupported by the old custom input path.
    Full Arrow CDI transparently handles Utf8/LargeUtf8 and any other type.

Scenario E (long strings, identity UDF) -- 1 col 1000-char strings, 100K batch
    Identity UDF isolates pure data-movement cost.  Pandas UDF must:
      (1) Serialize ~100 MB of string bytes over a loopback socket per batch.
      (2) Allocate 100K Python str objects when converting Arrow -> pandas Series.
      (3) Reverse-convert pandas Series -> Arrow for the result.
    In-process just passes native CDI pointers; the identity UDF touches
    no Python str objects at all.  Expected speedup: 5-15x.

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
from pyspark.sql.functions import col, lpad, pandas_udf
from pyspark.sql.types import LongType, StringType

from pyspark.inprocess.udf import inprocess_udf

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ITERATIONS  = 5   # timed runs per (udf_type, row_count) combination
WARMUP_RUNS = 2   # untimed warmup runs

# Scenario A: narrow schema, small batch -- jep overhead dominates
SCENARIO_A = {
    "name":       "Scenario A: 1 col, 10K batch (narrow/small -- baseline)",
    "n_cols":     1,
    "batch_size": 10_000,
    "dtype":      "long",
    "row_counts": [100_000, 1_000_000, 5_000_000],
}

# Scenario B: wide schema, large batch -- IPC serialization dominates
SCENARIO_B = {
    "name":       "Scenario B: 10 cols, 1M batch (wide/large -- in-process wins)",
    "n_cols":     10,
    "batch_size": 1_000_000,
    "dtype":      "long",
    "row_counts": [1_000_000, 5_000_000, 10_000_000],
}

# Scenario C: very wide schema -- IPC cost even larger per batch
SCENARIO_C = {
    "name":       "Scenario C: 100 cols, 100K batch (very wide -- in-process wins more)",
    "n_cols":     100,
    "batch_size": 100_000,
    "dtype":      "long",
    "row_counts": [1_000_000, 5_000_000, 10_000_000],
}

# Scenario D: StringType -- variable-length type, only supported via full CDI
# Each value is a ~20-char string (e.g. "row-0000000001234567").
# Pandas UDF must encode offsets + UTF-8 bytes over IPC; in-process passes
# the raw Arrow Utf8 buffers directly via CDI (zero-copy).
SCENARIO_D = {
    "name":       "Scenario D: 1 col StringType, 100K batch (CDI variable-length)",
    "n_cols":     1,
    "batch_size": 100_000,
    "dtype":      "string",
    "row_counts": [1_000_000, 5_000_000, 10_000_000],
}

# Scenario E: long strings -- in-process wins clearly.
# Each value is padded to exactly 1000 chars (e.g. "xxxxxxx...1234567").
# 100K rows x 1000 bytes = ~100 MB of string data per batch.
# Pandas UDF must (a) serialize that over IPC socket and (b) allocate
# 100K Python str objects per batch; identity UDF avoids both.
SCENARIO_E = {
    "name":       "Scenario E: 1 col 1000-char strings, 100K batch (in-process wins)",
    "n_cols":     1,
    "batch_size": 100_000,
    "dtype":      "large_string",
    "str_len":    1000,
    "row_counts": [500_000, 1_000_000, 2_000_000],
}

SCENARIOS = [SCENARIO_A, SCENARIO_B, SCENARIO_D, SCENARIO_E]

# ---------------------------------------------------------------------------
# UDF factories
# ---------------------------------------------------------------------------

def make_inprocess_udf(n_cols):
    """Sum n_cols LongType Arrow arrays; each arg is a pa.Array."""
    @inprocess_udf(return_type=LongType())
    def _sum(*cols):
        result = cols[0]
        for c in cols[1:]:
            result = pc.add(result, c)
        return result
    return _sum


def make_pandas_udf(n_cols):
    """Sum n_cols LongType pandas Series."""
    @pandas_udf(LongType())
    def _sum(*cols):
        result = cols[0]
        for c in cols[1:]:
            result = result + c
        return result
    return _sum


def make_inprocess_udf_string():
    """Uppercase a single StringType Arrow array; arg is a pa.Array of strings."""
    @inprocess_udf(return_type=StringType())
    def _upper(s):
        return pc.utf8_upper(s)
    return _upper


def make_pandas_udf_string():
    """Uppercase a single StringType pandas Series."""
    @pandas_udf(StringType())
    def _upper(s):
        return s.str.upper()
    return _upper


def make_inprocess_udf_identity():
    """Identity UDF: return the input Arrow array unchanged (zero data work)."""
    @inprocess_udf(return_type=StringType())
    def _identity(s):
        return s
    return _identity


def make_pandas_udf_identity():
    """Identity UDF: return the input pandas Series unchanged (zero data work)."""
    @pandas_udf(StringType())
    def _identity(s):
        return s
    return _identity

# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------

def _make_long_df(spark, n_rows, n_cols):
    """DataFrame with n_cols LongType columns (all equal to range id)."""
    base = spark.range(n_rows)
    return base.select(*[base["id"].alias(f"c{i}") for i in range(n_cols)])


def _make_string_df(spark, n_rows):
    """DataFrame with one StringType column (short strings: cast of range id)."""
    base = spark.range(n_rows)
    return base.select(col("id").cast("string").alias("s"))


def _make_large_string_df(spark, n_rows, str_len):
    """DataFrame with one StringType column of exactly str_len-char strings.

    Uses lpad to left-pad the string representation of the row id with 'x'
    characters, producing values like "xxxxxxxxxxxxxxxxxxxx1234567" where the
    total length is str_len.  All strings have the same byte count, which gives
    a predictable and reproducible payload size per batch.
    """
    base = spark.range(n_rows)
    return base.select(
        lpad(col("id").cast("string"), str_len, "x").alias("s")
    )


def _benchmark(spark, scenario):
    """Run one scenario; return list of result dicts."""
    n_cols     = scenario["n_cols"]
    batch_size = scenario["batch_size"]
    row_counts = scenario["row_counts"]
    dtype      = scenario.get("dtype", "long")

    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", batch_size)

    if dtype == "large_string":
        ip_udf = make_inprocess_udf_identity()
        pd_udf = make_pandas_udf_identity()
    elif dtype == "string":
        ip_udf = make_inprocess_udf_string()
        pd_udf = make_pandas_udf_string()
    else:
        ip_udf = make_inprocess_udf(n_cols)
        pd_udf = make_pandas_udf(n_cols)

    str_len = scenario.get("str_len", 0)

    results = []
    for n_rows in row_counts:
        if dtype == "large_string":
            df = _make_large_string_df(spark, n_rows, str_len)
            input_cols = [df["s"]]
        elif dtype == "string":
            df = _make_string_df(spark, n_rows)
            input_cols = [df["s"]]
        else:
            df = _make_long_df(spark, n_rows, n_cols)
            input_cols = [df[f"c{i}"] for i in range(n_cols)]

        df.cache()
        df.count()  # materialise cache

        def run(udf_fn):
            # Use the noop sink (as in Spark's own Scala micro-benchmarks).
            # Unlike count() -- which Spark rewrites to count(1) and then prunes
            # the entire UDF projection via ColumnPruning -- noop forces full
            # evaluation of every row and every output column, so the UDF
            # actually executes.
            t0 = time.perf_counter()
            df.select(udf_fn(*input_cols)).write.format("noop").mode("overwrite").save()
            return (time.perf_counter() - t0) * 1_000

        for udf_fn, label in [(ip_udf, "inprocess_udf"), (pd_udf, "pandas_udf   ")]:
            print(
                f"  [{n_rows:>10,} rows, {n_cols} col(s)]  "
                f"{label.strip()}  warming up …",
                flush=True,
            )
            for _ in range(WARMUP_RUNS):
                run(udf_fn)
            samples = [run(udf_fn) for _ in range(ITERATIONS)]
            results.append({
                "label":  label,
                "n":      n_rows,
                "median": statistics.median(samples),
                "mean":   statistics.mean(samples),
                "min":    min(samples),
                "max":    max(samples),
            })

        df.unpersist()

    return results


def _print_results(results, scenario):
    header = (
        f"{'UDF type':<14}  {'rows':>10}  {'median ms':>10}  "
        f"{'mean ms':>9}  {'min ms':>8}  {'max ms':>8}"
    )
    sep = "=" * len(header)
    n_cols     = scenario["n_cols"]
    batch_size = scenario["batch_size"]
    dtype      = scenario.get("dtype", "long")
    if dtype == "large_string":
        str_len = scenario.get("str_len", 0)
        data_mb = n_cols * batch_size * str_len / 1_048_576
        size_note = f"~{data_mb:.0f} MB input/batch ({str_len}-char strings)"
    elif dtype == "string":
        # ~7 bytes average (cast of range id: "0".."9999999")
        data_mb = n_cols * batch_size * 7 / 1_048_576
        size_note = f"~{data_mb:.0f} MB input/batch (est.)"
    else:
        data_mb = n_cols * batch_size * 8 / 1_048_576
        size_note = f"{data_mb:.0f} MB input/batch"

    print(f"\n{sep}")
    print(f"  {scenario['name']}")
    print(
        f"  Cols: {n_cols}  |  Batch: {batch_size:,} rows  "
        f"({size_note})  |  "
        f"{ITERATIONS} runs + {WARMUP_RUNS} warmup  |  local[1]"
    )
    print(sep)
    print(header)
    print("-" * len(header))

    prev_n = None
    for r in results:
        if prev_n is not None and r["n"] != prev_n:
            print()
        print(
            f"{r['label']:<14}  {r['n']:>10,}  "
            f"{r['median']:>10.1f}  {r['mean']:>9.1f}  "
            f"{r['min']:>8.1f}  {r['max']:>8.1f}"
        )
        prev_n = r["n"]

    print(sep)
    print("\n  Speedup (pandas_udf median / inprocess_udf median):")
    iproc = [r for r in results if "inprocess" in r["label"]]
    pand  = [r for r in results if "pandas"    in r["label"]]
    for ip, pd in zip(iproc, pand):
        speedup = pd["median"] / ip["median"]
        faster  = "faster" if speedup > 1 else "slower"
        print(f"    {ip['n']:>10,} rows : {speedup:.2f}x  ({faster})")
    print()

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
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for scenario in SCENARIOS:
        print(f"\n{'─' * 60}")
        print(f"  {scenario['name']}")
        print(f"{'─' * 60}")
        results = _benchmark(spark, scenario)
        _print_results(results, scenario)

    spark.stop()


if __name__ == "__main__":
    main()
