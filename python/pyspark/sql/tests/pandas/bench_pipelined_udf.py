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
Benchmark: Pipelined vs synchronous JVM-Python UDF data transfer.

Compares end-to-end execution time of Python UDFs with
spark.python.udf.pipelined.enabled = true vs false.

Because spark.python.udf.pipelined.enabled is a SparkConf-level config (read at
SparkContext startup), each benchmark scenario runs in a separate subprocess with
its own SparkSession to ensure the config takes effect.

Note: In local[1] mode (single core), pipelined mode may show overhead because
the writer thread and selector thread compete for the same CPU. The benefit of
pipeline parallelism is expected on multi-core executors where serialization can
overlap with output reading.

Usage:
    cd $SPARK_HOME
    # Build Spark first (needed for PySpark to find JVM jars):
    #   build/sbt -Phive package
    #   cd python && zip -r lib/pyspark.zip pyspark && cd ..
    python python/pyspark/sql/tests/pandas/bench_pipelined_udf.py \
        [--rows N] [--iterations N] [--partitions N] [--sleep-ms N]
"""

import argparse
import json
import os
import subprocess
import sys


SPARK_HOME = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../..")
PIPELINED_CONF = "spark.python.udf.pipelined.enabled"
QUEUE_DEPTH_CONF = "spark.python.udf.pipelined.queueDepth"


# ---- Subprocess worker script template ----
# Each benchmark scenario is run in a fresh Python process to get a fresh SparkContext.
WORKER_TEMPLATE = '''
import os, sys, time, json
sys.path.insert(0, "{spark_home}")

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import LongType

spark = (
    SparkSession.builder.master("{master}")
    .appName("PipelinedUDFBench")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.python.worker.reuse", "true")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("{pipelined_conf}", "{pipelined}")
    .config("{queue_depth_conf}", "{queue_depth}")
    .getOrCreate()
)

{udf_code}

df = {make_df_code}

# Warmup
for _ in range({warmup}):
    df.write.format("noop").mode("overwrite").save()

# Timed runs
times = []
for _ in range({iterations}):
    start = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()
    elapsed = time.perf_counter() - start
    times.append(elapsed)

# Output results as JSON to stdout
print("BENCH_RESULT:" + json.dumps(times))
spark.stop()
'''


def run_subprocess(pipelined, udf_code, make_df_code, args):
    """Run a benchmark in a fresh subprocess, return list of timing results."""
    script = WORKER_TEMPLATE.format(
        spark_home=os.path.abspath(SPARK_HOME),
        master=args.master,
        pipelined_conf=PIPELINED_CONF,
        pipelined="true" if pipelined else "false",
        queue_depth_conf=QUEUE_DEPTH_CONF,
        queue_depth=args.queue_depth,
        udf_code=udf_code,
        make_df_code=make_df_code,
        warmup=args.warmup,
        iterations=args.iterations,
    )
    env = os.environ.copy()
    env["SPARK_HOME"] = os.path.abspath(SPARK_HOME)
    py4j_zip = os.path.join(os.path.abspath(SPARK_HOME), "python/lib/py4j-0.10.9.9-src.zip")
    pyspark_path = os.path.join(os.path.abspath(SPARK_HOME), "python")
    env["PYTHONPATH"] = f"{pyspark_path}:{py4j_zip}:" + env.get("PYTHONPATH", "")

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, env=env, timeout=600
    )

    for line in result.stdout.splitlines():
        if line.startswith("BENCH_RESULT:"):
            return json.loads(line[len("BENCH_RESULT:"):])

    print("  ERROR: no BENCH_RESULT in output")
    print("  STDERR (last 500 chars):", result.stderr[-500:] if result.stderr else "<empty>")
    return None


def print_stats(label, times):
    if not times:
        print(f"    {label:40s}  FAILED")
        return 0.0
    avg = sum(times) / len(times)
    mn = min(times)
    mx = max(times)
    print(
        f"    {label:40s}  "
        f"avg = {avg * 1000:8.1f} ms   "
        f"min = {mn * 1000:8.1f} ms   "
        f"max = {mx * 1000:8.1f} ms   "
        f"({len(times)} iters)"
    )
    return avg


def run_benchmark(label, udf_code, make_df_code, args):
    """Run sync and pipelined in separate subprocesses, print comparison."""
    print(f"  [{label}]")

    sync_times = run_subprocess(False, udf_code, make_df_code, args)
    sync_avg = print_stats("sync  (pipelined=false)", sync_times)

    pipe_times = run_subprocess(True, udf_code, make_df_code, args)
    pipe_avg = print_stats("pipelined (pipelined=true)", pipe_times)

    if pipe_avg > 0 and sync_avg > 0:
        speedup = sync_avg / pipe_avg
        diff_ms = (sync_avg - pipe_avg) * 1000
        marker = "faster" if speedup > 1.0 else "slower"
        print(f"    --> pipelined is {speedup:.2f}x {marker} ({diff_ms:+.1f} ms)")
    print()
    return sync_avg, pipe_avg


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark pipelined vs synchronous Python UDF data transfer"
    )
    parser.add_argument("--rows", type=int, default=1_000_000,
                        help="Rows for standard benchmarks (default: 1000000)")
    parser.add_argument("--large-rows", type=int, default=5_000_000,
                        help="Rows for large data benchmark (default: 5000000)")
    parser.add_argument("--iterations", type=int, default=5,
                        help="Timed iterations per scenario (default: 5)")
    parser.add_argument("--warmup", type=int, default=2,
                        help="Warmup iterations (default: 2)")
    parser.add_argument("--partitions", type=int, default=1,
                        help="Number of partitions (default: 1)")
    parser.add_argument("--sleep-ms", type=float, default=10.0,
                        help="Sleep time in ms per batch for heavy UDF (default: 10.0)")
    parser.add_argument("--queue-depth", type=int, default=2,
                        help="Pipelined queue depth (default: 2)")
    parser.add_argument("--master", type=str, default="local[1]",
                        help="Spark master URL (default: local[1])")
    args = parser.parse_args()

    nparts = args.partitions

    print("=" * 78)
    print("  Pipelined vs Synchronous Python UDF Data Transfer Benchmark")
    print("=" * 78)
    print(f"  master={args.master}  rows={args.rows}  large_rows={args.large_rows}  "
          f"partitions={nparts}")
    print(f"  iterations={args.iterations}  warmup={args.warmup}  "
          f"sleep_ms={args.sleep_ms}  queue_depth={args.queue_depth}")
    print()

    # --- Benchmark 1: Light UDF ---
    run_benchmark(
        "Light UDF (x + 1)",
        udf_code="""
@pandas_udf(LongType())
def bench_udf(x: pd.Series) -> pd.Series:
    return x + 1
""",
        make_df_code=f'spark.range({args.rows}, numPartitions={nparts})'
                     f'.select(col("id"), bench_udf(col("id")).alias("result"))',
        args=args,
    )

    # --- Benchmark 2: CPU-bound UDF ---
    run_benchmark(
        "CPU-bound UDF (iterative computation)",
        udf_code="""
@pandas_udf(LongType())
def bench_udf(x: pd.Series) -> pd.Series:
    result = x + 1
    for _ in range(20):
        result = result + (x % 7) - 3
    return result
""",
        make_df_code=f'spark.range({args.rows}, numPartitions={nparts})'
                     f'.select(col("id"), bench_udf(col("id")).alias("result"))',
        args=args,
    )

    # --- Benchmark 3: Heavy UDF (sleep) ---
    run_benchmark(
        f"Heavy UDF ({args.sleep_ms}ms sleep/batch)",
        udf_code=f"""
import time as _time
@pandas_udf(LongType())
def bench_udf(x: pd.Series) -> pd.Series:
    _time.sleep({args.sleep_ms / 1000.0})
    return x + 1
""",
        make_df_code=f'spark.range({args.rows}, numPartitions={nparts})'
                     f'.select(col("id"), bench_udf(col("id")).alias("result"))',
        args=args,
    )

    # --- Benchmark 4: Large data ---
    run_benchmark(
        f"Large data ({args.large_rows} rows, x + 1)",
        udf_code="""
@pandas_udf(LongType())
def bench_udf(x: pd.Series) -> pd.Series:
    return x + 1
""",
        make_df_code=f'spark.range({args.large_rows}, numPartitions={nparts})'
                     f'.select(col("id"), bench_udf(col("id")).alias("result"))',
        args=args,
    )

    # --- Benchmark 5: Multiple UDF columns ---
    run_benchmark(
        "Multi-UDF (3 UDF columns)",
        udf_code="""
@pandas_udf(LongType())
def udf_a(x: pd.Series) -> pd.Series:
    return x + 1

@pandas_udf(LongType())
def udf_b(x: pd.Series) -> pd.Series:
    return x * 2

@pandas_udf(LongType())
def udf_c(x: pd.Series) -> pd.Series:
    return x - 1
""",
        make_df_code=f'spark.range({args.rows}, numPartitions={nparts})'
                     f'.select(col("id"), udf_a(col("id")).alias("a"), '
                     f'udf_b(col("id")).alias("b"), udf_c(col("id")).alias("c"))',
        args=args,
    )

    print("=" * 78)
    print("  Benchmark complete.")
    print("=" * 78)


if __name__ == "__main__":
    main()
