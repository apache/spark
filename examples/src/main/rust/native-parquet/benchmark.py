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

The runs of the readers alternate, in an order that rotates each round, so that a change in the
load of the host or in the state of the JIT compiler affects all readers alike. For each reader,
the script prints the median, minimum, and maximum of the wall-clock times, the median of the
CPU time of the tasks, and the median of the ratios of its times to those of Spark's reader in the
same round. The CPU time, from the task metrics of Spark, includes the native code, which runs on
the threads of the tasks, and depends less than the wall-clock time on other processes of the host.
"""

import argparse
import json
from pathlib import Path
import statistics
import time
import urllib.error
import urllib.request

from pyspark.sql import SparkSession

# `k` selects 1% of the rows for each value, in every row group, and `payload` is a 64-character
# string. `id` grows within each file, so that the statistics of the row groups bound it.
COLUMNS = ["id", "cast(id % 100 as int) as k", "repeat(md5(cast(id as string)), 2) as payload"]

QUERIES = {
    "one column": "SELECT sum(id) FROM {table}",
    "all columns": "SELECT sum(id), sum(k), max(length(payload)) FROM {table}",
    "10% of the rows": "SELECT sum(id), max(length(payload)) FROM {table} WHERE k < 10",
    "1% of the rows": "SELECT sum(id), max(length(payload)) FROM {table} WHERE k = 7",
    # Spark's reader skips the row groups whose statistics exclude the rows, and this source does
    # not, so this query shows what the row filter does not replace.
    "1% by id": "SELECT sum(id), max(length(payload)) FROM {table} WHERE id < {rows} DIV 100",
}

TABLES = ["spark_reader", "rust_row_filter", "rust_no_filter"]


def task_metrics(spark, group):
    """Returns the CPU and GC seconds of the tasks of the jobs of a job group."""
    tracker = spark.sparkContext.statusTracker()
    stages = sorted(
        stage
        for job in tracker.getJobIdsForGroup(group)
        for stage in tracker.getJobInfo(job).stageIds
    )
    url = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}"
    cpu_ns = gc_ms = 0
    for stage in stages:
        # The status store receives the metrics of a stage asynchronously, after the job ends.
        for _ in range(100):
            try:
                with urllib.request.urlopen(f"{url}/stages/{stage}?details=false") as response:
                    attempts = json.load(response)
            except urllib.error.HTTPError as e:
                # A stage that the job skipped, because it reused shuffle files, is unknown.
                if e.code != 404:
                    raise
                attempts = [{"status": "SKIPPED", "executorCpuTime": 0, "jvmGcTime": 0}]
            if all(a["status"] in ("COMPLETE", "SKIPPED") for a in attempts):
                break
            time.sleep(0.05)
        else:
            raise RuntimeError(f"stage {stage} did not complete: {attempts}")
        for attempt in attempts:
            cpu_ns += attempt["executorCpuTime"]
            gc_ms += attempt["jvmGcTime"]
    return cpu_ns / 1e9, gc_ms / 1e3


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True, help="Written if it does not exist")
    parser.add_argument("--rows", type=int, default=20_000_000)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--queries", nargs="*", choices=list(QUERIES), default=list(QUERIES))
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Rust native Parquet source benchmark").getOrCreate()
    try:
        spark.addArtifact(str(args.package.resolve(strict=True)))
        data = str(args.data.resolve())
        if not args.data.exists():
            spark.range(args.rows).selectExpr(*COLUMNS).write.parquet(data)
        rows = spark.read.parquet(data).count()
        spark.read.parquet(data).createOrReplaceTempView("spark_reader")
        native = spark.read.format("rust_sdk_parquet")
        native.load(data).createOrReplaceTempView("rust_row_filter")
        native.option("rowFilter", "false").load(data).createOrReplaceTempView("rust_no_filter")
        print(f"{rows} rows, {spark.sparkContext.defaultParallelism} cores")

        sequence = 0

        def run(query, table):
            nonlocal sequence
            sequence += 1
            group = f"benchmark-{sequence}"
            spark.sparkContext.setJobGroup(group, f"{table}: {query}")
            # A new DataFrame for each run: one that ran already reuses its shuffle files.
            start = time.perf_counter()
            result = tuple(spark.sql(query.format(table=table, rows=rows)).collect()[0])
            wall = time.perf_counter() - start
            return result, wall, *task_metrics(spark, group)

        header = "median s     min     max  cpu s   gc s  vs Spark"
        for name in args.queries:
            query = QUERIES[name]
            results = set()
            # The first rounds warm up the JIT compiler and the page cache.
            for _ in range(args.warmups):
                for table in TABLES:
                    results.add(run(query, table)[0])
            samples = {table: [] for table in TABLES}
            for rnd in range(args.runs):
                for i in range(len(TABLES)):
                    table = TABLES[(rnd + i) % len(TABLES)]
                    result, *metrics = run(query, table)
                    results.add(result)
                    samples[table].append(metrics)
            assert len(results) == 1, (name, results)

            print(f"\n{name}: {query.format(table='t', rows=rows)}")
            print(f"  {'':16} {header}")
            base = [wall for wall, _, _ in samples[TABLES[0]]]
            for table in TABLES:
                walls, cpus, gcs = zip(*samples[table])
                ratio = statistics.median(w / b for w, b in zip(walls, base))
                print(
                    f"  {table:16} {statistics.median(walls):8.3f} {min(walls):7.3f} "
                    f"{max(walls):7.3f} {statistics.median(cpus):6.2f} "
                    f"{statistics.median(gcs):6.2f} {ratio:9.3f}"
                )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
