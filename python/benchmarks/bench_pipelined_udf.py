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
End-to-end benchmarks for pipelined vs synchronous Python UDF execution.

Unlike the microbenchmarks in bench_eval_type.py (which test the Python worker
in isolation), these benchmarks run full Spark queries through a real
SparkSession to measure the JVM-Python socket I/O pipeline overlap.
"""

import pandas as pd

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType, StringType


class _PipelinedUDFBenchBase:
    """Base class for pipelined UDF benchmarks.

    Each benchmark parameterizes over pipelined=true/false to compare
    the two execution modes. SparkSession is created in setup() and
    stopped in teardown() because spark.python.udf.pipelined.enabled
    is a SparkConf-level config.
    """

    # Subclasses must define timeout (seconds per benchmark iteration).
    timeout = 120

    def _spark_conf(self, pipelined):
        return (
            SparkConf()
            .setMaster("local[1]")
            .setAppName("PipelinedUDFBench")
            .set("spark.sql.execution.arrow.pyspark.enabled", "true")
            .set("spark.python.worker.reuse", "true")
            .set("spark.ui.enabled", "false")
            .set("spark.sql.shuffle.partitions", "1")
            .set("spark.python.udf.pipelined.enabled", str(pipelined).lower())
        )

    def _setup_spark(self, pipelined):
        conf = self._spark_conf(pipelined)
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

        @pandas_udf(LongType())
        def _add_one(x: pd.Series) -> pd.Series:
            return x + 1

        self._add_one = _add_one

        # Warmup: start Python worker, JIT
        self.spark.range(100).select(_add_one(col("id"))).write.format("noop").mode(
            "overwrite"
        ).save()

    def _teardown_spark(self):
        if hasattr(self, "spark"):
            self.spark.stop()
            # Clear the active session so the next setup() creates a fresh one
            SparkSession.builder._options = {}


class ScalarUDFTimeBench(_PipelinedUDFBenchBase):
    """Benchmark scalar Arrow UDF with light computation (x + 1)."""

    params = [[False, True], [100000, 1000000]]
    param_names = ["pipelined", "n_rows"]

    def setup(self, pipelined, n_rows):
        self._setup_spark(pipelined)

    def teardown(self, pipelined, n_rows):
        self._teardown_spark()

    def time_scalar_udf(self, pipelined, n_rows):
        self.spark.range(n_rows).select(self._add_one(col("id")).alias("result")).write.format(
            "noop"
        ).mode("overwrite").save()

    def peakmem_scalar_udf(self, pipelined, n_rows):
        self.spark.range(n_rows).select(self._add_one(col("id")).alias("result")).write.format(
            "noop"
        ).mode("overwrite").save()


class MultiUDFTimeBench(_PipelinedUDFBenchBase):
    """Benchmark multiple UDF columns in a single query."""

    params = [[False, True], [100000, 1000000]]
    param_names = ["pipelined", "n_rows"]

    def setup(self, pipelined, n_rows):
        self._setup_spark(pipelined)

        @pandas_udf(LongType())
        def _mul_two(x: pd.Series) -> pd.Series:
            return x * 2

        @pandas_udf(LongType())
        def _sub_one(x: pd.Series) -> pd.Series:
            return x - 1

        self._mul_two = _mul_two
        self._sub_one = _sub_one

    def teardown(self, pipelined, n_rows):
        self._teardown_spark()

    def time_multi_udf(self, pipelined, n_rows):
        self.spark.range(n_rows).select(
            col("id"),
            self._add_one(col("id")).alias("a"),
            self._mul_two(col("id")).alias("b"),
            self._sub_one(col("id")).alias("c"),
        ).write.format("noop").mode("overwrite").save()

    def peakmem_multi_udf(self, pipelined, n_rows):
        self.spark.range(n_rows).select(
            col("id"),
            self._add_one(col("id")).alias("a"),
            self._mul_two(col("id")).alias("b"),
            self._sub_one(col("id")).alias("c"),
        ).write.format("noop").mode("overwrite").save()


class LargeDataUDFTimeBench(_PipelinedUDFBenchBase):
    """Benchmark scalar UDF with large data to exercise throughput."""

    params = [[False, True]]
    param_names = ["pipelined"]

    def setup(self, pipelined):
        self._setup_spark(pipelined)

    def teardown(self, pipelined):
        self._teardown_spark()

    def time_large_data(self, pipelined):
        self.spark.range(5000000).select(self._add_one(col("id")).alias("result")).write.format(
            "noop"
        ).mode("overwrite").save()

    def peakmem_large_data(self, pipelined):
        self.spark.range(5000000).select(self._add_one(col("id")).alias("result")).write.format(
            "noop"
        ).mode("overwrite").save()


class WideRowUDFTimeBench(_PipelinedUDFBenchBase):
    """Benchmark scalar UDF with larger per-batch in-memory size.

    Each row carries a wide string payload and the Arrow batch size is bumped so
    one batch is ~10-50 MB rather than ~80 KB. This exercises the regime that
    Yicong-Huang asked about in the SPARK-56642 review: how does pipelined mode
    behave when each batch is large enough that the queue's memory overhead is
    no longer negligible?
    """

    # (pipelined, n_rows, payload_chars, records_per_batch)
    # 50_000 rows * 1024 chars = ~50 MB raw per dataset; with records_per_batch
    # = 10_000 that's ~10 MB per Arrow batch.
    params = [
        [False, True],
        [(50_000, 1024, 10_000), (50_000, 4096, 5_000)],
    ]
    param_names = ["pipelined", "shape"]

    def setup(self, pipelined, shape):
        n_rows, payload_chars, records_per_batch = shape
        self._setup_spark(pipelined)
        self.spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(records_per_batch))
        self._n_rows = n_rows
        self._payload_chars = payload_chars

        @pandas_udf(StringType())
        def _wide_passthrough(s: pd.Series) -> pd.Series:
            # Non-trivial work proportional to row width so the UDF actually
            # holds a batch for a measurable amount of time.
            return s.str.upper()

        self._wide_udf = _wide_passthrough

    def teardown(self, pipelined, shape):
        self._teardown_spark()

    def _make_df(self):
        chars = self._payload_chars

        @pandas_udf(StringType())
        def _make_payload(x: pd.Series) -> pd.Series:
            return pd.Series(["x" * chars] * len(x))

        return self.spark.range(self._n_rows).select(_make_payload(col("id")).alias("payload"))

    def time_wide_row_udf(self, pipelined, shape):
        self._make_df().select(self._wide_udf(col("payload")).alias("result")).write.format(
            "noop"
        ).mode("overwrite").save()

    def peakmem_wide_row_udf(self, pipelined, shape):
        self._make_df().select(self._wide_udf(col("payload")).alias("result")).write.format(
            "noop"
        ).mode("overwrite").save()
