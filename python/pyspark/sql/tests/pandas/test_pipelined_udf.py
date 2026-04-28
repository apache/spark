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
Tests for pipelined Python UDF execution mode.

These tests run with spark.python.udf.pipelined.enabled=true to verify
correctness of the pipelined data transfer path for various UDF types.
"""

import os
import unittest

from pyspark import SparkConf
from pyspark.sql.functions import col, pandas_udf, udf
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructType,
    StructField,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

if have_pandas:
    import pandas as pd


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class PipelinedUDFTests(ReusedSQLTestCase):
    """Tests that run with pipelined mode enabled."""

    @classmethod
    def conf(cls):
        return (
            SparkConf()
            .set("spark.python.udf.pipelined.enabled", "true")
            .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        )

    def test_pipelined_mode_is_active(self):
        """Verify the pipelined code path is actually being used."""

        @pandas_udf(StringType())
        def check_env(x: pd.Series) -> pd.Series:
            flag = os.environ.get("SPARK_PIPELINED_UDF_ACTIVE", "not_set")
            return pd.Series([flag] * len(x))

        result = self.spark.range(1).select(check_env(col("id"))).first()[0]
        self.assertEqual(result, "1", "pipelined_process() should set SPARK_PIPELINED_UDF_ACTIVE=1")

    def test_scalar_arrow_udf(self):
        """Basic scalar Arrow UDF with pipelined mode."""

        @pandas_udf(LongType())
        def add_one(x: pd.Series) -> pd.Series:
            return x + 1

        result = self.spark.range(100).select(
            col("id"), add_one(col("id")).alias("result")
        ).collect()
        for row in result:
            self.assertEqual(row.result, row.id + 1)

    def test_scalar_arrow_udf_string(self):
        """Scalar Arrow UDF with string type."""

        @pandas_udf(StringType())
        def to_str(x: pd.Series) -> pd.Series:
            return x.astype(str) + "_val"

        result = self.spark.range(50).select(
            col("id"), to_str(col("id")).alias("s")
        ).collect()
        for row in result:
            self.assertEqual(row.s, f"{row.id}_val")

    def test_multiple_udf_columns(self):
        """Multiple UDF columns in a single query."""

        @pandas_udf(LongType())
        def udf_a(x: pd.Series) -> pd.Series:
            return x + 1

        @pandas_udf(LongType())
        def udf_b(x: pd.Series) -> pd.Series:
            return x * 2

        @pandas_udf(LongType())
        def udf_c(x: pd.Series) -> pd.Series:
            return x - 1

        result = self.spark.range(100).select(
            col("id"),
            udf_a(col("id")).alias("a"),
            udf_b(col("id")).alias("b"),
            udf_c(col("id")).alias("c"),
        ).collect()
        for row in result:
            self.assertEqual(row.a, row.id + 1)
            self.assertEqual(row.b, row.id * 2)
            self.assertEqual(row.c, row.id - 1)

    def test_multiple_partitions(self):
        """UDF across multiple partitions."""

        @pandas_udf(LongType())
        def add_one(x: pd.Series) -> pd.Series:
            return x + 1

        result = self.spark.range(1000, numPartitions=4).select(
            col("id"), add_one(col("id")).alias("result")
        ).collect()
        self.assertEqual(len(result), 1000)
        for row in result:
            self.assertEqual(row.result, row.id + 1)

    def test_empty_partition(self):
        """UDF with some empty partitions."""

        @pandas_udf(LongType())
        def add_one(x: pd.Series) -> pd.Series:
            return x + 1

        # 2 rows across 4 partitions means some partitions are empty
        result = (
            self.spark.range(2, numPartitions=4)
            .select(col("id"), add_one(col("id")).alias("result"))
            .collect()
        )
        self.assertEqual(len(result), 2)
        for row in result:
            self.assertEqual(row.result, row.id + 1)

    def test_chained_udf(self):
        """Chained UDF calls."""

        @pandas_udf(LongType())
        def add_one(x: pd.Series) -> pd.Series:
            return x + 1

        @pandas_udf(LongType())
        def double_it(x: pd.Series) -> pd.Series:
            return x * 2

        result = (
            self.spark.range(50).select(double_it(add_one(col("id"))).alias("result")).collect()
        )
        for row in result:
            self.assertEqual(row.result, (row.result // 2) * 2)  # even number

    def test_udf_with_null(self):
        """UDF handling null values."""

        @pandas_udf(LongType())
        def add_one(x: pd.Series) -> pd.Series:
            return x + 1

        df = self.spark.createDataFrame(
            [(1,), (None,), (3,), (None,), (5,)],
            schema=StructType([StructField("v", LongType(), True)]),
        )
        result = df.select(col("v"), add_one(col("v")).alias("result")).collect()
        expected = [(1, 2), (None, None), (3, 4), (None, None), (5, 6)]
        for row, (v, r) in zip(result, expected):
            self.assertEqual(row.v, v)
            self.assertEqual(row.result, r)

    def test_grouped_agg_udf(self):
        """Grouped aggregation UDF (UDAF) with pipelined mode."""

        @pandas_udf(DoubleType())
        def mean_udf(x: pd.Series) -> float:
            return float(x.mean())

        df = self.spark.range(100).selectExpr("id", "id % 5 as grp")
        result = df.groupBy("grp").agg(mean_udf(col("id")).alias("avg")).orderBy("grp").collect()

        self.assertEqual(len(result), 5)
        # grp=0: mean of 0,5,10,...,95 = 47.5
        self.assertAlmostEqual(result[0].avg, 47.5)

    def test_scalar_udf_large_data(self):
        """Scalar UDF with large data to exercise backpressure."""

        @pandas_udf(LongType())
        def add_one(x: pd.Series) -> pd.Series:
            return x + 1

        result = (
            self.spark.range(500000)
            .select(add_one(col("id")).alias("result"))
            .agg({"result": "sum"})
            .collect()
        )
        # sum of (1..500000) = 500000 * 500001 / 2 = 125000250000
        self.assertEqual(result[0][0], 125000250000)

    def test_batched_udf(self):
        """Non-Arrow batched UDF (pickle serialization)."""

        @udf(StringType())
        def simple_udf(x):
            return str(x) + "_done"

        result = self.spark.range(50).select(col("id"), simple_udf(col("id")).alias("s")).collect()
        for row in result:
            self.assertEqual(row.s, f"{row.id}_done")

    def test_udf_exception_propagation(self):
        """UDF that raises an exception should propagate correctly."""

        @pandas_udf(LongType())
        def bad_udf(x: pd.Series) -> pd.Series:
            raise ValueError("intentional error")

        with self.assertRaisesRegex(Exception, "intentional error"):
            self.spark.range(10).select(bad_udf(col("id"))).collect()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
