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
import unittest
from decimal import Decimal

from pyspark.sql import functions as sf
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    LongType,
    StructField,
    StructType,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pyarrow,
    pyarrow_requirement_message,
)


if have_pyarrow:
    from pyspark.sql.aggregator import Aggregator, udaf

    class Mean(Aggregator):
        @property
        def bufferSchema(self):
            return StructType([StructField("sum", DoubleType()), StructField("count", LongType())])

        @property
        def outputType(self):
            return DoubleType()

        def zero(self):
            return (0.0, 0)

        def reduce(self, buffer, value):
            (v,) = value
            if v is None:  # ignore nulls, like SQL avg
                return buffer
            return (buffer[0] + v, buffer[1] + 1)

        def merge(self, b1, b2):
            return (b1[0] + b2[0], b1[1] + b2[1])

        def finish(self, buffer):
            return buffer[0] / buffer[1] if buffer[1] else None

    class DecimalSum(Aggregator):
        # Non-trivial output/buffer type: the result column and the intermediate buffer are both
        # DecimalType, exercising explicit Arrow typing of the emitted arrays (a bare
        # ``pa.array([Decimal(...)])`` would infer a decimal type whose precision/scale need not
        # match the declared one).
        @property
        def bufferSchema(self):
            return StructType([StructField("total", DecimalType(20, 4))])

        @property
        def outputType(self):
            return DecimalType(20, 4)

        def zero(self):
            return (Decimal(0),)

        def reduce(self, buffer, value):
            (v,) = value
            return buffer if v is None else (buffer[0] + Decimal(str(v)),)

        def merge(self, b1, b2):
            return (b1[0] + b2[0],)

        def finish(self, buffer):
            return buffer[0]

    class SumSquares(Aggregator):
        @property
        def bufferSchema(self):
            return StructType([StructField("sumsq", DoubleType())])

        @property
        def outputType(self):
            return DoubleType()

        def zero(self):
            return (0.0,)

        def reduce(self, buffer, value):
            (v,) = value
            return (buffer[0] + float(v) * float(v),)

        def merge(self, b1, b2):
            return (b1[0] + b2[0],)

        def finish(self, buffer):
            return buffer[0]


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowPythonAggregatorTestsMixin:
    def _data(self):
        # 100 rows across 5 keys; repartition so each key is split across partitions,
        # exercising map-side PARTIAL combine + post-shuffle FINAL merge.
        return (
            self.spark.range(0, 100)
            .select((sf.col("id") % 5).alias("k"), sf.col("id").cast("double").alias("v"))
            .repartition(4, sf.col("v") % 3)
        )

    def test_incremental_aggregator_matches_builtin_mean(self):
        df = self._data()
        result = df.groupBy("k").agg(udaf(Mean())(sf.col("v")).alias("m")).orderBy("k").collect()
        expected = df.groupBy("k").agg(sf.avg("v").alias("m")).orderBy("k").collect()
        got = {r["k"]: r["m"] for r in result}
        exp = {r["k"]: r["m"] for r in expected}
        self.assertEqual(got, exp)

    def test_incremental_aggregator_no_group(self):
        df = self._data()
        result = df.agg(udaf(Mean())(sf.col("v")).alias("m")).collect()
        expected = df.agg(sf.avg("v").alias("m")).collect()
        self.assertAlmostEqual(result[0]["m"], expected[0]["m"], places=6)

    def test_incremental_aggregator_empty_global_input(self):
        # A global aggregation over empty input must still return one identity row: finish(zero).
        empty = self._data().limit(0)
        result = empty.agg(udaf(Mean())(sf.col("v")).alias("m")).collect()
        expected = empty.agg(sf.avg("v").alias("m")).collect()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["m"], expected[0]["m"])

    def test_incremental_aggregator_custom_buffer(self):
        df = self._data()
        result = (
            df.groupBy("k").agg(udaf(SumSquares())(sf.col("v")).alias("s")).orderBy("k").collect()
        )
        expected = (
            df.groupBy("k").agg(sf.sum(sf.col("v") * sf.col("v")).alias("s")).orderBy("k").collect()
        )
        got = {r["k"]: r["s"] for r in result}
        exp = {r["k"]: r["s"] for r in expected}
        for k in exp:
            self.assertAlmostEqual(got[k], exp[k], places=6)

    def test_incremental_aggregator_decimal_output(self):
        # Non-trivial output/buffer type (DecimalType), crossing the shuffle as a decimal buffer
        # and emitted as a decimal result -- guards the explicit Arrow typing of both stages.
        df = self._data()
        result = (
            df.groupBy("k").agg(udaf(DecimalSum())(sf.col("v")).alias("s")).orderBy("k").collect()
        )
        expected = df.groupBy("k").agg(sf.sum("v").alias("s")).orderBy("k").collect()
        got = {r["k"]: r["s"] for r in result}
        exp = {r["k"]: r["s"] for r in expected}
        for k in exp:
            self.assertIsInstance(got[k], Decimal)
            self.assertAlmostEqual(float(got[k]), exp[k], places=4)

    def test_incremental_aggregator_null_inputs(self):
        # reduce must tolerate null input values; the null-skipping Mean should match SQL avg,
        # including a group whose values are all null (identity buffer -> finish returns None).
        df = self.spark.createDataFrame(
            [("a", 1.0), ("a", None), ("a", 3.0), ("b", None), ("b", None)],
            "k string, v double",
        )
        result = df.groupBy("k").agg(udaf(Mean())(sf.col("v")).alias("m")).orderBy("k").collect()
        expected = df.groupBy("k").agg(sf.avg("v").alias("m")).orderBy("k").collect()
        got = {r["k"]: r["m"] for r in result}
        exp = {r["k"]: r["m"] for r in expected}
        self.assertEqual(got, exp)

    def test_multiple_incremental_aggregators(self):
        # Two aggregators with different buffer schemas over the same input in one agg call.
        df = self._data()
        result = (
            df.groupBy("k")
            .agg(
                udaf(Mean())(sf.col("v")).alias("m"),
                udaf(SumSquares())(sf.col("v")).alias("s"),
            )
            .orderBy("k")
            .collect()
        )
        expected = (
            df.groupBy("k")
            .agg(
                sf.avg("v").alias("m"),
                sf.sum(sf.col("v") * sf.col("v")).alias("s"),
            )
            .orderBy("k")
            .collect()
        )
        got_m = {r["k"]: r["m"] for r in result}
        got_s = {r["k"]: r["s"] for r in result}
        for r in expected:
            self.assertAlmostEqual(got_m[r["k"]], r["m"], places=6)
            self.assertAlmostEqual(got_s[r["k"]], r["s"], places=6)

    def test_result_independent_of_partition_count(self):
        # Partial buffers must merge to the same result regardless of how keys are split.
        base = self.spark.range(0, 60).select(
            (sf.col("id") % 3).alias("k"), sf.col("id").cast("double").alias("v")
        )
        results = []
        for n in (1, 2, 7):
            rows = (
                base.repartition(n, sf.col("v"))
                .groupBy("k")
                .agg(udaf(Mean())(sf.col("v")).alias("m"))
                .orderBy("k")
                .collect()
            )
            results.append({r["k"]: r["m"] for r in rows})
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[1], results[2])

    def test_sql_registration(self):
        # Register the aggregator and invoke it from SQL text.
        df = self._data()
        df.createOrReplaceTempView("agg_input")
        self.spark.udf.register("my_mean", udaf(Mean()))
        result = self.spark.sql(
            "SELECT k, my_mean(v) AS m FROM agg_input GROUP BY k ORDER BY k"
        ).collect()
        expected = df.groupBy("k").agg(sf.avg("v").alias("m")).orderBy("k").collect()
        got = {r["k"]: r["m"] for r in result}
        exp = {r["k"]: r["m"] for r in expected}
        self.assertEqual(got, exp)


class ArrowPythonAggregatorTests(ArrowPythonAggregatorTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
