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

from pyspark.sql import functions as sf
from pyspark.sql.types import (
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
            return StructType(
                [StructField("sum", DoubleType()), StructField("count", LongType())]
            )

        @property
        def outputType(self):
            return DoubleType()

        def zero(self):
            return (0.0, 0)

        def reduce(self, buffer, value):
            (v,) = value
            return (buffer[0] + (v or 0.0), buffer[1] + 1)

        def merge(self, b1, b2):
            return (b1[0] + b2[0], b1[1] + b2[1])

        def finish(self, buffer):
            return buffer[0] / buffer[1] if buffer[1] else None

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
        result = (
            df.groupBy("k").agg(udaf(Mean())(sf.col("v")).alias("m")).orderBy("k").collect()
        )
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
            df.groupBy("k")
            .agg(udaf(SumSquares())(sf.col("v")).alias("s"))
            .orderBy("k")
            .collect()
        )
        expected = (
            df.groupBy("k").agg(sf.sum(sf.col("v") * sf.col("v")).alias("s")).orderBy("k").collect()
        )
        got = {r["k"]: r["s"] for r in result}
        exp = {r["k"]: r["s"] for r in expected}
        for k in exp:
            self.assertAlmostEqual(got[k], exp[k], places=6)

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
    from pyspark.sql.tests.arrow.test_arrow_python_aggregator import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
