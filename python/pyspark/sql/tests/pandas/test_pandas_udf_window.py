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
from typing import cast

from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import (
    array,
    explode,
    col,
    lit,
    mean,
    min,
    max,
    rank,
    udf,
    pandas_udf,
    PandasUDFType,
)
from pyspark.sql.window import Window
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing.utils import QuietTest

if have_pandas:
    from pandas.testing import assert_frame_equal


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class WindowPandasUDFTests(ReusedSQLTestCase):
    @property
    def data(self):
        return (
            self.spark.range(10)
            .toDF("id")
            .withColumn("vs", array([lit(i * 1.0) + col("id") for i in range(20, 30)]))
            .withColumn("v", explode(col("vs")))
            .drop("vs")
            .withColumn("w", lit(1.0))
        )

    @property
    def python_plus_one(self):
        @udf("double")
        def plus_one(v):
            assert isinstance(v, float)
            return v + 1

        return plus_one

    @property
    def pandas_scalar_time_two(self):
        return pandas_udf(lambda v: v * 2, "double")

    @property
    def pandas_agg_count_udf(self):
        @pandas_udf("long", PandasUDFType.GROUPED_AGG)
        def count(v):
            return len(v)

        return count

    @property
    def pandas_agg_mean_udf(self):
        @pandas_udf("double", PandasUDFType.GROUPED_AGG)
        def avg(v):
            return v.mean()

        return avg

    @property
    def pandas_agg_max_udf(self):
        @pandas_udf("double", PandasUDFType.GROUPED_AGG)
        def max(v):
            return v.max()

        return max

    @property
    def pandas_agg_min_udf(self):
        @pandas_udf("double", PandasUDFType.GROUPED_AGG)
        def min(v):
            return v.min()

        return min

    @property
    def unbounded_window(self):
        return (
            Window.partitionBy("id")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            .orderBy("v")
        )

    @property
    def ordered_window(self):
        return Window.partitionBy("id").orderBy("v")

    @property
    def unpartitioned_window(self):
        return Window.partitionBy()

    @property
    def sliding_row_window(self):
        return Window.partitionBy("id").orderBy("v").rowsBetween(-2, 1)

    @property
    def sliding_range_window(self):
        return Window.partitionBy("id").orderBy("v").rangeBetween(-2, 4)

    @property
    def growing_row_window(self):
        return Window.partitionBy("id").orderBy("v").rowsBetween(Window.unboundedPreceding, 3)

    @property
    def growing_range_window(self):
        return Window.partitionBy("id").orderBy("v").rangeBetween(Window.unboundedPreceding, 4)

    @property
    def shrinking_row_window(self):
        return Window.partitionBy("id").orderBy("v").rowsBetween(-2, Window.unboundedFollowing)

    @property
    def shrinking_range_window(self):
        return Window.partitionBy("id").orderBy("v").rangeBetween(-3, Window.unboundedFollowing)

    def test_simple(self):
        df = self.data
        w = self.unbounded_window

        mean_udf = self.pandas_agg_mean_udf

        result1 = df.withColumn("mean_v", mean_udf(df["v"]).over(w))
        expected1 = df.withColumn("mean_v", mean(df["v"]).over(w))

        result2 = df.select(mean_udf(df["v"]).over(w))
        expected2 = df.select(mean(df["v"]).over(w))

        assert_frame_equal(expected1.toPandas(), result1.toPandas())
        assert_frame_equal(expected2.toPandas(), result2.toPandas())

    def test_multiple_udfs(self):
        df = self.data
        w = self.unbounded_window

        result1 = (
            df.withColumn("mean_v", self.pandas_agg_mean_udf(df["v"]).over(w))
            .withColumn("max_v", self.pandas_agg_max_udf(df["v"]).over(w))
            .withColumn("min_w", self.pandas_agg_min_udf(df["w"]).over(w))
        )

        expected1 = (
            df.withColumn("mean_v", mean(df["v"]).over(w))
            .withColumn("max_v", max(df["v"]).over(w))
            .withColumn("min_w", min(df["w"]).over(w))
        )

        assert_frame_equal(expected1.toPandas(), result1.toPandas())

    def test_replace_existing(self):
        df = self.data
        w = self.unbounded_window

        result1 = df.withColumn("v", self.pandas_agg_mean_udf(df["v"]).over(w))
        expected1 = df.withColumn("v", mean(df["v"]).over(w))

        assert_frame_equal(expected1.toPandas(), result1.toPandas())

    def test_mixed_sql(self):
        df = self.data
        w = self.unbounded_window
        mean_udf = self.pandas_agg_mean_udf

        result1 = df.withColumn("v", mean_udf(df["v"] * 2).over(w) + 1)
        expected1 = df.withColumn("v", mean(df["v"] * 2).over(w) + 1)

        assert_frame_equal(expected1.toPandas(), result1.toPandas())

    def test_mixed_udf(self):
        df = self.data
        w = self.unbounded_window

        plus_one = self.python_plus_one
        time_two = self.pandas_scalar_time_two
        mean_udf = self.pandas_agg_mean_udf

        result1 = df.withColumn("v2", plus_one(mean_udf(plus_one(df["v"])).over(w)))
        expected1 = df.withColumn("v2", plus_one(mean(plus_one(df["v"])).over(w)))

        result2 = df.withColumn("v2", time_two(mean_udf(time_two(df["v"])).over(w)))
        expected2 = df.withColumn("v2", time_two(mean(time_two(df["v"])).over(w)))

        assert_frame_equal(expected1.toPandas(), result1.toPandas())
        assert_frame_equal(expected2.toPandas(), result2.toPandas())

    def test_without_partitionBy(self):
        df = self.data
        w = self.unpartitioned_window
        mean_udf = self.pandas_agg_mean_udf

        result1 = df.withColumn("v2", mean_udf(df["v"]).over(w))
        expected1 = df.withColumn("v2", mean(df["v"]).over(w))

        result2 = df.select(mean_udf(df["v"]).over(w))
        expected2 = df.select(mean(df["v"]).over(w))

        assert_frame_equal(expected1.toPandas(), result1.toPandas())
        assert_frame_equal(expected2.toPandas(), result2.toPandas())

    def test_mixed_sql_and_udf(self):
        df = self.data
        w = self.unbounded_window
        ow = self.ordered_window
        max_udf = self.pandas_agg_max_udf
        min_udf = self.pandas_agg_min_udf

        result1 = df.withColumn("v_diff", max_udf(df["v"]).over(w) - min_udf(df["v"]).over(w))
        expected1 = df.withColumn("v_diff", max(df["v"]).over(w) - min(df["v"]).over(w))

        # Test mixing sql window function and window udf in the same expression
        result2 = df.withColumn("v_diff", max_udf(df["v"]).over(w) - min(df["v"]).over(w))
        expected2 = expected1

        # Test chaining sql aggregate function and udf
        result3 = (
            df.withColumn("max_v", max_udf(df["v"]).over(w))
            .withColumn("min_v", min(df["v"]).over(w))
            .withColumn("v_diff", col("max_v") - col("min_v"))
            .drop("max_v", "min_v")
        )
        expected3 = expected1

        # Test mixing sql window function and udf
        result4 = df.withColumn("max_v", max_udf(df["v"]).over(w)).withColumn(
            "rank", rank().over(ow)
        )
        expected4 = df.withColumn("max_v", max(df["v"]).over(w)).withColumn("rank", rank().over(ow))

        assert_frame_equal(expected1.toPandas(), result1.toPandas())
        assert_frame_equal(expected2.toPandas(), result2.toPandas())
        assert_frame_equal(expected3.toPandas(), result3.toPandas())
        assert_frame_equal(expected4.toPandas(), result4.toPandas())

    def test_array_type(self):
        df = self.data
        w = self.unbounded_window

        array_udf = pandas_udf(lambda x: [1.0, 2.0], "array<double>", PandasUDFType.GROUPED_AGG)
        result1 = df.withColumn("v2", array_udf(df["v"]).over(w))
        self.assertEqual(result1.first()["v2"], [1.0, 2.0])

    def test_invalid_args(self):
        df = self.data
        w = self.unbounded_window

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                AnalysisException, ".*not supported within a window function"
            ):
                foo_udf = pandas_udf(lambda x: x, "v double", PandasUDFType.GROUPED_MAP)
                df.withColumn("v2", foo_udf(df["v"]).over(w))

    def test_bounded_simple(self):
        from pyspark.sql.functions import mean, max, min, count

        df = self.data
        w1 = self.sliding_row_window
        w2 = self.shrinking_range_window

        plus_one = self.python_plus_one
        count_udf = self.pandas_agg_count_udf
        mean_udf = self.pandas_agg_mean_udf
        max_udf = self.pandas_agg_max_udf
        min_udf = self.pandas_agg_min_udf

        result1 = (
            df.withColumn("mean_v", mean_udf(plus_one(df["v"])).over(w1))
            .withColumn("count_v", count_udf(df["v"]).over(w2))
            .withColumn("max_v", max_udf(df["v"]).over(w2))
            .withColumn("min_v", min_udf(df["v"]).over(w1))
        )

        expected1 = (
            df.withColumn("mean_v", mean(plus_one(df["v"])).over(w1))
            .withColumn("count_v", count(df["v"]).over(w2))
            .withColumn("max_v", max(df["v"]).over(w2))
            .withColumn("min_v", min(df["v"]).over(w1))
        )

        assert_frame_equal(expected1.toPandas(), result1.toPandas())

    def test_growing_window(self):
        from pyspark.sql.functions import mean

        df = self.data
        w1 = self.growing_row_window
        w2 = self.growing_range_window

        mean_udf = self.pandas_agg_mean_udf

        result1 = df.withColumn("m1", mean_udf(df["v"]).over(w1)).withColumn(
            "m2", mean_udf(df["v"]).over(w2)
        )

        expected1 = df.withColumn("m1", mean(df["v"]).over(w1)).withColumn(
            "m2", mean(df["v"]).over(w2)
        )

        assert_frame_equal(expected1.toPandas(), result1.toPandas())

    def test_sliding_window(self):
        from pyspark.sql.functions import mean

        df = self.data
        w1 = self.sliding_row_window
        w2 = self.sliding_range_window

        mean_udf = self.pandas_agg_mean_udf

        result1 = df.withColumn("m1", mean_udf(df["v"]).over(w1)).withColumn(
            "m2", mean_udf(df["v"]).over(w2)
        )

        expected1 = df.withColumn("m1", mean(df["v"]).over(w1)).withColumn(
            "m2", mean(df["v"]).over(w2)
        )

        assert_frame_equal(expected1.toPandas(), result1.toPandas())

    def test_shrinking_window(self):
        from pyspark.sql.functions import mean

        df = self.data
        w1 = self.shrinking_row_window
        w2 = self.shrinking_range_window

        mean_udf = self.pandas_agg_mean_udf

        result1 = df.withColumn("m1", mean_udf(df["v"]).over(w1)).withColumn(
            "m2", mean_udf(df["v"]).over(w2)
        )

        expected1 = df.withColumn("m1", mean(df["v"]).over(w1)).withColumn(
            "m2", mean(df["v"]).over(w2)
        )

        assert_frame_equal(expected1.toPandas(), result1.toPandas())

    def test_bounded_mixed(self):
        from pyspark.sql.functions import mean, max

        df = self.data
        w1 = self.sliding_row_window
        w2 = self.unbounded_window

        mean_udf = self.pandas_agg_mean_udf
        max_udf = self.pandas_agg_max_udf

        result1 = (
            df.withColumn("mean_v", mean_udf(df["v"]).over(w1))
            .withColumn("max_v", max_udf(df["v"]).over(w2))
            .withColumn("mean_unbounded_v", mean_udf(df["v"]).over(w1))
        )

        expected1 = (
            df.withColumn("mean_v", mean(df["v"]).over(w1))
            .withColumn("max_v", max(df["v"]).over(w2))
            .withColumn("mean_unbounded_v", mean(df["v"]).over(w1))
        )

        assert_frame_equal(expected1.toPandas(), result1.toPandas())


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_udf_window import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
