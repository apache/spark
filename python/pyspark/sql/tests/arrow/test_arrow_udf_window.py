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
import logging

from pyspark.sql.functions import arrow_udf, ArrowUDFType
from pyspark.util import PythonEvalType, is_remote_only
from pyspark.sql import Row, functions as sf
from pyspark.sql.window import Window
from pyspark.errors import AnalysisException, PythonException, PySparkTypeError
from pyspark.testing.utils import (
    have_numpy,
    numpy_requirement_message,
    have_pyarrow,
    pyarrow_requirement_message,
    assertDataFrameEqual,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class WindowArrowUDFTestsMixin:
    @property
    def data(self):
        return (
            self.spark.range(10)
            .toDF("id")
            .withColumn("vs", sf.array([sf.lit(i * 1.0) + sf.col("id") for i in range(20, 30)]))
            .withColumn("v", sf.explode(sf.col("vs")))
            .drop("vs")
            .withColumn("w", sf.lit(1.0))
        )

    @property
    def python_plus_one(self):
        @sf.udf("double")
        def plus_one(v):
            assert isinstance(v, float)
            return v + 1

        return plus_one

    @property
    def arrow_scalar_time_two(self):
        import pyarrow as pa

        return arrow_udf(lambda v: pa.compute.multiply(v, 2), "double")

    @property
    def arrow_agg_count_udf(self):
        @arrow_udf("long", ArrowUDFType.GROUPED_AGG)
        def count(v):
            return len(v)

        return count

    @property
    def arrow_agg_mean_udf(self):
        import pyarrow as pa

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def avg(v):
            return pa.compute.mean(v)

        return avg

    @property
    def arrow_agg_max_udf(self):
        import pyarrow as pa

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def max(v):
            return pa.compute.max(v)

        return max

    @property
    def arrow_agg_min_udf(self):
        import pyarrow as pa

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def min(v):
            return pa.compute.min(v)

        return min

    @property
    def arrow_agg_weighted_mean_udf(self):
        import numpy as np

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def weighted_mean(v, w):
            return np.average(v, weights=w)

        return weighted_mean

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

        mean_udf = self.arrow_agg_mean_udf

        result1 = df.withColumn("mean_v", mean_udf(df["v"]).over(w))
        expected1 = df.withColumn("mean_v", sf.mean(df["v"]).over(w))

        result2 = df.select(mean_udf(df["v"]).over(w))
        expected2 = df.select(sf.mean(df["v"]).over(w))

        self.assertEqual(expected1.collect(), result1.collect())
        self.assertEqual(expected2.collect(), result2.collect())

    def test_multiple_udfs(self):
        df = self.data
        w = self.unbounded_window

        result1 = (
            df.withColumn("mean_v", self.arrow_agg_mean_udf(df["v"]).over(w))
            .withColumn("max_v", self.arrow_agg_max_udf(df["v"]).over(w))
            .withColumn("min_w", self.arrow_agg_min_udf(df["w"]).over(w))
        )

        expected1 = (
            df.withColumn("mean_v", sf.mean(df["v"]).over(w))
            .withColumn("max_v", sf.max(df["v"]).over(w))
            .withColumn("min_w", sf.min(df["w"]).over(w))
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_replace_existing(self):
        df = self.data
        w = self.unbounded_window

        result1 = df.withColumn("v", self.arrow_agg_mean_udf(df["v"]).over(w))
        expected1 = df.withColumn("v", sf.mean(df["v"]).over(w))

        self.assertEqual(expected1.collect(), result1.collect())

    def test_mixed_sql(self):
        df = self.data
        w = self.unbounded_window
        mean_udf = self.arrow_agg_mean_udf

        result1 = df.withColumn("v", mean_udf(df["v"] * 2).over(w) + 1)
        expected1 = df.withColumn("v", sf.mean(df["v"] * 2).over(w) + 1)

        self.assertEqual(expected1.collect(), result1.collect())

    def test_mixed_udf(self):
        df = self.data
        w = self.unbounded_window

        plus_one = self.python_plus_one
        time_two = self.arrow_scalar_time_two
        mean_udf = self.arrow_agg_mean_udf

        result1 = df.withColumn("v2", plus_one(mean_udf(plus_one(df["v"])).over(w)))
        expected1 = df.withColumn("v2", plus_one(sf.mean(plus_one(df["v"])).over(w)))

        result2 = df.withColumn("v2", time_two(mean_udf(time_two(df["v"])).over(w)))
        expected2 = df.withColumn("v2", time_two(sf.mean(time_two(df["v"])).over(w)))

        self.assertEqual(expected1.collect(), result1.collect())
        self.assertEqual(expected2.collect(), result2.collect())

    def test_without_partitionBy(self):
        df = self.data
        w = self.unpartitioned_window
        mean_udf = self.arrow_agg_mean_udf

        result1 = df.withColumn("v2", mean_udf(df["v"]).over(w))
        expected1 = df.withColumn("v2", sf.mean(df["v"]).over(w))

        result2 = df.select(mean_udf(df["v"]).over(w))
        expected2 = df.select(sf.mean(df["v"]).over(w))

        self.assertEqual(expected1.collect(), result1.collect())
        self.assertEqual(expected2.collect(), result2.collect())

    def test_mixed_sql_and_udf(self):
        df = self.data
        w = self.unbounded_window
        ow = self.ordered_window
        max_udf = self.arrow_agg_max_udf
        min_udf = self.arrow_agg_min_udf

        result1 = df.withColumn("v_diff", max_udf(df["v"]).over(w) - min_udf(df["v"]).over(w))
        expected1 = df.withColumn("v_diff", sf.max(df["v"]).over(w) - sf.min(df["v"]).over(w))

        # Test mixing sql window function and window udf in the same expression
        result2 = df.withColumn("v_diff", max_udf(df["v"]).over(w) - sf.min(df["v"]).over(w))
        expected2 = expected1

        # Test chaining sql aggregate function and udf
        result3 = (
            df.withColumn("max_v", max_udf(df["v"]).over(w))
            .withColumn("min_v", sf.min(df["v"]).over(w))
            .withColumn("v_diff", sf.col("max_v") - sf.col("min_v"))
            .drop("max_v", "min_v")
        )
        expected3 = expected1

        # Test mixing sql window function and udf
        result4 = df.withColumn("max_v", max_udf(df["v"]).over(w)).withColumn(
            "rank", sf.rank().over(ow)
        )
        expected4 = df.withColumn("max_v", sf.max(df["v"]).over(w)).withColumn(
            "rank", sf.rank().over(ow)
        )

        self.assertEqual(expected1.collect(), result1.collect())
        self.assertEqual(expected2.collect(), result2.collect())
        self.assertEqual(expected3.collect(), result3.collect())
        self.assertEqual(expected4.collect(), result4.collect())

    def test_array_type(self):
        df = self.data
        w = self.unbounded_window

        array_udf = arrow_udf(lambda x: [1.0, 2.0], "array<double>", ArrowUDFType.GROUPED_AGG)
        result1 = df.withColumn("v2", array_udf(df["v"]).over(w))
        self.assertEqual(result1.first()["v2"], [1.0, 2.0])

    def test_invalid_args(self):
        with self.quiet():
            self.check_invalid_args()

    def check_invalid_args(self):
        df = self.data
        w = self.unbounded_window

        with self.assertRaises(PySparkTypeError):
            foo_udf = arrow_udf(lambda x: x, "v double", PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF)
            df.withColumn("v2", foo_udf(df["v"]).over(w)).schema

    def test_bounded_simple(self):
        df = self.data
        w1 = self.sliding_row_window
        w2 = self.shrinking_range_window

        plus_one = self.python_plus_one
        count_udf = self.arrow_agg_count_udf
        mean_udf = self.arrow_agg_mean_udf
        max_udf = self.arrow_agg_max_udf
        min_udf = self.arrow_agg_min_udf

        result1 = (
            df.withColumn("mean_v", mean_udf(plus_one(df["v"])).over(w1))
            .withColumn("count_v", count_udf(df["v"]).over(w2))
            .withColumn("max_v", max_udf(df["v"]).over(w2))
            .withColumn("min_v", min_udf(df["v"]).over(w1))
        )

        expected1 = (
            df.withColumn("mean_v", sf.mean(plus_one(df["v"])).over(w1))
            .withColumn("count_v", sf.count(df["v"]).over(w2))
            .withColumn("max_v", sf.max(df["v"]).over(w2))
            .withColumn("min_v", sf.min(df["v"]).over(w1))
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_growing_window(self):
        df = self.data
        w1 = self.growing_row_window
        w2 = self.growing_range_window

        mean_udf = self.arrow_agg_mean_udf

        result1 = df.withColumn("m1", mean_udf(df["v"]).over(w1)).withColumn(
            "m2", mean_udf(df["v"]).over(w2)
        )

        expected1 = df.withColumn("m1", sf.mean(df["v"]).over(w1)).withColumn(
            "m2", sf.mean(df["v"]).over(w2)
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_sliding_window(self):
        df = self.data
        w1 = self.sliding_row_window
        w2 = self.sliding_range_window

        mean_udf = self.arrow_agg_mean_udf

        result1 = df.withColumn("m1", mean_udf(df["v"]).over(w1)).withColumn(
            "m2", mean_udf(df["v"]).over(w2)
        )

        expected1 = df.withColumn("m1", sf.mean(df["v"]).over(w1)).withColumn(
            "m2", sf.mean(df["v"]).over(w2)
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_shrinking_window(self):
        df = self.data
        w1 = self.shrinking_row_window
        w2 = self.shrinking_range_window

        mean_udf = self.arrow_agg_mean_udf

        result1 = df.withColumn("m1", mean_udf(df["v"]).over(w1)).withColumn(
            "m2", mean_udf(df["v"]).over(w2)
        )

        expected1 = df.withColumn("m1", sf.mean(df["v"]).over(w1)).withColumn(
            "m2", sf.mean(df["v"]).over(w2)
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_bounded_mixed(self):
        df = self.data
        w1 = self.sliding_row_window
        w2 = self.unbounded_window

        mean_udf = self.arrow_agg_mean_udf
        max_udf = self.arrow_agg_max_udf

        result1 = (
            df.withColumn("mean_v", mean_udf(df["v"]).over(w1))
            .withColumn("max_v", max_udf(df["v"]).over(w2))
            .withColumn("mean_unbounded_v", mean_udf(df["v"]).over(w1))
        )

        expected1 = (
            df.withColumn("mean_v", sf.mean(df["v"]).over(w1))
            .withColumn("max_v", sf.max(df["v"]).over(w2))
            .withColumn("mean_unbounded_v", sf.mean(df["v"]).over(w1))
        )

        self.assertEqual(expected1.collect(), result1.collect())

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_named_arguments(self):
        df = self.data
        weighted_mean = self.arrow_agg_weighted_mean_udf

        for w, bound in [(self.sliding_row_window, True), (self.unbounded_window, False)]:
            for i, windowed in enumerate(
                [
                    df.withColumn("wm", weighted_mean(df.v, w=df.w).over(w)),
                    df.withColumn("wm", weighted_mean(v=df.v, w=df.w).over(w)),
                    df.withColumn("wm", weighted_mean(w=df.w, v=df.v).over(w)),
                ]
            ):
                with self.subTest(bound=bound, query_no=i):
                    self.assertEqual(
                        windowed.collect(), df.withColumn("wm", sf.mean(df.v).over(w)).collect()
                    )

        with self.tempView("v"), self.temp_func("weighted_mean"):
            df.createOrReplaceTempView("v")
            self.spark.udf.register("weighted_mean", weighted_mean)

            for w in [
                "ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING",
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            ]:
                window_spec = f"PARTITION BY id ORDER BY v {w}"
                for i, func_call in enumerate(
                    [
                        "weighted_mean(v, w => w)",
                        "weighted_mean(v => v, w => w)",
                        "weighted_mean(w => w, v => v)",
                    ]
                ):
                    with self.subTest(window_spec=window_spec, query_no=i):
                        self.assertEqual(
                            self.spark.sql(
                                f"SELECT id, {func_call} OVER ({window_spec}) as wm FROM v"
                            ).collect(),
                            self.spark.sql(
                                f"SELECT id, mean(v) OVER ({window_spec}) as wm FROM v"
                            ).collect(),
                        )

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_named_arguments_negative(self):
        df = self.data
        weighted_mean = self.arrow_agg_weighted_mean_udf

        with self.tempView("v"), self.temp_func("weighted_mean"):
            df.createOrReplaceTempView("v")
            self.spark.udf.register("weighted_mean", weighted_mean)

            base_sql = "SELECT id, {func_call} OVER ({window_spec}) as wm FROM v"

            for w in [
                "ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING",
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            ]:
                window_spec = f"PARTITION BY id ORDER BY v {w}"
                with self.subTest(window_spec=window_spec):
                    with self.assertRaisesRegex(
                        AnalysisException,
                        "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
                    ):
                        self.spark.sql(
                            base_sql.format(
                                func_call="weighted_mean(v => v, v => w)", window_spec=window_spec
                            )
                        ).show()

                    with self.assertRaisesRegex(
                        AnalysisException, "UNEXPECTED_POSITIONAL_ARGUMENT"
                    ):
                        self.spark.sql(
                            base_sql.format(
                                func_call="weighted_mean(v => v, w)", window_spec=window_spec
                            )
                        ).show()

                    with self.assertRaisesRegex(
                        PythonException, r"weighted_mean\(\) got an unexpected keyword argument 'x'"
                    ):
                        self.spark.sql(
                            base_sql.format(
                                func_call="weighted_mean(v => v, x => w)", window_spec=window_spec
                            )
                        ).show()

                    with self.assertRaisesRegex(
                        PythonException, r"weighted_mean\(\) got multiple values for argument 'v'"
                    ):
                        self.spark.sql(
                            base_sql.format(
                                func_call="weighted_mean(v, v => w)", window_spec=window_spec
                            )
                        ).show()

    def test_kwargs(self):
        df = self.data

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def weighted_mean(**kwargs):
            import numpy as np

            return np.average(kwargs["v"], weights=kwargs["w"])

        for w, bound in [(self.sliding_row_window, True), (self.unbounded_window, False)]:
            for i, windowed in enumerate(
                [
                    df.withColumn("wm", weighted_mean(v=df.v, w=df.w).over(w)),
                    df.withColumn("wm", weighted_mean(w=df.w, v=df.v).over(w)),
                ]
            ):
                with self.subTest(bound=bound, query_no=i):
                    self.assertEqual(
                        windowed.collect(), df.withColumn("wm", sf.mean(df.v).over(w)).collect()
                    )

        with self.tempView("v"), self.temp_func("weighted_mean"):
            df.createOrReplaceTempView("v")
            self.spark.udf.register("weighted_mean", weighted_mean)

            base_sql = "SELECT id, {func_call} OVER ({window_spec}) as wm FROM v"

            for w in [
                "ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING",
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            ]:
                window_spec = f"PARTITION BY id ORDER BY v {w}"
                with self.subTest(window_spec=window_spec):
                    for i, func_call in enumerate(
                        [
                            "weighted_mean(v => v, w => w)",
                            "weighted_mean(w => w, v => v)",
                        ]
                    ):
                        with self.subTest(query_no=i):
                            self.assertEqual(
                                self.spark.sql(
                                    base_sql.format(func_call=func_call, window_spec=window_spec)
                                ).collect(),
                                self.spark.sql(
                                    base_sql.format(func_call="mean(v)", window_spec=window_spec)
                                ).collect(),
                            )

                    # negative
                    with self.assertRaisesRegex(
                        AnalysisException,
                        "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
                    ):
                        self.spark.sql(
                            base_sql.format(
                                func_call="weighted_mean(v => v, v => w)", window_spec=window_spec
                            )
                        ).show()

                    with self.assertRaisesRegex(
                        AnalysisException, "UNEXPECTED_POSITIONAL_ARGUMENT"
                    ):
                        self.spark.sql(
                            base_sql.format(
                                func_call="weighted_mean(v => v, w)", window_spec=window_spec
                            )
                        ).show()

    def test_complex_window_collect_set(self):
        import pyarrow as pa

        df = self.spark.createDataFrame([(1, 1), (1, 2), (2, 3), (2, 5), (2, 3)], ("id", "v"))
        w = Window.partitionBy("id").orderBy("v")

        @arrow_udf("array<int>")
        def arrow_collect_set(v: pa.Array) -> pa.Scalar:
            assert isinstance(v, pa.Array), str(type(v))
            s = sorted([x.as_py() for x in pa.compute.unique(v)])
            t = pa.list_(pa.int32())
            return pa.scalar(value=s, type=t)

        result1 = df.select(
            arrow_collect_set(df["v"]).over(w).alias("vs"),
        )

        expected1 = df.select(
            sf.sort_array(sf.collect_set(df["v"]).over(w)).alias("vs"),
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_complex_window_collect_list(self):
        import pyarrow as pa

        df = self.spark.createDataFrame([(1, 1), (1, 2), (2, 3), (2, 5), (2, 3)], ("id", "v"))
        w = Window.partitionBy("id").orderBy("v")

        @arrow_udf("array<int>")
        def arrow_collect_list(v: pa.Array) -> pa.Scalar:
            assert isinstance(v, pa.Array), str(type(v))
            s = sorted([x.as_py() for x in v])
            t = pa.list_(pa.int32())
            return pa.scalar(value=s, type=t)

        result1 = df.select(
            arrow_collect_list(df["v"]).over(w).alias("vs"),
        )

        expected1 = df.select(
            sf.sort_array(sf.collect_list(df["v"]).over(w)).alias("vs"),
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_complex_window_collect_as_map(self):
        import pyarrow as pa

        df = self.spark.createDataFrame(
            [(1, 2, 1), (1, 3, 2), (2, 4, 3), (2, 5, 5), (2, 6, 3)], ("id", "k", "v")
        )
        w = Window.partitionBy("id").orderBy("v")

        @arrow_udf("map<int, int>")
        def arrow_collect_as_map(id: pa.Array, v: pa.Array) -> pa.Scalar:
            assert isinstance(id, pa.Array), str(type(id))
            assert isinstance(v, pa.Array), str(type(v))
            d = {i: j for i, j in zip(id.to_pylist(), v.to_pylist())}
            t = pa.map_(pa.int32(), pa.int32())
            return pa.scalar(value=d, type=t)

        result1 = df.select(
            arrow_collect_as_map("k", "v").over(w).alias("map"),
        )

        expected1 = df.select(
            sf.map_from_arrays(
                sf.collect_list("k").over(w),
                sf.collect_list("v").over(w),
            ).alias("map")
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_complex_window_min_max_struct(self):
        import pyarrow as pa

        df = self.spark.createDataFrame([(1, 1), (1, 2), (2, 3), (2, 5), (2, 3)], ("id", "v"))
        w = Window.partitionBy("id").orderBy("v")

        @arrow_udf("struct<m1: int, m2:int>")
        def arrow_collect_min_max(id: pa.Array, v: pa.Array) -> pa.Scalar:
            assert isinstance(id, pa.Array), str(type(id))
            assert isinstance(v, pa.Array), str(type(v))
            m1 = pa.compute.min(id)
            m2 = pa.compute.max(v)
            t = pa.struct([pa.field("m1", pa.int32()), pa.field("m2", pa.int32())])
            return pa.scalar(value={"m1": m1.as_py(), "m2": m2.as_py()}, type=t)

        result1 = df.select(
            arrow_collect_min_max("id", "v").over(w).alias("struct"),
        )

        expected1 = df.select(
            sf.struct(
                sf.min("id").over(w).alias("m1"),
                sf.max("v").over(w).alias("m2"),
            ).alias("struct")
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_time_min(self):
        import pyarrow as pa

        df = self.spark.sql(
            """
            SELECT * FROM VALUES
            (1, TIME '12:34:56'),
            (1, TIME '1:2:3'),
            (2, TIME '0:58:59'),
            (2, TIME '10:58:59'),
            (2, TIME '10:00:03')
            AS tab(i, t)
            """
        )
        w1 = Window.partitionBy("i").orderBy("t")
        w2 = Window.orderBy("t")

        @arrow_udf("time", ArrowUDFType.GROUPED_AGG)
        def agg_min_time(v):
            assert isinstance(v, pa.Array)
            assert isinstance(v, pa.Time64Array)
            return pa.compute.min(v)

        expected1 = df.withColumn("res", sf.min("t").over(w1))
        result1 = df.withColumn("res", agg_min_time("t").over(w1))
        self.assertEqual(expected1.collect(), result1.collect())

        expected2 = df.withColumn("res", sf.min("t").over(w2))
        result2 = df.withColumn("res", agg_min_time("t").over(w2))
        self.assertEqual(expected2.collect(), result2.collect())

    def test_return_type_coercion(self):
        import pyarrow as pa

        df = self.spark.range(10).withColumn("v", sf.lit(1))
        w = Window.partitionBy("id").orderBy("v")

        @arrow_udf("long", ArrowUDFType.GROUPED_AGG)
        def agg_long(id: pa.Array) -> int:
            assert isinstance(id, pa.Array), str(type(id))
            return pa.scalar(value=len(id), type=pa.int64())

        result1 = df.select(agg_long("v").over(w).alias("res"))
        self.assertEqual(10, len(result1.collect()))

        # long -> int coercion
        @arrow_udf("int", ArrowUDFType.GROUPED_AGG)
        def agg_int1(id: pa.Array) -> int:
            assert isinstance(id, pa.Array), str(type(id))
            return pa.scalar(value=len(id), type=pa.int64())

        result2 = df.select(agg_int1("v").over(w).alias("res"))
        self.assertEqual(10, len(result2.collect()))

        # long -> int coercion, overflow
        @arrow_udf("int", ArrowUDFType.GROUPED_AGG)
        def agg_int2(id: pa.Array) -> int:
            assert isinstance(id, pa.Array), str(type(id))
            return pa.scalar(value=len(id) + 2147483647, type=pa.int64())

        result3 = df.select(agg_int2("id").alias("res"))
        with self.assertRaises(Exception):
            # pyarrow.lib.ArrowInvalid:
            # Integer value 2147483657 not in range: -2147483648 to 2147483647
            result3.collect()

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_return_numpy_scalar(self):
        import numpy as np
        import pyarrow as pa

        df = self.spark.range(10).withColumn("v", sf.lit(1))
        w = Window.partitionBy("id").orderBy("v")

        @arrow_udf("long")
        def np_max_udf(v: pa.Array) -> np.int64:
            assert isinstance(v, pa.Array)
            return np.max(v)

        @arrow_udf("long")
        def np_min_udf(v: pa.Array) -> np.int64:
            assert isinstance(v, pa.Array)
            return np.min(v)

        @arrow_udf("double")
        def np_avg_udf(v: pa.Array) -> np.float64:
            assert isinstance(v, pa.Array)
            return np.mean(v)

        expected = df.select(
            sf.max("id").over(w).alias("max"),
            sf.min("id").over(w).alias("min"),
            sf.avg("id").over(w).alias("avg"),
        )

        result = df.select(
            np_max_udf("id").over(w).alias("max"),
            np_min_udf("id").over(w).alias("min"),
            np_avg_udf("id").over(w).alias("avg"),
        )
        self.assertEqual(expected.collect(), result.collect())

    def test_arrow_batch_slicing(self):
        import pyarrow as pa

        df = self.spark.range(1000).select((sf.col("id") % 2).alias("key"), sf.col("id").alias("v"))

        w1 = Window.partitionBy("key").orderBy("v")
        w2 = (
            Window.partitionBy("key")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            .orderBy("v")
        )

        @arrow_udf("long", ArrowUDFType.GROUPED_AGG)
        def arrow_sum(v):
            return pa.compute.sum(v)

        @arrow_udf("long", ArrowUDFType.GROUPED_AGG)
        def arrow_sum_unbounded(v):
            assert len(v) == 1000 / 2, len(v)
            return pa.compute.sum(v)

        expected1 = df.select("*", sf.sum("v").over(w1).alias("res")).sort("key", "v").collect()
        expected2 = df.select("*", sf.sum("v").over(w2).alias("res")).sort("key", "v").collect()

        for maxRecords, maxBytes in [(10, 2**31 - 1), (0, 64), (10, 64)]:
            with self.subTest(maxRecords=maxRecords, maxBytes=maxBytes):
                with self.sql_conf(
                    {
                        "spark.sql.execution.arrow.maxRecordsPerBatch": maxRecords,
                        "spark.sql.execution.arrow.maxBytesPerBatch": maxBytes,
                    }
                ):
                    result1 = (
                        df.select("*", arrow_sum("v").over(w1).alias("res"))
                        .sort("key", "v")
                        .collect()
                    )
                    self.assertEqual(expected1, result1)

                    result2 = (
                        df.select("*", arrow_sum_unbounded("v").over(w2).alias("res"))
                        .sort("key", "v")
                        .collect()
                    )
                    self.assertEqual(expected2, result2)

    @unittest.skipIf(is_remote_only(), "Requires JVM access")
    def test_window_arrow_udf_with_logging(self):
        import pyarrow as pa

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def my_window_arrow_udf(x):
            assert isinstance(x, pa.Array)
            logger = logging.getLogger("test_window_arrow")
            logger.warning(f"window arrow udf: {x.to_pylist()}")
            return pa.compute.sum(x)

        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )
        w = Window.partitionBy("id").orderBy("v").rangeBetween(Window.unboundedPreceding, 0)

        with self.sql_conf({"spark.sql.pyspark.worker.logging.enabled": "true"}):
            assertDataFrameEqual(
                df.select("id", my_window_arrow_udf("v").over(w).alias("result")),
                [
                    Row(id=1, result=1.0),
                    Row(id=1, result=3.0),
                    Row(id=2, result=3.0),
                    Row(id=2, result=8.0),
                    Row(id=2, result=18.0),
                ],
            )

            logs = self.spark.tvf.python_worker_logs()

            assertDataFrameEqual(
                logs.select("level", "msg", "context", "logger"),
                [
                    Row(
                        level="WARNING",
                        msg=f"window arrow udf: {lst}",
                        context={"func_name": my_window_arrow_udf.__name__},
                        logger="test_window_arrow",
                    )
                    for lst in [[1.0], [1.0, 2.0], [3.0], [3.0, 5.0], [3.0, 5.0, 10.0]]
                ],
            )


class WindowArrowUDFTests(WindowArrowUDFTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_udf_window import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
