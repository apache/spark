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

from pyspark.sql.pandas.functions import arrow_udf, ArrowUDFType
from pyspark.util import PythonEvalType
from pyspark.sql import Row
from pyspark.sql.functions import (
    array,
    explode,
    col,
    lit,
    mean,
    sum,
    udf,
)
from pyspark.errors import AnalysisException, PythonException
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pyarrow,
    pyarrow_requirement_message,
)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class GroupedAggArrowUDFTestsMixin:
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
            assert isinstance(v, (int, float))
            return float(v + 1)

        return plus_one

    @property
    def arrow_scalar_plus_two(self):
        import pyarrow as pa

        @arrow_udf("double", ArrowUDFType.SCALAR)
        def plus_two(v):
            assert isinstance(v, pa.Array)
            return pa.compute.add(v, 2).cast(pa.float64())

        return plus_two

    @property
    def arrow_agg_mean_udf(self):
        import pyarrow as pa

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def avg(v):
            assert isinstance(v, pa.Array)
            return pa.compute.mean(v.cast(pa.float64()))

        return avg

    @property
    def arrow_agg_mean_arr_udf(self):
        import pyarrow as pa

        @arrow_udf("array<double>", ArrowUDFType.GROUPED_AGG)
        def avg(v):
            assert isinstance(v, pa.Array)
            assert isinstance(v, pa.ListArray)
            return [pa.compute.mean(v.flatten()).cast(pa.float64())]

        return avg

    @property
    def arrow_agg_sum_udf(self):
        import pyarrow as pa

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def sum(v):
            assert isinstance(v, pa.Array)
            return pa.compute.sum(v).cast(pa.float64())

        return sum

    @property
    def arrow_agg_weighted_mean_udf(self):
        import pyarrow as pa
        import numpy as np

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def weighted_mean(v, w):
            assert isinstance(v, pa.Array)
            assert isinstance(w, pa.Array)
            return float(np.average(v, weights=w))

        return weighted_mean

    def test_manual(self):
        df = self.data
        sum_udf = self.arrow_agg_sum_udf
        mean_udf = self.arrow_agg_mean_udf
        mean_arr_udf = self.arrow_agg_mean_arr_udf

        result = (
            df.groupby("id")
            .agg(sum_udf(df.v), mean_udf(df.v), mean_arr_udf(array(df.v)))
            .sort("id")
        )
        expected = self.spark.createDataFrame(
            [
                [0, 245.0, 24.5, [24.5]],
                [1, 255.0, 25.5, [25.5]],
                [2, 265.0, 26.5, [26.5]],
                [3, 275.0, 27.5, [27.5]],
                [4, 285.0, 28.5, [28.5]],
                [5, 295.0, 29.5, [29.5]],
                [6, 305.0, 30.5, [30.5]],
                [7, 315.0, 31.5, [31.5]],
                [8, 325.0, 32.5, [32.5]],
                [9, 335.0, 33.5, [33.5]],
            ],
            ["id", "sum(v)", "avg(v)", "avg(array(v))"],
        ).collect()

        self.assertEqual(expected, result.collect())

    def test_basic(self):
        df = self.data
        weighted_mean_udf = self.arrow_agg_weighted_mean_udf

        # Groupby one column and aggregate one UDF with literal
        result1 = df.groupby("id").agg(weighted_mean_udf(df.v, lit(1.0))).sort("id")
        expected1 = (
            df.groupby("id").agg(mean(df.v).alias("weighted_mean(v, 1.0)")).sort("id").collect()
        )
        self.assertEqual(expected1, result1.collect())

        # Groupby one expression and aggregate one UDF with literal
        result2 = df.groupby((col("id") + 1)).agg(weighted_mean_udf(df.v, lit(1.0))).sort(df.id + 1)
        expected2 = (
            df.groupby((col("id") + 1))
            .agg(mean(df.v).alias("weighted_mean(v, 1.0)"))
            .sort(df.id + 1)
        ).collect()
        self.assertEqual(expected2, result2.collect())

        # Groupby one column and aggregate one UDF without literal
        result3 = df.groupby("id").agg(weighted_mean_udf(df.v, df.w)).sort("id")
        expected3 = (
            df.groupby("id").agg(mean(df.v).alias("weighted_mean(v, w)")).sort("id").collect()
        )
        self.assertEqual(expected3, result3.collect())

        # Groupby one expression and aggregate one UDF without literal
        result4 = (
            df.groupby((col("id") + 1).alias("id")).agg(weighted_mean_udf(df.v, df.w)).sort("id")
        )
        expected4 = (
            df.groupby((col("id") + 1).alias("id"))
            .agg(mean(df.v).alias("weighted_mean(v, w)"))
            .sort("id")
        ).collect()
        self.assertEqual(expected4, result4.collect())

    def test_alias(self):
        df = self.data
        mean_udf = self.arrow_agg_mean_udf

        result = df.groupby("id").agg(mean_udf(df.v).alias("mean_alias"))
        expected = df.groupby("id").agg(mean(df.v).alias("mean_alias")).collect()

        self.assertEqual(expected, result.collect())

    def test_mixed_sql(self):
        """
        Test mixing group aggregate pandas UDF with sql expression.
        """
        df = self.data
        sum_udf = self.arrow_agg_sum_udf

        # Mix group aggregate pandas UDF with sql expression
        result1 = df.groupby("id").agg(sum_udf(df.v) + 1).sort("id")
        expected1 = df.groupby("id").agg(sum(df.v) + 1).sort("id").collect()

        # Mix group aggregate pandas UDF with sql expression (order swapped)
        result2 = df.groupby("id").agg(sum_udf(df.v + 1)).sort("id")

        expected2 = df.groupby("id").agg(sum(df.v + 1)).sort("id").collect()

        # Wrap group aggregate pandas UDF with two sql expressions
        result3 = df.groupby("id").agg(sum_udf(df.v + 1) + 2).sort("id")
        expected3 = df.groupby("id").agg(sum(df.v + 1) + 2).sort("id").collect()

        self.assertEqual(expected1, result1.collect())
        self.assertEqual(expected2, result2.collect())
        self.assertEqual(expected3, result3.collect())

    def test_mixed_udfs(self):
        """
        Test mixing group aggregate pandas UDF with python UDF and scalar pandas UDF.
        """
        df = self.data
        plus_one = self.python_plus_one
        plus_two = self.arrow_scalar_plus_two
        sum_udf = self.arrow_agg_sum_udf

        # Mix group aggregate pandas UDF and python UDF
        result1 = df.groupby("id").agg(plus_one(sum_udf(df.v))).sort("id")
        expected1 = df.groupby("id").agg(plus_one(sum(df.v))).sort("id").collect()

        # Mix group aggregate pandas UDF and python UDF (order swapped)
        result2 = df.groupby("id").agg(sum_udf(plus_one(df.v))).sort("id")
        expected2 = df.groupby("id").agg(sum(plus_one(df.v))).sort("id").collect()

        # Mix group aggregate pandas UDF and scalar pandas UDF
        result3 = df.groupby("id").agg(sum_udf(plus_two(df.v))).sort("id")
        expected3 = df.groupby("id").agg(sum(plus_two(df.v))).sort("id").collect()

        # Mix group aggregate pandas UDF and scalar pandas UDF (order swapped)
        result4 = df.groupby("id").agg(plus_two(sum_udf(df.v))).sort("id")
        expected4 = df.groupby("id").agg(plus_two(sum(df.v))).sort("id").collect()

        # Wrap group aggregate pandas UDF with two python UDFs and use python UDF in groupby
        result5 = (
            df.groupby(plus_one(df.id)).agg(plus_one(sum_udf(plus_one(df.v)))).sort("plus_one(id)")
        )
        expected5 = (
            df.groupby(plus_one(df.id)).agg(plus_one(sum(plus_one(df.v)))).sort("plus_one(id)")
        ).collect()

        # Wrap group aggregate pandas UDF with two scala pandas UDF and user scala pandas UDF in
        # groupby
        result6 = (
            df.groupby(plus_two(df.id)).agg(plus_two(sum_udf(plus_two(df.v)))).sort("plus_two(id)")
        )
        expected6 = (
            df.groupby(plus_two(df.id)).agg(plus_two(sum(plus_two(df.v)))).sort("plus_two(id)")
        ).collect()

        self.assertEqual(expected1, result1.collect())
        self.assertEqual(expected2, result2.collect())
        self.assertEqual(expected3, result3.collect())
        self.assertEqual(expected4, result4.collect())
        self.assertEqual(expected5, result5.collect())
        self.assertEqual(expected6, result6.collect())

    def test_multiple_udfs(self):
        """
        Test multiple group aggregate pandas UDFs in one agg function.
        """
        df = self.data
        mean_udf = self.arrow_agg_mean_udf
        sum_udf = self.arrow_agg_sum_udf
        weighted_mean_udf = self.arrow_agg_weighted_mean_udf

        result = (
            df.groupBy("id")
            .agg(mean_udf(df.v), sum_udf(df.v), weighted_mean_udf(df.v, df.w))
            .sort("id")
        )
        expected = (
            df.groupBy("id")
            .agg(mean(df.v), sum(df.v), mean(df.v).alias("weighted_mean(v, w)"))
            .sort("id")
            .collect()
        )

        self.assertEqual(expected, result.collect())

    def test_complex_groupby(self):
        df = self.data
        sum_udf = self.arrow_agg_sum_udf
        plus_one = self.python_plus_one
        plus_two = self.arrow_scalar_plus_two

        # groupby one expression
        result1 = df.groupby(df.v % 2).agg(sum_udf(df.v))
        expected1 = df.groupby(df.v % 2).agg(sum(df.v)).collect()

        # empty groupby
        result2 = df.groupby().agg(sum_udf(df.v))
        expected2 = df.groupby().agg(sum(df.v)).collect()

        # groupby one column and one sql expression
        result3 = df.groupby(df.id, df.v % 2).agg(sum_udf(df.v)).orderBy(df.id, df.v % 2)
        expected3 = df.groupby(df.id, df.v % 2).agg(sum(df.v)).orderBy(df.id, df.v % 2).collect()

        # groupby one python UDF
        result4 = df.groupby(plus_one(df.id)).agg(sum_udf(df.v)).sort("plus_one(id)")
        expected4 = df.groupby(plus_one(df.id)).agg(sum(df.v)).sort("plus_one(id)").collect()

        # groupby one scalar pandas UDF
        result5 = df.groupby(plus_two(df.id)).agg(sum_udf(df.v)).sort("sum(v)")
        expected5 = df.groupby(plus_two(df.id)).agg(sum(df.v)).sort("sum(v)").collect()

        # groupby one expression and one python UDF
        result6 = (
            df.groupby(df.v % 2, plus_one(df.id))
            .agg(sum_udf(df.v))
            .sort(["(v % 2)", "plus_one(id)"])
        )
        expected6 = (
            df.groupby(df.v % 2, plus_one(df.id)).agg(sum(df.v)).sort(["(v % 2)", "plus_one(id)"])
        ).collect()

        # groupby one expression and one scalar pandas UDF
        result7 = (
            df.groupby(df.v % 2, plus_two(df.id))
            .agg(sum_udf(df.v))
            .sort(["sum(v)", "plus_two(id)"])
        )
        expected7 = (
            df.groupby(df.v % 2, plus_two(df.id)).agg(sum(df.v)).sort(["sum(v)", "plus_two(id)"])
        ).collect()

        self.assertEqual(expected1, result1.collect())
        self.assertEqual(expected2, result2.collect())
        self.assertEqual(expected3, result3.collect())
        self.assertEqual(expected4, result4.collect())
        self.assertEqual(expected5, result5.collect())
        self.assertEqual(expected6, result6.collect())
        self.assertEqual(expected7, result7.collect())

    def test_complex_expressions(self):
        df = self.data
        plus_one = self.python_plus_one
        plus_two = self.arrow_scalar_plus_two
        sum_udf = self.arrow_agg_sum_udf

        # Test complex expressions with sql expression, python UDF and
        # group aggregate pandas UDF
        result1 = (
            df.withColumn("v1", plus_one(df.v))
            .withColumn("v2", df.v + 2)
            .groupby(df.id, df.v % 2)
            .agg(
                sum_udf(col("v")),
                sum_udf(col("v1") + 3),
                sum_udf(col("v2")) + 5,
                plus_one(sum_udf(col("v1"))),
                sum_udf(plus_one(col("v2"))),
            )
            .sort(["id", "(v % 2)"])
        )

        expected1 = (
            df.withColumn("v1", df.v + 1)
            .withColumn("v2", df.v + 2)
            .groupby(df.id, df.v % 2)
            .agg(
                sum(col("v")),
                sum(col("v1") + 3),
                sum(col("v2")) + 5,
                plus_one(sum(col("v1"))),
                sum(plus_one(col("v2"))),
            )
            .sort(["id", "(v % 2)"])
            .collect()
        )

        # Test complex expressions with sql expression, scala pandas UDF and
        # group aggregate pandas UDF
        result2 = (
            df.withColumn("v1", plus_one(df.v))
            .withColumn("v2", df.v + 2)
            .groupby(df.id, df.v % 2)
            .agg(
                sum_udf(col("v")),
                sum_udf(col("v1") + 3),
                sum_udf(col("v2")) + 5,
                plus_two(sum_udf(col("v1"))),
                sum_udf(plus_two(col("v2"))),
            )
            .sort(["id", "(v % 2)"])
        )

        expected2 = (
            df.withColumn("v1", df.v + 1)
            .withColumn("v2", df.v + 2)
            .groupby(df.id, df.v % 2)
            .agg(
                sum(col("v")),
                sum(col("v1") + 3),
                sum(col("v2")) + 5,
                plus_two(sum(col("v1"))),
                sum(plus_two(col("v2"))),
            )
            .sort(["id", "(v % 2)"])
            .collect()
        )

        # Test sequential groupby aggregate
        result3 = (
            df.groupby("id")
            .agg(sum_udf(df.v).alias("v"))
            .groupby("id")
            .agg(sum_udf(col("v")))
            .sort("id")
        )

        expected3 = (
            df.groupby("id")
            .agg(sum(df.v).alias("v"))
            .groupby("id")
            .agg(sum(col("v")))
            .sort("id")
            .collect()
        )

        self.assertEqual(expected1, result1.collect())
        self.assertEqual(expected2, result2.collect())
        self.assertEqual(expected3, result3.collect())

    def test_retain_group_columns(self):
        with self.sql_conf({"spark.sql.retainGroupColumns": False}):
            df = self.data
            sum_udf = self.arrow_agg_sum_udf

            result1 = df.groupby(df.id).agg(sum_udf(df.v))
            expected1 = df.groupby(df.id).agg(sum(df.v)).collect()
            self.assertEqual(expected1, result1.collect())

    def test_array_type(self):
        df = self.data

        array_udf = arrow_udf(lambda x: [1.0, 2.0], "array<double>", ArrowUDFType.GROUPED_AGG)
        result1 = df.groupby("id").agg(array_udf(df["v"]).alias("v2"))
        self.assertEqual(result1.first()["v2"], [1.0, 2.0])

    def test_invalid_args(self):
        with self.quiet():
            self.check_invalid_args()

    def check_invalid_args(self):
        df = self.data
        plus_one = self.python_plus_one
        mean_udf = self.arrow_agg_mean_udf
        with self.assertRaisesRegex(AnalysisException, "[MISSING_AGGREGATION]"):
            df.groupby(df.id).agg(plus_one(df.v)).collect()
        with self.assertRaisesRegex(
            AnalysisException, "aggregate function.*argument.*aggregate function"
        ):
            df.groupby(df.id).agg(mean_udf(mean_udf(df.v))).collect()
        with self.assertRaisesRegex(
            AnalysisException,
            "The group aggregate pandas UDF `avg` cannot be invoked together with as other, "
            "non-pandas aggregate functions.",
        ):
            df.groupby(df.id).agg(mean_udf(df.v), mean(df.v)).collect()

    def test_register_vectorized_udf_basic(self):
        import pyarrow as pa

        sum_arrow_udf = arrow_udf(
            lambda v: pa.compute.sum(v).cast(pa.int32()),
            "integer",
            PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF,
        )

        self.assertEqual(sum_arrow_udf.evalType, PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF)
        group_agg_pandas_udf = self.spark.udf.register("sum_arrow_udf", sum_arrow_udf)
        self.assertEqual(group_agg_pandas_udf.evalType, PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF)
        q = "SELECT sum_arrow_udf(v1) FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2) GROUP BY v2"
        actual = sorted(map(lambda r: r[0], self.spark.sql(q).collect()))
        expected = [1, 5]
        self.assertEqual(actual, expected)

    def test_grouped_with_empty_partition(self):
        import pyarrow as pa

        data = [Row(id=1, x=2), Row(id=1, x=3), Row(id=2, x=4)]
        expected = [Row(id=1, sum=5), Row(id=2, x=4)]
        num_parts = len(data) + 1
        df = self.spark.createDataFrame(data).repartition(num_parts)

        f = arrow_udf(lambda x: pa.compute.sum(x).cast(pa.int32()), "int", ArrowUDFType.GROUPED_AGG)

        result = df.groupBy("id").agg(f(df["x"]).alias("sum")).sort("id")
        self.assertEqual(result.collect(), expected)

    def test_grouped_without_group_by_clause(self):
        import pyarrow as pa

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def max_udf(v):
            return float(pa.compute.max(v).as_py())

        df = self.spark.range(0, 100)
        self.spark.udf.register("max_udf", max_udf)

        with self.tempView("table"):
            df.createTempView("table")

            agg1 = df.agg(max_udf(df["id"]))
            agg2 = self.spark.sql("select max_udf(id) from table")
            self.assertEqual(agg1.collect(), agg2.collect())

    def test_no_predicate_pushdown_through(self):
        import pyarrow as pa

        @arrow_udf("float", ArrowUDFType.GROUPED_AGG)
        def mean(x):
            return pa.compute.mean(x).cast(pa.float32())

        df = self.spark.createDataFrame([Row(id=1, foo=42), Row(id=2, foo=1), Row(id=2, foo=2)])

        agg = df.groupBy("id").agg(mean("foo").alias("mean"))
        filtered = agg.filter(agg["mean"] > 40.0)

        self.assertEqual(filtered.collect()[0]["mean"], 42.0)

        assert filtered.collect()[0]["mean"] == 42.0

    def test_named_arguments(self):
        df = self.data
        weighted_mean = self.arrow_agg_weighted_mean_udf

        with self.tempView("v"):
            df.createOrReplaceTempView("v")
            self.spark.udf.register("weighted_mean", weighted_mean)

            for i, aggregated in enumerate(
                [
                    df.groupby("id").agg(weighted_mean(df.v, w=df.w).alias("wm")),
                    df.groupby("id").agg(weighted_mean(v=df.v, w=df.w).alias("wm")),
                    df.groupby("id").agg(weighted_mean(w=df.w, v=df.v).alias("wm")),
                    self.spark.sql("SELECT id, weighted_mean(v, w => w) as wm FROM v GROUP BY id"),
                    self.spark.sql(
                        "SELECT id, weighted_mean(v => v, w => w) as wm FROM v GROUP BY id"
                    ),
                    self.spark.sql(
                        "SELECT id, weighted_mean(w => w, v => v) as wm FROM v GROUP BY id"
                    ),
                ]
            ):
                with self.subTest(query_no=i):
                    self.assertEqual(
                        aggregated.collect(), df.groupby("id").agg(mean(df.v).alias("wm")).collect()
                    )

    def test_named_arguments_negative(self):
        df = self.data
        weighted_mean = self.arrow_agg_weighted_mean_udf

        with self.tempView("v"):
            df.createOrReplaceTempView("v")
            self.spark.udf.register("weighted_mean", weighted_mean)

            with self.assertRaisesRegex(
                AnalysisException,
                "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
            ):
                self.spark.sql(
                    "SELECT id, weighted_mean(v => v, v => w) as wm FROM v GROUP BY id"
                ).show()

            with self.assertRaisesRegex(AnalysisException, "UNEXPECTED_POSITIONAL_ARGUMENT"):
                self.spark.sql(
                    "SELECT id, weighted_mean(v => v, w) as wm FROM v GROUP BY id"
                ).show()

            with self.assertRaisesRegex(
                PythonException, r"weighted_mean\(\) got an unexpected keyword argument 'x'"
            ):
                self.spark.sql(
                    "SELECT id, weighted_mean(v => v, x => w) as wm FROM v GROUP BY id"
                ).show()

            with self.assertRaisesRegex(
                PythonException, r"weighted_mean\(\) got multiple values for argument 'v'"
            ):
                self.spark.sql(
                    "SELECT id, weighted_mean(v, v => w) as wm FROM v GROUP BY id"
                ).show()

    def test_kwargs(self):
        df = self.data

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def weighted_mean(**kwargs):
            import numpy as np

            return np.average(kwargs["v"], weights=kwargs["w"])

        with self.tempView("v"):
            df.createOrReplaceTempView("v")
            self.spark.udf.register("weighted_mean", weighted_mean)

            for i, aggregated in enumerate(
                [
                    df.groupby("id").agg(weighted_mean(v=df.v, w=df.w).alias("wm")),
                    df.groupby("id").agg(weighted_mean(w=df.w, v=df.v).alias("wm")),
                    self.spark.sql(
                        "SELECT id, weighted_mean(v => v, w => w) as wm FROM v GROUP BY id"
                    ),
                    self.spark.sql(
                        "SELECT id, weighted_mean(w => w, v => v) as wm FROM v GROUP BY id"
                    ),
                ]
            ):
                with self.subTest(query_no=i):
                    self.assertEqual(
                        aggregated.collect(), df.groupby("id").agg(mean(df.v).alias("wm")).collect()
                    )

            # negative
            with self.assertRaisesRegex(
                AnalysisException,
                "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
            ):
                self.spark.sql(
                    "SELECT id, weighted_mean(v => v, v => w) as wm FROM v GROUP BY id"
                ).show()

            with self.assertRaisesRegex(AnalysisException, "UNEXPECTED_POSITIONAL_ARGUMENT"):
                self.spark.sql(
                    "SELECT id, weighted_mean(v => v, w) as wm FROM v GROUP BY id"
                ).show()

    def test_named_arguments_and_defaults(self):
        import pyarrow as pa

        df = self.data

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def biased_sum(v, w=None):
            return pa.compute.sum(v).as_py() + (pa.compute.sum(w).as_py() if w is not None else 100)

        with self.tempView("v"):
            df.createOrReplaceTempView("v")
            self.spark.udf.register("biased_sum", biased_sum)

            # without "w"
            for i, aggregated in enumerate(
                [
                    df.groupby("id").agg(biased_sum(df.v).alias("s")),
                    df.groupby("id").agg(biased_sum(v=df.v).alias("s")),
                    self.spark.sql("SELECT id, biased_sum(v) as s FROM v GROUP BY id"),
                    self.spark.sql("SELECT id, biased_sum(v => v) as s FROM v GROUP BY id"),
                ]
            ):
                with self.subTest(with_w=False, query_no=i):
                    self.assertEqual(
                        aggregated.collect(),
                        df.groupby("id").agg((sum(df.v) + lit(100)).alias("s")).collect(),
                    )

            # with "w"
            for i, aggregated in enumerate(
                [
                    df.groupby("id").agg(biased_sum(df.v, w=df.w).alias("s")),
                    df.groupby("id").agg(biased_sum(v=df.v, w=df.w).alias("s")),
                    df.groupby("id").agg(biased_sum(w=df.w, v=df.v).alias("s")),
                    self.spark.sql("SELECT id, biased_sum(v, w => w) as s FROM v GROUP BY id"),
                    self.spark.sql("SELECT id, biased_sum(v => v, w => w) as s FROM v GROUP BY id"),
                    self.spark.sql("SELECT id, biased_sum(w => w, v => v) as s FROM v GROUP BY id"),
                ]
            ):
                with self.subTest(with_w=True, query_no=i):
                    self.assertEqual(
                        aggregated.collect(),
                        df.groupby("id").agg((sum(df.v) + sum(df.w)).alias("s")).collect(),
                    )


class GroupedAggArrowUDFTests(GroupedAggArrowUDFTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_udf_grouped_agg import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
