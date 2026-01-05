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
from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType,
    YearMonthIntervalType,
    StructType,
    StructField,
    VariantType,
    VariantVal,
)
from pyspark.sql import functions as sf
from pyspark.errors import AnalysisException, PythonException
from pyspark.testing.utils import (
    have_numpy,
    numpy_requirement_message,
    have_pyarrow,
    pyarrow_requirement_message,
    assertDataFrameEqual,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase
from typing import Iterator, Tuple


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class GroupedAggArrowUDFTestsMixin:
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
            .agg(sum_udf(df.v), mean_udf(df.v), mean_arr_udf(sf.array(df.v)))
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

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_basic(self):
        df = self.data
        weighted_mean_udf = self.arrow_agg_weighted_mean_udf

        # Groupby one column and aggregate one UDF with literal
        result1 = df.groupby("id").agg(weighted_mean_udf(df.v, sf.lit(1.0))).sort("id")
        expected1 = (
            df.groupby("id").agg(sf.mean(df.v).alias("weighted_mean(v, 1.0)")).sort("id").collect()
        )
        self.assertEqual(expected1, result1.collect())

        # Groupby one expression and aggregate one UDF with literal
        result2 = (
            df.groupby((sf.col("id") + 1)).agg(weighted_mean_udf(df.v, sf.lit(1.0))).sort(df.id + 1)
        )
        expected2 = (
            df.groupby((sf.col("id") + 1))
            .agg(sf.mean(df.v).alias("weighted_mean(v, 1.0)"))
            .sort(df.id + 1)
        ).collect()
        self.assertEqual(expected2, result2.collect())

        # Groupby one column and aggregate one UDF without literal
        result3 = df.groupby("id").agg(weighted_mean_udf(df.v, df.w)).sort("id")
        expected3 = (
            df.groupby("id").agg(sf.mean(df.v).alias("weighted_mean(v, w)")).sort("id").collect()
        )
        self.assertEqual(expected3, result3.collect())

        # Groupby one expression and aggregate one UDF without literal
        result4 = (
            df.groupby((sf.col("id") + 1).alias("id")).agg(weighted_mean_udf(df.v, df.w)).sort("id")
        )
        expected4 = (
            df.groupby((sf.col("id") + 1).alias("id"))
            .agg(sf.mean(df.v).alias("weighted_mean(v, w)"))
            .sort("id")
        ).collect()
        self.assertEqual(expected4, result4.collect())

    def test_alias(self):
        df = self.data
        mean_udf = self.arrow_agg_mean_udf

        result = df.groupby("id").agg(mean_udf(df.v).alias("mean_alias"))
        expected = df.groupby("id").agg(sf.mean(df.v).alias("mean_alias")).collect()

        self.assertEqual(expected, result.collect())

    def test_mixed_sql(self):
        """
        Test mixing group aggregate pandas UDF with sql expression.
        """
        df = self.data
        sum_udf = self.arrow_agg_sum_udf

        # Mix group aggregate pandas UDF with sql expression
        result1 = df.groupby("id").agg(sum_udf(df.v) + 1).sort("id")
        expected1 = df.groupby("id").agg(sf.sum(df.v) + 1).sort("id").collect()

        # Mix group aggregate pandas UDF with sql expression (order swapped)
        result2 = df.groupby("id").agg(sum_udf(df.v + 1)).sort("id")

        expected2 = df.groupby("id").agg(sf.sum(df.v + 1)).sort("id").collect()

        # Wrap group aggregate pandas UDF with two sql expressions
        result3 = df.groupby("id").agg(sum_udf(df.v + 1) + 2).sort("id")
        expected3 = df.groupby("id").agg(sf.sum(df.v + 1) + 2).sort("id").collect()

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
        expected1 = df.groupby("id").agg(plus_one(sf.sum(df.v))).sort("id").collect()

        # Mix group aggregate pandas UDF and python UDF (order swapped)
        result2 = df.groupby("id").agg(sum_udf(plus_one(df.v))).sort("id")
        expected2 = df.groupby("id").agg(sf.sum(plus_one(df.v))).sort("id").collect()

        # Mix group aggregate pandas UDF and scalar pandas UDF
        result3 = df.groupby("id").agg(sum_udf(plus_two(df.v))).sort("id")
        expected3 = df.groupby("id").agg(sf.sum(plus_two(df.v))).sort("id").collect()

        # Mix group aggregate pandas UDF and scalar pandas UDF (order swapped)
        result4 = df.groupby("id").agg(plus_two(sum_udf(df.v))).sort("id")
        expected4 = df.groupby("id").agg(plus_two(sf.sum(df.v))).sort("id").collect()

        # Wrap group aggregate pandas UDF with two python UDFs and use python UDF in groupby
        result5 = (
            df.groupby(plus_one(df.id)).agg(plus_one(sum_udf(plus_one(df.v)))).sort("plus_one(id)")
        )
        expected5 = (
            df.groupby(plus_one(df.id)).agg(plus_one(sf.sum(plus_one(df.v)))).sort("plus_one(id)")
        ).collect()

        # Wrap group aggregate pandas UDF with two scala pandas UDF and user scala pandas UDF in
        # groupby
        result6 = (
            df.groupby(plus_two(df.id)).agg(plus_two(sum_udf(plus_two(df.v)))).sort("plus_two(id)")
        )
        expected6 = (
            df.groupby(plus_two(df.id)).agg(plus_two(sf.sum(plus_two(df.v)))).sort("plus_two(id)")
        ).collect()

        self.assertEqual(expected1, result1.collect())
        self.assertEqual(expected2, result2.collect())
        self.assertEqual(expected3, result3.collect())
        self.assertEqual(expected4, result4.collect())
        self.assertEqual(expected5, result5.collect())
        self.assertEqual(expected6, result6.collect())

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
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
            .agg(sf.mean(df.v), sf.sum(df.v), sf.mean(df.v).alias("weighted_mean(v, w)"))
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
        expected1 = df.groupby(df.v % 2).agg(sf.sum(df.v)).collect()

        # empty groupby
        result2 = df.groupby().agg(sum_udf(df.v))
        expected2 = df.groupby().agg(sf.sum(df.v)).collect()

        # groupby one column and one sql expression
        result3 = df.groupby(df.id, df.v % 2).agg(sum_udf(df.v)).orderBy(df.id, df.v % 2)
        expected3 = df.groupby(df.id, df.v % 2).agg(sf.sum(df.v)).orderBy(df.id, df.v % 2).collect()

        # groupby one python UDF
        result4 = df.groupby(plus_one(df.id)).agg(sum_udf(df.v)).sort("plus_one(id)")
        expected4 = df.groupby(plus_one(df.id)).agg(sf.sum(df.v)).sort("plus_one(id)").collect()

        # groupby one scalar pandas UDF
        result5 = df.groupby(plus_two(df.id)).agg(sum_udf(df.v)).sort("sum(v)")
        expected5 = df.groupby(plus_two(df.id)).agg(sf.sum(df.v)).sort("sum(v)").collect()

        # groupby one expression and one python UDF
        result6 = (
            df.groupby(df.v % 2, plus_one(df.id))
            .agg(sum_udf(df.v))
            .sort(["(v % 2)", "plus_one(id)"])
        )
        expected6 = (
            df.groupby(df.v % 2, plus_one(df.id))
            .agg(sf.sum(df.v))
            .sort(["(v % 2)", "plus_one(id)"])
        ).collect()

        # groupby one expression and one scalar pandas UDF
        result7 = (
            df.groupby(df.v % 2, plus_two(df.id))
            .agg(sum_udf(df.v))
            .sort(["sum(v)", "plus_two(id)"])
        )
        expected7 = (
            df.groupby(df.v % 2, plus_two(df.id)).agg(sf.sum(df.v)).sort(["sum(v)", "plus_two(id)"])
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
                sum_udf(sf.col("v")),
                sum_udf(sf.col("v1") + 3),
                sum_udf(sf.col("v2")) + 5,
                plus_one(sum_udf(sf.col("v1"))),
                sum_udf(plus_one(sf.col("v2"))),
            )
            .sort(["id", "(v % 2)"])
        )

        expected1 = (
            df.withColumn("v1", df.v + 1)
            .withColumn("v2", df.v + 2)
            .groupby(df.id, df.v % 2)
            .agg(
                sf.sum(sf.col("v")),
                sf.sum(sf.col("v1") + 3),
                sf.sum(sf.col("v2")) + 5,
                plus_one(sf.sum(sf.col("v1"))),
                sf.sum(plus_one(sf.col("v2"))),
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
                sum_udf(sf.col("v")),
                sum_udf(sf.col("v1") + 3),
                sum_udf(sf.col("v2")) + 5,
                plus_two(sum_udf(sf.col("v1"))),
                sum_udf(plus_two(sf.col("v2"))),
            )
            .sort(["id", "(v % 2)"])
        )

        expected2 = (
            df.withColumn("v1", df.v + 1)
            .withColumn("v2", df.v + 2)
            .groupby(df.id, df.v % 2)
            .agg(
                sf.sum(sf.col("v")),
                sf.sum(sf.col("v1") + 3),
                sf.sum(sf.col("v2")) + 5,
                plus_two(sf.sum(sf.col("v1"))),
                sf.sum(plus_two(sf.col("v2"))),
            )
            .sort(["id", "(v % 2)"])
            .collect()
        )

        # Test sequential groupby aggregate
        result3 = (
            df.groupby("id")
            .agg(sum_udf(df.v).alias("v"))
            .groupby("id")
            .agg(sum_udf(sf.col("v")))
            .sort("id")
        )

        expected3 = (
            df.groupby("id")
            .agg(sf.sum(df.v).alias("v"))
            .groupby("id")
            .agg(sf.sum(sf.col("v")))
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
            expected1 = df.groupby(df.id).agg(sf.sum(df.v)).collect()
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
            df.groupby(df.id).agg(mean_udf(df.v), sf.mean(df.v)).collect()

    def test_register_vectorized_udf_basic(self):
        import pyarrow as pa

        sum_arrow_udf = arrow_udf(
            lambda v: pa.compute.sum(v).cast(pa.int32()),
            "integer",
            PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF,
        )

        self.assertEqual(sum_arrow_udf.evalType, PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF)

        with self.temp_func("sum_arrow_udf"):
            group_agg_pandas_udf = self.spark.udf.register("sum_arrow_udf", sum_arrow_udf)
            self.assertEqual(
                group_agg_pandas_udf.evalType, PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF
            )
            q = """
                SELECT sum_arrow_udf(v1)
                FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2) GROUP BY v2
                """
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

        with self.tempView("table"), self.temp_func("max_udf"):
            df.createTempView("table")
            self.spark.udf.register("max_udf", max_udf)

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

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_named_arguments(self):
        df = self.data
        weighted_mean = self.arrow_agg_weighted_mean_udf

        with self.tempView("v"), self.temp_func("weighted_mean"):
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
                        aggregated.collect(),
                        df.groupby("id").agg(sf.mean(df.v).alias("wm")).collect(),
                    )

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_named_arguments_negative(self):
        df = self.data
        weighted_mean = self.arrow_agg_weighted_mean_udf

        with self.tempView("v"), self.temp_func("weighted_mean"):
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

        with self.tempView("v"), self.temp_func("weighted_mean"):
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
                        aggregated.collect(),
                        df.groupby("id").agg(sf.mean(df.v).alias("wm")).collect(),
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

        with self.tempView("v"), self.temp_func("biased_sum"):
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
                        df.groupby("id").agg((sf.sum(df.v) + sf.lit(100)).alias("s")).collect(),
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
                        df.groupby("id").agg((sf.sum(df.v) + sf.sum(df.w)).alias("s")).collect(),
                    )

    def test_complex_agg_collect_set(self):
        import pyarrow as pa

        df = self.spark.createDataFrame([(1, 1), (1, 2), (2, 3), (2, 5), (2, 1)], ("id", "v"))

        @arrow_udf("array<int>")
        def arrow_collect_set(v: pa.Array) -> pa.Scalar:
            assert isinstance(v, pa.Array), str(type(v))
            s = sorted([x.as_py() for x in pa.compute.unique(v)])
            t = pa.list_(pa.int32())
            return pa.scalar(value=s, type=t)

        result1 = df.select(
            arrow_collect_set(df["id"]).alias("ids"),
            arrow_collect_set(df["v"]).alias("vs"),
        )

        expected1 = df.select(
            sf.sort_array(sf.collect_set(df["id"])).alias("ids"),
            sf.sort_array(sf.collect_set(df["v"])).alias("vs"),
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_complex_agg_collect_list(self):
        import pyarrow as pa

        df = self.spark.createDataFrame([(1, 1), (1, 2), (2, 3), (2, 5), (2, 1)], ("id", "v"))

        @arrow_udf("array<int>")
        def arrow_collect_list(v: pa.Array) -> pa.Scalar:
            assert isinstance(v, pa.Array), str(type(v))
            s = sorted([x.as_py() for x in v])
            t = pa.list_(pa.int32())
            return pa.scalar(value=s, type=t)

        result1 = df.select(
            arrow_collect_list(df["id"]).alias("ids"),
            arrow_collect_list(df["v"]).alias("vs"),
        )

        expected1 = df.select(
            sf.sort_array(sf.collect_list(df["id"])).alias("ids"),
            sf.sort_array(sf.collect_list(df["v"])).alias("vs"),
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_complex_agg_collect_as_map(self):
        import pyarrow as pa

        df = self.spark.createDataFrame([(1, 1), (2, 2), (3, 5)], ("id", "v"))

        @arrow_udf("map<int, int>")
        def arrow_collect_as_map(id: pa.Array, v: pa.Array) -> pa.Scalar:
            assert isinstance(id, pa.Array), str(type(id))
            assert isinstance(v, pa.Array), str(type(v))
            d = {i: j for i, j in zip(id.to_pylist(), v.to_pylist())}
            t = pa.map_(pa.int32(), pa.int32())
            return pa.scalar(value=d, type=t)

        result1 = df.select(
            arrow_collect_as_map("id", "v").alias("map"),
        )

        expected1 = df.select(
            sf.map_from_arrays(sf.collect_list("id"), sf.collect_list("v")).alias("map"),
        )

        self.assertEqual(expected1.collect(), result1.collect())

    def test_complex_agg_min_max_struct(self):
        import pyarrow as pa

        df = self.spark.createDataFrame([(1, 1), (2, 2), (3, 5)], ("id", "v"))

        @arrow_udf("struct<m1: int, m2:int>")
        def arrow_collect_min_max(id: pa.Array, v: pa.Array) -> pa.Scalar:
            assert isinstance(id, pa.Array), str(type(id))
            assert isinstance(v, pa.Array), str(type(v))
            m1 = pa.compute.min(id)
            m2 = pa.compute.max(v)
            t = pa.struct([pa.field("m1", pa.int32()), pa.field("m2", pa.int32())])
            return pa.scalar(value={"m1": m1.as_py(), "m2": m2.as_py()}, type=t)

        result1 = df.select(
            arrow_collect_min_max("id", "v").alias("struct"),
        )

        expected1 = df.select(
            sf.struct(sf.min("id").alias("m1"), sf.max("v").alias("m2")).alias("struct"),
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

        @arrow_udf("time", ArrowUDFType.GROUPED_AGG)
        def agg_min_time(v):
            assert isinstance(v, pa.Array)
            assert isinstance(v, pa.Time64Array)
            return pa.compute.min(v)

        expected1 = df.select(sf.min("t").alias("res"))
        result1 = df.select(agg_min_time("t").alias("res"))
        self.assertEqual(expected1.collect(), result1.collect())

        expected2 = df.groupby("i").agg(sf.min("t").alias("res")).sort("i")
        result2 = df.groupby("i").agg(agg_min_time("t").alias("res")).sort("i")
        self.assertEqual(expected2.collect(), result2.collect())

    def test_input_output_variant(self):
        import pyarrow as pa

        @arrow_udf("variant")
        def first_variant(v: pa.Array) -> pa.Scalar:
            assert isinstance(v, pa.Array)
            assert isinstance(v, pa.StructArray)
            assert isinstance(v.field("metadata"), pa.BinaryArray)
            assert isinstance(v.field("value"), pa.BinaryArray)
            return v[0]

        @arrow_udf("variant")
        def last_variant(v: pa.Array) -> pa.Scalar:
            assert isinstance(v, pa.Array)
            assert isinstance(v, pa.StructArray)
            assert isinstance(v.field("metadata"), pa.BinaryArray)
            assert isinstance(v.field("value"), pa.BinaryArray)
            return v[-1]

        df = self.spark.range(0, 10).selectExpr("parse_json(cast(id as string)) v")
        result = df.select(
            first_variant("v").alias("first"),
            last_variant("v").alias("last"),
        )
        self.assertEqual(
            result.schema,
            StructType(
                [
                    StructField("first", VariantType(), True),
                    StructField("last", VariantType(), True),
                ]
            ),
        )

        row = result.first()
        self.assertIsInstance(row.first, VariantVal)
        self.assertIsInstance(row.last, VariantVal)

    def test_return_type_coercion(self):
        import pyarrow as pa

        df = self.spark.range(10)

        @arrow_udf("long", ArrowUDFType.GROUPED_AGG)
        def agg_long(id: pa.Array) -> int:
            assert isinstance(id, pa.Array), str(type(id))
            return pa.scalar(value=len(id), type=pa.int64())

        result1 = df.select(agg_long("id").alias("res"))
        self.assertEqual(1, len(result1.collect()))

        # long -> int coercion
        @arrow_udf("int", ArrowUDFType.GROUPED_AGG)
        def agg_int1(id: pa.Array) -> int:
            assert isinstance(id, pa.Array), str(type(id))
            return pa.scalar(value=len(id), type=pa.int64())

        result2 = df.select(agg_int1("id").alias("res"))
        self.assertEqual(1, len(result2.collect()))

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

        df = self.spark.range(10)
        expected = df.select(
            sf.max("id").alias("max"),
            sf.min("id").alias("min"),
            sf.avg("id").alias("avg"),
        )

        result = df.select(
            np_max_udf("id").alias("max"),
            np_min_udf("id").alias("min"),
            np_avg_udf("id").alias("avg"),
        )
        self.assertEqual(expected.collect(), result.collect())

    def test_unsupported_return_types(self):
        import pyarrow as pa

        with self.quiet():
            with self.assertRaisesRegex(
                NotImplementedError,
                "Invalid return type with grouped aggregate "
                "Arrow UDFs.*ArrayType.*YearMonthIntervalType",
            ):
                arrow_udf(
                    lambda x: x,
                    ArrayType(ArrayType(YearMonthIntervalType())),
                    ArrowUDFType.GROUPED_AGG,
                )

            with self.assertRaisesRegex(
                NotImplementedError,
                "Invalid return type with grouped aggregate "
                "Arrow UDFs.*ArrayType.*YearMonthIntervalType",
            ):

                @arrow_udf(ArrayType(ArrayType(YearMonthIntervalType())), ArrowUDFType.GROUPED_AGG)
                def func_a(a: pa.Array) -> pa.Scalar:
                    return pa.compute.max(a)

    def test_0_args(self):
        import pyarrow as pa

        df = self.spark.range(10).withColumn("k", sf.col("id") % 3)

        @arrow_udf("long", ArrowUDFType.GROUPED_AGG)
        def arrow_max(v) -> int:
            return pa.compute.max(v).as_py()

        @arrow_udf("long", ArrowUDFType.GROUPED_AGG)
        def arrow_lit_1() -> int:
            return 1

        expected1 = df.select(sf.max("id").alias("res1"), sf.lit(1).alias("res1"))
        result1 = df.select(arrow_max("id").alias("res1"), arrow_lit_1().alias("res1"))
        self.assertEqual(expected1.collect(), result1.collect())

        expected2 = (
            df.groupby("k")
            .agg(
                sf.max("id").alias("res1"),
                sf.lit(1).alias("res1"),
            )
            .sort("k")
        )
        result2 = (
            df.groupby("k")
            .agg(
                arrow_max("id").alias("res1"),
                arrow_lit_1().alias("res1"),
            )
            .sort("k")
        )
        self.assertEqual(expected2.collect(), result2.collect())

    def test_arrow_batch_slicing(self):
        import pyarrow as pa

        df = self.spark.range(10000000).select(
            (sf.col("id") % 2).alias("key"), sf.col("id").alias("v")
        )

        @arrow_udf("long", ArrowUDFType.GROUPED_AGG)
        def arrow_max(v):
            assert len(v) == 10000000 / 2, len(v)
            return pa.compute.max(v)

        expected = (df.groupby("key").agg(sf.max("v").alias("res")).sort("key")).collect()

        for maxRecords, maxBytes in [(1000, 2**31 - 1), (0, 1048576), (1000, 1048576)]:
            with self.subTest(maxRecords=maxRecords, maxBytes=maxBytes):
                with self.sql_conf(
                    {
                        "spark.sql.execution.arrow.maxRecordsPerBatch": maxRecords,
                        "spark.sql.execution.arrow.maxBytesPerBatch": maxBytes,
                    }
                ):
                    result = (
                        df.groupBy("key").agg(arrow_max("v").alias("res")).sort("key")
                    ).collect()

                    self.assertEqual(expected, result)

    @unittest.skipIf(is_remote_only(), "Requires JVM access")
    def test_grouped_agg_arrow_udf_with_logging(self):
        import pyarrow as pa

        @arrow_udf("double", ArrowUDFType.GROUPED_AGG)
        def my_grouped_agg_arrow_udf(x):
            assert isinstance(x, pa.Array)
            logger = logging.getLogger("test_grouped_agg_arrow")
            logger.warning(f"grouped agg arrow udf: {len(x)}")
            return pa.compute.sum(x)

        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )

        with self.sql_conf({"spark.sql.pyspark.worker.logging.enabled": "true"}):
            assertDataFrameEqual(
                df.groupby("id").agg(my_grouped_agg_arrow_udf("v").alias("result")),
                [Row(id=1, result=3.0), Row(id=2, result=18.0)],
            )

            logs = self.spark.tvf.python_worker_logs()

            assertDataFrameEqual(
                logs.select("level", "msg", "context", "logger"),
                [
                    Row(
                        level="WARNING",
                        msg=f"grouped agg arrow udf: {n}",
                        context={"func_name": my_grouped_agg_arrow_udf.__name__},
                        logger="test_grouped_agg_arrow",
                    )
                    for n in [2, 3]
                ],
            )

    def test_iterator_grouped_agg_single_column(self):
        """
        Test iterator API for grouped aggregation with single column.
        """
        import pyarrow as pa
        from typing import Iterator

        @arrow_udf("double")
        def arrow_mean_iter(it: Iterator[pa.Array]) -> float:
            sum_val = 0.0
            cnt = 0
            for v in it:
                assert isinstance(v, pa.Array)
                sum_val += pa.compute.sum(v).as_py()
                cnt += len(v)
            return sum_val / cnt if cnt > 0 else 0.0

        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )

        result = df.groupby("id").agg(arrow_mean_iter(df["v"]).alias("mean")).sort("id")
        expected = df.groupby("id").agg(sf.mean(df["v"]).alias("mean")).sort("id").collect()

        self.assertEqual(expected, result.collect())

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_iterator_grouped_agg_multiple_columns(self):
        """
        Test iterator API for grouped aggregation with multiple columns.
        """
        import pyarrow as pa
        import numpy as np

        @arrow_udf("double")
        def arrow_weighted_mean_iter(it: Iterator[Tuple[pa.Array, pa.Array]]) -> float:
            weighted_sum = 0.0
            weight = 0.0
            for v, w in it:
                assert isinstance(v, pa.Array)
                assert isinstance(w, pa.Array)
                weighted_sum += np.dot(v, w)
                weight += pa.compute.sum(w).as_py()
            return weighted_sum / weight if weight > 0 else 0.0

        df = self.spark.createDataFrame(
            [(1, 1.0, 1.0), (1, 2.0, 2.0), (2, 3.0, 1.0), (2, 5.0, 2.0), (2, 10.0, 3.0)],
            ("id", "v", "w"),
        )

        result = (
            df.groupby("id")
            .agg(arrow_weighted_mean_iter(df["v"], df["w"]).alias("wm"))
            .sort("id")
            .collect()
        )

        # Expected weighted means:
        # Group 1: (1.0*1.0 + 2.0*2.0) / (1.0 + 2.0) = 5.0 / 3.0
        # Group 2: (3.0*1.0 + 5.0*2.0 + 10.0*3.0) / (1.0 + 2.0 + 3.0) = 43.0 / 6.0
        expected = [(1, 5.0 / 3.0), (2, 43.0 / 6.0)]

        self.assertEqual(len(result), len(expected))
        for r, (exp_id, exp_wm) in zip(result, expected):
            self.assertEqual(r["id"], exp_id)
            self.assertAlmostEqual(r["wm"], exp_wm, places=5)

    def test_iterator_grouped_agg_eval_type(self):
        """
        Test that the eval type is correctly inferred for iterator grouped agg UDFs.
        """
        import pyarrow as pa
        from typing import Iterator

        @arrow_udf("double")
        def arrow_sum_iter(it: Iterator[pa.Array]) -> float:
            total = 0.0
            for v in it:
                total += pa.compute.sum(v).as_py()
            return total

        self.assertEqual(arrow_sum_iter.evalType, PythonEvalType.SQL_GROUPED_AGG_ARROW_ITER_UDF)

    def test_iterator_grouped_agg_partial_consumption(self):
        """
        Test that iterator grouped agg UDF can partially consume batches.
        This ensures that batches are processed one by one without loading all data into memory.
        """
        import pyarrow as pa
        from typing import Iterator

        # Create a dataset with multiple batches per group
        # Use small batch size to ensure multiple batches per group
        # Use same value for all data points to avoid ordering issues
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": 2}):
            df = self.spark.createDataFrame(
                [(1, 1.0), (1, 1.0), (1, 1.0), (1, 1.0), (2, 1.0), (2, 1.0)], ("id", "v")
            )

            @arrow_udf("struct<count:bigint,sum:double>")
            def arrow_count_sum_partial(it: Iterator[pa.Array]) -> dict:
                # Only consume first two batches, then return
                # This tests that partial consumption works correctly
                total = 0.0
                count = 0
                for i, v in enumerate(it):
                    if i < 2:  # Only process first 2 batches
                        total += pa.compute.sum(v).as_py()
                        count += len(v)
                    else:
                        # Stop early - partial consumption
                        break
                return {"count": count, "sum": total}

            result = (
                df.groupby("id").agg(arrow_count_sum_partial(df["v"]).alias("result")).sort("id")
            )

            # Verify results are correct for partial consumption
            # With batch size = 2:
            # Group 1 (id=1): 4 values in 2 batches -> processes both batches
            #   Batch 1: [1.0, 1.0], Batch 2: [1.0, 1.0]
            #   Result: count=4, sum=4.0
            # Group 2 (id=2): 2 values in 1 batch -> processes 1 batch (only 1 batch available)
            #   Batch 1: [1.0, 1.0]
            #   Result: count=2, sum=2.0
            actual = result.collect()
            self.assertEqual(len(actual), 2, "Should have results for both groups")

            # Verify both groups were processed correctly
            # Group 1: processes 2 batches (all available)
            group1_result = next(row for row in actual if row["id"] == 1)
            self.assertEqual(
                group1_result["result"]["count"],
                4,
                msg="Group 1 should process 4 values (2 batches)",
            )
            self.assertAlmostEqual(
                group1_result["result"]["sum"], 4.0, places=5, msg="Group 1 should sum to 4.0"
            )

            # Group 2: processes 1 batch (only batch available)
            group2_result = next(row for row in actual if row["id"] == 2)
            self.assertEqual(
                group2_result["result"]["count"],
                2,
                msg="Group 2 should process 2 values (1 batch)",
            )
            self.assertAlmostEqual(
                group2_result["result"]["sum"], 2.0, places=5, msg="Group 2 should sum to 2.0"
            )

    def test_iterator_grouped_agg_sql_single_column(self):
        """
        Test iterator API for grouped aggregation with single column in SQL.
        """
        import pyarrow as pa

        @arrow_udf("double")
        def arrow_mean_iter(it: Iterator[pa.Array]) -> float:
            sum_val = 0.0
            cnt = 0
            for v in it:
                assert isinstance(v, pa.Array)
                sum_val += pa.compute.sum(v).as_py()
                cnt += len(v)
            return sum_val / cnt if cnt > 0 else 0.0

        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )

        with self.tempView("test_table"), self.temp_func("arrow_mean_iter"):
            df.createOrReplaceTempView("test_table")
            self.spark.udf.register("arrow_mean_iter", arrow_mean_iter)

            # Test SQL query with GROUP BY
            result_sql = self.spark.sql(
                "SELECT id, arrow_mean_iter(v) as mean FROM test_table GROUP BY id ORDER BY id"
            )
            expected = df.groupby("id").agg(sf.mean(df["v"]).alias("mean")).sort("id").collect()

            self.assertEqual(expected, result_sql.collect())

    def test_iterator_grouped_agg_sql_multiple_columns(self):
        """
        Test iterator API for grouped aggregation with multiple columns in SQL.
        """
        import pyarrow as pa

        @arrow_udf("double")
        def arrow_weighted_mean_iter(it: Iterator[Tuple[pa.Array, pa.Array]]) -> float:
            weighted_sum = 0.0
            weight = 0.0
            for v, w in it:
                assert isinstance(v, pa.Array)
                assert isinstance(w, pa.Array)
                weighted_sum += pa.compute.sum(pa.compute.multiply(v, w)).as_py()
                weight += pa.compute.sum(w).as_py()
            return weighted_sum / weight if weight > 0 else 0.0

        df = self.spark.createDataFrame(
            [(1, 1.0, 1.0), (1, 2.0, 2.0), (2, 3.0, 1.0), (2, 5.0, 2.0), (2, 10.0, 3.0)],
            ("id", "v", "w"),
        )

        with self.tempView("test_table"), self.temp_func("arrow_weighted_mean_iter"):
            df.createOrReplaceTempView("test_table")
            self.spark.udf.register("arrow_weighted_mean_iter", arrow_weighted_mean_iter)

            # Test SQL query with GROUP BY and multiple columns
            result_sql = self.spark.sql(
                "SELECT id, arrow_weighted_mean_iter(v, w) as wm "
                "FROM test_table GROUP BY id ORDER BY id"
            )

            # Expected weighted means:
            # Group 1: (1.0*1.0 + 2.0*2.0) / (1.0 + 2.0) = 5.0 / 3.0
            # Group 2: (3.0*1.0 + 5.0*2.0 + 10.0*3.0) / (1.0 + 2.0 + 3.0) = 43.0 / 6.0
            expected = [Row(id=1, wm=5.0 / 3.0), Row(id=2, wm=43.0 / 6.0)]

            actual_results = result_sql.collect()
            self.assertEqual(actual_results, expected)


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
