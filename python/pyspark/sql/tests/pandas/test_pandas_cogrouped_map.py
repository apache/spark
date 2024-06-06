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

from pyspark.sql.functions import array, explode, col, lit, udf, pandas_udf, sum
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StructType,
    StructField,
    YearMonthIntervalType,
    Row,
)
from pyspark.sql.window import Window
from pyspark.errors import IllegalArgumentException, PythonException
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

if have_pandas:
    import pandas as pd
    from pandas.testing import assert_frame_equal

if have_pyarrow:
    import pyarrow as pa  # noqa: F401


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class CogroupedApplyInPandasTestsMixin:
    @property
    def data1(self):
        return (
            self.spark.range(10)
            .withColumn("ks", array([lit(i) for i in range(20, 30)]))
            .withColumn("k", explode(col("ks")))
            .withColumn("v", col("k") * 10)
            .drop("ks")
        )

    @property
    def data2(self):
        return (
            self.spark.range(10)
            .withColumn("ks", array([lit(i) for i in range(20, 30)]))
            .withColumn("k", explode(col("ks")))
            .withColumn("v2", col("k") * 100)
            .drop("ks")
        )

    def test_simple(self):
        self._test_merge(self.data1, self.data2)

    def test_left_group_empty(self):
        left = self.data1.where(col("id") % 2 == 0)
        self._test_merge(left, self.data2)

    def test_right_group_empty(self):
        right = self.data2.where(col("id") % 2 == 0)
        self._test_merge(self.data1, right)

    def test_different_schemas(self):
        right = self.data2.withColumn("v3", lit("a"))
        self._test_merge(
            self.data1, right, output_schema="id long, k int, v int, v2 int, v3 string"
        )

    def test_different_keys(self):
        left = self.data1
        right = self.data2

        def merge_pandas(lft, rgt):
            return pd.merge(lft.rename(columns={"id2": "id"}), rgt, on=["id", "k"])

        result = (
            left.withColumnRenamed("id", "id2")
            .groupby("id2")
            .cogroup(right.groupby("id"))
            .applyInPandas(merge_pandas, "id long, k int, v int, v2 int")
            .sort(["id", "k"])
            .toPandas()
        )

        left = left.toPandas()
        right = right.toPandas()

        expected = pd.merge(left, right, on=["id", "k"]).sort_values(by=["id", "k"])

        assert_frame_equal(expected, result)

    def test_complex_group_by(self):
        left = pd.DataFrame.from_dict({"id": [1, 2, 3], "k": [5, 6, 7], "v": [9, 10, 11]})

        right = pd.DataFrame.from_dict({"id": [11, 12, 13], "k": [5, 6, 7], "v2": [90, 100, 110]})

        left_gdf = self.spark.createDataFrame(left).groupby(col("id") % 2 == 0)

        right_gdf = self.spark.createDataFrame(right).groupby(col("id") % 2 == 0)

        def merge_pandas(lft, rgt):
            return pd.merge(lft[["k", "v"]], rgt[["k", "v2"]], on=["k"])

        result = (
            left_gdf.cogroup(right_gdf)
            .applyInPandas(merge_pandas, "k long, v long, v2 long")
            .sort(["k"])
            .toPandas()
        )

        expected = pd.DataFrame.from_dict({"k": [5, 6, 7], "v": [9, 10, 11], "v2": [90, 100, 110]})

        assert_frame_equal(expected, result)

    def test_empty_group_by(self):
        self._test_merge(self.data1, self.data2, by=[])

    def test_different_group_key_cardinality(self):
        with self.quiet():
            self.check_different_group_key_cardinality()

    def check_different_group_key_cardinality(self):
        left = self.data1
        right = self.data2

        def merge_pandas(lft, _):
            return lft

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "requirement failed: Cogroup keys must have same size: 2 != 1",
        ):
            (left.groupby("id", "k").cogroup(right.groupby("id"))).applyInPandas(
                merge_pandas, "id long, k int, v int"
            ).schema

    def test_apply_in_pandas_not_returning_pandas_dataframe(self):
        with self.quiet():
            self.check_apply_in_pandas_not_returning_pandas_dataframe()

    def check_apply_in_pandas_not_returning_pandas_dataframe(self):
        self._test_merge_error(
            fn=lambda lft, rgt: lft.size + rgt.size,
            error_class=PythonException,
            error_message_regex="Return type of the user-defined function "
            "should be pandas.DataFrame, but is int",
        )

    def test_apply_in_pandas_returning_column_names(self):
        self._test_merge(fn=lambda lft, rgt: pd.merge(lft, rgt, on=["id", "k"]))

    def test_apply_in_pandas_returning_no_column_names(self):
        def merge_pandas(lft, rgt):
            res = pd.merge(lft, rgt, on=["id", "k"])
            res.columns = range(res.columns.size)
            return res

        self._test_merge(fn=merge_pandas)

    def test_apply_in_pandas_returning_column_names_sometimes(self):
        def merge_pandas(lft, rgt):
            res = pd.merge(lft, rgt, on=["id", "k"])
            if 0 in lft["id"] and lft["id"][0] % 2 == 0:
                return res
            res.columns = range(res.columns.size)
            return res

        self._test_merge(fn=merge_pandas)

    def test_apply_in_pandas_returning_wrong_column_names(self):
        with self.quiet():
            self.check_apply_in_pandas_returning_wrong_column_names()

    def check_apply_in_pandas_returning_wrong_column_names(self):
        def merge_pandas(lft, rgt):
            if 0 in lft["id"] and lft["id"][0] % 2 == 0:
                lft["add"] = 0
            if 0 in rgt["id"] and rgt["id"][0] % 3 == 0:
                rgt["more"] = 1
            return pd.merge(lft, rgt, on=["id", "k"])

        self._test_merge_error(
            fn=merge_pandas,
            error_class=PythonException,
            error_message_regex="Column names of the returned pandas.DataFrame "
            "do not match specified schema. Unexpected: add, more.\n",
        )

    def test_apply_in_pandas_returning_no_column_names_and_wrong_amount(self):
        with self.quiet():
            self.check_apply_in_pandas_returning_no_column_names_and_wrong_amount()

    def check_apply_in_pandas_returning_no_column_names_and_wrong_amount(self):
        def merge_pandas(lft, rgt):
            if 0 in lft["id"] and lft["id"][0] % 2 == 0:
                lft[3] = 0
            if 0 in rgt["id"] and rgt["id"][0] % 3 == 0:
                rgt[3] = 1
            res = pd.merge(lft, rgt, on=["id", "k"])
            res.columns = range(res.columns.size)
            return res

        self._test_merge_error(
            fn=merge_pandas,
            error_class=PythonException,
            error_message_regex="Number of columns of the returned pandas.DataFrame "
            "doesn't match specified schema. Expected: 4 Actual: 6\n",
        )

    def test_apply_in_pandas_returning_empty_dataframe(self):
        def merge_pandas(lft, rgt):
            if 0 in lft["id"] and lft["id"][0] % 2 == 0:
                return pd.DataFrame()
            if 0 in rgt["id"] and rgt["id"][0] % 3 == 0:
                return pd.DataFrame()
            return pd.merge(lft, rgt, on=["id", "k"])

        self._test_merge_empty(fn=merge_pandas)

    def test_apply_in_pandas_returning_incompatible_type(self):
        with self.quiet():
            self.check_apply_in_pandas_returning_incompatible_type()

    def check_apply_in_pandas_returning_incompatible_type(self):
        for safely in [True, False]:
            with self.subTest(convertToArrowArraySafely=safely), self.sql_conf(
                {"spark.sql.execution.pandas.convertToArrowArraySafely": safely}
            ):
                # sometimes we see ValueErrors
                with self.subTest(convert="string to double"):
                    expected = (
                        r"ValueError: Exception thrown when converting pandas.Series \(object\) "
                        r"with name 'k' to Arrow Array \(double\)."
                    )
                    if safely:
                        expected = expected + (
                            " It can be caused by overflows or other "
                            "unsafe conversions warned by Arrow. Arrow safe type check "
                            "can be disabled by using SQL config "
                            "`spark.sql.execution.pandas.convertToArrowArraySafely`."
                        )
                    self._test_merge_error(
                        fn=lambda lft, rgt: pd.DataFrame({"id": [1], "k": ["2.0"]}),
                        output_schema="id long, k double",
                        error_class=PythonException,
                        error_message_regex=expected,
                    )

                # sometimes we see TypeErrors
                with self.subTest(convert="double to string"):
                    expected = (
                        r"TypeError: Exception thrown when converting pandas.Series \(float64\) "
                        r"with name 'k' to Arrow Array \(string\).\n"
                    )
                    self._test_merge_error(
                        fn=lambda lft, rgt: pd.DataFrame({"id": [1], "k": [2.0]}),
                        output_schema="id long, k string",
                        error_class=PythonException,
                        error_message_regex=expected,
                    )

    def test_mixed_scalar_udfs_followed_by_cogrouby_apply(self):
        df = self.spark.range(0, 10).toDF("v1")
        df = df.withColumn("v2", udf(lambda x: x + 1, "int")(df["v1"])).withColumn(
            "v3", pandas_udf(lambda x: x + 2, "int")(df["v1"])
        )

        result = (
            df.groupby()
            .cogroup(df.groupby())
            .applyInPandas(
                lambda x, y: pd.DataFrame([(x.sum().sum(), y.sum().sum())]), "sum1 int, sum2 int"
            )
            .collect()
        )

        self.assertEqual(result[0]["sum1"], 165)
        self.assertEqual(result[0]["sum2"], 165)

    def test_with_key_left(self):
        self._test_with_key(self.data1, self.data1, isLeft=True)

    def test_with_key_right(self):
        self._test_with_key(self.data1, self.data1, isLeft=False)

    def test_with_key_left_group_empty(self):
        left = self.data1.where(col("id") % 2 == 0)
        self._test_with_key(left, self.data1, isLeft=True)

    def test_with_key_right_group_empty(self):
        right = self.data1.where(col("id") % 2 == 0)
        self._test_with_key(self.data1, right, isLeft=False)

    def test_with_key_complex(self):
        def left_assign_key(key, lft, _):
            return lft.assign(key=key[0])

        result = (
            self.data1.groupby(col("id") % 2 == 0)
            .cogroup(self.data2.groupby(col("id") % 2 == 0))
            .applyInPandas(left_assign_key, "id long, k int, v int, key boolean")
            .sort(["id", "k"])
            .toPandas()
        )

        expected = self.data1.toPandas()
        expected = expected.assign(key=expected.id % 2 == 0)

        assert_frame_equal(expected, result)

    def test_wrong_return_type(self):
        with self.quiet():
            self.check_wrong_return_type()

    def check_wrong_return_type(self):
        # Test that we get a sensible exception invalid values passed to apply
        self._test_merge_error(
            fn=lambda l, r: l,
            output_schema=(
                StructType().add("id", LongType()).add("v", ArrayType(YearMonthIntervalType()))
            ),
            error_class=NotImplementedError,
            error_message_regex="Invalid return type.*ArrayType.*YearMonthIntervalType",
        )

    def test_wrong_args(self):
        with self.quiet():
            self.check_wrong_args()

    def check_wrong_args(self):
        self.__test_merge_error(
            fn=lambda: 1,
            output_schema=StructType([StructField("d", DoubleType())]),
            error_class=ValueError,
            error_message_regex="Invalid function",
        )

    def test_case_insensitive_grouping_column(self):
        # SPARK-31915: case-insensitive grouping column should work.
        df1 = self.spark.createDataFrame([(1, 1)], ("column", "value"))

        row = (
            df1.groupby("ColUmn")
            .cogroup(df1.groupby("COLUMN"))
            .applyInPandas(lambda r, l: r + l, "column long, value long")
            .first()
        )
        self.assertEqual(row.asDict(), Row(column=2, value=2).asDict())

        df2 = self.spark.createDataFrame([(1, 1)], ("column", "value"))

        row = (
            df1.groupby("ColUmn")
            .cogroup(df2.groupby("COLUMN"))
            .applyInPandas(lambda r, l: r + l, "column long, value long")
            .first()
        )
        self.assertEqual(row.asDict(), Row(column=2, value=2).asDict())

    def test_self_join(self):
        # SPARK-34319: self-join with FlatMapCoGroupsInPandas
        df = self.spark.createDataFrame([(1, 1)], ("column", "value"))

        row = (
            df.groupby("ColUmn")
            .cogroup(df.groupby("COLUMN"))
            .applyInPandas(lambda r, l: r + l, "column long, value long")
        )

        row = row.join(row).first()

        self.assertEqual(row.asDict(), Row(column=2, value=2).asDict())

    def test_with_window_function(self):
        # SPARK-42168: a window function with same partition keys but differing key order
        ids = 2
        days = 100
        vals = 10000
        parts = 10

        id_df = self.spark.range(ids)
        day_df = self.spark.range(days).withColumnRenamed("id", "day")
        vals_df = self.spark.range(vals).withColumnRenamed("id", "value")
        df = id_df.join(day_df).join(vals_df)

        left_df = df.withColumnRenamed("value", "left").repartition(parts).cache()
        # SPARK-42132: this bug requires us to alias all columns from df here
        right_df = (
            df.select(col("id").alias("id"), col("day").alias("day"), col("value").alias("right"))
            .repartition(parts)
            .cache()
        )

        # note the column order is different to the groupBy("id", "day") column order below
        window = Window.partitionBy("day", "id")

        left_grouped_df = left_df.groupBy("id", "day")
        right_grouped_df = right_df.withColumn("day_sum", sum(col("day")).over(window)).groupBy(
            "id", "day"
        )

        def cogroup(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "id": left["id"][0]
                        if not left.empty
                        else (right["id"][0] if not right.empty else None),
                        "day": left["day"][0]
                        if not left.empty
                        else (right["day"][0] if not right.empty else None),
                        "lefts": len(left.index),
                        "rights": len(right.index),
                    }
                ]
            )

        df = left_grouped_df.cogroup(right_grouped_df).applyInPandas(
            cogroup, schema="id long, day long, lefts integer, rights integer"
        )

        actual = df.orderBy("id", "day").take(days)
        self.assertEqual(actual, [Row(0, day, vals, vals) for day in range(days)])

    def test_with_local_data(self):
        df1 = self.spark.createDataFrame(
            [(1, 1.0, "a"), (2, 2.0, "b"), (1, 3.0, "c"), (2, 4.0, "d")], ("id", "v1", "v2")
        )
        df2 = self.spark.createDataFrame([(1, "x"), (2, "y"), (1, "z")], ("id", "v3"))

        def summarize(left, right):
            return pd.DataFrame(
                {
                    "left_rows": [len(left)],
                    "left_columns": [len(left.columns)],
                    "right_rows": [len(right)],
                    "right_columns": [len(right.columns)],
                }
            )

        df = (
            df1.groupby("id")
            .cogroup(df2.groupby("id"))
            .applyInPandas(
                summarize,
                schema="left_rows long, left_columns long, right_rows long, right_columns long",
            )
        )

        self.assertEqual(
            df._show_string(),
            "+---------+------------+----------+-------------+\n"
            "|left_rows|left_columns|right_rows|right_columns|\n"
            "+---------+------------+----------+-------------+\n"
            "|        2|           3|         2|            2|\n"
            "|        2|           3|         1|            2|\n"
            "+---------+------------+----------+-------------+\n",
        )

    @staticmethod
    def _test_with_key(left, right, isLeft):
        def right_assign_key(key, lft, rgt):
            return lft.assign(key=key[0]) if isLeft else rgt.assign(key=key[0])

        result = (
            left.groupby("id")
            .cogroup(right.groupby("id"))
            .applyInPandas(right_assign_key, "id long, k int, v int, key long")
            .toPandas()
        )

        expected = left.toPandas() if isLeft else right.toPandas()
        expected = expected.assign(key=expected.id)

        assert_frame_equal(expected, result)

    def _test_merge_empty(self, fn):
        left = self.data1.toPandas()
        right = self.data2.toPandas()

        expected = pd.merge(
            left[left["id"] % 2 != 0], right[right["id"] % 3 != 0], on=["id", "k"]
        ).sort_values(by=["id", "k"])

        self._test_merge(self.data1, self.data2, fn=fn, expected=expected)

    def _test_merge(
        self,
        left=None,
        right=None,
        by=["id"],
        fn=lambda lft, rgt: pd.merge(lft, rgt, on=["id", "k"]),
        output_schema="id long, k int, v int, v2 int",
        expected=None,
    ):
        def fn_with_key(_, lft, rgt):
            return fn(lft, rgt)

        # Test fn with and without key argument
        with self.subTest("without key"):
            self.__test_merge(left, right, by, fn, output_schema, expected)
        with self.subTest("with key"):
            self.__test_merge(left, right, by, fn_with_key, output_schema, expected)

    def __test_merge(
        self,
        left=None,
        right=None,
        by=["id"],
        fn=lambda lft, rgt: pd.merge(lft, rgt, on=["id", "k"]),
        output_schema="id long, k int, v int, v2 int",
        expected=None,
    ):
        # Test fn as is, cf. _test_merge
        left = self.data1 if left is None else left
        right = self.data2 if right is None else right

        result = (
            left.groupby(*by)
            .cogroup(right.groupby(*by))
            .applyInPandas(fn, output_schema)
            .sort(["id", "k"])
            .toPandas()
        )

        left = left.toPandas()
        right = right.toPandas()

        expected = (
            pd.merge(left, right, on=["id", "k"]).sort_values(by=["id", "k"])
            if expected is None
            else expected
        )

        assert_frame_equal(expected, result)

    def _test_merge_error(
        self,
        error_class,
        error_message_regex,
        left=None,
        right=None,
        by=["id"],
        fn=lambda lft, rgt: pd.merge(lft, rgt, on=["id", "k"]),
        output_schema="id long, k int, v int, v2 int",
    ):
        def fn_with_key(_, lft, rgt):
            return fn(lft, rgt)

        # Test fn with and without key argument
        with self.subTest("without key"):
            self.__test_merge_error(
                left=left,
                right=right,
                by=by,
                fn=fn,
                output_schema=output_schema,
                error_class=error_class,
                error_message_regex=error_message_regex,
            )
        with self.subTest("with key"):
            self.__test_merge_error(
                left=left,
                right=right,
                by=by,
                fn=fn_with_key,
                output_schema=output_schema,
                error_class=error_class,
                error_message_regex=error_message_regex,
            )

    def __test_merge_error(
        self,
        error_class,
        error_message_regex,
        left=None,
        right=None,
        by=["id"],
        fn=lambda lft, rgt: pd.merge(lft, rgt, on=["id", "k"]),
        output_schema="id long, k int, v int, v2 int",
    ):
        # Test fn as is, cf. _test_merge_error
        with self.assertRaisesRegex(error_class, error_message_regex):
            self.__test_merge(left, right, by, fn, output_schema)


class CogroupedApplyInPandasTests(CogroupedApplyInPandasTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_cogrouped_map import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
