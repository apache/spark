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

from pyspark.sql.functions import array, explode, col, lit, udf, pandas_udf
from pyspark.sql.types import DoubleType, StructType, StructField, Row
from pyspark.sql.utils import IllegalArgumentException, PythonException
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing.utils import QuietTest

if have_pandas:
    import pandas as pd
    from pandas.testing import assert_frame_equal

if have_pyarrow:
    import pyarrow as pa  # noqa: F401


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class CogroupedMapInPandasTests(ReusedSQLTestCase):
    @property
    def data1(self):
        return (
            self.spark.range(10)
            .toDF("id")
            .withColumn("ks", array([lit(i) for i in range(20, 30)]))
            .withColumn("k", explode(col("ks")))
            .withColumn("v", col("k") * 10)
            .drop("ks")
        )

    @property
    def data2(self):
        return (
            self.spark.range(10)
            .toDF("id")
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
        self._test_merge(self.data1, right, "id long, k int, v int, v2 int, v3 string")

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
        left = self.data1
        right = self.data2

        def merge_pandas(lft, rgt):
            return pd.merge(lft, rgt, on=["id", "k"])

        result = (
            left.groupby()
            .cogroup(right.groupby())
            .applyInPandas(merge_pandas, "id long, k int, v int, v2 int")
            .sort(["id", "k"])
            .toPandas()
        )

        left = left.toPandas()
        right = right.toPandas()

        expected = pd.merge(left, right, on=["id", "k"]).sort_values(by=["id", "k"])

        assert_frame_equal(expected, result)

    def test_different_group_key_cardinality(self):
        left = self.data1
        right = self.data2

        def merge_pandas(lft, _):
            return lft

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                IllegalArgumentException,
                "requirement failed: Cogroup keys must have same size: 2 != 1",
            ):
                (left.groupby("id", "k").cogroup(right.groupby("id"))).applyInPandas(
                    merge_pandas, "id long, k int, v int"
                )

    def test_apply_in_pandas_not_returning_pandas_dataframe(self):
        left = self.data1
        right = self.data2

        def merge_pandas(lft, rgt):
            return lft.size + rgt.size

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                PythonException,
                "Return type of the user-defined function should be pandas.DataFrame, "
                "but is <class 'numpy.int64'>",
            ):
                (
                    left.groupby("id")
                    .cogroup(right.groupby("id"))
                    .applyInPandas(merge_pandas, "id long, k int, v int, v2 int")
                    .collect()
                )

    def test_apply_in_pandas_returning_wrong_number_of_columns(self):
        left = self.data1
        right = self.data2

        def merge_pandas(lft, rgt):
            if 0 in lft["id"] and lft["id"][0] % 2 == 0:
                lft["add"] = 0
            if 0 in rgt["id"] and rgt["id"][0] % 3 == 0:
                rgt["more"] = 1
            return pd.merge(lft, rgt, on=["id", "k"])

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                PythonException,
                "Number of columns of the returned pandas.DataFrame "
                "doesn't match specified schema. Expected: 4 Actual: 6",
            ):
                (
                    # merge_pandas returns two columns for even keys while we set schema to four
                    left.groupby("id")
                    .cogroup(right.groupby("id"))
                    .applyInPandas(merge_pandas, "id long, k int, v int, v2 int")
                    .collect()
                )

    def test_apply_in_pandas_returning_empty_dataframe(self):
        left = self.data1
        right = self.data2

        def merge_pandas(lft, rgt):
            if 0 in lft["id"] and lft["id"][0] % 2 == 0:
                return pd.DataFrame([])
            if 0 in rgt["id"] and rgt["id"][0] % 3 == 0:
                return pd.DataFrame([])
            return pd.merge(lft, rgt, on=["id", "k"])

        result = (
            left.groupby("id")
            .cogroup(right.groupby("id"))
            .applyInPandas(merge_pandas, "id long, k int, v int, v2 int")
            .sort(["id", "k"])
            .toPandas()
        )

        left = left.toPandas()
        right = right.toPandas()

        expected = pd.merge(
            left[left["id"] % 2 != 0], right[right["id"] % 3 != 0], on=["id", "k"]
        ).sort_values(by=["id", "k"])

        assert_frame_equal(expected, result)

    def test_apply_in_pandas_returning_empty_dataframe_and_wrong_number_of_columns(self):
        left = self.data1
        right = self.data2

        def merge_pandas(lft, rgt):
            if 0 in lft["id"] and lft["id"][0] % 2 == 0:
                return pd.DataFrame([], columns=["id", "k"])
            return pd.merge(lft, rgt, on=["id", "k"])

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                PythonException,
                "Number of columns of the returned pandas.DataFrame doesn't "
                "match specified schema. Expected: 4 Actual: 2",
            ):
                (
                    # merge_pandas returns two columns for even keys while we set schema to four
                    left.groupby("id")
                    .cogroup(right.groupby("id"))
                    .applyInPandas(merge_pandas, "id long, k int, v int, v2 int")
                    .collect()
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
        # Test that we get a sensible exception invalid values passed to apply
        left = self.data1
        right = self.data2
        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                NotImplementedError, "Invalid return type.*ArrayType.*TimestampType"
            ):
                left.groupby("id").cogroup(right.groupby("id")).applyInPandas(
                    lambda l, r: l, "id long, v array<timestamp>"
                )

    def test_wrong_args(self):
        left = self.data1
        right = self.data2
        with self.assertRaisesRegex(ValueError, "Invalid function"):
            left.groupby("id").cogroup(right.groupby("id")).applyInPandas(
                lambda: 1, StructType([StructField("d", DoubleType())])
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

    @staticmethod
    def _test_merge(left, right, output_schema="id long, k int, v int, v2 int"):
        def merge_pandas(lft, rgt):
            return pd.merge(lft, rgt, on=["id", "k"])

        result = (
            left.groupby("id")
            .cogroup(right.groupby("id"))
            .applyInPandas(merge_pandas, output_schema)
            .sort(["id", "k"])
            .toPandas()
        )

        left = left.toPandas()
        right = right.toPandas()

        expected = pd.merge(left, right, on=["id", "k"]).sort_values(by=["id", "k"])

        assert_frame_equal(expected, result)


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_cogrouped_map import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
