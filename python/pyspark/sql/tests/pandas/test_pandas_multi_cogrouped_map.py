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

import functools
import itertools
import unittest
from typing import cast

from pyspark.sql.functions import array, explode, col, lit, udf, pandas_udf
from pyspark.sql.types import DoubleType, StructType, StructField, Row
from pyspark.errors import IllegalArgumentException, PythonException
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


# TODO (Santosh):
# Use a list of df to better represent the test
# Use itertools to cover all the cases of empty or key
# import itertools
# list(itertools.permutations([1, 2, 3]))

@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class MultiCogroupedMapInPandasTests(ReusedSQLTestCase):

    def _get_df(self, index):
        return (
            self.spark.range(10)
            .toDF("id")
            .withColumn("ks", array([lit(i) for i in range(20, 30)]))
            .withColumn("k", explode(col("ks")))
            .withColumn(f"v{index}", col("k") * 10 * index)
            .drop("ks")
        )

    @property
    def dfs(self):
        return [self._get_df(index) for index in range(0, 4)]

    @property
    def combinations(self):
        combos = []
        for index in range(0, 4):
            combos += list(itertools.combinations(range(0, 4), index))
        return combos

    def test_simple(self):
        self._test_merge(self.dfs)

    def test_with_empty_groups(self):
        for combination in self.combinations:
            dfs = [df for df in self.dfs]
            with self.subTest("Test empty groups with empty_group_df_index", empty_group_df_index=combination):
                for empty_group_df_index in combination:
                    dfs[empty_group_df_index] = dfs[empty_group_df_index].where(col("id") % 2 == 0)
                self._test_merge(dfs)

    def test_different_schemas(self):
        new_columns = [f"random{index}" for index, _ in enumerate(self.dfs)]
        dfs = [df.withColumn(new_column, lit("a")) for df, new_column in zip(self.dfs, new_columns)]
        extra_schemas = ", ".join([f"v{index} int, random{index} string" for index, _ in enumerate(self.dfs)])
        self._test_merge(dfs, f"id long, k int, {extra_schemas}")

    def test_different_keys(self):
        def merge_pandas(*dfs):
            return functools.reduce(lambda df1, df2: pd.merge(df1.rename(columns={"id2": "id"}), df2, on=["id", "k"]), dfs)

        df_head, *df_tail = self.dfs

        result = (
            df_head.withColumnRenamed("id", "id2")
            .groupby("id2")
            .cogroup(*[df.groupby("id") for df in df_tail])
            .applyInPandas(merge_pandas, "id long, k int, v0 int, v1 int, v2 int, v3 int")
            .sort(["id", "k"])
            .toPandas()
        )

        dfs = [df.toPandas() for df in self.dfs]
        expected = functools.reduce(lambda df1, df2: pd.merge(df1, df2, on=["id", "k"]), dfs)

        assert_frame_equal(expected, result)

    def test_complex_group_by(self):
        df1 = pd.DataFrame.from_dict({"id": [1, 2, 3], "k": [5, 6, 7], "v": [9, 10, 11]})

        df2 = pd.DataFrame.from_dict({"id": [11, 12, 13], "k": [5, 6, 7], "v2": [90, 100, 110]})

        df3 = pd.DataFrame.from_dict({"id": [21, 22, 23], "k": [5, 6, 7], "v3": [91, 101, 111]})

        df1_gdf = self.spark.createDataFrame(df1).groupby(col("id") % 2 == 0)

        df2_gdf = self.spark.createDataFrame(df2).groupby(col("id") % 2 == 0)

        df3_gdf = self.spark.createDataFrame(df3).groupby(col("id") % 2 == 0)

        def merge_pandas(df1, df2, df3):
            df = pd.merge(df1[["k", "v"]], df2[["k", "v2"]], on=["k"])
            return pd.merge(df, df3[["k", "v3"]], on=["k"])

        result = (
            df1_gdf.cogroup(df2_gdf, df3_gdf)
            .applyInPandas(merge_pandas, "k long, v long, v2 long, v3 long")
            .sort(["k"])
            .toPandas()
        )

        expected = pd.DataFrame.from_dict({"k": [5, 6, 7], "v": [9, 10, 11], "v2": [90, 100, 110], "v3": [91, 101, 111]})

        assert_frame_equal(expected, result)

    def test_empty_group_by(self):

        def merge_pandas(*dfs):
            return functools.reduce(lambda df1, df2: pd.merge(df1, df2, on=["id", "k"]), dfs)

        df_head, *df_tail = self.dfs

        result = (
            df_head.groupby()
            .cogroup(*[df.groupby() for df in df_tail])
            .applyInPandas(merge_pandas, "id long, k int, v0 int, v1 int, v2 int, v3 int")
            .sort(["id", "k"])
            .toPandas()
        )

        pd_dfs = [df.toPandas() for df in self.dfs]
        expected = functools.reduce(lambda df1, df2: pd.merge(df1, df2, on=["id", "k"]), pd_dfs)
        expected = expected.sort_values(by=["id", "k"])

        assert_frame_equal(expected, result)

    def test_different_group_key_cardinality(self):
        def merge_pandas(lft, _):
            return lft

        df1, df2, df3, *_ = self.dfs
        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                IllegalArgumentException,
                "requirement failed: Cogroup keys must have same size.",
            ):
                (df1.groupby("id").cogroup(df2.groupby("id"), df3.groupby("id", "k"))
                    .applyInPandas(merge_pandas, "id long, k int, v int"))

    def test_apply_in_pandas_not_returning_pandas_dataframe(self):
        def merge_pandas(lft, rgt, *_):
            return lft.size + rgt.size

        df1, df2, df3, *_ = self.dfs
        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                PythonException,
                "Return type of the user-defined function should be pandas.DataFrame, "
                "but is <class 'numpy.int64'>",
            ):
                (
                    df1.groupby("id")
                    .cogroup(df2.groupby("id"), df3.groupby("id"))
                    .applyInPandas(merge_pandas, "id long, k int, v int, v2 int")
                    .collect()
                )

    def test_apply_in_pandas_returning_wrong_number_of_columns(self):
        def merge_pandas(df1, df2, df3, df4):
            if 0 in df1["id"] and df1["id"][0] % 2 == 0:
                df1["add"] = 0
            if 0 in df2["id"] and df2["id"][0] % 3 == 0:
                df2["more"] = 1
            df = pd.merge(df1, df2, on=["id", "k"])
            df = pd.merge(df, df3, on=["id", "k"])
            return pd.merge(df, df4, on=["id", "k"])

        df1, df2, df3, df4, *_ = self.dfs

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                PythonException,
                "Number of columns of the returned pandas.DataFrame "
                "doesn't match specified schema. Expected: 6 Actual: 8",
            ):
                (
                    # merge_pandas returns two columns for even keys while we set schema to four
                    df1.groupby("id")
                    .cogroup(df2.groupby("id"), df3.groupby("id"), df4.groupby("id"))
                    .applyInPandas(merge_pandas, "id long, k int, v0 int, v1 int, v2 int, v3 int")
                    .collect()
                )

    def test_apply_in_pandas_returning_empty_dataframe(self):
        def merge_pandas(df1, df2, df3, df4):
            if 0 in df1["id"] and df1["id"][0] % 2 == 0:
                return pd.DataFrame([])
            if 0 in df2["id"] and df2["id"][0] % 3 == 0:
                return pd.DataFrame([])
            df = pd.merge(df1, df2, on=["id", "k"])
            df = pd.merge(df, df3, on=["id", "k"])
            return pd.merge(df, df4, on=["id", "k"])

        df1, df2, df3, df4, *_ = self.dfs
        result = (
            df1.groupby("id")
            .cogroup(df2.groupby("id"), df3.groupby("id"), df4.groupby("id"))
            .applyInPandas(merge_pandas, "id long, k int, v0 int, v1 int, v2 int, v3 int")
            .sort(["id", "k"])
            .toPandas()
        )

        df1, df2, df3, df4 = [df.toPandas() for df in self.dfs]

        expected = pd.merge(df1[df1["id"] % 2 != 0], df2[df2["id"] % 3 != 0], on=["id", "k"])
        expected = pd.merge(expected, df3, on=["id", "k"])
        expected = pd.merge(expected, df4, on=["id", "k"]).sort_values(by=["id", "k"])

        assert_frame_equal(expected, result)

    def test_apply_in_pandas_returning_empty_dataframe_and_wrong_number_of_columns(self):
        def merge_pandas(df1, df2, df3, df4):
            if 0 in df1["id"] and df1["id"][0] % 2 == 0:
                return pd.DataFrame([], columns=["id", "k"])
            df = pd.merge(df1, df2, on=["id", "k"])
            df = pd.merge(df, df3, on=["id", "k"])
            return pd.merge(df, df4, on=["id", "k"])

        df1, df2, df3, df4, *_ = self.dfs

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                PythonException,
                "Number of columns of the returned pandas.DataFrame doesn't "
                "match specified schema. Expected: 6 Actual: 2",
            ):
                (
                    df1.groupby("id")
                    .cogroup(df2.groupby("id"), df3.groupby("id"), df4.groupby("id"))
                    .applyInPandas(merge_pandas, "id long, k int, v0 int, v1 int, v2 int, v3 int")
                    .collect()
                )

    def test_mixed_scalar_udfs_followed_by_cogrouby_apply(self):
        df = self.spark.range(0, 10).toDF("v1")
        df = df.withColumn("v2", udf(lambda x: x + 1, "int")(df["v1"])).withColumn(
            "v3", pandas_udf(lambda x: x + 2, "int")(df["v1"])
        )

        result = (
            df.groupby()
            .cogroup(df.groupby(), df.groupby())
            .applyInPandas(
                lambda x, y, z: pd.DataFrame([(x.sum().sum(), y.sum().sum(), z.sum().sum())]),
                "sum1 int, sum2 int, sum3 int"
            )
            .collect()
        )

        self.assertEqual(result[0]["sum1"], 165)
        self.assertEqual(result[0]["sum2"], 165)
        self.assertEqual(result[0]["sum3"], 165)

    def test_with_key_groups(self):
        for index in range(0, 4):
            with self.subTest("Test with key", index=index):
                self._test_with_key(self.dfs, index)

    def test_with_key_empty_groups(self):
        for empty_group_df_index in range(0, 4):
            dfs = [df for df in self.dfs]
            with self.subTest("Test with keys for empty_group_df_index", empty_group_df_index=empty_group_df_index):
                dfs[empty_group_df_index] = dfs[empty_group_df_index].where(col("id") % 2 == 0)
                self._test_with_key(dfs, empty_group_df_index)

    def test_with_key_complex(self):
        def left_assign_key(key, lft, *_):
            return lft.assign(key=key[0])

        df_head, *df_tail = self.dfs

        result = (
            df_head.groupby(col("id") % 2 == 0)
            .cogroup(*[df.groupby(col("id") % 2 == 0) for df in df_tail])
            .applyInPandas(left_assign_key, "id long, k int, v0 int, key boolean", pass_key=True)
            .sort(["id", "k"])
            .toPandas()
        )

        expected = df_head.toPandas()
        expected = expected.assign(key=expected.id % 2 == 0)

        assert_frame_equal(expected, result)

    def test_wrong_return_type(self):
        # Test that we get a sensible exception invalid values passed to apply
        df1, df2, df3, *_ = self.dfs
        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                NotImplementedError, "Invalid return type.*ArrayType.*TimestampType"
            ):
                (df1.groupby("id")
                    .cogroup(df2.groupby("id"), df3.groupby("id"))
                    .applyInPandas(lambda df1, df2, df3: df1, "id long, v array<timestamp>")
                )

    def test_wrong_args(self):
        df1, df2, df3, *_ = self.dfs
        with self.assertRaisesRegex(ValueError, "Invalid function"):
            (df1.groupby("id")
                .cogroup(df2.groupby("id"), df3.groupby("id"))
                .applyInPandas(lambda: 1, StructType([StructField("d", DoubleType())]))
            )

    def test_case_insensitive_grouping_column(self):
        df1 = self.spark.createDataFrame([(1, 1)], ("column", "value"))

        row = (
            df1.groupby("ColUmn")
            .cogroup(df1.groupby("COLUMN"), df1.groupby("COLUMN"))
            .applyInPandas(lambda a, b, c: a + b + c, "column long, value long")
            .first()
        )
        self.assertEqual(row.asDict(), Row(column=3, value=3).asDict())

        df2 = self.spark.createDataFrame([(1, 1)], ("column", "value"))

        row = (
            df1.groupby("ColUmn")
            .cogroup(df2.groupby("COLUMN"), df2.groupby("COLUMN"))
            .applyInPandas(lambda a, b, c: a + b + c, "column long, value long")
            .first()
        )
        self.assertEqual(row.asDict(), Row(column=3, value=3).asDict())

    def test_self_join(self):
        df = self.spark.createDataFrame([(1, 1)], ("column", "value"))

        row = (
            df.groupby("ColUmn")
            .cogroup(df.groupby("COLUMN"), df.groupby("COLUMN"))
            .applyInPandas(lambda df1, df2, df3: df1 + df2 + df3, "column long, value long")
        )

        row = row.join(row).first()

        self.assertEqual(row.asDict(), Row(column=3, value=3).asDict())

    @staticmethod
    def _test_with_key(dfs, index):
        def assign_key(key, *dfs):
            return dfs[index].assign(key=key[0])

        df_head, *df_tail = dfs

        result = (
            df_head.groupby("id")
            .cogroup(*[df.groupby("id") for df in df_tail])
            .applyInPandas(assign_key, f"id long, k int, v{index} int, key long", pass_key=True)
            .toPandas()
        )
        expected = dfs[index].toPandas()
        expected = expected.assign(key=expected.id)

        assert_frame_equal(expected, result)

    @staticmethod
    def _test_merge(dfs, output_schema="id long, k int, v0 int, v1 int, v2 int, v3 int"):

        def merge_pandas(*dfs):
            return functools.reduce(lambda df1, df2: pd.merge(df1, df2, on=["id", "k"]), dfs)

        df_head, *df_tail = dfs

        result = (
            df_head.groupby("id")
            .cogroup(*[df.groupby("id") for df in df_tail])
            .applyInPandas(merge_pandas, output_schema)
            .sort(["id", "k"])
            .toPandas()
        )

        pd_dfs = [df.toPandas() for df in dfs]
        expected = functools.reduce(lambda df1, df2: pd.merge(df1, df2, on=["id", "k"]), pd_dfs)
        expected = expected.sort_values(by=["id", "k"])

        assert_frame_equal(expected, result)


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_cogrouped_map import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
