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
import inspect
import sys
import unittest
from io import StringIO

import numpy as np
import pandas as pd

from pyspark import StorageLevel
from pyspark.ml.linalg import SparseVector
from pyspark.sql.types import StructType
from pyspark import pandas as ps
from pyspark.pandas.frame import CachedDataFrame
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.frame import MissingPandasLikeDataFrame
from pyspark.testing.pandasutils import PandasOnSparkTestCase, SPARK_CONF_ARROW_ENABLED
from pyspark.testing.sqlutils import SQLTestUtils


# This file contains test cases for 'Spark-related'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#spark-related
class FrameSparkMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_empty_dataframe(self):
        pdf = pd.DataFrame({"a": pd.Series([], dtype="i1"), "b": pd.Series([], dtype="str")})

        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf, pdf)

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            psdf = ps.from_pandas(pdf)
            self.assert_eq(psdf, pdf)

    def test_all_null_dataframe(self):
        pdf = pd.DataFrame(
            {
                "a": [None, None, None, "a"],
                "b": [None, None, None, 1],
                "c": [None, None, None] + list(np.arange(1, 2).astype("i1")),
                "d": [None, None, None, 1.0],
                "e": [None, None, None, True],
                "f": [None, None, None] + list(pd.date_range("20130101", periods=1)),
            },
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.iloc[:-1], pdf.iloc[:-1])

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            self.assert_eq(psdf.iloc[:-1], pdf.iloc[:-1])

        pdf = pd.DataFrame(
            {
                "a": pd.Series([None, None, None], dtype="float64"),
                "b": pd.Series([None, None, None], dtype="str"),
            },
        )

        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf, pdf)

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            psdf = ps.from_pandas(pdf)
            self.assert_eq(psdf, pdf)

    def test_nullable_object(self):
        pdf = pd.DataFrame(
            {
                "a": list("abc") + [np.nan, None],
                "b": list(range(1, 4)) + [np.nan, None],
                "c": list(np.arange(3, 6).astype("i1")) + [np.nan, None],
                "d": list(np.arange(4.0, 7.0, dtype="float64")) + [np.nan, None],
                "e": [True, False, True, np.nan, None],
                "f": list(pd.date_range("20130101", periods=3)) + [np.nan, None],
            },
            index=np.random.rand(5),
        )

        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf, pdf)

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            psdf = ps.from_pandas(pdf)
            self.assert_eq(psdf, pdf)

    def test_dot_in_column_name(self):
        self.assert_eq(
            ps.DataFrame(ps.range(1)._internal.spark_frame.selectExpr("1L as `a.b`"))["a.b"],
            ps.Series([1], name="a.b"),
        )

    def test_spark_schema(self):
        psdf = ps.DataFrame(
            {
                "a": list("abc"),
                "b": list(range(1, 4)),
                "c": np.arange(3, 6).astype("i1"),
                "d": np.arange(4.0, 7.0, dtype="float64"),
                "e": [True, False, True],
                "f": pd.date_range("20130101", periods=3),
            },
            columns=["a", "b", "c", "d", "e", "f"],
        )

        actual = psdf.spark.schema()
        expected = (
            StructType()
            .add("a", "string", False)
            .add("b", "long", False)
            .add("c", "byte", False)
            .add("d", "double", False)
            .add("e", "boolean", False)
            .add("f", "timestamp", False)
        )
        self.assertEqual(actual, expected)

        actual = psdf.spark.schema("index")
        expected = (
            StructType()
            .add("index", "long", False)
            .add("a", "string", False)
            .add("b", "long", False)
            .add("c", "byte", False)
            .add("d", "double", False)
            .add("e", "boolean", False)
            .add("f", "timestamp", False)
        )
        self.assertEqual(actual, expected)

    def test_print_schema(self):
        psdf = ps.DataFrame(
            {"a": list("abc"), "b": list(range(1, 4)), "c": np.arange(3, 6).astype("i1")},
            columns=["a", "b", "c"],
        )

        prev = sys.stdout
        try:
            out = StringIO()
            sys.stdout = out
            psdf.spark.print_schema()
            actual = out.getvalue().strip()

            self.assertTrue("a: string" in actual, actual)
            self.assertTrue("b: long" in actual, actual)
            self.assertTrue("c: byte" in actual, actual)

            out = StringIO()
            sys.stdout = out
            psdf.spark.print_schema(index_col="index")
            actual = out.getvalue().strip()

            self.assertTrue("index: long" in actual, actual)
            self.assertTrue("a: string" in actual, actual)
            self.assertTrue("b: long" in actual, actual)
            self.assertTrue("c: byte" in actual, actual)
        finally:
            sys.stdout = prev

    def test_explain_hint(self):
        psdf1 = ps.DataFrame(
            {"lkey": ["foo", "bar", "baz", "foo"], "value": [1, 2, 3, 5]},
            columns=["lkey", "value"],
        )
        psdf2 = ps.DataFrame(
            {"rkey": ["foo", "bar", "baz", "foo"], "value": [5, 6, 7, 8]},
            columns=["rkey", "value"],
        )
        merged = psdf1.merge(psdf2.spark.hint("broadcast"), left_on="lkey", right_on="rkey")
        prev = sys.stdout
        try:
            out = StringIO()
            sys.stdout = out
            merged.spark.explain()
            actual = out.getvalue().strip()

            self.assertTrue("Broadcast" in actual, actual)
        finally:
            sys.stdout = prev

    def test_cache(self):
        pdf = pd.DataFrame(
            [(0.2, 0.3), (0.0, 0.6), (0.6, 0.0), (0.2, 0.1)], columns=["dogs", "cats"]
        )
        psdf = ps.from_pandas(pdf)

        with psdf.spark.cache() as cached_df:
            self.assert_eq(isinstance(cached_df, CachedDataFrame), True)
            self.assert_eq(
                repr(cached_df.spark.storage_level), repr(StorageLevel(True, True, False, True))
            )

    def test_persist(self):
        pdf = pd.DataFrame(
            [(0.2, 0.3), (0.0, 0.6), (0.6, 0.0), (0.2, 0.1)], columns=["dogs", "cats"]
        )
        psdf = ps.from_pandas(pdf)
        storage_levels = [
            StorageLevel.DISK_ONLY,
            StorageLevel.MEMORY_AND_DISK,
            StorageLevel.MEMORY_ONLY,
            StorageLevel.OFF_HEAP,
        ]

        for storage_level in storage_levels:
            with psdf.spark.persist(storage_level) as cached_df:
                self.assert_eq(isinstance(cached_df, CachedDataFrame), True)
                self.assert_eq(repr(cached_df.spark.storage_level), repr(storage_level))

        self.assertRaises(TypeError, lambda: psdf.spark.persist("DISK_ONLY"))

    def test_udt(self):
        sparse_values = {0: 0.1, 1: 1.1}
        sparse_vector = SparseVector(len(sparse_values), sparse_values)
        pdf = pd.DataFrame({"a": [sparse_vector], "b": [10]})

        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf, pdf)

    def test_missing(self):
        psdf = self.psdf

        missing_functions = inspect.getmembers(MissingPandasLikeDataFrame, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*DataFrame.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*DataFrame.*{}.*is deprecated".format(name)
            ):
                getattr(psdf, name)()

        missing_properties = inspect.getmembers(
            MissingPandasLikeDataFrame, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*DataFrame.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf, name)
        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*DataFrame.*{}.*is deprecated".format(name)
            ):
                getattr(psdf, name)


class FrameSparkTests(
    FrameSparkMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_spark import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
