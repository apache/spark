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
from datetime import datetime
import unittest

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


# This file contains test cases for 'Attributes and underlying data'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#attributes-and-underlying-data
class FrameAttrsMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    @property
    def df_pair(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)
        return pdf, psdf

    def test_column_names(self):
        pdf, psdf = self.df_pair

        self.assert_eq(psdf.columns, pdf.columns)
        self.assert_eq(psdf[["b", "a"]].columns, pdf[["b", "a"]].columns)
        self.assert_eq(psdf["a"].name, pdf["a"].name)
        self.assert_eq((psdf["a"] + 1).name, (pdf["a"] + 1).name)

        self.assert_eq((psdf.a + psdf.b).name, (pdf.a + pdf.b).name)
        self.assert_eq((psdf.a + psdf.b.rename("a")).name, (pdf.a + pdf.b.rename("a")).name)
        self.assert_eq((psdf.a + psdf.b.rename()).name, (pdf.a + pdf.b.rename()).name)
        self.assert_eq((psdf.a.rename() + psdf.b).name, (pdf.a.rename() + pdf.b).name)
        self.assert_eq(
            (psdf.a.rename() + psdf.b.rename()).name, (pdf.a.rename() + pdf.b.rename()).name
        )

    def test_rename_columns(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7], "b": [7, 6, 5, 4, 3, 2, 1]}, index=np.random.rand(7)
        )
        psdf = ps.from_pandas(pdf)

        psdf.columns = ["x", "y"]
        pdf.columns = ["x", "y"]
        self.assert_eq(psdf.columns, pd.Index(["x", "y"]))
        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf._internal.data_spark_column_names, ["x", "y"])
        self.assert_eq(psdf.to_spark().columns, ["x", "y"])
        self.assert_eq(psdf.to_spark(index_col="index").columns, ["index", "x", "y"])

        columns = pdf.columns
        columns.name = "lvl_1"

        psdf.columns = columns
        self.assert_eq(psdf.columns.names, ["lvl_1"])
        self.assert_eq(psdf, pdf)

        msg = "Length mismatch: Expected axis has 2 elements, new values have 4 elements"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.columns = [1, 2, 3, 4]

        # Multi-index columns
        pdf = pd.DataFrame(
            {("A", "0"): [1, 2, 2, 3], ("B", "1"): [1, 2, 3, 4]}, index=np.random.rand(4)
        )
        psdf = ps.from_pandas(pdf)

        columns = pdf.columns
        self.assert_eq(psdf.columns, columns)
        self.assert_eq(psdf, pdf)

        pdf.columns = ["x", "y"]
        psdf.columns = ["x", "y"]
        self.assert_eq(psdf.columns, pd.Index(["x", "y"]))
        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf._internal.data_spark_column_names, ["x", "y"])
        self.assert_eq(psdf.to_spark().columns, ["x", "y"])
        self.assert_eq(psdf.to_spark(index_col="index").columns, ["index", "x", "y"])

        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(psdf.columns, columns)
        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf._internal.data_spark_column_names, ["(A, 0)", "(B, 1)"])
        self.assert_eq(psdf.to_spark().columns, ["(A, 0)", "(B, 1)"])
        self.assert_eq(psdf.to_spark(index_col="index").columns, ["index", "(A, 0)", "(B, 1)"])

        columns.names = ["lvl_1", "lvl_2"]

        psdf.columns = columns
        self.assert_eq(psdf.columns.names, ["lvl_1", "lvl_2"])
        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf._internal.data_spark_column_names, ["(A, 0)", "(B, 1)"])
        self.assert_eq(psdf.to_spark().columns, ["(A, 0)", "(B, 1)"])
        self.assert_eq(psdf.to_spark(index_col="index").columns, ["index", "(A, 0)", "(B, 1)"])

    def test_multi_index_dtypes(self):
        # SPARK-36930: Support ps.MultiIndex.dtypes
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        pmidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psmidx.dtypes, pmidx.dtypes)

        # multiple labels
        pmidx = pd.MultiIndex.from_arrays(arrays, names=[("zero", "first"), ("one", "second")])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psmidx.dtypes, pmidx.dtypes)

    def test_multi_index_dtypes_not_unique_name(self):
        # Regression test for https://github.com/pandas-dev/pandas/issues/45174
        pmidx = pd.MultiIndex.from_arrays([[1], [2]], names=[1, 1])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psmidx.dtypes, pmidx.dtypes)

    def test_dtype(self):
        pdf = pd.DataFrame(
            {
                "a": list("abc"),
                "b": list(range(1, 4)),
                "c": np.arange(3, 6).astype("i1"),
                "d": np.arange(4.0, 7.0, dtype="float64"),
                "e": [True, False, True],
                "f": pd.date_range("20130101", periods=3),
            },
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf, pdf)
        self.assertTrue((psdf.dtypes == pdf.dtypes).all())

        # multi-index columns
        columns = pd.MultiIndex.from_tuples(zip(list("xxxyyz"), list("abcdef")))
        pdf.columns = columns
        psdf.columns = columns
        self.assertTrue((psdf.dtypes == pdf.dtypes).all())

    def test_axes(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.axes, psdf.axes)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.axes, psdf.axes)

    def test_inplace(self):
        pdf, psdf = self.df_pair

        pser = pdf.a
        psser = psdf.a

        pdf["a"] = pdf["a"] + 10
        psdf["a"] = psdf["a"] + 10

        self.assert_eq(psdf, pdf)
        self.assert_eq(psser, pser)

    def test_dataframe_multiindex_columns(self):
        pdf = pd.DataFrame(
            {
                ("x", "a", "1"): [1, 2, 3],
                ("x", "b", "2"): [4, 5, 6],
                ("y.z", "c.d", "3"): [7, 8, 9],
                ("x", "b", "4"): [10, 11, 12],
            },
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf["x"], pdf["x"])
        self.assert_eq(psdf["y.z"], pdf["y.z"])
        self.assert_eq(psdf["x"]["b"], pdf["x"]["b"])
        self.assert_eq(psdf["x"]["b"]["2"], pdf["x"]["b"]["2"])

        self.assert_eq(psdf.x, pdf.x)
        self.assert_eq(psdf.x.b, pdf.x.b)
        self.assert_eq(psdf.x.b["2"], pdf.x.b["2"])

        self.assertRaises(KeyError, lambda: psdf["z"])
        self.assertRaises(AttributeError, lambda: psdf.z)

        self.assert_eq(psdf[("x",)], pdf[("x",)])
        self.assert_eq(psdf[("x", "a")], pdf[("x", "a")])
        self.assert_eq(psdf[("x", "a", "1")], pdf[("x", "a", "1")])

    def test_dataframe_column_level_name(self):
        column = pd.Index(["A", "B", "C"], name="X")
        pdf = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=column, index=np.random.rand(2))
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf.columns.names, pdf.columns.names)
        self.assert_eq(psdf._to_pandas().columns.names, pdf.columns.names)

    def test_dataframe_multiindex_names_level(self):
        columns = pd.MultiIndex.from_tuples(
            [("X", "A", "Z"), ("X", "B", "Z"), ("Y", "C", "Z"), ("Y", "D", "Z")],
            names=["lvl_1", "lvl_2", "lv_3"],
        )
        pdf = pd.DataFrame(
            [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12], [13, 14, 15, 16], [17, 18, 19, 20]],
            columns=columns,
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.columns.names, pdf.columns.names)
        self.assert_eq(psdf._to_pandas().columns.names, pdf.columns.names)

        psdf1 = ps.from_pandas(pdf)
        self.assert_eq(psdf1.columns.names, pdf.columns.names)

        self.assertRaises(
            AssertionError,
            lambda: ps.DataFrame(psdf1._internal.copy(column_label_names=("level",))),
        )

        self.assert_eq(psdf["X"], pdf["X"])
        self.assert_eq(psdf["X"].columns.names, pdf["X"].columns.names)
        self.assert_eq(psdf["X"]._to_pandas().columns.names, pdf["X"].columns.names)
        self.assert_eq(psdf["X"]["A"], pdf["X"]["A"])
        self.assert_eq(psdf["X"]["A"].columns.names, pdf["X"]["A"].columns.names)
        self.assert_eq(psdf["X"]["A"]._to_pandas().columns.names, pdf["X"]["A"].columns.names)
        self.assert_eq(psdf[("X", "A")], pdf[("X", "A")])
        self.assert_eq(psdf[("X", "A")].columns.names, pdf[("X", "A")].columns.names)
        self.assert_eq(psdf[("X", "A")]._to_pandas().columns.names, pdf[("X", "A")].columns.names)
        self.assert_eq(psdf[("X", "A", "Z")], pdf[("X", "A", "Z")])

    def test_repr_cache_invalidation(self):
        # If there is any cache, inplace operations should invalidate it.
        df = ps.range(10)
        df.__repr__()
        df["a"] = df["id"]
        self.assertEqual(df.__repr__(), df._to_pandas().__repr__())

    def test_repr_html_cache_invalidation(self):
        # If there is any cache, inplace operations should invalidate it.
        df = ps.range(10)
        df._repr_html_()
        df["a"] = df["id"]
        self.assertEqual(df._repr_html_(), df._to_pandas()._repr_html_())

    def test_assign(self):
        pdf, psdf = self.df_pair

        psdf["w"] = 1.0
        pdf["w"] = 1.0

        self.assert_eq(psdf, pdf)

        psdf.w = 10.0
        pdf.w = 10.0

        self.assert_eq(psdf, pdf)

        psdf[1] = 1.0
        pdf[1] = 1.0

        self.assert_eq(psdf, pdf)

        psdf = psdf.assign(a=psdf["a"] * 2)
        pdf = pdf.assign(a=pdf["a"] * 2)

        self.assert_eq(psdf, pdf)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "w"), ("y", "v")])
        pdf.columns = columns
        psdf.columns = columns

        psdf[("a", "c")] = "def"
        pdf[("a", "c")] = "def"

        self.assert_eq(psdf, pdf)

        psdf = psdf.assign(Z="ZZ")
        pdf = pdf.assign(Z="ZZ")

        self.assert_eq(psdf, pdf)

        psdf["x"] = "ghi"
        pdf["x"] = "ghi"

        self.assert_eq(psdf, pdf)

    def test_attributes(self):
        psdf = self.psdf

        self.assertIn("a", dir(psdf))
        self.assertNotIn("foo", dir(psdf))
        self.assertRaises(AttributeError, lambda: psdf.foo)

        psdf = ps.DataFrame({"a b c": [1, 2, 3]})
        self.assertNotIn("a b c", dir(psdf))
        psdf = ps.DataFrame({"a": [1, 2], 5: [1, 2]})
        self.assertIn("a", dir(psdf))
        self.assertNotIn(5, dir(psdf))

    def test_empty_timestamp(self):
        pdf = pd.DataFrame(
            {
                "t": [
                    datetime(2019, 1, 1, 0, 0, 0),
                    datetime(2019, 1, 2, 0, 0, 0),
                    datetime(2019, 1, 3, 0, 0, 0),
                ]
            },
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf[psdf["t"] != psdf["t"]], pdf[pdf["t"] != pdf["t"]])
        self.assert_eq(psdf[psdf["t"] != psdf["t"]].dtypes, pdf[pdf["t"] != pdf["t"]].dtypes)


class FrameAttrsTests(
    FrameAttrsMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_attrs import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
