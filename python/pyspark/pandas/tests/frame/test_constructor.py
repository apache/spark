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
from datetime import datetime, timedelta
import unittest

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.typedef.typehints import (
    extension_dtypes_available,
    extension_float_dtypes_available,
    extension_object_dtypes_available,
)
from pyspark.pandas.utils import is_testing

from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.testing.utils import is_ansi_mode_test, ansi_mode_not_supported_message


# This file contains test cases for 'Constructor'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#constructor
# as well as extensions.
class FrameConstructorMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def df_pair(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)
        return pdf, psdf

    def test_dataframe(self):
        pdf, psdf = self.df_pair

        self.assert_eq(psdf["a"] + 1, pdf["a"] + 1)

        self.assert_eq(psdf.columns, pd.Index(["a", "b"]))

        self.assert_eq(psdf[psdf["b"] > 2], pdf[pdf["b"] > 2])
        self.assert_eq(-psdf[psdf["b"] > 2], -pdf[pdf["b"] > 2])
        self.assert_eq(psdf[["a", "b"]], pdf[["a", "b"]])
        self.assert_eq(psdf.a, pdf.a)
        self.assert_eq(psdf.b.mean(), pdf.b.mean())
        self.assert_eq(psdf.b.var(), pdf.b.var())
        self.assert_eq(psdf.b.std(), pdf.b.std())

        pdf, psdf = self.df_pair
        self.assert_eq(psdf[["a", "b"]], pdf[["a", "b"]])

        self.assertEqual(psdf.a.notnull().rename("x").name, "x")

        # check ps.DataFrame(ps.Series)
        pser = pd.Series([1, 2, 3], name="x", index=np.random.rand(3))
        psser = ps.from_pandas(pser)
        self.assert_eq(pd.DataFrame(pser), ps.DataFrame(psser))

        # check ps.DataFrame(ps.Series) with `columns`
        self.assert_eq(pd.DataFrame(pser, columns=["x"]), ps.DataFrame(psser, columns=["x"]))
        self.assert_eq(pd.DataFrame(pser, columns=("x",)), ps.DataFrame(psser, columns=("x",)))
        self.assert_eq(
            pd.DataFrame(pser, columns={"x": None}), ps.DataFrame(psser, columns={"x": None})
        )

        # check psdf[pd.Index]
        pdf, psdf = self.df_pair
        column_mask = pdf.columns.isin(["a", "b"])
        index_cols = pdf.columns[column_mask]
        self.assert_eq(psdf[index_cols], pdf[index_cols])

        if is_testing():
            err_msg = "pandas-on-Spark doesn't allow columns to be created via a new attribute name"
            with self.assertRaisesRegex(AssertionError, err_msg):
                psdf.X = [10, 20, 30, 40, 50, 60, 70, 80, 90]
        else:
            with self.assertWarns(UserWarning):
                psdf.X = [10, 20, 30, 40, 50, 60, 70, 80, 90]
            # If a new column is created, the following test would fail.
            # It means that the pandas have changed their behavior, so we should follow.
            self.assert_eq(pdf, psdf)

    def test_creation_index(self):
        data = np.random.randn(5, 3)

        # test local data with pd.Index
        self.assert_eq(
            ps.DataFrame(data=[1, 2], index=pd.Index([1, 2])),
            pd.DataFrame(data=[1, 2], index=pd.Index([1, 2])),
        )
        self.assert_eq(
            ps.DataFrame(data=[1, 2], index=pd.Index([2, 3])),
            pd.DataFrame(data=[1, 2], index=pd.Index([2, 3])),
        )
        self.assert_eq(
            ps.DataFrame(data=[1, 2], index=pd.Index([3, 4])),
            pd.DataFrame(data=[1, 2], index=pd.Index([3, 4])),
        )
        self.assert_eq(
            ps.DataFrame(data=data, index=pd.Index([1, 2, 3, 5, 6])),
            pd.DataFrame(data=data, index=pd.Index([1, 2, 3, 5, 6])),
        )

        # test local data with ps.Index
        self.assert_eq(
            ps.DataFrame(data=[1, 2], index=ps.Index([1, 2])),
            pd.DataFrame(data=[1, 2], index=pd.Index([1, 2])),
        )
        self.assert_eq(
            ps.DataFrame(data=[1, 2], index=ps.Index([2, 3])),
            pd.DataFrame(data=[1, 2], index=pd.Index([2, 3])),
        )
        self.assert_eq(
            ps.DataFrame(data=[1, 2], index=ps.Index([3, 4])),
            pd.DataFrame(data=[1, 2], index=pd.Index([3, 4])),
        )
        self.assert_eq(
            ps.DataFrame(data=data, index=ps.Index([1, 2, 3, 5, 6])),
            pd.DataFrame(data=data, index=pd.Index([1, 2, 3, 5, 6])),
        )

        with ps.option_context("compute.ops_on_diff_frames", False):
            err_msg = "Cannot combine the series or dataframe"
            with self.assertRaisesRegex(ValueError, err_msg):
                # test ps.DataFrame with ps.Index
                ps.DataFrame(data=ps.DataFrame([1, 2]), index=ps.Index([1, 2]))
            with self.assertRaisesRegex(ValueError, err_msg):
                # test ps.DataFrame with pd.Index
                ps.DataFrame(data=ps.DataFrame([1, 2]), index=pd.Index([3, 4]))

        with ps.option_context("compute.ops_on_diff_frames", True):
            # test pd.DataFrame with pd.Index
            self.assert_eq(
                ps.DataFrame(data=pd.DataFrame([1, 2]), index=pd.Index([0, 1])),
                pd.DataFrame(data=pd.DataFrame([1, 2]), index=pd.Index([0, 1])),
            )
            self.assert_eq(
                ps.DataFrame(data=pd.DataFrame([1, 2]), index=pd.Index([1, 2])),
                pd.DataFrame(data=pd.DataFrame([1, 2]), index=pd.Index([1, 2])),
            )

            # test ps.DataFrame with ps.Index
            self.assert_eq(
                ps.DataFrame(data=ps.DataFrame([1, 2]), index=ps.Index([0, 1])),
                pd.DataFrame(data=pd.DataFrame([1, 2]), index=pd.Index([0, 1])),
            )
            self.assert_eq(
                ps.DataFrame(data=ps.DataFrame([1, 2]), index=ps.Index([1, 2])),
                pd.DataFrame(data=pd.DataFrame([1, 2]), index=pd.Index([1, 2])),
            )

            # test ps.DataFrame with pd.Index
            self.assert_eq(
                ps.DataFrame(data=ps.DataFrame([1, 2]), index=pd.Index([0, 1])),
                pd.DataFrame(data=pd.DataFrame([1, 2]), index=pd.Index([0, 1])),
            )
            self.assert_eq(
                ps.DataFrame(data=ps.DataFrame([1, 2]), index=pd.Index([1, 2])),
                pd.DataFrame(data=pd.DataFrame([1, 2]), index=pd.Index([1, 2])),
            )

        # test with multi data columns
        pdf = pd.DataFrame(data=data, columns=["A", "B", "C"])
        psdf = ps.from_pandas(pdf)

        # test with pd.DataFrame and pd.Index
        self.assert_eq(
            ps.DataFrame(data=pdf, index=pd.Index([2, 3, 4, 5, 6])),
            pd.DataFrame(data=pdf, index=pd.Index([2, 3, 4, 5, 6])),
        )

        # test with pd.DataFrame and ps.Index
        self.assert_eq(
            ps.DataFrame(data=pdf, index=ps.Index([2, 3, 4, 5, 6])),
            pd.DataFrame(data=pdf, index=pd.Index([2, 3, 4, 5, 6])),
        )

        with ps.option_context("compute.ops_on_diff_frames", True):
            # test with ps.DataFrame and pd.Index
            self.assert_eq(
                ps.DataFrame(data=psdf, index=pd.Index([2, 3, 4, 5, 6])).sort_index(),
                pd.DataFrame(data=pdf, index=pd.Index([2, 3, 4, 5, 6])).sort_index(),
            )

            # test with ps.DataFrame and ps.Index
            self.assert_eq(
                ps.DataFrame(data=psdf, index=ps.Index([2, 3, 4, 5, 6])).sort_index(),
                pd.DataFrame(data=pdf, index=pd.Index([2, 3, 4, 5, 6])).sort_index(),
            )

        # test String Index
        pdf = pd.DataFrame(
            data={
                "s": ["Hello", "World", "Databricks"],
                "x": [2002, 2003, 2004],
            }
        )
        pdf = pdf.set_index("s")
        pdf.index.name = None
        psdf = ps.from_pandas(pdf)

        # test with pd.DataFrame and pd.Index
        self.assert_eq(
            ps.DataFrame(data=pdf, index=pd.Index(["Hello", "Universe", "Databricks"])),
            pd.DataFrame(data=pdf, index=pd.Index(["Hello", "Universe", "Databricks"])),
        )

        # test with pd.DataFrame and ps.Index
        self.assert_eq(
            ps.DataFrame(data=pdf, index=ps.Index(["Hello", "Universe", "Databricks"])),
            pd.DataFrame(data=pdf, index=pd.Index(["Hello", "Universe", "Databricks"])),
        )

        with ps.option_context("compute.ops_on_diff_frames", True):
            # test with ps.DataFrame and pd.Index
            self.assert_eq(
                ps.DataFrame(
                    data=psdf, index=pd.Index(["Hello", "Universe", "Databricks"])
                ).sort_index(),
                pd.DataFrame(
                    data=pdf, index=pd.Index(["Hello", "Universe", "Databricks"])
                ).sort_index(),
            )

            # test with ps.DataFrame and ps.Index
            self.assert_eq(
                ps.DataFrame(
                    data=psdf, index=ps.Index(["Hello", "Universe", "Databricks"])
                ).sort_index(),
                pd.DataFrame(
                    data=pdf, index=pd.Index(["Hello", "Universe", "Databricks"])
                ).sort_index(),
            )

        # test DatetimeIndex
        pdf = pd.DataFrame(
            data={
                "t": [
                    datetime(2022, 9, 1, 0, 0, 0, 0),
                    datetime(2022, 9, 2, 0, 0, 0, 0),
                    datetime(2022, 9, 3, 0, 0, 0, 0),
                ],
                "x": [2002, 2003, 2004],
            }
        )
        pdf = pdf.set_index("t")
        pdf.index.name = None
        psdf = ps.from_pandas(pdf)

        # test with pd.DataFrame and pd.DatetimeIndex
        self.assert_eq(
            ps.DataFrame(
                data=pdf,
                index=pd.DatetimeIndex(["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]),
            ).sort_index(),
            pd.DataFrame(
                data=pdf,
                index=pd.DatetimeIndex(["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]),
            ).sort_index(),
        )

        # test with pd.DataFrame and ps.DatetimeIndex
        self.assert_eq(
            ps.DataFrame(
                data=pdf,
                index=ps.DatetimeIndex(["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]),
            ).sort_index(),
            pd.DataFrame(
                data=pdf,
                index=pd.DatetimeIndex(["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]),
            ).sort_index(),
        )

        with ps.option_context("compute.ops_on_diff_frames", True):
            # test with ps.DataFrame and pd.DatetimeIndex
            self.assert_eq(
                ps.DataFrame(
                    data=psdf,
                    index=pd.DatetimeIndex(
                        ["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]
                    ),
                ).sort_index(),
                pd.DataFrame(
                    data=pdf,
                    index=pd.DatetimeIndex(
                        ["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]
                    ),
                ).sort_index(),
            )

            # test with ps.DataFrame and ps.DatetimeIndex
            self.assert_eq(
                ps.DataFrame(
                    data=psdf,
                    index=ps.DatetimeIndex(
                        ["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]
                    ),
                ).sort_index(),
                pd.DataFrame(
                    data=pdf,
                    index=pd.DatetimeIndex(
                        ["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]
                    ),
                ).sort_index(),
            )

        # test MultiIndex
        # test local data with ps.MultiIndex
        self.assert_eq(
            ps.DataFrame(data=[1, 2], index=ps.MultiIndex.from_tuples([(1, 3), (2, 4)])),
            pd.DataFrame(data=[1, 2], index=pd.MultiIndex.from_tuples([(1, 3), (2, 4)])),
        )

        # test distributed data with ps.MultiIndex
        err_msg = "Cannot combine a Distributed Dataset with a MultiIndex"
        with ps.option_context("compute.ops_on_diff_frames", True):
            with self.assertRaisesRegex(ValueError, err_msg):
                # test ps.DataFrame with ps.Index
                ps.DataFrame(
                    data=ps.DataFrame([1, 2]), index=ps.MultiIndex.from_tuples([(1, 3), (2, 4)])
                )
            with self.assertRaisesRegex(ValueError, err_msg):
                # test ps.DataFrame with pd.Index
                ps.DataFrame(
                    data=ps.DataFrame([1, 2]), index=ps.MultiIndex.from_tuples([(1, 3), (2, 4)])
                )

    def test_creation_index_same_anchor(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, None, 4],
                "b": [1, None, None, 4],
                "c": [1, 2, None, None],
                "d": [None, 2, None, 4],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.DataFrame(data=psdf, index=psdf.index),
            pd.DataFrame(data=pdf, index=pdf.index),
        )
        self.assert_eq(
            ps.DataFrame(data=psdf + 1, index=psdf.index),
            pd.DataFrame(data=pdf + 1, index=pdf.index),
        )
        self.assert_eq(
            ps.DataFrame(data=psdf[["a", "c"]] * 2, index=psdf.index),
            pd.DataFrame(data=pdf[["a", "c"]] * 2, index=pdf.index),
        )

        # test String Index
        pdf = pd.DataFrame(
            data={"s": ["Hello", "World", "Databricks"], "x": [2002, 2003, 2004], "y": [4, 5, 6]}
        )
        pdf = pdf.set_index("s")
        pdf.index.name = None
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.DataFrame(data=psdf, index=psdf.index),
            pd.DataFrame(data=pdf, index=pdf.index),
        )
        self.assert_eq(
            ps.DataFrame(data=psdf + 1, index=psdf.index),
            pd.DataFrame(data=pdf + 1, index=pdf.index),
        )
        self.assert_eq(
            ps.DataFrame(data=psdf[["y"]] * 2, index=psdf.index),
            pd.DataFrame(data=pdf[["y"]] * 2, index=pdf.index),
        )

        # test DatetimeIndex
        pdf = pd.DataFrame(
            data={
                "t": [
                    datetime(2022, 9, 1, 0, 0, 0, 0),
                    datetime(2022, 9, 2, 0, 0, 0, 0),
                    datetime(2022, 9, 3, 0, 0, 0, 0),
                ],
                "x": [2002, 2003, 2004],
                "y": [4, 5, 6],
            }
        )
        pdf = pdf.set_index("t")
        pdf.index.name = None
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.DataFrame(data=psdf, index=psdf.index),
            pd.DataFrame(data=pdf, index=pdf.index),
        )
        self.assert_eq(
            ps.DataFrame(data=psdf + 1, index=psdf.index),
            pd.DataFrame(data=pdf + 1, index=pdf.index),
        )
        self.assert_eq(
            ps.DataFrame(data=psdf[["y"]] * 2, index=psdf.index),
            pd.DataFrame(data=pdf[["y"]] * 2, index=pdf.index),
        )

        # test TimedeltaIndex
        pdf = pd.DataFrame(
            data={
                "t": [
                    timedelta(1),
                    timedelta(3),
                    timedelta(5),
                ],
                "x": [2002, 2003, 2004],
                "y": [4, 5, 6],
            }
        )
        pdf = pdf.set_index("t")
        pdf.index.name = None
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.DataFrame(data=psdf, index=psdf.index),
            pd.DataFrame(data=pdf, index=pdf.index),
        )
        self.assert_eq(
            ps.DataFrame(data=psdf + 1, index=psdf.index),
            pd.DataFrame(data=pdf + 1, index=pdf.index),
        )
        self.assert_eq(
            ps.DataFrame(data=psdf[["y"]] * 2, index=psdf.index),
            pd.DataFrame(data=pdf[["y"]] * 2, index=pdf.index),
        )

        # test CategoricalIndex
        pdf = pd.DataFrame(
            data={
                "z": [-1, -2, -3, -4],
                "x": [2002, 2003, 2004, 2005],
                "y": [4, 5, 6, 7],
            },
            index=pd.CategoricalIndex(["a", "c", "b", "a"], categories=["a", "b", "c"]),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.DataFrame(data=psdf, index=psdf.index),
            pd.DataFrame(data=pdf, index=pdf.index),
        )
        self.assert_eq(
            ps.DataFrame(data=psdf + 1, index=psdf.index),
            pd.DataFrame(data=pdf + 1, index=pdf.index),
        )
        self.assert_eq(
            ps.DataFrame(data=psdf[["y"]] * 2, index=psdf.index),
            pd.DataFrame(data=pdf[["y"]] * 2, index=pdf.index),
        )

        # test distributed data with ps.MultiIndex
        pdf = pd.DataFrame(
            data={
                "z": [-1, -2, -3, -4],
                "x": [2002, 2003, 2004, 2005],
                "y": [4, 5, 6, 7],
            },
            index=pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z"), ("a", "x")]),
        )
        psdf = ps.from_pandas(pdf)

        err_msg = "Cannot combine a Distributed Dataset with a MultiIndex"
        with self.assertRaisesRegex(ValueError, err_msg):
            # test ps.DataFrame with ps.MultiIndex
            ps.DataFrame(data=psdf, index=psdf.index)
        with self.assertRaisesRegex(ValueError, err_msg):
            # test ps.DataFrame with pd.MultiIndex
            ps.DataFrame(data=psdf, index=pdf.index)

    @unittest.skipIf(not extension_dtypes_available, "pandas extension dtypes are not available")
    def test_extension_dtypes(self):
        pdf = pd.DataFrame(
            {
                "a": pd.Series([1, 2, None, 4], dtype="Int8"),
                "b": pd.Series([1, None, None, 4], dtype="Int16"),
                "c": pd.Series([1, 2, None, None], dtype="Int32"),
                "d": pd.Series([None, 2, None, 4], dtype="Int64"),
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf + psdf, pdf + pdf)

    @unittest.skipIf(not extension_dtypes_available, "pandas extension dtypes are not available")
    def test_astype_extension_dtypes(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, None, 4],
                "b": [1, None, None, 4],
                "c": [1, 2, None, None],
                "d": [None, 2, None, 4],
            }
        )
        psdf = ps.from_pandas(pdf)

        astype = {"a": "Int8", "b": "Int16", "c": "Int32", "d": "Int64"}

        self.assert_eq(psdf.astype(astype), pdf.astype(astype))

    @unittest.skipIf(
        not extension_object_dtypes_available, "pandas extension object dtypes are not available"
    )
    def test_extension_object_dtypes(self):
        pdf = pd.DataFrame(
            {
                "a": pd.Series(["a", "b", None, "c"], dtype="string"),
                "b": pd.Series([True, None, False, True], dtype="boolean"),
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf, pdf)

    @unittest.skipIf(
        not extension_object_dtypes_available, "pandas extension object dtypes are not available"
    )
    def test_astype_extension_object_dtypes(self):
        pdf = pd.DataFrame({"a": ["a", "b", None, "c"], "b": [True, None, False, True]})
        psdf = ps.from_pandas(pdf)

        astype = {"a": "string", "b": "boolean"}

        self.assert_eq(psdf.astype(astype), pdf.astype(astype))

    @unittest.skipIf(
        not extension_float_dtypes_available, "pandas extension float dtypes are not available"
    )
    @unittest.skipIf(is_ansi_mode_test, ansi_mode_not_supported_message)
    def test_extension_float_dtypes(self):
        pdf = pd.DataFrame(
            {
                "a": pd.Series([1.0, 2.0, None, 4.0], dtype="Float32"),
                "b": pd.Series([1.0, None, 3.0, 4.0], dtype="Float64"),
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf + 1, pdf + 1)
        self.assert_eq(psdf + psdf, pdf + pdf)

    @unittest.skipIf(
        not extension_float_dtypes_available, "pandas extension float dtypes are not available"
    )
    def test_astype_extension_float_dtypes(self):
        pdf = pd.DataFrame({"a": [1.0, 2.0, None, 4.0], "b": [1.0, None, 3.0, 4.0]})
        psdf = ps.from_pandas(pdf)

        astype = {"a": "Float32", "b": "Float64"}

        self.assert_eq(psdf.astype(astype), pdf.astype(astype))


class FrameConstructorTests(
    FrameConstructorMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_constructor import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
