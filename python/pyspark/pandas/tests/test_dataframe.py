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
from distutils.version import LooseVersion
import inspect
import unittest

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.frame import MissingPandasLikeDataFrame
from pyspark.pandas.typedef.typehints import (
    extension_dtypes,
    extension_dtypes_available,
    extension_float_dtypes_available,
    extension_object_dtypes_available,
)
from pyspark.testing.pandasutils import (
    have_tabulate,
    ComparisonTestBase,
    SPARK_CONF_ARROW_ENABLED,
    tabulate_requirement_message,
)
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.pandas.utils import name_like_string, is_testing


class DataFrameTest(ComparisonTestBase, SQLTestUtils):
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
                ps.DataFrame(data=psdf, index=pd.Index([2, 3, 4, 5, 6])),
                pd.DataFrame(data=pdf, index=pd.Index([2, 3, 4, 5, 6])),
            )

            # test with ps.DataFrame and ps.Index
            self.assert_eq(
                ps.DataFrame(data=psdf, index=ps.Index([2, 3, 4, 5, 6])),
                pd.DataFrame(data=pdf, index=pd.Index([2, 3, 4, 5, 6])),
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
                ps.DataFrame(data=psdf, index=pd.Index(["Hello", "Universe", "Databricks"])),
                pd.DataFrame(data=pdf, index=pd.Index(["Hello", "Universe", "Databricks"])),
            )

            # test with ps.DataFrame and ps.Index
            self.assert_eq(
                ps.DataFrame(data=psdf, index=ps.Index(["Hello", "Universe", "Databricks"])),
                pd.DataFrame(data=pdf, index=pd.Index(["Hello", "Universe", "Databricks"])),
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
            ),
            pd.DataFrame(
                data=pdf,
                index=pd.DatetimeIndex(["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]),
            ),
        )

        # test with pd.DataFrame and ps.DatetimeIndex
        self.assert_eq(
            ps.DataFrame(
                data=pdf,
                index=ps.DatetimeIndex(["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]),
            ),
            pd.DataFrame(
                data=pdf,
                index=pd.DatetimeIndex(["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]),
            ),
        )

        with ps.option_context("compute.ops_on_diff_frames", True):
            # test with ps.DataFrame and pd.DatetimeIndex
            self.assert_eq(
                ps.DataFrame(
                    data=psdf,
                    index=pd.DatetimeIndex(
                        ["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]
                    ),
                ),
                pd.DataFrame(
                    data=pdf,
                    index=pd.DatetimeIndex(
                        ["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]
                    ),
                ),
            )

            # test with ps.DataFrame and ps.DatetimeIndex
            self.assert_eq(
                ps.DataFrame(
                    data=psdf,
                    index=ps.DatetimeIndex(
                        ["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]
                    ),
                ),
                pd.DataFrame(
                    data=pdf,
                    index=pd.DatetimeIndex(
                        ["2022-08-31", "2022-09-02", "2022-09-03", "2022-09-05"]
                    ),
                ),
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

    def _check_extension(self, psdf, pdf):
        if LooseVersion("1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.2.2"):
            self.assert_eq(psdf, pdf, check_exact=False)
            for dtype in psdf.dtypes:
                self.assertTrue(isinstance(dtype, extension_dtypes))
        else:
            self.assert_eq(psdf, pdf)

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

        self._check_extension(psdf, pdf)
        self._check_extension(psdf + psdf, pdf + pdf)

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

        self._check_extension(psdf.astype(astype), pdf.astype(astype))

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

        self._check_extension(psdf, pdf)

    @unittest.skipIf(
        not extension_object_dtypes_available, "pandas extension object dtypes are not available"
    )
    def test_astype_extension_object_dtypes(self):
        pdf = pd.DataFrame({"a": ["a", "b", None, "c"], "b": [True, None, False, True]})
        psdf = ps.from_pandas(pdf)

        astype = {"a": "string", "b": "boolean"}

        self._check_extension(psdf.astype(astype), pdf.astype(astype))

    @unittest.skipIf(
        not extension_float_dtypes_available, "pandas extension float dtypes are not available"
    )
    def test_extension_float_dtypes(self):
        pdf = pd.DataFrame(
            {
                "a": pd.Series([1.0, 2.0, None, 4.0], dtype="Float32"),
                "b": pd.Series([1.0, None, 3.0, 4.0], dtype="Float64"),
            }
        )
        psdf = ps.from_pandas(pdf)

        self._check_extension(psdf, pdf)
        self._check_extension(psdf + 1, pdf + 1)
        self._check_extension(psdf + psdf, pdf + pdf)

    @unittest.skipIf(
        not extension_float_dtypes_available, "pandas extension float dtypes are not available"
    )
    def test_astype_extension_float_dtypes(self):
        pdf = pd.DataFrame({"a": [1.0, 2.0, None, 4.0], "b": [1.0, None, 3.0, 4.0]})
        psdf = ps.from_pandas(pdf)

        astype = {"a": "Float32", "b": "Float64"}

        self._check_extension(psdf.astype(astype), pdf.astype(astype))

    def test_insert(self):
        #
        # Basic DataFrame
        #
        pdf = pd.DataFrame([1, 2, 3])
        psdf = ps.from_pandas(pdf)

        psdf.insert(1, "b", 10)
        pdf.insert(1, "b", 10)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)
        psdf.insert(2, "c", 0.1)
        pdf.insert(2, "c", 0.1)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)
        psdf.insert(3, "d", psdf.b + 1)
        pdf.insert(3, "d", pdf.b + 1)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)

        psser = ps.Series([4, 5, 6])
        self.assertRaises(ValueError, lambda: psdf.insert(0, "y", psser))
        self.assertRaisesRegex(
            ValueError, "cannot insert b, already exists", lambda: psdf.insert(1, "b", 10)
        )
        self.assertRaisesRegex(
            TypeError,
            '"column" should be a scalar value or tuple that contains scalar values',
            lambda: psdf.insert(0, list("abc"), psser),
        )
        self.assertRaisesRegex(
            TypeError,
            "loc must be int",
            lambda: psdf.insert((1,), "b", 10),
        )
        self.assertRaisesRegex(
            NotImplementedError,
            "Assigning column name as tuple is only supported for MultiIndex columns for now.",
            lambda: psdf.insert(0, ("e",), 10),
        )

        self.assertRaises(ValueError, lambda: psdf.insert(0, "e", [7, 8, 9, 10]))
        self.assertRaises(ValueError, lambda: psdf.insert(0, "f", ps.Series([7, 8])))
        self.assertRaises(AssertionError, lambda: psdf.insert(100, "y", psser))
        self.assertRaises(AssertionError, lambda: psdf.insert(1, "y", psser, allow_duplicates=True))

        #
        # DataFrame with MultiIndex as columns
        #
        pdf = pd.DataFrame({("x", "a", "b"): [1, 2, 3]})
        psdf = ps.from_pandas(pdf)

        psdf.insert(1, "b", 10)
        pdf.insert(1, "b", 10)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)
        psdf.insert(2, "c", 0.1)
        pdf.insert(2, "c", 0.1)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)
        psdf.insert(3, "d", psdf.b + 1)
        pdf.insert(3, "d", pdf.b + 1)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)

        self.assertRaisesRegex(
            ValueError, "cannot insert d, already exists", lambda: psdf.insert(4, "d", 11)
        )
        self.assertRaisesRegex(
            ValueError,
            r"cannot insert \('x', 'a', 'b'\), already exists",
            lambda: psdf.insert(4, ("x", "a", "b"), 11),
        )
        self.assertRaisesRegex(
            ValueError,
            '"column" must have length equal to number of column levels.',
            lambda: psdf.insert(4, ("e",), 11),
        )

    def test_inplace(self):
        pdf, psdf = self.df_pair

        pser = pdf.a
        psser = psdf.a

        pdf["a"] = pdf["a"] + 10
        psdf["a"] = psdf["a"] + 10

        self.assert_eq(psdf, pdf)
        # SPARK-38946: Since Spark 3.4, df.__setitem__ generate a new dataframe to follow
        # pandas 1.4 behaviors
        if LooseVersion(pd.__version__) >= LooseVersion("1.4.0"):
            self.assert_eq(psser, pser)
        else:
            # Follow pandas latest behavior
            with self.assertRaisesRegex(AssertionError, "Series are different"):
                self.assert_eq(psser, pser)

    def test_assign_list(self):
        pdf, psdf = self.df_pair

        pser = pdf.a
        psser = psdf.a

        pdf["x"] = [10, 20, 30, 40, 50, 60, 70, 80, 90]
        psdf["x"] = [10, 20, 30, 40, 50, 60, 70, 80, 90]

        self.assert_eq(psdf.sort_index(), pdf.sort_index())
        self.assert_eq(psser, pser)

        with self.assertRaisesRegex(ValueError, "Length of values does not match length of index"):
            psdf["z"] = [10, 20, 30, 40, 50, 60, 70, 80]

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

    def test_itertuples(self):
        pdf = pd.DataFrame({"num_legs": [4, 2], "num_wings": [0, 2]}, index=["dog", "hawk"])
        psdf = ps.from_pandas(pdf)

        for ptuple, ktuple in zip(
            pdf.itertuples(index=False, name="Animal"), psdf.itertuples(index=False, name="Animal")
        ):
            self.assert_eq(ptuple, ktuple)
        for ptuple, ktuple in zip(pdf.itertuples(name=None), psdf.itertuples(name=None)):
            self.assert_eq(ptuple, ktuple)
        for ptuple, ktuple in zip(
            pdf.itertuples(index=False, name=None), psdf.itertuples(index=False, name=None)
        ):
            self.assert_eq(ptuple, ktuple)

        pdf.index = pd.MultiIndex.from_arrays(
            [[1, 2], ["black", "brown"]], names=("count", "color")
        )
        psdf = ps.from_pandas(pdf)
        for ptuple, ktuple in zip(pdf.itertuples(name="Animal"), psdf.itertuples(name="Animal")):
            self.assert_eq(ptuple, ktuple)

        pdf.columns = pd.MultiIndex.from_arrays(
            [["CA", "WA"], ["age", "children"]], names=("origin", "info")
        )
        psdf = ps.from_pandas(pdf)
        for ptuple, ktuple in zip(pdf.itertuples(name="Animal"), psdf.itertuples(name="Animal")):
            self.assert_eq(ptuple, ktuple)

        pdf = pd.DataFrame([1, 2, 3])
        psdf = ps.from_pandas(pdf)
        for ptuple, ktuple in zip(
            (pdf + 1).itertuples(name="num"), (psdf + 1).itertuples(name="num")
        ):
            self.assert_eq(ptuple, ktuple)

        # DataFrames with a large number of columns (>254)
        pdf = pd.DataFrame(np.random.random((1, 255)))
        psdf = ps.from_pandas(pdf)
        for ptuple, ktuple in zip(pdf.itertuples(name="num"), psdf.itertuples(name="num")):
            self.assert_eq(ptuple, ktuple)

    def test_iterrows(self):
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

        for (pdf_k, pdf_v), (psdf_k, psdf_v) in zip(pdf.iterrows(), psdf.iterrows()):
            self.assert_eq(pdf_k, psdf_k)
            self.assert_eq(pdf_v, psdf_v)

        # MultiIndex
        pmidx = pd.Index([(1, 2), (3, 4), (5, 6)])
        pdf.index = pmidx
        psdf = ps.from_pandas(pdf)

        for (pdf_k, pdf_v), (psdf_k, psdf_v) in zip(pdf.iterrows(), psdf.iterrows()):
            self.assert_eq(pdf_k, psdf_k)
            self.assert_eq(pdf_v, psdf_v)

    def test_reset_index(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=np.random.rand(3))
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.reset_index(), pdf.reset_index())
        self.assert_eq(psdf.reset_index().index, pdf.reset_index().index)
        self.assert_eq(psdf.reset_index(drop=True), pdf.reset_index(drop=True))

        pdf.index.name = "a"
        psdf.index.name = "a"

        with self.assertRaisesRegex(ValueError, "cannot insert a, already exists"):
            psdf.reset_index()

        self.assert_eq(psdf.reset_index(drop=True), pdf.reset_index(drop=True))

        # inplace
        pser = pdf.a
        psser = psdf.a
        pdf.reset_index(drop=True, inplace=True)
        psdf.reset_index(drop=True, inplace=True)
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser, pser)

        pdf.columns = ["index", "b"]
        psdf.columns = ["index", "b"]
        self.assert_eq(psdf.reset_index(), pdf.reset_index())

    def test_reset_index_with_default_index_types(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=np.random.rand(3))
        psdf = ps.from_pandas(pdf)

        with ps.option_context("compute.default_index_type", "sequence"):
            self.assert_eq(psdf.reset_index(), pdf.reset_index())

        with ps.option_context("compute.default_index_type", "distributed-sequence"):
            self.assert_eq(psdf.reset_index(), pdf.reset_index())

        with ps.option_context("compute.default_index_type", "distributed"):
            # the index is different.
            self.assert_eq(
                psdf.reset_index()._to_pandas().reset_index(drop=True), pdf.reset_index()
            )

    def test_reset_index_with_multiindex_columns(self):
        index = pd.MultiIndex.from_tuples(
            [("bird", "falcon"), ("bird", "parrot"), ("mammal", "lion"), ("mammal", "monkey")],
            names=["class", "name"],
        )
        columns = pd.MultiIndex.from_tuples([("speed", "max"), ("species", "type")])
        pdf = pd.DataFrame(
            [(389.0, "fly"), (24.0, "fly"), (80.5, "run"), (np.nan, "jump")],
            index=index,
            columns=columns,
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf.reset_index(), pdf.reset_index())
        self.assert_eq(psdf.reset_index(level="class"), pdf.reset_index(level="class"))
        self.assert_eq(
            psdf.reset_index(level="class", col_level=1),
            pdf.reset_index(level="class", col_level=1),
        )
        self.assert_eq(
            psdf.reset_index(level="class", col_level=1, col_fill="species"),
            pdf.reset_index(level="class", col_level=1, col_fill="species"),
        )
        self.assert_eq(
            psdf.reset_index(level="class", col_level=1, col_fill="genus"),
            pdf.reset_index(level="class", col_level=1, col_fill="genus"),
        )

        with self.assertRaisesRegex(IndexError, "Index has only 2 levels, not 3"):
            psdf.reset_index(col_level=2)

        pdf.index.names = [("x", "class"), ("y", "name")]
        psdf.index.names = [("x", "class"), ("y", "name")]

        self.assert_eq(psdf.reset_index(), pdf.reset_index())

        with self.assertRaisesRegex(ValueError, "Item must have length equal to number of levels."):
            psdf.reset_index(col_level=1)

    def test_index_to_frame_reset_index(self):
        def check(psdf, pdf):
            self.assert_eq(psdf.reset_index(), pdf.reset_index())
            self.assert_eq(psdf.reset_index(drop=True), pdf.reset_index(drop=True))

            pdf.reset_index(drop=True, inplace=True)
            psdf.reset_index(drop=True, inplace=True)
            self.assert_eq(psdf, pdf)

        pdf, psdf = self.df_pair
        check(psdf.index.to_frame(), pdf.index.to_frame())
        check(psdf.index.to_frame(index=False), pdf.index.to_frame(index=False))

        check(psdf.index.to_frame(name="a"), pdf.index.to_frame(name="a"))
        check(psdf.index.to_frame(index=False, name="a"), pdf.index.to_frame(index=False, name="a"))
        check(psdf.index.to_frame(name=("x", "a")), pdf.index.to_frame(name=("x", "a")))
        check(
            psdf.index.to_frame(index=False, name=("x", "a")),
            pdf.index.to_frame(index=False, name=("x", "a")),
        )

    def test_multiindex_column_access(self):
        columns = pd.MultiIndex.from_tuples(
            [
                ("a", "", "", "b"),
                ("c", "", "d", ""),
                ("e", "", "f", ""),
                ("e", "g", "", ""),
                ("", "", "", "h"),
                ("i", "", "", ""),
            ]
        )

        pdf = pd.DataFrame(
            [
                (1, "a", "x", 10, 100, 1000),
                (2, "b", "y", 20, 200, 2000),
                (3, "c", "z", 30, 300, 3000),
            ],
            columns=columns,
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf["a"], pdf["a"])
        self.assert_eq(psdf["a"]["b"], pdf["a"]["b"])
        self.assert_eq(psdf["c"], pdf["c"])
        self.assert_eq(psdf["c"]["d"], pdf["c"]["d"])
        self.assert_eq(psdf["e"], pdf["e"])
        self.assert_eq(psdf["e"][""]["f"], pdf["e"][""]["f"])
        self.assert_eq(psdf["e"]["g"], pdf["e"]["g"])
        self.assert_eq(psdf[""], pdf[""])
        self.assert_eq(psdf[""]["h"], pdf[""]["h"])
        self.assert_eq(psdf["i"], pdf["i"])

        self.assert_eq(psdf[["a", "e"]], pdf[["a", "e"]])
        self.assert_eq(psdf[["e", "a"]], pdf[["e", "a"]])

        self.assert_eq(psdf[("a",)], pdf[("a",)])
        self.assert_eq(psdf[("e", "g")], pdf[("e", "g")])
        # self.assert_eq(psdf[("i",)], pdf[("i",)])
        self.assert_eq(psdf[("i", "")], pdf[("i", "")])

        self.assertRaises(KeyError, lambda: psdf[("a", "b")])

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

    def test_head(self):
        pdf, psdf = self.df_pair

        self.assert_eq(psdf.head(2), pdf.head(2))
        self.assert_eq(psdf.head(3), pdf.head(3))
        self.assert_eq(psdf.head(0), pdf.head(0))
        self.assert_eq(psdf.head(-3), pdf.head(-3))
        self.assert_eq(psdf.head(-10), pdf.head(-10))
        with option_context("compute.ordered_head", True):
            self.assert_eq(psdf.head(), pdf.head())

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

    def test_rename_dataframe(self):
        pdf1 = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        psdf1 = ps.from_pandas(pdf1)

        self.assert_eq(
            psdf1.rename(columns={"A": "a", "B": "b"}), pdf1.rename(columns={"A": "a", "B": "b"})
        )

        result_psdf = psdf1.rename(index={1: 10, 2: 20})
        result_pdf = pdf1.rename(index={1: 10, 2: 20})
        self.assert_eq(result_psdf, result_pdf)

        # inplace
        pser = result_pdf.A
        psser = result_psdf.A
        result_psdf.rename(index={10: 100, 20: 200}, inplace=True)
        result_pdf.rename(index={10: 100, 20: 200}, inplace=True)
        self.assert_eq(result_psdf, result_pdf)
        self.assert_eq(psser, pser)

        def str_lower(s) -> str:
            return str.lower(s)

        self.assert_eq(
            psdf1.rename(str_lower, axis="columns"), pdf1.rename(str_lower, axis="columns")
        )

        def mul10(x) -> int:
            return x * 10

        self.assert_eq(psdf1.rename(mul10, axis="index"), pdf1.rename(mul10, axis="index"))

        self.assert_eq(
            psdf1.rename(columns=str_lower, index={1: 10, 2: 20}),
            pdf1.rename(columns=str_lower, index={1: 10, 2: 20}),
        )

        self.assert_eq(
            psdf1.rename(columns=lambda x: str.lower(x)),
            pdf1.rename(columns=lambda x: str.lower(x)),
        )

        idx = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Y", "D")])
        pdf2 = pd.DataFrame([[1, 2, 3, 4], [5, 6, 7, 8]], columns=idx)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(psdf2.rename(columns=str_lower), pdf2.rename(columns=str_lower))
        self.assert_eq(
            psdf2.rename(columns=lambda x: str.lower(x)),
            pdf2.rename(columns=lambda x: str.lower(x)),
        )

        self.assert_eq(
            psdf2.rename(columns=str_lower, level=0), pdf2.rename(columns=str_lower, level=0)
        )
        self.assert_eq(
            psdf2.rename(columns=str_lower, level=1), pdf2.rename(columns=str_lower, level=1)
        )

        pdf3 = pd.DataFrame([[1, 2], [3, 4], [5, 6], [7, 8]], index=idx, columns=list("ab"))
        psdf3 = ps.from_pandas(pdf3)

        self.assert_eq(psdf3.rename(index=str_lower), pdf3.rename(index=str_lower))
        self.assert_eq(
            psdf3.rename(index=str_lower, level=0), pdf3.rename(index=str_lower, level=0)
        )
        self.assert_eq(
            psdf3.rename(index=str_lower, level=1), pdf3.rename(index=str_lower, level=1)
        )

        pdf4 = pdf2 + 1
        psdf4 = psdf2 + 1
        self.assert_eq(psdf4.rename(columns=str_lower), pdf4.rename(columns=str_lower))

        pdf5 = pdf3 + 1
        psdf5 = psdf3 + 1
        self.assert_eq(psdf5.rename(index=str_lower), pdf5.rename(index=str_lower))

        msg = "Either `index` or `columns` should be provided."
        with self.assertRaisesRegex(ValueError, msg):
            psdf1.rename()
        msg = "`mapper` or `index` or `columns` should be either dict-like or function type."
        with self.assertRaisesRegex(ValueError, msg):
            psdf1.rename(mapper=[str_lower], axis=1)
        msg = "Mapper dict should have the same value type."
        with self.assertRaisesRegex(ValueError, msg):
            psdf1.rename({"A": "a", "B": 2}, axis=1)
        msg = r"level should be an integer between \[0, column_labels_level\)"
        with self.assertRaisesRegex(ValueError, msg):
            psdf2.rename(columns=str_lower, level=2)
        msg = r"level should be an integer between \[0, 2\)"
        with self.assertRaisesRegex(ValueError, msg):
            psdf3.rename(index=str_lower, level=2)

    def test_rename_axis(self):
        index = pd.Index(["A", "B", "C"], name="index")
        columns = pd.Index(["numbers", "values"], name="cols")
        pdf = pd.DataFrame([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]], index=index, columns=columns)
        psdf = ps.from_pandas(pdf)

        for axis in [0, "index"]:
            self.assert_eq(
                pdf.rename_axis("index2", axis=axis).sort_index(),
                psdf.rename_axis("index2", axis=axis).sort_index(),
            )
            self.assert_eq(
                pdf.rename_axis(["index2"], axis=axis).sort_index(),
                psdf.rename_axis(["index2"], axis=axis).sort_index(),
            )

        for axis in [1, "columns"]:
            self.assert_eq(
                pdf.rename_axis("cols2", axis=axis).sort_index(),
                psdf.rename_axis("cols2", axis=axis).sort_index(),
            )
            self.assert_eq(
                pdf.rename_axis(["cols2"], axis=axis).sort_index(),
                psdf.rename_axis(["cols2"], axis=axis).sort_index(),
            )

        pdf2 = pdf.copy()
        psdf2 = psdf.copy()
        pdf2.rename_axis("index2", axis="index", inplace=True)
        psdf2.rename_axis("index2", axis="index", inplace=True)
        self.assert_eq(pdf2.sort_index(), psdf2.sort_index())

        self.assertRaises(ValueError, lambda: psdf.rename_axis(["index2", "index3"], axis=0))
        self.assertRaises(ValueError, lambda: psdf.rename_axis(["cols2", "cols3"], axis=1))
        self.assertRaises(TypeError, lambda: psdf.rename_axis(mapper=["index2"], index=["index3"]))
        self.assertRaises(ValueError, lambda: psdf.rename_axis(ps))

        self.assert_eq(
            pdf.rename_axis(index={"index": "index2"}, columns={"cols": "cols2"}).sort_index(),
            psdf.rename_axis(index={"index": "index2"}, columns={"cols": "cols2"}).sort_index(),
        )

        self.assert_eq(
            pdf.rename_axis(index={"missing": "index2"}, columns={"missing": "cols2"}).sort_index(),
            psdf.rename_axis(
                index={"missing": "index2"}, columns={"missing": "cols2"}
            ).sort_index(),
        )

        self.assert_eq(
            pdf.rename_axis(index=str.upper, columns=str.upper).sort_index(),
            psdf.rename_axis(index=str.upper, columns=str.upper).sort_index(),
        )

        index = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["index1", "index2"]
        )
        columns = pd.MultiIndex.from_tuples(
            [("numbers", "first"), ("values", "second")], names=["cols1", "cols2"]
        )
        pdf = pd.DataFrame([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]], index=index, columns=columns)
        psdf = ps.from_pandas(pdf)

        for axis in [0, "index"]:
            self.assert_eq(
                pdf.rename_axis(["index3", "index4"], axis=axis).sort_index(),
                psdf.rename_axis(["index3", "index4"], axis=axis).sort_index(),
            )

        for axis in [1, "columns"]:
            self.assert_eq(
                pdf.rename_axis(["cols3", "cols4"], axis=axis).sort_index(),
                psdf.rename_axis(["cols3", "cols4"], axis=axis).sort_index(),
            )

        self.assertRaises(
            ValueError, lambda: psdf.rename_axis(["index3", "index4", "index5"], axis=0)
        )
        self.assertRaises(ValueError, lambda: psdf.rename_axis(["cols3", "cols4", "cols5"], axis=1))

        self.assert_eq(
            pdf.rename_axis(index={"index1": "index3"}, columns={"cols1": "cols3"}).sort_index(),
            psdf.rename_axis(index={"index1": "index3"}, columns={"cols1": "cols3"}).sort_index(),
        )

        self.assert_eq(
            pdf.rename_axis(index={"missing": "index3"}, columns={"missing": "cols3"}).sort_index(),
            psdf.rename_axis(
                index={"missing": "index3"}, columns={"missing": "cols3"}
            ).sort_index(),
        )

        self.assert_eq(
            pdf.rename_axis(
                index={"index1": "index3", "index2": "index4"},
                columns={"cols1": "cols3", "cols2": "cols4"},
            ).sort_index(),
            psdf.rename_axis(
                index={"index1": "index3", "index2": "index4"},
                columns={"cols1": "cols3", "cols2": "cols4"},
            ).sort_index(),
        )

        self.assert_eq(
            pdf.rename_axis(index=str.upper, columns=str.upper).sort_index(),
            psdf.rename_axis(index=str.upper, columns=str.upper).sort_index(),
        )

    def test_dot(self):
        psdf = self.psdf

        with self.assertRaisesRegex(TypeError, "Unsupported type DataFrame"):
            psdf.dot(psdf)

    def test_dot_in_column_name(self):
        self.assert_eq(
            ps.DataFrame(ps.range(1)._internal.spark_frame.selectExpr("1L as `a.b`"))["a.b"],
            ps.Series([1], name="a.b"),
        )

    def test_aggregate(self):
        pdf = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9], [np.nan, np.nan, np.nan]], columns=["A", "B", "C"]
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.agg(["sum", "min"])[["A", "B", "C"]].sort_index(),  # TODO?: fix column order
            pdf.agg(["sum", "min"])[["A", "B", "C"]].sort_index(),
        )
        self.assert_eq(
            psdf.agg({"A": ["sum", "min"], "B": ["min", "max"]})[["A", "B"]].sort_index(),
            pdf.agg({"A": ["sum", "min"], "B": ["min", "max"]})[["A", "B"]].sort_index(),
        )

        self.assertRaises(KeyError, lambda: psdf.agg({"A": ["sum", "min"], "X": ["min", "max"]}))

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.agg(["sum", "min"])[[("X", "A"), ("X", "B"), ("Y", "C")]].sort_index(),
            pdf.agg(["sum", "min"])[[("X", "A"), ("X", "B"), ("Y", "C")]].sort_index(),
        )
        self.assert_eq(
            psdf.agg({("X", "A"): ["sum", "min"], ("X", "B"): ["min", "max"]})[
                [("X", "A"), ("X", "B")]
            ].sort_index(),
            pdf.agg({("X", "A"): ["sum", "min"], ("X", "B"): ["min", "max"]})[
                [("X", "A"), ("X", "B")]
            ].sort_index(),
        )

        self.assertRaises(TypeError, lambda: psdf.agg({"X": ["sum", "min"], "Y": ["min", "max"]}))

        # non-string names
        pdf = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9], [np.nan, np.nan, np.nan]], columns=[10, 20, 30]
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.agg(["sum", "min"])[[10, 20, 30]].sort_index(),
            pdf.agg(["sum", "min"])[[10, 20, 30]].sort_index(),
        )
        self.assert_eq(
            psdf.agg({10: ["sum", "min"], 20: ["min", "max"]})[[10, 20]].sort_index(),
            pdf.agg({10: ["sum", "min"], 20: ["min", "max"]})[[10, 20]].sort_index(),
        )

        columns = pd.MultiIndex.from_tuples([("X", 10), ("X", 20), ("Y", 30)])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.agg(["sum", "min"])[[("X", 10), ("X", 20), ("Y", 30)]].sort_index(),
            pdf.agg(["sum", "min"])[[("X", 10), ("X", 20), ("Y", 30)]].sort_index(),
        )
        self.assert_eq(
            psdf.agg({("X", 10): ["sum", "min"], ("X", 20): ["min", "max"]})[
                [("X", 10), ("X", 20)]
            ].sort_index(),
            pdf.agg({("X", 10): ["sum", "min"], ("X", 20): ["min", "max"]})[
                [("X", 10), ("X", 20)]
            ].sort_index(),
        )

        pdf = pd.DataFrame(
            [datetime(2019, 2, 2, 0, 0, 0, 0), datetime(2019, 2, 3, 0, 0, 0, 0)],
            columns=["timestamp"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.timestamp.min(), pdf.timestamp.min())
        self.assert_eq(psdf.timestamp.max(), pdf.timestamp.max())

        self.assertRaises(ValueError, lambda: psdf.agg(("sum", "min")))

    def test_droplevel(self):
        pdf = (
            pd.DataFrame([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
            .set_index([0, 1])
            .rename_axis(["a", "b"])
        )
        pdf.columns = pd.MultiIndex.from_tuples(
            [("c", "e"), ("d", "f")], names=["level_1", "level_2"]
        )
        psdf = ps.from_pandas(pdf)

        self.assertRaises(ValueError, lambda: psdf.droplevel(["a", "b"]))
        self.assertRaises(ValueError, lambda: psdf.droplevel([1, 1, 1, 1, 1]))
        self.assertRaises(IndexError, lambda: psdf.droplevel(2))
        self.assertRaises(IndexError, lambda: psdf.droplevel(-3))
        self.assertRaises(KeyError, lambda: psdf.droplevel({"a"}))
        self.assertRaises(KeyError, lambda: psdf.droplevel({"a": 1}))

        self.assertRaises(ValueError, lambda: psdf.droplevel(["level_1", "level_2"], axis=1))
        self.assertRaises(IndexError, lambda: psdf.droplevel(2, axis=1))
        self.assertRaises(IndexError, lambda: psdf.droplevel(-3, axis=1))
        self.assertRaises(KeyError, lambda: psdf.droplevel({"level_1"}, axis=1))
        self.assertRaises(KeyError, lambda: psdf.droplevel({"level_1": 1}, axis=1))

        self.assert_eq(pdf.droplevel("a"), psdf.droplevel("a"))
        self.assert_eq(pdf.droplevel(["a"]), psdf.droplevel(["a"]))
        self.assert_eq(pdf.droplevel(("a",)), psdf.droplevel(("a",)))
        self.assert_eq(pdf.droplevel(0), psdf.droplevel(0))
        self.assert_eq(pdf.droplevel(-1), psdf.droplevel(-1))

        self.assert_eq(pdf.droplevel("level_1", axis=1), psdf.droplevel("level_1", axis=1))
        self.assert_eq(pdf.droplevel(["level_1"], axis=1), psdf.droplevel(["level_1"], axis=1))
        self.assert_eq(pdf.droplevel(("level_1",), axis=1), psdf.droplevel(("level_1",), axis=1))
        self.assert_eq(pdf.droplevel(0, axis=1), psdf.droplevel(0, axis=1))
        self.assert_eq(pdf.droplevel(-1, axis=1), psdf.droplevel(-1, axis=1))

        # Tupled names
        pdf.columns.names = [("level", 1), ("level", 2)]
        pdf.index.names = [("a", 10), ("x", 20)]
        psdf = ps.from_pandas(pdf)

        self.assertRaises(KeyError, lambda: psdf.droplevel("a"))
        self.assertRaises(KeyError, lambda: psdf.droplevel(("a", 10)))

        self.assert_eq(pdf.droplevel([("a", 10)]), psdf.droplevel([("a", 10)]))
        self.assert_eq(
            pdf.droplevel([("level", 1)], axis=1), psdf.droplevel([("level", 1)], axis=1)
        )

        # non-string names
        pdf = (
            pd.DataFrame([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
            .set_index([0, 1])
            .rename_axis([10.0, 20.0])
        )
        pdf.columns = pd.MultiIndex.from_tuples([("c", "e"), ("d", "f")], names=[100.0, 200.0])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.droplevel(10.0), psdf.droplevel(10.0))
        self.assert_eq(pdf.droplevel([10.0]), psdf.droplevel([10.0]))
        self.assert_eq(pdf.droplevel((10.0,)), psdf.droplevel((10.0,)))
        self.assert_eq(pdf.droplevel(0), psdf.droplevel(0))
        self.assert_eq(pdf.droplevel(-1), psdf.droplevel(-1))
        self.assert_eq(pdf.droplevel(100.0, axis=1), psdf.droplevel(100.0, axis=1))
        self.assert_eq(pdf.droplevel(0, axis=1), psdf.droplevel(0, axis=1))

    def test_drop(self):
        pdf = pd.DataFrame({"x": [1, 2], "y": [3, 4], "z": [5, 6]}, index=np.random.rand(2))
        psdf = ps.from_pandas(pdf)

        # Assert 'labels' or 'columns' parameter is set
        expected_error_message = "Need to specify at least one of 'labels' or 'columns'"
        with self.assertRaisesRegex(ValueError, expected_error_message):
            psdf.drop()

        #
        # Drop columns
        #

        # Assert using a str for 'labels' works
        self.assert_eq(psdf.drop("x", axis=1), pdf.drop("x", axis=1))
        self.assert_eq((psdf + 1).drop("x", axis=1), (pdf + 1).drop("x", axis=1))
        # Assert using a list for 'labels' works
        self.assert_eq(psdf.drop(["y", "z"], axis=1), pdf.drop(["y", "z"], axis=1))
        self.assert_eq(psdf.drop(["x", "y", "z"], axis=1), pdf.drop(["x", "y", "z"], axis=1))
        # Assert using 'columns' instead of 'labels' produces the same results
        self.assert_eq(psdf.drop(columns="x"), pdf.drop(columns="x"))
        self.assert_eq(psdf.drop(columns=["y", "z"]), pdf.drop(columns=["y", "z"]))
        self.assert_eq(psdf.drop(columns=["x", "y", "z"]), pdf.drop(columns=["x", "y", "z"]))
        self.assert_eq(psdf.drop(columns=[]), pdf.drop(columns=[]))

        columns = pd.MultiIndex.from_tuples([(1, "x"), (1, "y"), (2, "z")])
        pdf.columns = columns
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.drop(columns=1), pdf.drop(columns=1))
        self.assert_eq(psdf.drop(columns=(1, "x")), pdf.drop(columns=(1, "x")))
        self.assert_eq(psdf.drop(columns=[(1, "x"), 2]), pdf.drop(columns=[(1, "x"), 2]))
        self.assert_eq(
            psdf.drop(columns=[(1, "x"), (1, "y"), (2, "z")]),
            pdf.drop(columns=[(1, "x"), (1, "y"), (2, "z")]),
        )

        self.assertRaises(KeyError, lambda: psdf.drop(columns=3))
        self.assertRaises(KeyError, lambda: psdf.drop(columns=(1, "z")))

        pdf.index = pd.MultiIndex.from_tuples([("i", 0), ("j", 1)])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.drop(columns=[(1, "x"), (1, "y"), (2, "z")]),
            pdf.drop(columns=[(1, "x"), (1, "y"), (2, "z")]),
        )

        # non-string names
        pdf = pd.DataFrame({10: [1, 2], 20: [3, 4], 30: [5, 6]}, index=np.random.rand(2))
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.drop(10, axis=1), pdf.drop(10, axis=1))
        self.assert_eq(psdf.drop([20, 30], axis=1), pdf.drop([20, 30], axis=1))

        #
        # Drop rows
        #

        pdf = pd.DataFrame({"X": [1, 2, 3], "Y": [4, 5, 6], "Z": [7, 8, 9]}, index=["A", "B", "C"])
        psdf = ps.from_pandas(pdf)

        # Given labels (and axis = 0)
        self.assert_eq(psdf.drop(labels="A", axis=0), pdf.drop(labels="A", axis=0))
        self.assert_eq(psdf.drop(labels="A"), pdf.drop(labels="A"))
        self.assert_eq((psdf + 1).drop(labels="A"), (pdf + 1).drop(labels="A"))
        self.assert_eq(psdf.drop(labels=["A", "C"], axis=0), pdf.drop(labels=["A", "C"], axis=0))
        self.assert_eq(
            psdf.drop(labels=["A", "B", "C"], axis=0), pdf.drop(labels=["A", "B", "C"], axis=0)
        )

        with ps.option_context("compute.isin_limit", 2):
            self.assert_eq(
                psdf.drop(labels=["A", "B", "C"], axis=0), pdf.drop(labels=["A", "B", "C"], axis=0)
            )

        # Given index
        self.assert_eq(psdf.drop(index="A"), pdf.drop(index="A"))
        self.assert_eq(psdf.drop(index=["A", "C"]), pdf.drop(index=["A", "C"]))
        self.assert_eq(psdf.drop(index=["A", "B", "C"]), pdf.drop(index=["A", "B", "C"]))
        self.assert_eq(psdf.drop(index=[]), pdf.drop(index=[]))

        with ps.option_context("compute.isin_limit", 2):
            self.assert_eq(psdf.drop(index=["A", "B", "C"]), pdf.drop(index=["A", "B", "C"]))

        # Non-string names
        pdf.index = [10, 20, 30]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.drop(labels=10, axis=0), pdf.drop(labels=10, axis=0))
        self.assert_eq(psdf.drop(labels=[10, 30], axis=0), pdf.drop(labels=[10, 30], axis=0))
        self.assert_eq(
            psdf.drop(labels=[10, 20, 30], axis=0), pdf.drop(labels=[10, 20, 30], axis=0)
        )

        with ps.option_context("compute.isin_limit", 2):
            self.assert_eq(
                psdf.drop(labels=[10, 20, 30], axis=0), pdf.drop(labels=[10, 20, 30], axis=0)
            )

        # MultiIndex
        pdf.index = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assertRaises(NotImplementedError, lambda: psdf.drop(labels=[("a", "x")]))

        #
        # Drop rows and columns
        #
        pdf = pd.DataFrame({"X": [1, 2, 3], "Y": [4, 5, 6], "Z": [7, 8, 9]}, index=["A", "B", "C"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.drop(index="A", columns="X"), pdf.drop(index="A", columns="X"))
        self.assert_eq(
            psdf.drop(index=["A", "C"], columns=["X", "Z"]),
            pdf.drop(index=["A", "C"], columns=["X", "Z"]),
        )
        self.assert_eq(
            psdf.drop(index=["A", "B", "C"], columns=["X", "Z"]),
            pdf.drop(index=["A", "B", "C"], columns=["X", "Z"]),
        )
        with ps.option_context("compute.isin_limit", 2):
            self.assert_eq(
                psdf.drop(index=["A", "B", "C"], columns=["X", "Z"]),
                pdf.drop(index=["A", "B", "C"], columns=["X", "Z"]),
            )
        self.assert_eq(
            psdf.drop(index=[], columns=["X", "Z"]),
            pdf.drop(index=[], columns=["X", "Z"]),
        )
        self.assert_eq(
            psdf.drop(index=["A", "B", "C"], columns=[]),
            pdf.drop(index=["A", "B", "C"], columns=[]),
        )
        self.assert_eq(
            psdf.drop(index=[], columns=[]),
            pdf.drop(index=[], columns=[]),
        )
        self.assertRaises(
            ValueError,
            lambda: psdf.drop(labels="A", axis=0, columns="X"),
        )

    def _test_dropna(self, pdf, axis):
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.dropna(axis=axis), pdf.dropna(axis=axis))
        self.assert_eq(psdf.dropna(axis=axis, how="all"), pdf.dropna(axis=axis, how="all"))
        self.assert_eq(psdf.dropna(axis=axis, subset=["x"]), pdf.dropna(axis=axis, subset=["x"]))
        self.assert_eq(psdf.dropna(axis=axis, subset="x"), pdf.dropna(axis=axis, subset=["x"]))
        self.assert_eq(
            psdf.dropna(axis=axis, subset=["y", "z"]), pdf.dropna(axis=axis, subset=["y", "z"])
        )
        self.assert_eq(
            psdf.dropna(axis=axis, subset=["y", "z"], how="all"),
            pdf.dropna(axis=axis, subset=["y", "z"], how="all"),
        )

        self.assert_eq(psdf.dropna(axis=axis, thresh=2), pdf.dropna(axis=axis, thresh=2))
        self.assert_eq(
            psdf.dropna(axis=axis, thresh=1, subset=["y", "z"]),
            pdf.dropna(axis=axis, thresh=1, subset=["y", "z"]),
        )

        pdf2 = pdf.copy()
        psdf2 = psdf.copy()
        pser = pdf2[pdf2.columns[0]]
        psser = psdf2[psdf2.columns[0]]
        pdf2.dropna(inplace=True, axis=axis)
        psdf2.dropna(inplace=True, axis=axis)
        self.assert_eq(psdf2, pdf2)
        self.assert_eq(psser, pser)

        # multi-index
        columns = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z")])
        if axis == 0:
            pdf.columns = columns
        else:
            pdf.index = columns
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.dropna(axis=axis), pdf.dropna(axis=axis))
        self.assert_eq(psdf.dropna(axis=axis, how="all"), pdf.dropna(axis=axis, how="all"))
        self.assert_eq(
            psdf.dropna(axis=axis, subset=[("a", "x")]), pdf.dropna(axis=axis, subset=[("a", "x")])
        )
        self.assert_eq(
            psdf.dropna(axis=axis, subset=("a", "x")), pdf.dropna(axis=axis, subset=[("a", "x")])
        )
        self.assert_eq(
            psdf.dropna(axis=axis, subset=[("a", "y"), ("b", "z")]),
            pdf.dropna(axis=axis, subset=[("a", "y"), ("b", "z")]),
        )
        self.assert_eq(
            psdf.dropna(axis=axis, subset=[("a", "y"), ("b", "z")], how="all"),
            pdf.dropna(axis=axis, subset=[("a", "y"), ("b", "z")], how="all"),
        )

        self.assert_eq(psdf.dropna(axis=axis, thresh=2), pdf.dropna(axis=axis, thresh=2))
        self.assert_eq(
            psdf.dropna(axis=axis, thresh=1, subset=[("a", "y"), ("b", "z")]),
            pdf.dropna(axis=axis, thresh=1, subset=[("a", "y"), ("b", "z")]),
        )

    def test_dropna_axis_index(self):
        pdf = pd.DataFrame(
            {
                "x": [np.nan, 2, 3, 4, np.nan, 6],
                "y": [1, 2, np.nan, 4, np.nan, np.nan],
                "z": [1, 2, 3, 4, np.nan, np.nan],
            },
            index=np.random.rand(6),
        )

        self._test_dropna(pdf, axis=0)

        # empty
        pdf = pd.DataFrame(index=np.random.rand(6))
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.dropna(), pdf.dropna())
        self.assert_eq(psdf.dropna(how="all"), pdf.dropna(how="all"))
        self.assert_eq(psdf.dropna(thresh=0), pdf.dropna(thresh=0))
        self.assert_eq(psdf.dropna(thresh=1), pdf.dropna(thresh=1))

        # Only NA value
        pdf["a"] = [np.nan] * 6
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.dropna(), pdf.dropna())
        self.assert_eq(psdf.dropna(how="all"), pdf.dropna(how="all"))
        self.assert_eq(psdf.dropna(thresh=0), pdf.dropna(thresh=0))
        self.assert_eq(psdf.dropna(thresh=1), pdf.dropna(thresh=1))

        with self.assertRaisesRegex(ValueError, "No axis named foo"):
            psdf.dropna(axis="foo")

        self.assertRaises(KeyError, lambda: psdf.dropna(subset="1"))
        with self.assertRaisesRegex(ValueError, "invalid how option: 1"):
            psdf.dropna(how=1)
        with self.assertRaisesRegex(TypeError, "must specify how or thresh"):
            psdf.dropna(how=None)

    def test_dropna_axis_column(self):
        pdf = pd.DataFrame(
            {
                "x": [np.nan, 2, 3, 4, np.nan, 6],
                "y": [1, 2, np.nan, 4, np.nan, np.nan],
                "z": [1, 2, 3, 4, np.nan, np.nan],
            },
            index=[str(r) for r in np.random.rand(6)],
        ).T

        self._test_dropna(pdf, axis=1)

        psdf = ps.from_pandas(pdf)
        with self.assertRaisesRegex(
            ValueError, "The length of each subset must be the same as the index size."
        ):
            psdf.dropna(subset=(["x", "y"]), axis=1)

        # empty
        pdf = pd.DataFrame({"x": [], "y": [], "z": []})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.dropna(axis=1), pdf.dropna(axis=1))
        self.assert_eq(psdf.dropna(axis=1, how="all"), pdf.dropna(axis=1, how="all"))
        self.assert_eq(psdf.dropna(axis=1, thresh=0), pdf.dropna(axis=1, thresh=0))
        self.assert_eq(psdf.dropna(axis=1, thresh=1), pdf.dropna(axis=1, thresh=1))

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

    def test_fillna(self):
        pdf = pd.DataFrame(
            {
                "x": [np.nan, 2, 3, 4, np.nan, 6],
                "y": [1, 2, np.nan, 4, np.nan, np.nan],
                "z": [1, 2, 3, 4, np.nan, np.nan],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf.fillna(-1), pdf.fillna(-1))
        self.assert_eq(
            psdf.fillna({"x": -1, "y": -2, "z": -5}), pdf.fillna({"x": -1, "y": -2, "z": -5})
        )
        self.assert_eq(pdf.fillna(method="ffill"), psdf.fillna(method="ffill"))
        self.assert_eq(pdf.fillna(method="ffill", limit=2), psdf.fillna(method="ffill", limit=2))
        self.assert_eq(pdf.fillna(method="bfill"), psdf.fillna(method="bfill"))
        self.assert_eq(pdf.fillna(method="bfill", limit=2), psdf.fillna(method="bfill", limit=2))

        pdf = pdf.set_index(["x", "y"])
        psdf = ps.from_pandas(pdf)
        # check multi index
        self.assert_eq(psdf.fillna(-1), pdf.fillna(-1))
        self.assert_eq(pdf.fillna(method="bfill"), psdf.fillna(method="bfill"))
        self.assert_eq(pdf.fillna(method="ffill"), psdf.fillna(method="ffill"))

        pser = pdf.z
        psser = psdf.z
        pdf.fillna({"x": -1, "y": -2, "z": -5}, inplace=True)
        psdf.fillna({"x": -1, "y": -2, "z": -5}, inplace=True)
        self.assert_eq(psdf, pdf)
        # Skip due to pandas bug: https://github.com/pandas-dev/pandas/issues/47188
        if not (LooseVersion("1.4.0") <= LooseVersion(pd.__version__) <= LooseVersion("1.4.2")):
            self.assert_eq(psser, pser)

        pser = pdf.z
        psser = psdf.z
        pdf.fillna(0, inplace=True)
        psdf.fillna(0, inplace=True)
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser, pser)

        s_nan = pd.Series([-1, -2, -5], index=["x", "y", "z"], dtype=int)
        self.assert_eq(psdf.fillna(s_nan), pdf.fillna(s_nan))

        with self.assertRaisesRegex(NotImplementedError, "fillna currently only"):
            psdf.fillna(-1, axis=1)
        with self.assertRaisesRegex(NotImplementedError, "fillna currently only"):
            psdf.fillna(-1, axis="columns")
        with self.assertRaisesRegex(ValueError, "limit parameter for value is not support now"):
            psdf.fillna(-1, limit=1)
        with self.assertRaisesRegex(TypeError, "Unsupported.*DataFrame"):
            psdf.fillna(pd.DataFrame({"x": [-1], "y": [-1], "z": [-1]}))
        with self.assertRaisesRegex(TypeError, "Unsupported.*int64"):
            psdf.fillna({"x": np.int64(-6), "y": np.int64(-4), "z": -5})
        with self.assertRaisesRegex(ValueError, "Expecting 'pad', 'ffill', 'backfill' or 'bfill'."):
            psdf.fillna(method="xxx")
        with self.assertRaisesRegex(
            ValueError, "Must specify a fillna 'value' or 'method' parameter."
        ):
            psdf.fillna()

        # multi-index columns
        pdf = pd.DataFrame(
            {
                ("x", "a"): [np.nan, 2, 3, 4, np.nan, 6],
                ("x", "b"): [1, 2, np.nan, 4, np.nan, np.nan],
                ("y", "c"): [1, 2, 3, 4, np.nan, np.nan],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.fillna(-1), pdf.fillna(-1))
        self.assert_eq(
            psdf.fillna({("x", "a"): -1, ("x", "b"): -2, ("y", "c"): -5}),
            pdf.fillna({("x", "a"): -1, ("x", "b"): -2, ("y", "c"): -5}),
        )
        self.assert_eq(pdf.fillna(method="ffill"), psdf.fillna(method="ffill"))
        self.assert_eq(pdf.fillna(method="ffill", limit=2), psdf.fillna(method="ffill", limit=2))
        self.assert_eq(pdf.fillna(method="bfill"), psdf.fillna(method="bfill"))
        self.assert_eq(pdf.fillna(method="bfill", limit=2), psdf.fillna(method="bfill", limit=2))

        # See also: https://github.com/pandas-dev/pandas/issues/47649
        if LooseVersion("1.4.3") != LooseVersion(pd.__version__):
            self.assert_eq(psdf.fillna({"x": -1}), pdf.fillna({"x": -1}))
            self.assert_eq(
                psdf.fillna({"x": -1, ("x", "b"): -2}), pdf.fillna({"x": -1, ("x", "b"): -2})
            )
            self.assert_eq(
                psdf.fillna({("x", "b"): -2, "x": -1}), pdf.fillna({("x", "b"): -2, "x": -1})
            )

        # check multi index
        pdf = pdf.set_index([("x", "a"), ("x", "b")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.fillna(-1), pdf.fillna(-1))
        self.assert_eq(
            psdf.fillna({("x", "a"): -1, ("x", "b"): -2, ("y", "c"): -5}),
            pdf.fillna({("x", "a"): -1, ("x", "b"): -2, ("y", "c"): -5}),
        )

    def test_isnull(self):
        pdf = pd.DataFrame(
            {"x": [1, 2, 3, 4, None, 6], "y": list("abdabd")}, index=np.random.rand(6)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.notnull(), pdf.notnull())
        self.assert_eq(psdf.isnull(), pdf.isnull())

    def test_to_datetime(self):
        pdf = pd.DataFrame(
            {"year": [2015, 2016], "month": [2, 3], "day": [4, 5]}, index=np.random.rand(2)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))

    def test_nunique(self):
        pdf = pd.DataFrame({"A": [1, 2, 3], "B": [np.nan, 3, np.nan]}, index=np.random.rand(3))
        psdf = ps.from_pandas(pdf)

        # Assert NaNs are dropped by default
        self.assert_eq(psdf.nunique(), pdf.nunique())

        # Assert including NaN values
        self.assert_eq(psdf.nunique(dropna=False), pdf.nunique(dropna=False))

        # Assert approximate counts
        self.assert_eq(
            ps.DataFrame({"A": range(100)}).nunique(approx=True),
            pd.Series([103], index=["A"]),
        )
        self.assert_eq(
            ps.DataFrame({"A": range(100)}).nunique(approx=True, rsd=0.01),
            pd.Series([100], index=["A"]),
        )

        # Assert unsupported axis value yet
        msg = 'axis should be either 0 or "index" currently.'
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.nunique(axis=1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("Y", "B")], names=["1", "2"])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.nunique(), pdf.nunique())
        self.assert_eq(psdf.nunique(dropna=False), pdf.nunique(dropna=False))

    def test_sort_values(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, None, 7], "b": [7, 6, 5, 4, 3, 2, 1]}, index=np.random.rand(7)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.sort_values("b"), pdf.sort_values("b"))
        self.assert_eq(
            psdf.sort_values("b", ignore_index=True), pdf.sort_values("b", ignore_index=True)
        )

        for ascending in [True, False]:
            for na_position in ["first", "last"]:
                self.assert_eq(
                    psdf.sort_values("a", ascending=ascending, na_position=na_position),
                    pdf.sort_values("a", ascending=ascending, na_position=na_position),
                )

        self.assert_eq(psdf.sort_values(["a", "b"]), pdf.sort_values(["a", "b"]))
        self.assert_eq(
            psdf.sort_values(["a", "b"], ignore_index=True),
            pdf.sort_values(["a", "b"], ignore_index=True),
        )
        self.assert_eq(
            psdf.sort_values(["a", "b"], ascending=[False, True]),
            pdf.sort_values(["a", "b"], ascending=[False, True]),
        )

        self.assertRaises(ValueError, lambda: psdf.sort_values(["b", "a"], ascending=[False]))

        self.assert_eq(
            psdf.sort_values(["a", "b"], na_position="first"),
            pdf.sort_values(["a", "b"], na_position="first"),
        )

        self.assertRaises(ValueError, lambda: psdf.sort_values(["b", "a"], na_position="invalid"))

        pserA = pdf.a
        psserA = psdf.a
        self.assert_eq(psdf.sort_values("b", inplace=True), pdf.sort_values("b", inplace=True))
        self.assert_eq(psdf, pdf)
        self.assert_eq(psserA, pserA)

        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, None, 7], "b": [7, 6, 5, 4, 3, 2, 1]}, index=np.random.rand(7)
        )
        psdf = ps.from_pandas(pdf)
        pserA = pdf.a
        psserA = psdf.a
        self.assert_eq(
            psdf.sort_values("b", inplace=True, ignore_index=True),
            pdf.sort_values("b", inplace=True, ignore_index=True),
        )
        self.assert_eq(psdf, pdf)
        self.assert_eq(psserA, pserA)

        # multi-index indexes

        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, None, 7], "b": [7, 6, 5, 4, 3, 2, 1]},
            index=pd.MultiIndex.from_tuples(
                [
                    ("bar", "one"),
                    ("bar", "two"),
                    ("baz", "one"),
                    ("baz", "two"),
                    ("foo", "one"),
                    ("foo", "two"),
                    ("qux", "one"),
                ]
            ),
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.sort_values("b"), pdf.sort_values("b"))
        self.assert_eq(
            psdf.sort_values("b", ignore_index=True), pdf.sort_values("b", ignore_index=True)
        )

        # multi-index columns
        pdf = pd.DataFrame(
            {("X", 10): [1, 2, 3, 4, 5, None, 7], ("X", 20): [7, 6, 5, 4, 3, 2, 1]},
            index=np.random.rand(7),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.sort_values(("X", 20)), pdf.sort_values(("X", 20)))
        self.assert_eq(
            psdf.sort_values([("X", 20), ("X", 10)]), pdf.sort_values([("X", 20), ("X", 10)])
        )

        self.assertRaisesRegex(
            ValueError,
            "For a multi-index, the label must be a tuple with elements",
            lambda: psdf.sort_values(["X"]),
        )

        # non-string names
        pdf = pd.DataFrame(
            {10: [1, 2, 3, 4, 5, None, 7], 20: [7, 6, 5, 4, 3, 2, 1]}, index=np.random.rand(7)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.sort_values(20), pdf.sort_values(20))
        self.assert_eq(psdf.sort_values([20, 10]), pdf.sort_values([20, 10]))

    def test_sort_index(self):
        pdf = pd.DataFrame(
            {"A": [2, 1, np.nan], "B": [np.nan, 0, np.nan]}, index=["b", "a", np.nan]
        )
        psdf = ps.from_pandas(pdf)

        # Assert invalid parameters
        self.assertRaises(NotImplementedError, lambda: psdf.sort_index(axis=1))
        self.assertRaises(NotImplementedError, lambda: psdf.sort_index(kind="mergesort"))
        self.assertRaises(ValueError, lambda: psdf.sort_index(na_position="invalid"))

        # Assert default behavior without parameters
        self.assert_eq(psdf.sort_index(), pdf.sort_index())
        # Assert ignoring index
        self.assert_eq(psdf.sort_index(ignore_index=True), pdf.sort_index(ignore_index=True))
        # Assert sorting descending
        self.assert_eq(psdf.sort_index(ascending=False), pdf.sort_index(ascending=False))
        # Assert sorting NA indices first
        self.assert_eq(psdf.sort_index(na_position="first"), pdf.sort_index(na_position="first"))
        # Assert sorting descending and NA indices first
        self.assert_eq(
            psdf.sort_index(ascending=False, na_position="first"),
            pdf.sort_index(ascending=False, na_position="first"),
        )

        # Assert sorting inplace
        pserA = pdf.A
        psserA = psdf.A
        self.assertEqual(psdf.sort_index(inplace=True), pdf.sort_index(inplace=True))
        self.assert_eq(psdf, pdf)
        self.assert_eq(psserA, pserA)
        pserA = pdf.A
        psserA = psdf.A
        self.assertEqual(
            psdf.sort_index(inplace=True, ascending=False, ignore_index=True),
            pdf.sort_index(inplace=True, ascending=False, ignore_index=True),
        )
        self.assert_eq(psdf, pdf)
        self.assert_eq(psserA, pserA)

        # Assert multi-indices
        pdf = pd.DataFrame(
            {"A": range(4), "B": range(4)[::-1]}, index=[["b", "b", "a", "a"], [1, 0, 1, 0]]
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.sort_index(), pdf.sort_index())
        self.assert_eq(psdf.sort_index(level=[1, 0]), pdf.sort_index(level=[1, 0]))
        self.assert_eq(psdf.reset_index().sort_index(), pdf.reset_index().sort_index())
        # Assert ignoring index
        self.assert_eq(psdf.sort_index(ignore_index=True), pdf.sort_index(ignore_index=True))

        # Assert with multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.sort_index(), pdf.sort_index())

    def test_swaplevel(self):
        # MultiIndex with two levels
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame({"x1": ["a", "b", "c", "d"], "x2": ["a", "b", "c", "d"]}, index=pidx)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.swaplevel(), psdf.swaplevel())
        self.assert_eq(pdf.swaplevel(0, 1), psdf.swaplevel(0, 1))
        self.assert_eq(pdf.swaplevel(1, 1), psdf.swaplevel(1, 1))
        self.assert_eq(pdf.swaplevel("number", "color"), psdf.swaplevel("number", "color"))

        # MultiIndex with more than two levels
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"], ["l", "m", "s", "xs"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color", "size"))
        pdf = pd.DataFrame({"x1": ["a", "b", "c", "d"], "x2": ["a", "b", "c", "d"]}, index=pidx)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.swaplevel(), psdf.swaplevel())
        self.assert_eq(pdf.swaplevel(0, 1), psdf.swaplevel(0, 1))
        self.assert_eq(pdf.swaplevel(0, 2), psdf.swaplevel(0, 2))
        self.assert_eq(pdf.swaplevel(1, 2), psdf.swaplevel(1, 2))
        self.assert_eq(pdf.swaplevel(1, 1), psdf.swaplevel(1, 1))
        self.assert_eq(pdf.swaplevel(-1, -2), psdf.swaplevel(-1, -2))
        self.assert_eq(pdf.swaplevel("number", "color"), psdf.swaplevel("number", "color"))
        self.assert_eq(pdf.swaplevel("number", "size"), psdf.swaplevel("number", "size"))
        self.assert_eq(pdf.swaplevel("color", "size"), psdf.swaplevel("color", "size"))
        self.assert_eq(
            pdf.swaplevel("color", "size", axis="index"),
            psdf.swaplevel("color", "size", axis="index"),
        )
        self.assert_eq(
            pdf.swaplevel("color", "size", axis=0), psdf.swaplevel("color", "size", axis=0)
        )

        pdf = pd.DataFrame(
            {
                "x1": ["a", "b", "c", "d"],
                "x2": ["a", "b", "c", "d"],
                "x3": ["a", "b", "c", "d"],
                "x4": ["a", "b", "c", "d"],
            }
        )
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color", "size"))
        pdf.columns = pidx
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.swaplevel(axis=1), psdf.swaplevel(axis=1))
        self.assert_eq(pdf.swaplevel(0, 1, axis=1), psdf.swaplevel(0, 1, axis=1))
        self.assert_eq(pdf.swaplevel(0, 2, axis=1), psdf.swaplevel(0, 2, axis=1))
        self.assert_eq(pdf.swaplevel(1, 2, axis=1), psdf.swaplevel(1, 2, axis=1))
        self.assert_eq(pdf.swaplevel(1, 1, axis=1), psdf.swaplevel(1, 1, axis=1))
        self.assert_eq(pdf.swaplevel(-1, -2, axis=1), psdf.swaplevel(-1, -2, axis=1))
        self.assert_eq(
            pdf.swaplevel("number", "color", axis=1), psdf.swaplevel("number", "color", axis=1)
        )
        self.assert_eq(
            pdf.swaplevel("number", "size", axis=1), psdf.swaplevel("number", "size", axis=1)
        )
        self.assert_eq(
            pdf.swaplevel("color", "size", axis=1), psdf.swaplevel("color", "size", axis=1)
        )
        self.assert_eq(
            pdf.swaplevel("color", "size", axis="columns"),
            psdf.swaplevel("color", "size", axis="columns"),
        )

        # Error conditions
        self.assertRaises(AssertionError, lambda: ps.DataFrame([1, 2]).swaplevel())
        self.assertRaises(IndexError, lambda: psdf.swaplevel(0, 9, axis=1))
        self.assertRaises(KeyError, lambda: psdf.swaplevel("not_number", "color", axis=1))
        self.assertRaises(ValueError, lambda: psdf.swaplevel(axis=2))

    def test_swapaxes(self):
        pdf = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]], index=["x", "y", "z"], columns=["a", "b", "c"]
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.swapaxes(0, 1), pdf.swapaxes(0, 1))
        self.assert_eq(psdf.swapaxes(1, 0), pdf.swapaxes(1, 0))
        self.assert_eq(psdf.swapaxes("index", "columns"), pdf.swapaxes("index", "columns"))
        self.assert_eq(psdf.swapaxes("columns", "index"), pdf.swapaxes("columns", "index"))
        self.assert_eq((psdf + 1).swapaxes(0, 1), (pdf + 1).swapaxes(0, 1))

        self.assertRaises(AssertionError, lambda: psdf.swapaxes(0, 1, copy=False))
        self.assertRaises(ValueError, lambda: psdf.swapaxes(0, -1))

    def test_nlargest(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, None, 7], "b": [7, 6, 5, 4, 3, 2, 1], "c": [1, 1, 2, 2, 3, 3, 3]},
            index=np.random.rand(7),
        )
        psdf = ps.from_pandas(pdf)
        # see also: https://github.com/pandas-dev/pandas/issues/46589
        if not (LooseVersion("1.4.0") <= LooseVersion(pd.__version__) <= LooseVersion("1.4.2")):
            self.assert_eq(psdf.nlargest(5, columns="a"), pdf.nlargest(5, columns="a"))
            self.assert_eq(
                psdf.nlargest(5, columns=["a", "b"]), pdf.nlargest(5, columns=["a", "b"])
            )
        self.assert_eq(psdf.nlargest(5, columns=["c"]), pdf.nlargest(5, columns=["c"]))
        self.assert_eq(
            psdf.nlargest(5, columns=["c"], keep="first"),
            pdf.nlargest(5, columns=["c"], keep="first"),
        )
        self.assert_eq(
            psdf.nlargest(5, columns=["c"], keep="last"),
            pdf.nlargest(5, columns=["c"], keep="last"),
        )
        msg = "`keep`=all is not implemented yet."
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.nlargest(5, columns=["c"], keep="all")
        msg = 'keep must be either "first", "last" or "all".'
        with self.assertRaisesRegex(ValueError, msg):
            psdf.nlargest(5, columns=["c"], keep="xx")

    def test_nsmallest(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, None, 7], "b": [7, 6, 5, 4, 3, 2, 1], "c": [1, 1, 2, 2, 3, 3, 3]},
            index=np.random.rand(7),
        )
        psdf = ps.from_pandas(pdf)
        # see also: https://github.com/pandas-dev/pandas/issues/46589
        if not (LooseVersion("1.4.0") <= LooseVersion(pd.__version__) <= LooseVersion("1.4.2")):
            self.assert_eq(psdf.nsmallest(n=5, columns="a"), pdf.nsmallest(5, columns="a"))
            self.assert_eq(
                psdf.nsmallest(n=5, columns=["a", "b"]), pdf.nsmallest(5, columns=["a", "b"])
            )
        self.assert_eq(psdf.nsmallest(n=5, columns=["c"]), pdf.nsmallest(5, columns=["c"]))
        self.assert_eq(
            psdf.nsmallest(n=5, columns=["c"], keep="first"),
            pdf.nsmallest(5, columns=["c"], keep="first"),
        )
        self.assert_eq(
            psdf.nsmallest(n=5, columns=["c"], keep="last"),
            pdf.nsmallest(5, columns=["c"], keep="last"),
        )
        msg = "`keep`=all is not implemented yet."
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.nlargest(5, columns=["c"], keep="all")
        msg = 'keep must be either "first", "last" or "all".'
        with self.assertRaisesRegex(ValueError, msg):
            psdf.nlargest(5, columns=["c"], keep="xx")

    def test_xs(self):
        d = {
            "num_legs": [4, 4, 2, 2],
            "num_wings": [0, 0, 2, 2],
            "class": ["mammal", "mammal", "mammal", "bird"],
            "animal": ["cat", "dog", "bat", "penguin"],
            "locomotion": ["walks", "walks", "flies", "walks"],
        }
        pdf = pd.DataFrame(data=d)
        pdf = pdf.set_index(["class", "animal", "locomotion"])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.xs("mammal"), pdf.xs("mammal"))
        self.assert_eq(psdf.xs(("mammal",)), pdf.xs(("mammal",)))
        self.assert_eq(psdf.xs(("mammal", "dog", "walks")), pdf.xs(("mammal", "dog", "walks")))
        self.assert_eq(
            ps.concat([psdf, psdf]).xs(("mammal", "dog", "walks")),
            pd.concat([pdf, pdf]).xs(("mammal", "dog", "walks")),
        )
        self.assert_eq(psdf.xs("cat", level=1), pdf.xs("cat", level=1))
        self.assert_eq(psdf.xs("flies", level=2), pdf.xs("flies", level=2))
        self.assert_eq(psdf.xs("mammal", level=-3), pdf.xs("mammal", level=-3))

        msg = 'axis should be either 0 or "index" currently.'
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.xs("num_wings", axis=1)
        with self.assertRaises(KeyError):
            psdf.xs(("mammal", "dog", "walk"))
        msg = r"'Key length \(4\) exceeds index depth \(3\)'"
        with self.assertRaisesRegex(KeyError, msg):
            psdf.xs(("mammal", "dog", "walks", "foo"))
        msg = "'key' should be a scalar value or tuple that contains scalar values"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.xs(["mammal", "dog", "walks", "foo"])

        self.assertRaises(IndexError, lambda: psdf.xs("foo", level=-4))
        self.assertRaises(IndexError, lambda: psdf.xs("foo", level=3))

        self.assertRaises(KeyError, lambda: psdf.xs(("dog", "walks"), level=1))

        # non-string names
        pdf = pd.DataFrame(data=d)
        pdf = pdf.set_index(["class", "animal", "num_legs", "num_wings"])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.xs(("mammal", "dog", 4)), pdf.xs(("mammal", "dog", 4)))
        self.assert_eq(psdf.xs(2, level=2), pdf.xs(2, level=2))

        self.assert_eq((psdf + "a").xs(("mammal", "dog", 4)), (pdf + "a").xs(("mammal", "dog", 4)))
        self.assert_eq((psdf + "a").xs(2, level=2), (pdf + "a").xs(2, level=2))

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

    def test_to_numpy(self):
        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, 2, 9, 4, 2, 4],
                "c": ["one", "three", "six", "seven", "one", "5"],
            },
            index=np.random.rand(6),
        )

        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.to_numpy(), pdf.values)

    def test_to_pandas(self):
        pdf, psdf = self.df_pair
        self.assert_eq(psdf._to_pandas(), pdf)

    def test_isin(self):
        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, 2, 9, 4, 2, 4],
                "c": ["one", "three", "six", "seven", "one", "5"],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.isin([4, "six"]), pdf.isin([4, "six"]))
        # Seems like pandas has a bug when passing `np.array` as parameter
        self.assert_eq(psdf.isin(np.array([4, "six"])), pdf.isin([4, "six"]))
        self.assert_eq(
            psdf.isin({"a": [2, 8], "c": ["three", "one"]}),
            pdf.isin({"a": [2, 8], "c": ["three", "one"]}),
        )
        self.assert_eq(
            psdf.isin({"a": np.array([2, 8]), "c": ["three", "one"]}),
            pdf.isin({"a": np.array([2, 8]), "c": ["three", "one"]}),
        )

        msg = "'DataFrame' object has no attribute {'e'}"
        with self.assertRaisesRegex(AttributeError, msg):
            psdf.isin({"e": [5, 7], "a": [1, 6]})

        msg = "DataFrame and Series are not supported"
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.isin(pdf)

        msg = "Values should be iterable, Series, DataFrame or dict."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.isin(1)

        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, None, 9, 4, None, 4],
                "c": [None, 5, None, 3, 2, 1],
            },
        )
        psdf = ps.from_pandas(pdf)

        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            self.assert_eq(psdf.isin([4, 3, 1, 1, None]), pdf.isin([4, 3, 1, 1, None]))
        else:
            expected = pd.DataFrame(
                {
                    "a": [True, False, True, True, False, False],
                    "b": [True, False, False, True, False, True],
                    "c": [False, False, False, True, False, True],
                }
            )
            self.assert_eq(psdf.isin([4, 3, 1, 1, None]), expected)

        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            self.assert_eq(
                psdf.isin({"b": [4, 3, 1, 1, None]}), pdf.isin({"b": [4, 3, 1, 1, None]})
            )
        else:
            expected = pd.DataFrame(
                {
                    "a": [False, False, False, False, False, False],
                    "b": [True, False, False, True, False, True],
                    "c": [False, False, False, False, False, False],
                }
            )
            self.assert_eq(psdf.isin({"b": [4, 3, 1, 1, None]}), expected)

    def test_merge(self):
        left_pdf = pd.DataFrame(
            {
                "lkey": ["foo", "bar", "baz", "foo", "bar", "l"],
                "value": [1, 2, 3, 5, 6, 7],
                "x": list("abcdef"),
            },
            columns=["lkey", "value", "x"],
        )
        right_pdf = pd.DataFrame(
            {
                "rkey": ["baz", "foo", "bar", "baz", "foo", "r"],
                "value": [4, 5, 6, 7, 8, 9],
                "y": list("efghij"),
            },
            columns=["rkey", "value", "y"],
        )
        right_ps = pd.Series(list("defghi"), name="x", index=[5, 6, 7, 8, 9, 10])

        left_psdf = ps.from_pandas(left_pdf)
        right_psdf = ps.from_pandas(right_pdf)
        right_psser = ps.from_pandas(right_ps)

        def check(op, right_psdf=right_psdf, right_pdf=right_pdf):
            k_res = op(left_psdf, right_psdf)
            k_res = k_res._to_pandas()
            k_res = k_res.sort_values(by=list(k_res.columns))
            k_res = k_res.reset_index(drop=True)
            p_res = op(left_pdf, right_pdf)
            p_res = p_res.sort_values(by=list(p_res.columns))
            p_res = p_res.reset_index(drop=True)
            self.assert_eq(k_res, p_res)

        check(lambda left, right: left.merge(right))
        check(lambda left, right: left.merge(right, on="value"))
        check(lambda left, right: left.merge(right, on=("value",)))
        check(lambda left, right: left.merge(right, left_on="lkey", right_on="rkey"))
        check(lambda left, right: left.set_index("lkey").merge(right.set_index("rkey")))
        check(
            lambda left, right: left.set_index("lkey").merge(
                right, left_index=True, right_on="rkey"
            )
        )
        check(
            lambda left, right: left.merge(
                right.set_index("rkey"), left_on="lkey", right_index=True
            )
        )
        check(
            lambda left, right: left.set_index("lkey").merge(
                right.set_index("rkey"), left_index=True, right_index=True
            )
        )

        # MultiIndex
        check(
            lambda left, right: left.merge(
                right, left_on=["lkey", "value"], right_on=["rkey", "value"]
            )
        )
        check(
            lambda left, right: left.set_index(["lkey", "value"]).merge(
                right, left_index=True, right_on=["rkey", "value"]
            )
        )
        check(
            lambda left, right: left.merge(
                right.set_index(["rkey", "value"]), left_on=["lkey", "value"], right_index=True
            )
        )
        # TODO: when both left_index=True and right_index=True with multi-index
        # check(lambda left, right: left.set_index(['lkey', 'value']).merge(
        #     right.set_index(['rkey', 'value']), left_index=True, right_index=True))

        # join types
        for how in ["inner", "left", "right", "outer"]:
            check(lambda left, right: left.merge(right, on="value", how=how))
            check(lambda left, right: left.merge(right, left_on="lkey", right_on="rkey", how=how))

        # suffix
        check(
            lambda left, right: left.merge(
                right, left_on="lkey", right_on="rkey", suffixes=["_left", "_right"]
            )
        )

        # Test Series on the right
        check(lambda left, right: left.merge(right), right_psser, right_ps)
        check(
            lambda left, right: left.merge(right, left_on="x", right_on="x"), right_psser, right_ps
        )
        check(
            lambda left, right: left.set_index("x").merge(right, left_index=True, right_on="x"),
            right_psser,
            right_ps,
        )

        # Test join types with Series
        for how in ["inner", "left", "right", "outer"]:
            check(lambda left, right: left.merge(right, how=how), right_psser, right_ps)
            check(
                lambda left, right: left.merge(right, left_on="x", right_on="x", how=how),
                right_psser,
                right_ps,
            )

        # suffix with Series
        check(
            lambda left, right: left.merge(
                right,
                suffixes=["_left", "_right"],
                how="outer",
                left_index=True,
                right_index=True,
            ),
            right_psser,
            right_ps,
        )

        # multi-index columns
        left_columns = pd.MultiIndex.from_tuples([(10, "lkey"), (10, "value"), (20, "x")])
        left_pdf.columns = left_columns
        left_psdf.columns = left_columns

        right_columns = pd.MultiIndex.from_tuples([(10, "rkey"), (10, "value"), (30, "y")])
        right_pdf.columns = right_columns
        right_psdf.columns = right_columns

        check(lambda left, right: left.merge(right))
        check(lambda left, right: left.merge(right, on=[(10, "value")]))
        check(
            lambda left, right: (left.set_index((10, "lkey")).merge(right.set_index((10, "rkey"))))
        )
        check(
            lambda left, right: (
                left.set_index((10, "lkey")).merge(
                    right.set_index((10, "rkey")), left_index=True, right_index=True
                )
            )
        )
        # TODO: when both left_index=True and right_index=True with multi-index columns
        # check(lambda left, right: left.merge(right,
        #                                      left_on=[('a', 'lkey')], right_on=[('a', 'rkey')]))
        # check(lambda left, right: (left.set_index(('a', 'lkey'))
        #                            .merge(right, left_index=True, right_on=[('a', 'rkey')])))

        # non-string names
        left_pdf.columns = [10, 100, 1000]
        left_psdf.columns = [10, 100, 1000]

        right_pdf.columns = [20, 100, 2000]
        right_psdf.columns = [20, 100, 2000]

        check(lambda left, right: left.merge(right))
        check(lambda left, right: left.merge(right, on=[100]))
        check(lambda left, right: (left.set_index(10).merge(right.set_index(20))))
        check(
            lambda left, right: (
                left.set_index(10).merge(right.set_index(20), left_index=True, right_index=True)
            )
        )

    def test_merge_same_anchor(self):
        pdf = pd.DataFrame(
            {
                "lkey": ["foo", "bar", "baz", "foo", "bar", "l"],
                "rkey": ["baz", "foo", "bar", "baz", "foo", "r"],
                "value": [1, 1, 3, 5, 6, 7],
                "x": list("abcdef"),
                "y": list("efghij"),
            },
            columns=["lkey", "rkey", "value", "x", "y"],
        )
        psdf = ps.from_pandas(pdf)

        left_pdf = pdf[["lkey", "value", "x"]]
        right_pdf = pdf[["rkey", "value", "y"]]
        left_psdf = psdf[["lkey", "value", "x"]]
        right_psdf = psdf[["rkey", "value", "y"]]

        def check(op, right_psdf=right_psdf, right_pdf=right_pdf):
            k_res = op(left_psdf, right_psdf)
            k_res = k_res._to_pandas()
            k_res = k_res.sort_values(by=list(k_res.columns))
            k_res = k_res.reset_index(drop=True)
            p_res = op(left_pdf, right_pdf)
            p_res = p_res.sort_values(by=list(p_res.columns))
            p_res = p_res.reset_index(drop=True)
            self.assert_eq(k_res, p_res)

        check(lambda left, right: left.merge(right))
        check(lambda left, right: left.merge(right, on="value"))
        check(lambda left, right: left.merge(right, left_on="lkey", right_on="rkey"))
        check(lambda left, right: left.set_index("lkey").merge(right.set_index("rkey")))
        check(
            lambda left, right: left.set_index("lkey").merge(
                right, left_index=True, right_on="rkey"
            )
        )
        check(
            lambda left, right: left.merge(
                right.set_index("rkey"), left_on="lkey", right_index=True
            )
        )
        check(
            lambda left, right: left.set_index("lkey").merge(
                right.set_index("rkey"), left_index=True, right_index=True
            )
        )

    def test_merge_retains_indices(self):
        left_pdf = pd.DataFrame({"A": [0, 1]})
        right_pdf = pd.DataFrame({"B": [1, 2]}, index=[1, 2])
        left_psdf = ps.from_pandas(left_pdf)
        right_psdf = ps.from_pandas(right_pdf)

        self.assert_eq(
            left_psdf.merge(right_psdf, left_index=True, right_index=True),
            left_pdf.merge(right_pdf, left_index=True, right_index=True),
        )
        self.assert_eq(
            left_psdf.merge(right_psdf, left_on="A", right_index=True),
            left_pdf.merge(right_pdf, left_on="A", right_index=True),
        )
        self.assert_eq(
            left_psdf.merge(right_psdf, left_index=True, right_on="B"),
            left_pdf.merge(right_pdf, left_index=True, right_on="B"),
        )
        self.assert_eq(
            left_psdf.merge(right_psdf, left_on="A", right_on="B"),
            left_pdf.merge(right_pdf, left_on="A", right_on="B"),
        )

    def test_merge_how_parameter(self):
        left_pdf = pd.DataFrame({"A": [1, 2]})
        right_pdf = pd.DataFrame({"B": ["x", "y"]}, index=[1, 2])
        left_psdf = ps.from_pandas(left_pdf)
        right_psdf = ps.from_pandas(right_pdf)

        psdf = left_psdf.merge(right_psdf, left_index=True, right_index=True)
        pdf = left_pdf.merge(right_pdf, left_index=True, right_index=True)
        self.assert_eq(
            psdf.sort_values(by=list(psdf.columns)).reset_index(drop=True),
            pdf.sort_values(by=list(pdf.columns)).reset_index(drop=True),
        )

        psdf = left_psdf.merge(right_psdf, left_index=True, right_index=True, how="left")
        pdf = left_pdf.merge(right_pdf, left_index=True, right_index=True, how="left")
        self.assert_eq(
            psdf.sort_values(by=list(psdf.columns)).reset_index(drop=True),
            pdf.sort_values(by=list(pdf.columns)).reset_index(drop=True),
        )

        psdf = left_psdf.merge(right_psdf, left_index=True, right_index=True, how="right")
        pdf = left_pdf.merge(right_pdf, left_index=True, right_index=True, how="right")
        self.assert_eq(
            psdf.sort_values(by=list(psdf.columns)).reset_index(drop=True),
            pdf.sort_values(by=list(pdf.columns)).reset_index(drop=True),
        )

        psdf = left_psdf.merge(right_psdf, left_index=True, right_index=True, how="outer")
        pdf = left_pdf.merge(right_pdf, left_index=True, right_index=True, how="outer")
        self.assert_eq(
            psdf.sort_values(by=list(psdf.columns)).reset_index(drop=True),
            pdf.sort_values(by=list(pdf.columns)).reset_index(drop=True),
        )

    def test_merge_raises(self):
        left = ps.DataFrame(
            {"value": [1, 2, 3, 5, 6], "x": list("abcde")},
            columns=["value", "x"],
            index=["foo", "bar", "baz", "foo", "bar"],
        )
        right = ps.DataFrame(
            {"value": [4, 5, 6, 7, 8], "y": list("fghij")},
            columns=["value", "y"],
            index=["baz", "foo", "bar", "baz", "foo"],
        )

        with self.assertRaisesRegex(ValueError, "No common columns to perform merge on"):
            left[["x"]].merge(right[["y"]])

        with self.assertRaisesRegex(ValueError, "not a combination of both"):
            left.merge(right, on="value", left_on="x")

        with self.assertRaisesRegex(ValueError, "Must pass right_on or right_index=True"):
            left.merge(right, left_on="x")

        with self.assertRaisesRegex(ValueError, "Must pass right_on or right_index=True"):
            left.merge(right, left_index=True)

        with self.assertRaisesRegex(ValueError, "Must pass left_on or left_index=True"):
            left.merge(right, right_on="y")

        with self.assertRaisesRegex(ValueError, "Must pass left_on or left_index=True"):
            left.merge(right, right_index=True)

        with self.assertRaisesRegex(
            ValueError, "len\\(left_keys\\) must equal len\\(right_keys\\)"
        ):
            left.merge(right, left_on="value", right_on=["value", "y"])

        with self.assertRaisesRegex(
            ValueError, "len\\(left_keys\\) must equal len\\(right_keys\\)"
        ):
            left.merge(right, left_on=["value", "x"], right_on="value")

        with self.assertRaisesRegex(ValueError, "['inner', 'left', 'right', 'full', 'outer']"):
            left.merge(right, left_index=True, right_index=True, how="foo")

        with self.assertRaisesRegex(KeyError, "id"):
            left.merge(right, on="id")

    def test_append(self):
        pdf = pd.DataFrame([[1, 2], [3, 4]], columns=list("AB"))
        psdf = ps.from_pandas(pdf)
        other_pdf = pd.DataFrame([[3, 4], [5, 6]], columns=list("BC"), index=[2, 3])
        other_psdf = ps.from_pandas(other_pdf)

        self.assert_eq(psdf.append(psdf), pdf.append(pdf))
        self.assert_eq(psdf.append(psdf, ignore_index=True), pdf.append(pdf, ignore_index=True))

        # Assert DataFrames with non-matching columns
        self.assert_eq(psdf.append(other_psdf), pdf.append(other_pdf))

        # Assert appending a Series fails
        msg = "DataFrames.append() does not support appending Series to DataFrames"
        with self.assertRaises(TypeError, msg=msg):
            psdf.append(psdf["A"])

        # Assert using the sort parameter raises an exception
        msg = "The 'sort' parameter is currently not supported"
        with self.assertRaises(NotImplementedError, msg=msg):
            psdf.append(psdf, sort=True)

        # Assert using 'verify_integrity' only raises an exception for overlapping indices
        self.assert_eq(
            psdf.append(other_psdf, verify_integrity=True),
            pdf.append(other_pdf, verify_integrity=True),
        )
        msg = "Indices have overlapping values"
        with self.assertRaises(ValueError, msg=msg):
            psdf.append(psdf, verify_integrity=True)

        # Skip integrity verification when ignore_index=True
        self.assert_eq(
            psdf.append(psdf, ignore_index=True, verify_integrity=True),
            pdf.append(pdf, ignore_index=True, verify_integrity=True),
        )

        # Assert appending multi-index DataFrames
        multi_index_pdf = pd.DataFrame([[1, 2], [3, 4]], columns=list("AB"), index=[[2, 3], [4, 5]])
        multi_index_psdf = ps.from_pandas(multi_index_pdf)
        other_multi_index_pdf = pd.DataFrame(
            [[5, 6], [7, 8]], columns=list("AB"), index=[[2, 3], [6, 7]]
        )
        other_multi_index_psdf = ps.from_pandas(other_multi_index_pdf)

        self.assert_eq(
            multi_index_psdf.append(multi_index_psdf), multi_index_pdf.append(multi_index_pdf)
        )

        # Assert DataFrames with non-matching columns
        self.assert_eq(
            multi_index_psdf.append(other_multi_index_psdf),
            multi_index_pdf.append(other_multi_index_pdf),
        )

        # Assert using 'verify_integrity' only raises an exception for overlapping indices
        self.assert_eq(
            multi_index_psdf.append(other_multi_index_psdf, verify_integrity=True),
            multi_index_pdf.append(other_multi_index_pdf, verify_integrity=True),
        )
        with self.assertRaises(ValueError, msg=msg):
            multi_index_psdf.append(multi_index_psdf, verify_integrity=True)

        # Skip integrity verification when ignore_index=True
        self.assert_eq(
            multi_index_psdf.append(multi_index_psdf, ignore_index=True, verify_integrity=True),
            multi_index_pdf.append(multi_index_pdf, ignore_index=True, verify_integrity=True),
        )

        # Assert trying to append DataFrames with different index levels
        msg = "Both DataFrames have to have the same number of index levels"
        with self.assertRaises(ValueError, msg=msg):
            psdf.append(multi_index_psdf)

        # Skip index level check when ignore_index=True
        self.assert_eq(
            psdf.append(multi_index_psdf, ignore_index=True),
            pdf.append(multi_index_pdf, ignore_index=True),
        )

        columns = pd.MultiIndex.from_tuples([("A", "X"), ("A", "Y")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.append(psdf), pdf.append(pdf))

    def test_clip(self):
        pdf = pd.DataFrame(
            {"A": [0, 2, 4], "B": [4, 2, 0], "X": [-1, 10, 0]}, index=np.random.rand(3)
        )
        psdf = ps.from_pandas(pdf)

        # Assert list-like values are not accepted for 'lower' and 'upper'
        msg = "List-like value are not supported for 'lower' and 'upper' at the moment"
        with self.assertRaises(TypeError, msg=msg):
            psdf.clip(lower=[1])
        with self.assertRaises(TypeError, msg=msg):
            psdf.clip(upper=[1])

        # Assert no lower or upper
        self.assert_eq(psdf.clip(), pdf.clip())
        # Assert lower only
        self.assert_eq(psdf.clip(1), pdf.clip(1))
        # Assert upper only
        self.assert_eq(psdf.clip(upper=3), pdf.clip(upper=3))
        # Assert lower and upper
        self.assert_eq(psdf.clip(1, 3), pdf.clip(1, 3))

        pdf["clip"] = pdf.A.clip(lower=1, upper=3)
        psdf["clip"] = psdf.A.clip(lower=1, upper=3)
        self.assert_eq(psdf, pdf)

        # Assert behavior on string values
        str_psdf = ps.DataFrame({"A": ["a", "b", "c"]}, index=np.random.rand(3))
        self.assert_eq(str_psdf.clip(1, 3), str_psdf)

    def test_binary_operators(self):
        pdf = pd.DataFrame(
            {"A": [0, 2, 4], "B": [4, 2, 0], "X": [-1, 10, 0]}, index=np.random.rand(3)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf + psdf.copy(), pdf + pdf.copy())
        self.assert_eq(psdf + psdf.loc[:, ["A", "B"]], pdf + pdf.loc[:, ["A", "B"]])
        self.assert_eq(psdf.loc[:, ["A", "B"]] + psdf, pdf.loc[:, ["A", "B"]] + pdf)

        self.assertRaisesRegex(
            ValueError,
            "it comes from a different dataframe",
            lambda: ps.range(10).add(ps.range(10)),
        )

        self.assertRaisesRegex(
            TypeError,
            "add with a sequence is currently not supported",
            lambda: ps.range(10).add(ps.range(10).id),
        )

        psdf_other = psdf.copy()
        psdf_other.columns = pd.MultiIndex.from_tuples([("A", "Z"), ("B", "X"), ("C", "C")])
        self.assertRaisesRegex(
            ValueError,
            "cannot join with no overlapping index names",
            lambda: psdf.add(psdf_other),
        )

    def test_binary_operator_add(self):
        # Positive
        pdf = pd.DataFrame({"a": ["x"], "b": ["y"], "c": [1], "d": [2]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["a"] + psdf["b"], pdf["a"] + pdf["b"])
        self.assert_eq(psdf["c"] + psdf["d"], pdf["c"] + pdf["d"])

        # Negative
        ks_err_msg = "Addition can not be applied to given types"

        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] + psdf["c"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["c"] + psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["c"] + "literal")
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" + psdf["c"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 1 + psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] + 1)

    def test_binary_operator_sub(self):
        # Positive
        pdf = pd.DataFrame({"a": [2], "b": [1]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["a"] - psdf["b"], pdf["a"] - pdf["b"])

        # Negative
        psdf = ps.DataFrame({"a": ["x"], "b": [1]})
        ks_err_msg = "Subtraction can not be applied to given types"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] - psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] - "literal")
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" - psdf["b"])

        ks_err_msg = "Subtraction can not be applied to strings"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] - psdf["b"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 1 - psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] - 1)

        psdf = ps.DataFrame({"a": ["x"], "b": ["y"]})
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] - psdf["b"])

    def test_binary_operator_truediv(self):
        # Positive
        pdf = pd.DataFrame({"a": [3], "b": [2]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["a"] / psdf["b"], pdf["a"] / pdf["b"])

        # Negative
        psdf = ps.DataFrame({"a": ["x"], "b": [1]})

        ks_err_msg = "True division can not be applied to given types"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] / psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] / "literal")
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" / psdf["b"])

        ks_err_msg = "True division can not be applied to strings"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] / psdf["b"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 1 / psdf["a"])

    def test_binary_operator_floordiv(self):
        psdf = ps.DataFrame({"a": ["x"], "b": [1]})

        ks_err_msg = "Floor division can not be applied to strings"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] // psdf["b"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 1 // psdf["a"])

        ks_err_msg = "Floor division can not be applied to given types"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] // psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] // "literal")
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" // psdf["b"])

    def test_binary_operator_mod(self):
        # Positive
        pdf = pd.DataFrame({"a": [3], "b": [2]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["a"] % psdf["b"], pdf["a"] % pdf["b"])

        # Negative
        psdf = ps.DataFrame({"a": ["x"], "b": [1]})
        ks_err_msg = "Modulo can not be applied to given types"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] % psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] % "literal")

        ks_err_msg = "Modulo can not be applied to strings"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] % psdf["b"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 1 % psdf["a"])

    def test_binary_operator_multiply(self):
        # Positive
        pdf = pd.DataFrame({"a": ["x", "y"], "b": [1, 2], "c": [3, 4]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["b"] * psdf["c"], pdf["b"] * pdf["c"])
        self.assert_eq(psdf["c"] * psdf["b"], pdf["c"] * pdf["b"])
        self.assert_eq(psdf["a"] * psdf["b"], pdf["a"] * pdf["b"])
        self.assert_eq(psdf["b"] * psdf["a"], pdf["b"] * pdf["a"])
        self.assert_eq(psdf["a"] * 2, pdf["a"] * 2)
        self.assert_eq(psdf["b"] * 2, pdf["b"] * 2)
        self.assert_eq(2 * psdf["a"], 2 * pdf["a"])
        self.assert_eq(2 * psdf["b"], 2 * pdf["b"])

        # Negative
        psdf = ps.DataFrame({"a": ["x"], "b": [2]})
        ks_err_msg = "Multiplication can not be applied to given types"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] * "literal")
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" * psdf["b"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] * "literal")

        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] * psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] * 0.1)
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 0.1 * psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" * psdf["a"])

    def test_sample(self):
        psdf = ps.DataFrame({"A": [0, 2, 4]}, index=["x", "y", "z"])

        # Make sure the tests run, but we can't check the result because they are non-deterministic.
        psdf.sample(frac=0.1)
        psdf.sample(frac=0.2, replace=True)
        psdf.sample(frac=0.2, random_state=5)
        psdf["A"].sample(frac=0.2)
        psdf["A"].sample(frac=0.2, replace=True)
        psdf["A"].sample(frac=0.2, random_state=5)

        self.assert_eq(psdf.sample(frac=0.1, ignore_index=True).index.dtype, np.int64)
        self.assert_eq(psdf.sample(frac=0.2, replace=True, ignore_index=True).index.dtype, np.int64)
        self.assert_eq(
            psdf.sample(frac=0.2, random_state=5, ignore_index=True).index.dtype, np.int64
        )
        self.assert_eq(psdf["A"].sample(frac=0.2, ignore_index=True).index.dtype, np.int64)
        self.assert_eq(
            psdf["A"].sample(frac=0.2, replace=True, ignore_index=True).index.dtype, np.int64
        )
        self.assert_eq(
            psdf["A"].sample(frac=0.2, random_state=5, ignore_index=True).index.dtype, np.int64
        )

        with self.assertRaises(ValueError):
            psdf.sample()
        with self.assertRaises(NotImplementedError):
            psdf.sample(n=1)

    def test_add_prefix(self):
        pdf = pd.DataFrame({"A": [1, 2, 3, 4], "B": [3, 4, 5, 6]}, index=np.random.rand(4))
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.add_prefix("col_"), psdf.add_prefix("col_"))

        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.add_prefix("col_"), psdf.add_prefix("col_"))

    def test_add_suffix(self):
        pdf = pd.DataFrame({"A": [1, 2, 3, 4], "B": [3, 4, 5, 6]}, index=np.random.rand(4))
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.add_suffix("first_series"), psdf.add_suffix("first_series"))

        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.add_suffix("first_series"), psdf.add_suffix("first_series"))

    def test_join(self):
        # check basic function
        pdf1 = pd.DataFrame(
            {"key": ["K0", "K1", "K2", "K3"], "A": ["A0", "A1", "A2", "A3"]}, columns=["key", "A"]
        )
        pdf2 = pd.DataFrame(
            {"key": ["K0", "K1", "K2"], "B": ["B0", "B1", "B2"]}, columns=["key", "B"]
        )
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        join_pdf = pdf1.join(pdf2, lsuffix="_left", rsuffix="_right")
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.join(psdf2, lsuffix="_left", rsuffix="_right")
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)

        self.assert_eq(join_pdf, join_psdf)

        # join with duplicated columns in Series
        with self.assertRaisesRegex(ValueError, "columns overlap but no suffix specified"):
            ks1 = ps.Series(["A1", "A5"], index=[1, 2], name="A")
            psdf1.join(ks1, how="outer")
        # join with duplicated columns in DataFrame
        with self.assertRaisesRegex(ValueError, "columns overlap but no suffix specified"):
            psdf1.join(psdf2, how="outer")

        # check `on` parameter
        join_pdf = pdf1.join(pdf2.set_index("key"), on="key", lsuffix="_left", rsuffix="_right")
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.join(psdf2.set_index("key"), on="key", lsuffix="_left", rsuffix="_right")
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)
        self.assert_eq(join_pdf.reset_index(drop=True), join_psdf.reset_index(drop=True))

        join_pdf = pdf1.set_index("key").join(
            pdf2.set_index("key"), on="key", lsuffix="_left", rsuffix="_right"
        )
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.set_index("key").join(
            psdf2.set_index("key"), on="key", lsuffix="_left", rsuffix="_right"
        )
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)
        self.assert_eq(join_pdf.reset_index(drop=True), join_psdf.reset_index(drop=True))

        # multi-index columns
        columns1 = pd.MultiIndex.from_tuples([("x", "key"), ("Y", "A")])
        columns2 = pd.MultiIndex.from_tuples([("x", "key"), ("Y", "B")])
        pdf1.columns = columns1
        pdf2.columns = columns2
        psdf1.columns = columns1
        psdf2.columns = columns2

        join_pdf = pdf1.join(pdf2, lsuffix="_left", rsuffix="_right")
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.join(psdf2, lsuffix="_left", rsuffix="_right")
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)

        self.assert_eq(join_pdf, join_psdf)

        # check `on` parameter
        join_pdf = pdf1.join(
            pdf2.set_index(("x", "key")), on=[("x", "key")], lsuffix="_left", rsuffix="_right"
        )
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.join(
            psdf2.set_index(("x", "key")), on=[("x", "key")], lsuffix="_left", rsuffix="_right"
        )
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)

        self.assert_eq(join_pdf.reset_index(drop=True), join_psdf.reset_index(drop=True))

        join_pdf = pdf1.set_index(("x", "key")).join(
            pdf2.set_index(("x", "key")), on=[("x", "key")], lsuffix="_left", rsuffix="_right"
        )
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.set_index(("x", "key")).join(
            psdf2.set_index(("x", "key")), on=[("x", "key")], lsuffix="_left", rsuffix="_right"
        )
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)

        self.assert_eq(join_pdf.reset_index(drop=True), join_psdf.reset_index(drop=True))

        # multi-index
        midx1 = pd.MultiIndex.from_tuples(
            [("w", "a"), ("x", "b"), ("y", "c"), ("z", "d")], names=["index1", "index2"]
        )
        midx2 = pd.MultiIndex.from_tuples(
            [("w", "a"), ("x", "b"), ("y", "c")], names=["index1", "index2"]
        )
        pdf1.index = midx1
        pdf2.index = midx2
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        join_pdf = pdf1.join(pdf2, on=["index1", "index2"], rsuffix="_right")
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.join(psdf2, on=["index1", "index2"], rsuffix="_right")
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)

        self.assert_eq(join_pdf, join_psdf)

        with self.assertRaisesRegex(
            ValueError, r'len\(left_on\) must equal the number of levels in the index of "right"'
        ):
            psdf1.join(psdf2, on=["index1"], rsuffix="_right")

    def test_replace(self):
        pdf = pd.DataFrame(
            {
                "name": ["Ironman", "Captain America", "Thor", "Hulk"],
                "weapon": ["Mark-45", "Shield", "Mjolnir", "Smash"],
            },
            index=np.random.rand(4),
        )
        psdf = ps.from_pandas(pdf)

        with self.assertRaisesRegex(
            NotImplementedError, "replace currently works only for method='pad"
        ):
            psdf.replace(method="bfill")
        with self.assertRaisesRegex(
            NotImplementedError, "replace currently works only when limit=None"
        ):
            psdf.replace(limit=10)
        with self.assertRaisesRegex(
            NotImplementedError, "replace currently doesn't supports regex"
        ):
            psdf.replace(regex="")

        with self.assertRaisesRegex(ValueError, "Length of to_replace and value must be same"):
            psdf.replace(to_replace=["Ironman"], value=["Spiderman", "Doctor Strange"])
        with self.assertRaisesRegex(TypeError, "Unsupported type function"):
            psdf.replace("Ironman", lambda x: "Spiderman")
        with self.assertRaisesRegex(TypeError, "Unsupported type function"):
            psdf.replace(lambda x: "Ironman", "Spiderman")

        self.assert_eq(psdf.replace("Ironman", "Spiderman"), pdf.replace("Ironman", "Spiderman"))
        self.assert_eq(
            psdf.replace(["Ironman", "Captain America"], ["Rescue", "Hawkeye"]),
            pdf.replace(["Ironman", "Captain America"], ["Rescue", "Hawkeye"]),
        )
        self.assert_eq(
            psdf.replace(("Ironman", "Captain America"), ("Rescue", "Hawkeye")),
            pdf.replace(("Ironman", "Captain America"), ("Rescue", "Hawkeye")),
        )

        # inplace
        pser = pdf.name
        psser = psdf.name
        pdf.replace("Ironman", "Spiderman", inplace=True)
        psdf.replace("Ironman", "Spiderman", inplace=True)
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser, pser)

        pdf = pd.DataFrame(
            {"A": [0, 1, 2, 3, np.nan], "B": [5, 6, 7, 8, np.nan], "C": ["a", "b", "c", "d", None]},
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.replace([0, 1, 2, 3, 5, 6], 4), pdf.replace([0, 1, 2, 3, 5, 6], 4))

        self.assert_eq(
            psdf.replace([0, 1, 2, 3, 5, 6], [6, 5, 4, 3, 2, 1]),
            pdf.replace([0, 1, 2, 3, 5, 6], [6, 5, 4, 3, 2, 1]),
        )

        self.assert_eq(psdf.replace({0: 10, 1: 100, 7: 200}), pdf.replace({0: 10, 1: 100, 7: 200}))

        self.assert_eq(
            psdf.replace({"A": [0, np.nan], "B": [5, np.nan]}, 100),
            pdf.replace({"A": [0, np.nan], "B": [5, np.nan]}, 100),
        )

        self.assert_eq(
            psdf.replace({"A": {0: 100, 4: 400, np.nan: 700}}),
            pdf.replace({"A": {0: 100, 4: 400, np.nan: 700}}),
        )
        self.assert_eq(
            psdf.replace({"X": {0: 100, 4: 400, np.nan: 700}}),
            pdf.replace({"X": {0: 100, 4: 400, np.nan: 700}}),
        )

        self.assert_eq(psdf.replace({"C": ["a", None]}, "e"), pdf.replace({"C": ["a", None]}, "e"))

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.replace([0, 1, 2, 3, 5, 6], 4), pdf.replace([0, 1, 2, 3, 5, 6], 4))

        self.assert_eq(
            psdf.replace([0, 1, 2, 3, 5, 6], [6, 5, 4, 3, 2, 1]),
            pdf.replace([0, 1, 2, 3, 5, 6], [6, 5, 4, 3, 2, 1]),
        )

        self.assert_eq(psdf.replace({0: 10, 1: 100, 7: 200}), pdf.replace({0: 10, 1: 100, 7: 200}))

        self.assert_eq(
            psdf.replace({("X", "A"): [0, np.nan], ("X", "B"): 5}, 100),
            pdf.replace({("X", "A"): [0, np.nan], ("X", "B"): 5}, 100),
        )

        self.assert_eq(
            psdf.replace({("X", "A"): {0: 100, 4: 400, np.nan: 700}}),
            pdf.replace({("X", "A"): {0: 100, 4: 400, np.nan: 700}}),
        )
        self.assert_eq(
            psdf.replace({("X", "B"): {0: 100, 4: 400, np.nan: 700}}),
            pdf.replace({("X", "B"): {0: 100, 4: 400, np.nan: 700}}),
        )

        self.assert_eq(
            psdf.replace({("Y", "C"): ["a", None]}, "e"),
            pdf.replace({("Y", "C"): ["a", None]}, "e"),
        )

    def test_update(self):
        # check base function
        def get_data(left_columns=None, right_columns=None):
            left_pdf = pd.DataFrame(
                {"A": ["1", "2", "3", "4"], "B": ["100", "200", np.nan, np.nan]}, columns=["A", "B"]
            )
            right_pdf = pd.DataFrame(
                {"B": ["x", np.nan, "y", np.nan], "C": ["100", "200", "300", "400"]},
                columns=["B", "C"],
            )

            left_psdf = ps.DataFrame(
                {"A": ["1", "2", "3", "4"], "B": ["100", "200", None, None]}, columns=["A", "B"]
            )
            right_psdf = ps.DataFrame(
                {"B": ["x", None, "y", None], "C": ["100", "200", "300", "400"]}, columns=["B", "C"]
            )
            if left_columns is not None:
                left_pdf.columns = left_columns
                left_psdf.columns = left_columns
            if right_columns is not None:
                right_pdf.columns = right_columns
                right_psdf.columns = right_columns
            return left_psdf, left_pdf, right_psdf, right_pdf

        left_psdf, left_pdf, right_psdf, right_pdf = get_data()
        pser = left_pdf.B
        psser = left_psdf.B
        left_pdf.update(right_pdf)
        left_psdf.update(right_psdf)
        self.assert_eq(left_pdf.sort_values(by=["A", "B"]), left_psdf.sort_values(by=["A", "B"]))
        # Skip due to pandas bug: https://github.com/pandas-dev/pandas/issues/47188
        if not (LooseVersion("1.4.0") <= LooseVersion(pd.__version__) <= LooseVersion("1.4.2")):
            self.assert_eq(psser.sort_index(), pser.sort_index())

        left_psdf, left_pdf, right_psdf, right_pdf = get_data()
        left_pdf.update(right_pdf, overwrite=False)
        left_psdf.update(right_psdf, overwrite=False)
        self.assert_eq(left_pdf.sort_values(by=["A", "B"]), left_psdf.sort_values(by=["A", "B"]))

        with self.assertRaises(NotImplementedError):
            left_psdf.update(right_psdf, join="right")

        # multi-index columns
        left_columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B")])
        right_columns = pd.MultiIndex.from_tuples([("X", "B"), ("Y", "C")])

        left_psdf, left_pdf, right_psdf, right_pdf = get_data(
            left_columns=left_columns, right_columns=right_columns
        )
        left_pdf.update(right_pdf)
        left_psdf.update(right_psdf)
        self.assert_eq(
            left_pdf.sort_values(by=[("X", "A"), ("X", "B")]),
            left_psdf.sort_values(by=[("X", "A"), ("X", "B")]),
        )

        left_psdf, left_pdf, right_psdf, right_pdf = get_data(
            left_columns=left_columns, right_columns=right_columns
        )
        left_pdf.update(right_pdf, overwrite=False)
        left_psdf.update(right_psdf, overwrite=False)
        self.assert_eq(
            left_pdf.sort_values(by=[("X", "A"), ("X", "B")]),
            left_psdf.sort_values(by=[("X", "A"), ("X", "B")]),
        )

        right_columns = pd.MultiIndex.from_tuples([("Y", "B"), ("Y", "C")])
        left_psdf, left_pdf, right_psdf, right_pdf = get_data(
            left_columns=left_columns, right_columns=right_columns
        )
        left_pdf.update(right_pdf)
        left_psdf.update(right_psdf)
        self.assert_eq(
            left_pdf.sort_values(by=[("X", "A"), ("X", "B")]),
            left_psdf.sort_values(by=[("X", "A"), ("X", "B")]),
        )

    def test_pivot_table_dtypes(self):
        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, 2, 2, 4, 2, 4],
                "e": [1, 2, 2, 4, 2, 4],
                "c": [1, 2, 9, 4, 7, 4],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        # Skip columns comparison by reset_index
        res_df = psdf.pivot_table(
            index=["c"], columns="a", values=["b"], aggfunc={"b": "mean"}
        ).dtypes.reset_index(drop=True)
        exp_df = pdf.pivot_table(
            index=["c"], columns="a", values=["b"], aggfunc={"b": "mean"}
        ).dtypes.reset_index(drop=True)
        self.assert_eq(res_df, exp_df)

        # Results don't have the same column's name

        # Todo: self.assert_eq(psdf.pivot_table(columns="a", values="b").dtypes,
        #  pdf.pivot_table(columns="a", values="b").dtypes)

        # Todo: self.assert_eq(psdf.pivot_table(index=['c'], columns="a", values="b").dtypes,
        #  pdf.pivot_table(index=['c'], columns="a", values="b").dtypes)

        # Todo: self.assert_eq(psdf.pivot_table(index=['e', 'c'], columns="a", values="b").dtypes,
        #  pdf.pivot_table(index=['e', 'c'], columns="a", values="b").dtypes)

        # Todo: self.assert_eq(psdf.pivot_table(index=['e', 'c'],
        #  columns="a", values="b", fill_value=999).dtypes, pdf.pivot_table(index=['e', 'c'],
        #  columns="a", values="b", fill_value=999).dtypes)

    def test_pivot_table(self):
        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, 2, 2, 4, 2, 4],
                "e": [10, 20, 20, 40, 20, 40],
                "c": [1, 2, 9, 4, 7, 4],
                "d": [-1, -2, -3, -4, -5, -6],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        # Checking if both DataFrames have the same results
        self.assert_eq(
            psdf.pivot_table(columns="a", values="b").sort_index(),
            pdf.pivot_table(columns="a", values="b").sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(index=["c"], columns="a", values="b").sort_index(),
            pdf.pivot_table(index=["c"], columns="a", values="b").sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(index=["c"], columns="a", values="b", aggfunc="sum").sort_index(),
            pdf.pivot_table(index=["c"], columns="a", values="b", aggfunc="sum").sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(index=["c"], columns="a", values=["b"], aggfunc="sum").sort_index(),
            pdf.pivot_table(index=["c"], columns="a", values=["b"], aggfunc="sum").sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(
                index=["c"], columns="a", values=["b", "e"], aggfunc="sum"
            ).sort_index(),
            pdf.pivot_table(
                index=["c"], columns="a", values=["b", "e"], aggfunc="sum"
            ).sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(
                index=["c"], columns="a", values=["b", "e", "d"], aggfunc="sum"
            ).sort_index(),
            pdf.pivot_table(
                index=["c"], columns="a", values=["b", "e", "d"], aggfunc="sum"
            ).sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(
                index=["c"], columns="a", values=["b", "e"], aggfunc={"b": "mean", "e": "sum"}
            ).sort_index(),
            pdf.pivot_table(
                index=["c"], columns="a", values=["b", "e"], aggfunc={"b": "mean", "e": "sum"}
            ).sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(index=["e", "c"], columns="a", values="b").sort_index(),
            pdf.pivot_table(index=["e", "c"], columns="a", values="b").sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(
                index=["e", "c"], columns="a", values="b", fill_value=999
            ).sort_index(),
            pdf.pivot_table(index=["e", "c"], columns="a", values="b", fill_value=999).sort_index(),
            almost=True,
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "b"), ("y", "e"), ("z", "c"), ("w", "d")]
        )
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.pivot_table(columns=("x", "a"), values=("x", "b")).sort_index(),
            pdf.pivot_table(columns=[("x", "a")], values=[("x", "b")]).sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(
                index=[("z", "c")], columns=("x", "a"), values=[("x", "b")]
            ).sort_index(),
            pdf.pivot_table(
                index=[("z", "c")], columns=[("x", "a")], values=[("x", "b")]
            ).sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(
                index=[("z", "c")], columns=("x", "a"), values=[("x", "b"), ("y", "e")]
            ).sort_index(),
            pdf.pivot_table(
                index=[("z", "c")], columns=[("x", "a")], values=[("x", "b"), ("y", "e")]
            ).sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(
                index=[("z", "c")], columns=("x", "a"), values=[("x", "b"), ("y", "e"), ("w", "d")]
            ).sort_index(),
            pdf.pivot_table(
                index=[("z", "c")],
                columns=[("x", "a")],
                values=[("x", "b"), ("y", "e"), ("w", "d")],
            ).sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(
                index=[("z", "c")],
                columns=("x", "a"),
                values=[("x", "b"), ("y", "e")],
                aggfunc={("x", "b"): "mean", ("y", "e"): "sum"},
            ).sort_index(),
            pdf.pivot_table(
                index=[("z", "c")],
                columns=[("x", "a")],
                values=[("x", "b"), ("y", "e")],
                aggfunc={("x", "b"): "mean", ("y", "e"): "sum"},
            ).sort_index(),
            almost=True,
        )

    def test_pivot_table_and_index(self):
        # https://github.com/databricks/koalas/issues/805
        pdf = pd.DataFrame(
            {
                "A": ["foo", "foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar"],
                "B": ["one", "one", "one", "two", "two", "one", "one", "two", "two"],
                "C": [
                    "small",
                    "large",
                    "large",
                    "small",
                    "small",
                    "large",
                    "small",
                    "small",
                    "large",
                ],
                "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
                "E": [2, 4, 5, 5, 6, 6, 8, 9, 9],
            },
            columns=["A", "B", "C", "D", "E"],
            index=np.random.rand(9),
        )
        psdf = ps.from_pandas(pdf)

        ptable = pdf.pivot_table(
            values="D", index=["A", "B"], columns="C", aggfunc="sum", fill_value=0
        ).sort_index()
        ktable = psdf.pivot_table(
            values="D", index=["A", "B"], columns="C", aggfunc="sum", fill_value=0
        ).sort_index()

        self.assert_eq(ktable, ptable)
        self.assert_eq(ktable.index, ptable.index)
        self.assert_eq(repr(ktable.index), repr(ptable.index))

    def test_stack(self):
        pdf_single_level_cols = pd.DataFrame(
            [[0, 1], [2, 3]], index=["cat", "dog"], columns=["weight", "height"]
        )
        psdf_single_level_cols = ps.from_pandas(pdf_single_level_cols)

        self.assert_eq(
            psdf_single_level_cols.stack().sort_index(), pdf_single_level_cols.stack().sort_index()
        )

        multicol1 = pd.MultiIndex.from_tuples(
            [("weight", "kg"), ("weight", "pounds")], names=["x", "y"]
        )
        pdf_multi_level_cols1 = pd.DataFrame(
            [[1, 2], [2, 4]], index=["cat", "dog"], columns=multicol1
        )
        psdf_multi_level_cols1 = ps.from_pandas(pdf_multi_level_cols1)

        self.assert_eq(
            psdf_multi_level_cols1.stack().sort_index(), pdf_multi_level_cols1.stack().sort_index()
        )

        multicol2 = pd.MultiIndex.from_tuples([("weight", "kg"), ("height", "m")])
        pdf_multi_level_cols2 = pd.DataFrame(
            [[1.0, 2.0], [3.0, 4.0]], index=["cat", "dog"], columns=multicol2
        )
        psdf_multi_level_cols2 = ps.from_pandas(pdf_multi_level_cols2)

        self.assert_eq(
            psdf_multi_level_cols2.stack().sort_index(), pdf_multi_level_cols2.stack().sort_index()
        )

        pdf = pd.DataFrame(
            {
                ("y", "c"): [True, True],
                ("x", "b"): [False, False],
                ("x", "c"): [True, False],
                ("y", "a"): [False, True],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.stack().sort_index(), pdf.stack().sort_index())
        self.assert_eq(psdf[[]].stack().sort_index(), pdf[[]].stack().sort_index(), almost=True)

    def test_unstack(self):
        pdf = pd.DataFrame(
            np.random.randn(3, 3),
            index=pd.MultiIndex.from_tuples([("rg1", "x"), ("rg1", "y"), ("rg2", "z")]),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.unstack().sort_index(), pdf.unstack().sort_index(), almost=True)
        self.assert_eq(
            psdf.unstack().unstack().sort_index(), pdf.unstack().unstack().sort_index(), almost=True
        )

    def test_pivot_errors(self):
        psdf = ps.range(10)

        with self.assertRaisesRegex(ValueError, "columns should be set"):
            psdf.pivot(index="id")

        with self.assertRaisesRegex(ValueError, "values should be set"):
            psdf.pivot(index="id", columns="id")

    def test_pivot_table_errors(self):
        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, 2, 2, 4, 2, 4],
                "e": [1, 2, 2, 4, 2, 4],
                "c": [1, 2, 9, 4, 7, 4],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        self.assertRaises(KeyError, lambda: psdf.pivot_table(index=["c"], columns="a", values=5))

        msg = "index should be a None or a list of columns."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(index="c", columns="a", values="b")

        msg = "pivot_table doesn't support aggfunc as dict and without index."
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.pivot_table(columns="a", values=["b", "e"], aggfunc={"b": "mean", "e": "sum"})

        msg = "columns should be one column name."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(columns=["a"], values=["b"], aggfunc={"b": "mean", "e": "sum"})

        msg = "Columns in aggfunc must be the same as values."
        with self.assertRaisesRegex(ValueError, msg):
            psdf.pivot_table(
                index=["e", "c"], columns="a", values="b", aggfunc={"b": "mean", "e": "sum"}
            )

        msg = "values can't be a list without index."
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.pivot_table(columns="a", values=["b", "e"])

        msg = "Wrong columns A."
        with self.assertRaisesRegex(ValueError, msg):
            psdf.pivot_table(
                index=["c"], columns="A", values=["b", "e"], aggfunc={"b": "mean", "e": "sum"}
            )

        msg = "values should be one column or list of columns."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(columns="a", values=(["b"], ["c"]))

        msg = "aggfunc must be a dict mapping from column name to aggregate functions"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(columns="a", values="b", aggfunc={"a": lambda x: sum(x)})

        psdf = ps.DataFrame(
            {
                "A": ["foo", "foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar"],
                "B": ["one", "one", "one", "two", "two", "one", "one", "two", "two"],
                "C": [
                    "small",
                    "large",
                    "large",
                    "small",
                    "small",
                    "large",
                    "small",
                    "small",
                    "large",
                ],
                "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
                "E": [2, 4, 5, 5, 6, 6, 8, 9, 9],
            },
            columns=["A", "B", "C", "D", "E"],
            index=np.random.rand(9),
        )

        msg = "values should be a numeric type."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(
                index=["C"], columns="A", values=["B", "E"], aggfunc={"B": "mean", "E": "sum"}
            )

        msg = "values should be a numeric type."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(index=["C"], columns="A", values="B", aggfunc={"B": "mean"})

    def test_transpose(self):
        # TODO: what if with random index?
        pdf1 = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]}, columns=["col1", "col2"])
        psdf1 = ps.from_pandas(pdf1)

        pdf2 = pd.DataFrame(
            data={"score": [9, 8], "kids": [0, 0], "age": [12, 22]},
            columns=["score", "kids", "age"],
        )
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.transpose().sort_index(), psdf1.transpose().sort_index())
        self.assert_eq(pdf2.transpose().sort_index(), psdf2.transpose().sort_index())

        with option_context("compute.max_rows", None):
            self.assert_eq(pdf1.transpose().sort_index(), psdf1.transpose().sort_index())

            self.assert_eq(pdf2.transpose().sort_index(), psdf2.transpose().sort_index())

        pdf3 = pd.DataFrame(
            {
                ("cg1", "a"): [1, 2, 3],
                ("cg1", "b"): [4, 5, 6],
                ("cg2", "c"): [7, 8, 9],
                ("cg3", "d"): [9, 9, 9],
            },
            index=pd.MultiIndex.from_tuples([("rg1", "x"), ("rg1", "y"), ("rg2", "z")]),
        )
        psdf3 = ps.from_pandas(pdf3)

        self.assert_eq(pdf3.transpose().sort_index(), psdf3.transpose().sort_index())

        with option_context("compute.max_rows", None):
            self.assert_eq(pdf3.transpose().sort_index(), psdf3.transpose().sort_index())

    def _test_cummin(self, pdf, psdf):
        self.assert_eq(pdf.cummin(), psdf.cummin())
        self.assert_eq(pdf.cummin(skipna=False), psdf.cummin(skipna=False))
        self.assert_eq(pdf.cummin().sum(), psdf.cummin().sum())

    def test_cummin(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0], [5, None], [1.0, 0.0], [2.0, 4.0], [4.0, 9.0]],
            columns=list("AB"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cummin(pdf, psdf)

    def test_cummin_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cummin(pdf, psdf)

    def _test_cummax(self, pdf, psdf):
        self.assert_eq(pdf.cummax(), psdf.cummax())
        self.assert_eq(pdf.cummax(skipna=False), psdf.cummax(skipna=False))
        self.assert_eq(pdf.cummax().sum(), psdf.cummax().sum())

    def test_cummax(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0], [5, None], [1.0, 0.0], [2.0, 4.0], [4.0, 9.0]],
            columns=list("AB"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cummax(pdf, psdf)

    def test_cummax_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cummax(pdf, psdf)

    def _test_cumsum(self, pdf, psdf):
        self.assert_eq(pdf.cumsum(), psdf.cumsum())
        self.assert_eq(pdf.cumsum(skipna=False), psdf.cumsum(skipna=False))
        self.assert_eq(pdf.cumsum().sum(), psdf.cumsum().sum())

    def test_cumsum(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0], [5, None], [1.0, 0.0], [2.0, 4.0], [4.0, 9.0]],
            columns=list("AB"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cumsum(pdf, psdf)

    def test_cumsum_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cumsum(pdf, psdf)

    def _test_cumprod(self, pdf, psdf):
        self.assert_eq(pdf.cumprod(), psdf.cumprod(), almost=True)
        self.assert_eq(pdf.cumprod(skipna=False), psdf.cumprod(skipna=False), almost=True)
        self.assert_eq(pdf.cumprod().sum(), psdf.cumprod().sum(), almost=True)

    def test_cumprod(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0, 1], [5, None, 2], [1.0, -1.0, -3], [2.0, 0, 4], [4.0, 9.0, 5]],
            columns=list("ABC"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cumprod(pdf, psdf)

    def test_cumprod_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.rand(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cumprod(pdf, psdf)

    def test_drop_duplicates(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 2, 2, 3], "b": ["a", "a", "a", "c", "d"]}, index=np.random.rand(5)
        )
        psdf = ps.from_pandas(pdf)

        # inplace is False
        for keep in ["first", "last", False]:
            with self.subTest(keep=keep):
                self.assert_eq(
                    pdf.drop_duplicates(keep=keep).sort_index(),
                    psdf.drop_duplicates(keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.drop_duplicates("a", keep=keep).sort_index(),
                    psdf.drop_duplicates("a", keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.drop_duplicates(["a", "b"], keep=keep).sort_index(),
                    psdf.drop_duplicates(["a", "b"], keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.set_index("a", append=True).drop_duplicates(keep=keep).sort_index(),
                    psdf.set_index("a", append=True).drop_duplicates(keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.set_index("a", append=True).drop_duplicates("b", keep=keep).sort_index(),
                    psdf.set_index("a", append=True).drop_duplicates("b", keep=keep).sort_index(),
                )

        columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b")])
        pdf.columns = columns
        psdf.columns = columns

        # inplace is False
        for keep in ["first", "last", False]:
            with self.subTest("multi-index columns", keep=keep):
                self.assert_eq(
                    pdf.drop_duplicates(keep=keep).sort_index(),
                    psdf.drop_duplicates(keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.drop_duplicates(("x", "a"), keep=keep).sort_index(),
                    psdf.drop_duplicates(("x", "a"), keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.drop_duplicates([("x", "a"), ("y", "b")], keep=keep).sort_index(),
                    psdf.drop_duplicates([("x", "a"), ("y", "b")], keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.drop_duplicates(
                        [("x", "a"), ("y", "b")], keep=keep, ignore_index=True
                    ).sort_index(),
                    psdf.drop_duplicates(
                        [("x", "a"), ("y", "b")], keep=keep, ignore_index=True
                    ).sort_index(),
                )

        # inplace is True
        subset_list = [None, "a", ["a", "b"]]
        for subset in subset_list:
            pdf = pd.DataFrame(
                {"a": [1, 2, 2, 2, 3], "b": ["a", "a", "a", "c", "d"]}, index=np.random.rand(5)
            )
            psdf = ps.from_pandas(pdf)
            pser = pdf.a
            psser = psdf.a
            pdf.drop_duplicates(subset=subset, inplace=True)
            psdf.drop_duplicates(subset=subset, inplace=True)
            self.assert_eq(psdf.sort_index(), pdf.sort_index())
            self.assert_eq(psser.sort_index(), pser.sort_index())

        # multi-index columns, inplace is True
        subset_list = [None, ("x", "a"), [("x", "a"), ("y", "b")]]
        for subset in subset_list:
            pdf = pd.DataFrame(
                {"a": [1, 2, 2, 2, 3], "b": ["a", "a", "a", "c", "d"]}, index=np.random.rand(5)
            )
            psdf = ps.from_pandas(pdf)
            columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b")])
            pdf.columns = columns
            psdf.columns = columns
            pser = pdf[("x", "a")]
            psser = psdf[("x", "a")]
            pdf.drop_duplicates(subset=subset, inplace=True)
            pdf.drop_duplicates(subset=subset, inplace=True, ignore_index=True)
            psdf.drop_duplicates(subset=subset, inplace=True)
            psdf.drop_duplicates(subset=subset, inplace=True, ignore_index=True)
            self.assert_eq(psdf.sort_index(), pdf.sort_index())
            self.assert_eq(psser.sort_index(), pser.sort_index())

        # non-string names
        pdf = pd.DataFrame(
            {10: [1, 2, 2, 2, 3], 20: ["a", "a", "a", "c", "d"]}, index=np.random.rand(5)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.drop_duplicates(10, keep=keep).sort_index(),
            psdf.drop_duplicates(10, keep=keep).sort_index(),
        )
        self.assert_eq(
            pdf.drop_duplicates([10, 20], keep=keep).sort_index(),
            psdf.drop_duplicates([10, 20], keep=keep).sort_index(),
        )

    def test_reindex(self):
        index = pd.Index(["A", "B", "C", "D", "E"])
        columns = pd.Index(["numbers"])
        pdf = pd.DataFrame([1.0, 2.0, 3.0, 4.0, None], index=index, columns=columns)
        psdf = ps.from_pandas(pdf)

        columns2 = pd.Index(["numbers", "2", "3"], name="cols2")
        self.assert_eq(
            pdf.reindex(columns=columns2).sort_index(),
            psdf.reindex(columns=columns2).sort_index(),
        )

        columns = pd.Index(["numbers"], name="cols")
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            pdf.reindex(["A", "B", "C"], columns=["numbers", "2", "3"]).sort_index(),
            psdf.reindex(["A", "B", "C"], columns=["numbers", "2", "3"]).sort_index(),
        )

        self.assert_eq(
            pdf.reindex(["A", "B", "C"], index=["numbers", "2", "3"]).sort_index(),
            psdf.reindex(["A", "B", "C"], index=["numbers", "2", "3"]).sort_index(),
        )

        self.assert_eq(
            pdf.reindex(index=["A", "B"]).sort_index(), psdf.reindex(index=["A", "B"]).sort_index()
        )

        self.assert_eq(
            pdf.reindex(index=["A", "B", "2", "3"]).sort_index(),
            psdf.reindex(index=["A", "B", "2", "3"]).sort_index(),
        )

        self.assert_eq(
            pdf.reindex(index=["A", "E", "2", "3"], fill_value=0).sort_index(),
            psdf.reindex(index=["A", "E", "2", "3"], fill_value=0).sort_index(),
        )

        self.assert_eq(
            pdf.reindex(columns=["numbers"]).sort_index(),
            psdf.reindex(columns=["numbers"]).sort_index(),
        )

        self.assert_eq(
            pdf.reindex(columns=["numbers"], copy=True).sort_index(),
            psdf.reindex(columns=["numbers"], copy=True).sort_index(),
        )

        # Using float as fill_value to avoid int64/32 clash
        self.assert_eq(
            pdf.reindex(columns=["numbers", "2", "3"], fill_value=0.0).sort_index(),
            psdf.reindex(columns=["numbers", "2", "3"], fill_value=0.0).sort_index(),
        )

        columns2 = pd.Index(["numbers", "2", "3"])
        self.assert_eq(
            pdf.reindex(columns=columns2).sort_index(),
            psdf.reindex(columns=columns2).sort_index(),
        )

        columns2 = pd.Index(["numbers", "2", "3"], name="cols2")
        self.assert_eq(
            pdf.reindex(columns=columns2).sort_index(),
            psdf.reindex(columns=columns2).sort_index(),
        )

        # Reindexing single Index on single Index
        pindex2 = pd.Index(["A", "C", "D", "E", "0"], name="index2")
        kindex2 = ps.from_pandas(pindex2)

        for fill_value in [None, 0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        pindex2 = pd.DataFrame({"index2": ["A", "C", "D", "E", "0"]}).set_index("index2").index
        kindex2 = ps.from_pandas(pindex2)

        for fill_value in [None, 0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        # Reindexing MultiIndex on single Index
        pindex = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("F", "G")], names=["name1", "name2"]
        )
        kindex = ps.from_pandas(pindex)

        self.assert_eq(
            pdf.reindex(index=pindex, fill_value=0.0).sort_index(),
            psdf.reindex(index=kindex, fill_value=0.0).sort_index(),
        )

        # Specifying the `labels` parameter
        new_index = ["V", "W", "X", "Y", "Z"]
        self.assert_eq(
            pdf.reindex(labels=new_index, fill_value=0.0, axis=0).sort_index(),
            psdf.reindex(labels=new_index, fill_value=0.0, axis=0).sort_index(),
        )
        self.assert_eq(
            pdf.reindex(labels=new_index, fill_value=0.0, axis=1).sort_index(),
            psdf.reindex(labels=new_index, fill_value=0.0, axis=1).sort_index(),
        )

        self.assertRaises(TypeError, lambda: psdf.reindex(columns=["numbers", "2", "3"], axis=1))
        self.assertRaises(TypeError, lambda: psdf.reindex(columns=["numbers", "2", "3"], axis=2))
        self.assertRaises(TypeError, lambda: psdf.reindex(columns="numbers"))
        self.assertRaises(TypeError, lambda: psdf.reindex(index=["A", "B", "C"], axis=1))
        self.assertRaises(TypeError, lambda: psdf.reindex(index=123))

        # Reindexing MultiIndex on MultiIndex
        pdf = pd.DataFrame({"numbers": [1.0, 2.0, None]}, index=pindex)
        psdf = ps.from_pandas(pdf)
        pindex2 = pd.MultiIndex.from_tuples(
            [("A", "G"), ("C", "D"), ("I", "J")], names=["name1", "name2"]
        )
        kindex2 = ps.from_pandas(pindex2)

        for fill_value in [None, 0.0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        pindex2 = (
            pd.DataFrame({"index_level_1": ["A", "C", "I"], "index_level_2": ["G", "D", "J"]})
            .set_index(["index_level_1", "index_level_2"])
            .index
        )
        kindex2 = ps.from_pandas(pindex2)

        for fill_value in [None, 0.0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        columns = pd.MultiIndex.from_tuples([("X", "numbers")], names=["cols1", "cols2"])
        pdf.columns = columns
        psdf.columns = columns

        # Reindexing MultiIndex index on MultiIndex columns and MultiIndex index
        for fill_value in [None, 0.0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        index = pd.Index(["A", "B", "C", "D", "E"])
        pdf = pd.DataFrame(data=[1.0, 2.0, 3.0, 4.0, None], index=index, columns=columns)
        psdf = ps.from_pandas(pdf)
        pindex2 = pd.Index(["A", "C", "D", "E", "0"], name="index2")
        kindex2 = ps.from_pandas(pindex2)

        # Reindexing single Index on MultiIndex columns and single Index
        for fill_value in [None, 0.0]:
            self.assert_eq(
                pdf.reindex(index=pindex2, fill_value=fill_value).sort_index(),
                psdf.reindex(index=kindex2, fill_value=fill_value).sort_index(),
            )

        for fill_value in [None, 0.0]:
            self.assert_eq(
                pdf.reindex(
                    columns=[("X", "numbers"), ("Y", "2"), ("Y", "3")], fill_value=fill_value
                ).sort_index(),
                psdf.reindex(
                    columns=[("X", "numbers"), ("Y", "2"), ("Y", "3")], fill_value=fill_value
                ).sort_index(),
            )

        columns2 = pd.MultiIndex.from_tuples(
            [("X", "numbers"), ("Y", "2"), ("Y", "3")], names=["cols3", "cols4"]
        )
        self.assert_eq(
            pdf.reindex(columns=columns2).sort_index(),
            psdf.reindex(columns=columns2).sort_index(),
        )

        self.assertRaises(TypeError, lambda: psdf.reindex(columns=["X"]))
        self.assertRaises(ValueError, lambda: psdf.reindex(columns=[("X",)]))

    def test_reindex_like(self):
        data = [[1.0, 2.0], [3.0, None], [None, 4.0]]
        index = pd.Index(["A", "B", "C"], name="index")
        columns = pd.Index(["numbers", "values"], name="cols")
        pdf = pd.DataFrame(data=data, index=index, columns=columns)
        psdf = ps.from_pandas(pdf)

        # Reindexing single Index on single Index
        data2 = [[5.0, None], [6.0, 7.0], [8.0, None]]
        index2 = pd.Index(["A", "C", "D"], name="index2")
        columns2 = pd.Index(["numbers", "F"], name="cols2")
        pdf2 = pd.DataFrame(data=data2, index=index2, columns=columns2)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(
            pdf.reindex_like(pdf2).sort_index(),
            psdf.reindex_like(psdf2).sort_index(),
        )

        pdf2 = pd.DataFrame({"index_level_1": ["A", "C", "I"]})
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(
            pdf.reindex_like(pdf2.set_index(["index_level_1"])).sort_index(),
            psdf.reindex_like(psdf2.set_index(["index_level_1"])).sort_index(),
        )

        # Reindexing MultiIndex on single Index
        index2 = pd.MultiIndex.from_tuples(
            [("A", "G"), ("C", "D"), ("I", "J")], names=["name3", "name4"]
        )
        pdf2 = pd.DataFrame(data=data2, index=index2)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(
            pdf.reindex_like(pdf2).sort_index(),
            psdf.reindex_like(psdf2).sort_index(),
        )

        self.assertRaises(TypeError, lambda: psdf.reindex_like(index2))
        self.assertRaises(AssertionError, lambda: psdf2.reindex_like(psdf))

        # Reindexing MultiIndex on MultiIndex
        columns2 = pd.MultiIndex.from_tuples(
            [("numbers", "third"), ("values", "second")], names=["cols3", "cols4"]
        )
        pdf2.columns = columns2
        psdf2.columns = columns2

        columns = pd.MultiIndex.from_tuples(
            [("numbers", "first"), ("values", "second")], names=["cols1", "cols2"]
        )
        index = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["name1", "name2"]
        )
        pdf = pd.DataFrame(data=data, index=index, columns=columns)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.reindex_like(pdf2).sort_index(),
            psdf.reindex_like(psdf2).sort_index(),
        )

    def test_melt(self):
        pdf = pd.DataFrame(
            {"A": [1, 3, 5], "B": [2, 4, 6], "C": [7, 8, 9]}, index=np.random.rand(3)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.melt().sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt().sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars="A").sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt(id_vars="A").sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=["A", "B"]).sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt(id_vars=["A", "B"]).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=("A", "B")).sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt(id_vars=("A", "B")).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=["A"], value_vars=["C"])
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=["A"], value_vars=["C"]).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=["A"], value_vars=["B"], var_name="myVarname", value_name="myValname")
            .sort_values(["myVarname", "myValname"])
            .reset_index(drop=True),
            pdf.melt(
                id_vars=["A"], value_vars=["B"], var_name="myVarname", value_name="myValname"
            ).sort_values(["myVarname", "myValname"]),
        )
        self.assert_eq(
            psdf.melt(value_vars=("A", "B"))
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(value_vars=("A", "B")).sort_values(["variable", "value"]),
        )

        self.assertRaises(KeyError, lambda: psdf.melt(id_vars="Z"))
        self.assertRaises(KeyError, lambda: psdf.melt(value_vars="Z"))

        # multi-index columns
        TEN = 10.0
        TWELVE = 20.0

        columns = pd.MultiIndex.from_tuples([(TEN, "A"), (TEN, "B"), (TWELVE, "C")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.melt().sort_values(["variable_0", "variable_1", "value"]).reset_index(drop=True),
            pdf.melt().sort_values(["variable_0", "variable_1", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=[(TEN, "A")])
            .sort_values(["variable_0", "variable_1", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=[(TEN, "A")])
            .sort_values(["variable_0", "variable_1", "value"])
            .rename(columns=name_like_string),
        )
        self.assert_eq(
            psdf.melt(id_vars=[(TEN, "A")], value_vars=[(TWELVE, "C")])
            .sort_values(["variable_0", "variable_1", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=[(TEN, "A")], value_vars=[(TWELVE, "C")])
            .sort_values(["variable_0", "variable_1", "value"])
            .rename(columns=name_like_string),
        )
        self.assert_eq(
            psdf.melt(
                id_vars=[(TEN, "A")],
                value_vars=[(TEN, "B")],
                var_name=["myV1", "myV2"],
                value_name="myValname",
            )
            .sort_values(["myV1", "myV2", "myValname"])
            .reset_index(drop=True),
            pdf.melt(
                id_vars=[(TEN, "A")],
                value_vars=[(TEN, "B")],
                var_name=["myV1", "myV2"],
                value_name="myValname",
            )
            .sort_values(["myV1", "myV2", "myValname"])
            .rename(columns=name_like_string),
        )

        columns.names = ["v0", "v1"]
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.melt().sort_values(["v0", "v1", "value"]).reset_index(drop=True),
            pdf.melt().sort_values(["v0", "v1", "value"]),
        )

        self.assertRaises(ValueError, lambda: psdf.melt(id_vars=(TEN, "A")))
        self.assertRaises(ValueError, lambda: psdf.melt(value_vars=(TEN, "A")))
        self.assertRaises(KeyError, lambda: psdf.melt(id_vars=[TEN]))
        self.assertRaises(KeyError, lambda: psdf.melt(id_vars=[(TWELVE, "A")]))
        self.assertRaises(KeyError, lambda: psdf.melt(value_vars=[TWELVE]))
        self.assertRaises(KeyError, lambda: psdf.melt(value_vars=[(TWELVE, "A")]))

        # non-string names
        pdf.columns = [10.0, 20.0, 30.0]
        psdf.columns = [10.0, 20.0, 30.0]

        self.assert_eq(
            psdf.melt().sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt().sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=10.0).sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt(id_vars=10.0).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=[10.0, 20.0])
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=[10.0, 20.0]).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=(10.0, 20.0))
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=(10.0, 20.0)).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=[10.0], value_vars=[30.0])
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=[10.0], value_vars=[30.0]).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(value_vars=(10.0, 20.0))
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(value_vars=(10.0, 20.0)).sort_values(["variable", "value"]),
        )

    def test_all(self):
        pdf = pd.DataFrame(
            {
                "col1": [False, False, False],
                "col2": [True, False, False],
                "col3": [0, 0, 1],
                "col4": [0, 1, 2],
                "col5": [False, False, None],
                "col6": [True, False, None],
            },
            index=np.random.rand(3),
        )
        pdf.name = "x"
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.all(), pdf.all())
        self.assert_eq(psdf.all(bool_only=True), pdf.all(bool_only=True))
        self.assert_eq(psdf.all(bool_only=False), pdf.all(bool_only=False))
        self.assert_eq(psdf[["col5"]].all(bool_only=True), pdf[["col5"]].all(bool_only=True))
        self.assert_eq(psdf[["col5"]].all(bool_only=False), pdf[["col5"]].all(bool_only=False))

        columns = pd.MultiIndex.from_tuples(
            [
                ("a", "col1"),
                ("a", "col2"),
                ("a", "col3"),
                ("b", "col4"),
                ("b", "col5"),
                ("c", "col6"),
            ]
        )
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.all(), pdf.all())
        self.assert_eq(psdf.all(bool_only=True), pdf.all(bool_only=True))
        self.assert_eq(psdf.all(bool_only=False), pdf.all(bool_only=False))

        columns.names = ["X", "Y"]
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.all(), pdf.all())
        self.assert_eq(psdf.all(bool_only=True), pdf.all(bool_only=True))
        self.assert_eq(psdf.all(bool_only=False), pdf.all(bool_only=False))

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psdf.all(axis=1)

        # Test skipna
        pdf = pd.DataFrame({"A": [True, True], "B": [1, np.nan], "C": [True, None]})
        pdf.name = "x"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf[["A", "B"]].all(skipna=False), pdf[["A", "B"]].all(skipna=False))
        self.assert_eq(psdf[["A", "C"]].all(skipna=False), pdf[["A", "C"]].all(skipna=False))
        self.assert_eq(psdf[["B", "C"]].all(skipna=False), pdf[["B", "C"]].all(skipna=False))
        self.assert_eq(psdf.all(skipna=False), pdf.all(skipna=False))
        self.assert_eq(psdf.all(skipna=True), pdf.all(skipna=True))
        self.assert_eq(psdf.all(), pdf.all())
        self.assert_eq(
            ps.DataFrame([np.nan]).all(skipna=False), pd.DataFrame([np.nan]).all(skipna=False)
        )
        self.assert_eq(ps.DataFrame([None]).all(skipna=True), pd.DataFrame([None]).all(skipna=True))

    def test_any(self):
        pdf = pd.DataFrame(
            {
                "col1": [False, False, False],
                "col2": [True, False, False],
                "col3": [0, 0, 1],
                "col4": [0, 1, 2],
                "col5": [False, False, None],
                "col6": [True, False, None],
            },
            index=np.random.rand(3),
        )
        pdf.name = "x"
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.any(), pdf.any())
        self.assert_eq(psdf.any(bool_only=True), pdf.any(bool_only=True))
        self.assert_eq(psdf.any(bool_only=False), pdf.any(bool_only=False))
        self.assert_eq(psdf[["col5"]].all(bool_only=True), pdf[["col5"]].all(bool_only=True))
        self.assert_eq(psdf[["col5"]].all(bool_only=False), pdf[["col5"]].all(bool_only=False))

        columns = pd.MultiIndex.from_tuples(
            [
                ("a", "col1"),
                ("a", "col2"),
                ("a", "col3"),
                ("b", "col4"),
                ("b", "col5"),
                ("c", "col6"),
            ]
        )
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.any(), pdf.any())
        self.assert_eq(psdf.any(bool_only=True), pdf.any(bool_only=True))
        self.assert_eq(psdf.any(bool_only=False), pdf.any(bool_only=False))

        columns.names = ["X", "Y"]
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.any(), pdf.any())
        self.assert_eq(psdf.any(bool_only=True), pdf.any(bool_only=True))
        self.assert_eq(psdf.any(bool_only=False), pdf.any(bool_only=False))

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psdf.any(axis=1)


if __name__ == "__main__":
    from pyspark.pandas.tests.test_dataframe import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
