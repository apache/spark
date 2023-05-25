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
from distutils.version import LooseVersion
import unittest

import numpy as np
import pandas as pd
from pandas.tseries.offsets import DateOffset

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.testing.pandasutils import ComparisonTestBase
from pyspark.testing.sqlutils import SQLTestUtils


# This file contains test cases for 'Reindexing / Selection / Label manipulation'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#reindexing-selection-label-manipulation
class FrameReindexingMixin:
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

    def test_align(self):
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]}, index=[10, 20, 30])
        psdf1 = ps.from_pandas(pdf1)

        for join in ["outer", "inner", "left", "right"]:
            for axis in [None, 0, 1]:
                psdf_l, psdf_r = psdf1.align(psdf1[["b"]], join=join, axis=axis)
                pdf_l, pdf_r = pdf1.align(pdf1[["b"]], join=join, axis=axis)
                self.assert_eq(psdf_l, pdf_l)
                self.assert_eq(psdf_r, pdf_r)

                psdf_l, psdf_r = psdf1[["a"]].align(psdf1[["b", "a"]], join=join, axis=axis)
                pdf_l, pdf_r = pdf1[["a"]].align(pdf1[["b", "a"]], join=join, axis=axis)
                self.assert_eq(psdf_l, pdf_l)
                self.assert_eq(psdf_r, pdf_r)

                psdf_l, psdf_r = psdf1[["b", "a"]].align(psdf1[["a"]], join=join, axis=axis)
                pdf_l, pdf_r = pdf1[["b", "a"]].align(pdf1[["a"]], join=join, axis=axis)
                self.assert_eq(psdf_l, pdf_l)
                self.assert_eq(psdf_r, pdf_r)

        psdf_l, psdf_r = psdf1.align(psdf1["b"], axis=0)
        pdf_l, pdf_r = pdf1.align(pdf1["b"], axis=0)
        self.assert_eq(psdf_l, pdf_l)
        self.assert_eq(psdf_r, pdf_r)

        psdf_l, psser_b = psdf1[["a"]].align(psdf1["b"], axis=0)
        pdf_l, pser_b = pdf1[["a"]].align(pdf1["b"], axis=0)
        self.assert_eq(psdf_l, pdf_l)
        self.assert_eq(psser_b, pser_b)

        self.assertRaises(ValueError, lambda: psdf1.align(psdf1, join="unknown"))
        self.assertRaises(ValueError, lambda: psdf1.align(psdf1["b"]))
        self.assertRaises(TypeError, lambda: psdf1.align(["b"]))
        self.assertRaises(NotImplementedError, lambda: psdf1.align(psdf1["b"], axis=1))

        pdf2 = pd.DataFrame({"a": [4, 5, 6], "d": ["d", "e", "f"]}, index=[10, 11, 12])
        psdf2 = ps.from_pandas(pdf2)

        for join in ["outer", "inner", "left", "right"]:
            psdf_l, psdf_r = psdf1.align(psdf2, join=join, axis=1)
            pdf_l, pdf_r = pdf1.align(pdf2, join=join, axis=1)
            self.assert_eq(psdf_l.sort_index(), pdf_l.sort_index())
            self.assert_eq(psdf_r.sort_index(), pdf_r.sort_index())

    def test_at_time(self):
        idx = pd.date_range("2018-04-09", periods=4, freq="1D20min")
        pdf = pd.DataFrame({"A": [1, 2, 3, 4]}, index=idx)
        psdf = ps.from_pandas(pdf)
        psdf.at_time("0:20")
        self.assert_eq(
            pdf.at_time("0:20").sort_index(),
            psdf.at_time("0:20").sort_index(),
        )

        # Index name is 'ts'
        pdf.index.name = "ts"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.at_time("0:20").sort_index(),
            psdf.at_time("0:20").sort_index(),
        )

        # Index name is 'ts', column label is 'index'
        pdf.columns = pd.Index(["index"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.at_time("0:40").sort_index(),
            psdf.at_time("0:40").sort_index(),
        )

        # Both index name and column label are 'index'
        pdf.index.name = "index"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.at_time("0:40").sort_index(),
            psdf.at_time("0:40").sort_index(),
        )

        # Index name is 'index', column label is ('X', 'A')
        pdf.columns = pd.MultiIndex.from_arrays([["X"], ["A"]])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.at_time("0:40").sort_index(),
            psdf.at_time("0:40").sort_index(),
        )

        with self.assertRaisesRegex(NotImplementedError, "'asof' argument is not supported"):
            psdf.at_time("0:15", asof=True)

        with self.assertRaisesRegex(NotImplementedError, "at_time currently only works for axis=0"):
            psdf.at_time("0:15", axis=1)

        psdf = ps.DataFrame({"A": [1, 2, 3, 4]})
        with self.assertRaisesRegex(TypeError, "Index must be DatetimeIndex"):
            psdf.at_time("0:15")

    def test_between_time(self):
        idx = pd.date_range("2018-04-09", periods=4, freq="1D20min")
        pdf = pd.DataFrame({"A": [1, 2, 3, 4]}, index=idx)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.between_time("0:15", "0:45").sort_index(),
            psdf.between_time("0:15", "0:45").sort_index(),
        )

        pdf.index.name = "ts"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.between_time("0:15", "0:45").sort_index(),
            psdf.between_time("0:15", "0:45").sort_index(),
        )

        # Column label is 'index'
        pdf.columns = pd.Index(["index"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.between_time("0:15", "0:45").sort_index(),
            psdf.between_time("0:15", "0:45").sort_index(),
        )

        # Both index name and column label are 'index'
        pdf.index.name = "index"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.between_time("0:15", "0:45").sort_index(),
            psdf.between_time("0:15", "0:45").sort_index(),
        )

        # Index name is 'index', column label is ('X', 'A')
        pdf.columns = pd.MultiIndex.from_arrays([["X"], ["A"]])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.between_time("0:15", "0:45").sort_index(),
            psdf.between_time("0:15", "0:45").sort_index(),
        )

        with self.assertRaisesRegex(
            NotImplementedError, "between_time currently only works for axis=0"
        ):
            psdf.between_time("0:15", "0:45", axis=1)

        psdf = ps.DataFrame({"A": [1, 2, 3, 4]})
        with self.assertRaisesRegex(TypeError, "Index must be DatetimeIndex"):
            psdf.between_time("0:15", "0:45")

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

    def test_duplicated(self):
        pdf = pd.DataFrame(
            {"a": [1, 1, 2, 3], "b": [1, 1, 1, 4], "c": [1, 1, 1, 5]}, index=np.random.rand(4)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.duplicated().sort_index(), psdf.duplicated().sort_index())
        self.assert_eq(
            pdf.duplicated(keep="last").sort_index(),
            psdf.duplicated(keep="last").sort_index(),
        )
        self.assert_eq(
            pdf.duplicated(keep=False).sort_index(),
            psdf.duplicated(keep=False).sort_index(),
        )
        self.assert_eq(
            pdf.duplicated(subset="b").sort_index(),
            psdf.duplicated(subset="b").sort_index(),
        )
        self.assert_eq(
            pdf.duplicated(subset=["b"]).sort_index(),
            psdf.duplicated(subset=["b"]).sort_index(),
        )
        with self.assertRaisesRegex(ValueError, "'keep' only supports 'first', 'last' and False"):
            psdf.duplicated(keep="false")
        with self.assertRaisesRegex(KeyError, "'d'"):
            psdf.duplicated(subset=["d"])

        pdf.index.name = "x"
        psdf.index.name = "x"
        self.assert_eq(pdf.duplicated().sort_index(), psdf.duplicated().sort_index())

        # multi-index
        self.assert_eq(
            pdf.set_index("a", append=True).duplicated().sort_index(),
            psdf.set_index("a", append=True).duplicated().sort_index(),
        )
        self.assert_eq(
            pdf.set_index("a", append=True).duplicated(keep=False).sort_index(),
            psdf.set_index("a", append=True).duplicated(keep=False).sort_index(),
        )
        self.assert_eq(
            pdf.set_index("a", append=True).duplicated(subset=["b"]).sort_index(),
            psdf.set_index("a", append=True).duplicated(subset=["b"]).sort_index(),
        )

        # mutli-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.duplicated().sort_index(), psdf.duplicated().sort_index())
        self.assert_eq(
            pdf.duplicated(subset=("x", "b")).sort_index(),
            psdf.duplicated(subset=("x", "b")).sort_index(),
        )
        self.assert_eq(
            pdf.duplicated(subset=[("x", "b")]).sort_index(),
            psdf.duplicated(subset=[("x", "b")]).sort_index(),
        )

        # non-string names
        pdf = pd.DataFrame(
            {10: [1, 1, 2, 3], 20: [1, 1, 1, 4], 30: [1, 1, 1, 5]}, index=np.random.rand(4)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.duplicated().sort_index(), psdf.duplicated().sort_index())
        self.assert_eq(
            pdf.duplicated(subset=10).sort_index(),
            psdf.duplicated(subset=10).sort_index(),
        )

    def test_filter(self):
        pdf = pd.DataFrame(
            {
                "aa": ["aa", "bd", "bc", "ab", "ce"],
                "ba": [1, 2, 3, 4, 5],
                "cb": [1.0, 2.0, 3.0, 4.0, 5.0],
                "db": [1.0, np.nan, 3.0, np.nan, 5.0],
            }
        )
        pdf = pdf.set_index("aa")
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.filter(items=["ab", "aa"], axis=0).sort_index(),
            pdf.filter(items=["ab", "aa"], axis=0).sort_index(),
        )

        with option_context("compute.isin_limit", 0):
            self.assert_eq(
                psdf.filter(items=["ab", "aa"], axis=0).sort_index(),
                pdf.filter(items=["ab", "aa"], axis=0).sort_index(),
            )

        self.assert_eq(
            psdf.filter(items=["ba", "db"], axis=1).sort_index(),
            pdf.filter(items=["ba", "db"], axis=1).sort_index(),
        )

        self.assert_eq(psdf.filter(like="b", axis="index"), pdf.filter(like="b", axis="index"))
        self.assert_eq(psdf.filter(like="c", axis="columns"), pdf.filter(like="c", axis="columns"))

        self.assert_eq(
            psdf.filter(regex="b.*", axis="index"), pdf.filter(regex="b.*", axis="index")
        )
        self.assert_eq(
            psdf.filter(regex="b.*", axis="columns"), pdf.filter(regex="b.*", axis="columns")
        )

        pdf = pdf.set_index("ba", append=True)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.filter(items=[("aa", 1), ("bd", 2)], axis=0).sort_index(),
            pdf.filter(items=[("aa", 1), ("bd", 2)], axis=0).sort_index(),
        )

        with self.assertRaisesRegex(TypeError, "Unsupported type list"):
            psdf.filter(items=[["aa", 1], ("bd", 2)], axis=0)

        with self.assertRaisesRegex(ValueError, "The item should not be empty."):
            psdf.filter(items=[(), ("bd", 2)], axis=0)

        self.assert_eq(psdf.filter(like="b", axis=0), pdf.filter(like="b", axis=0))

        self.assert_eq(psdf.filter(regex="b.*", axis=0), pdf.filter(regex="b.*", axis=0))

        with self.assertRaisesRegex(ValueError, "items should be a list-like object"):
            psdf.filter(items="b")

        with self.assertRaisesRegex(ValueError, "No axis named"):
            psdf.filter(regex="b.*", axis=123)

        with self.assertRaisesRegex(TypeError, "Must pass either `items`, `like`"):
            psdf.filter()

        with self.assertRaisesRegex(TypeError, "mutually exclusive"):
            psdf.filter(regex="b.*", like="aaa")

        # multi-index columns
        pdf = pd.DataFrame(
            {
                ("x", "aa"): ["aa", "ab", "bc", "bd", "ce"],
                ("x", "ba"): [1, 2, 3, 4, 5],
                ("y", "cb"): [1.0, 2.0, 3.0, 4.0, 5.0],
                ("z", "db"): [1.0, np.nan, 3.0, np.nan, 5.0],
            }
        )
        pdf = pdf.set_index(("x", "aa"))
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.filter(items=["ab", "aa"], axis=0).sort_index(),
            pdf.filter(items=["ab", "aa"], axis=0).sort_index(),
        )
        self.assert_eq(
            psdf.filter(items=[("x", "ba"), ("z", "db")], axis=1).sort_index(),
            pdf.filter(items=[("x", "ba"), ("z", "db")], axis=1).sort_index(),
        )

        self.assert_eq(psdf.filter(like="b", axis="index"), pdf.filter(like="b", axis="index"))
        self.assert_eq(psdf.filter(like="c", axis="columns"), pdf.filter(like="c", axis="columns"))

        self.assert_eq(
            psdf.filter(regex="b.*", axis="index"), pdf.filter(regex="b.*", axis="index")
        )
        self.assert_eq(
            psdf.filter(regex="b.*", axis="columns"), pdf.filter(regex="b.*", axis="columns")
        )

    def test_last(self):
        index = pd.date_range("2018-04-09", periods=4, freq="2D")
        pdf = pd.DataFrame([1, 2, 3, 4], index=index)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.last("1D"), psdf.last("1D"))
        self.assert_eq(pdf.last(DateOffset(days=1)), psdf.last(DateOffset(days=1)))
        with self.assertRaisesRegex(TypeError, "'last' only supports a DatetimeIndex"):
            ps.DataFrame([1, 2, 3, 4]).last("1D")

    def test_first(self):
        index = pd.date_range("2018-04-09", periods=4, freq="2D")
        pdf = pd.DataFrame([1, 2, 3, 4], index=index)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.first("1D"), psdf.first("1D"))
        self.assert_eq(pdf.first(DateOffset(days=1)), psdf.first(DateOffset(days=1)))
        with self.assertRaisesRegex(TypeError, "'first' only supports a DatetimeIndex"):
            ps.DataFrame([1, 2, 3, 4]).first("1D")

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

    def test_take(self):
        pdf = pd.DataFrame(
            {"A": range(0, 50000), "B": range(100000, 0, -2), "C": range(100000, 50000, -1)}
        )
        psdf = ps.from_pandas(pdf)

        # axis=0 (default)
        self.assert_eq(psdf.take([1, 2]).sort_index(), pdf.take([1, 2]).sort_index())
        self.assert_eq(psdf.take([-1, -2]).sort_index(), pdf.take([-1, -2]).sort_index())
        self.assert_eq(
            psdf.take(range(100, 110)).sort_index(), pdf.take(range(100, 110)).sort_index()
        )
        self.assert_eq(
            psdf.take(range(-110, -100)).sort_index(), pdf.take(range(-110, -100)).sort_index()
        )
        self.assert_eq(
            psdf.take([10, 100, 1000, 10000]).sort_index(),
            pdf.take([10, 100, 1000, 10000]).sort_index(),
        )
        self.assert_eq(
            psdf.take([-10, -100, -1000, -10000]).sort_index(),
            pdf.take([-10, -100, -1000, -10000]).sort_index(),
        )

        # axis=1
        self.assert_eq(
            psdf.take([1, 2], axis=1).sort_index(), pdf.take([1, 2], axis=1).sort_index()
        )
        self.assert_eq(
            psdf.take([-1, -2], axis=1).sort_index(), pdf.take([-1, -2], axis=1).sort_index()
        )
        self.assert_eq(
            psdf.take(range(1, 3), axis=1).sort_index(),
            pdf.take(range(1, 3), axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take(range(-1, -3), axis=1).sort_index(),
            pdf.take(range(-1, -3), axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take([2, 1], axis=1).sort_index(),
            pdf.take([2, 1], axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take([-1, -2], axis=1).sort_index(),
            pdf.take([-1, -2], axis=1).sort_index(),
        )

        # MultiIndex columns
        columns = pd.MultiIndex.from_tuples([("A", "Z"), ("B", "X"), ("C", "C")])
        psdf.columns = columns
        pdf.columns = columns

        # MultiIndex columns with axis=0 (default)
        self.assert_eq(psdf.take([1, 2]).sort_index(), pdf.take([1, 2]).sort_index())
        self.assert_eq(psdf.take([-1, -2]).sort_index(), pdf.take([-1, -2]).sort_index())
        self.assert_eq(
            psdf.take(range(100, 110)).sort_index(), pdf.take(range(100, 110)).sort_index()
        )
        self.assert_eq(
            psdf.take(range(-110, -100)).sort_index(), pdf.take(range(-110, -100)).sort_index()
        )
        self.assert_eq(
            psdf.take([10, 100, 1000, 10000]).sort_index(),
            pdf.take([10, 100, 1000, 10000]).sort_index(),
        )
        self.assert_eq(
            psdf.take([-10, -100, -1000, -10000]).sort_index(),
            pdf.take([-10, -100, -1000, -10000]).sort_index(),
        )

        # axis=1
        self.assert_eq(
            psdf.take([1, 2], axis=1).sort_index(), pdf.take([1, 2], axis=1).sort_index()
        )
        self.assert_eq(
            psdf.take([-1, -2], axis=1).sort_index(), pdf.take([-1, -2], axis=1).sort_index()
        )
        self.assert_eq(
            psdf.take(range(1, 3), axis=1).sort_index(),
            pdf.take(range(1, 3), axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take(range(-1, -3), axis=1).sort_index(),
            pdf.take(range(-1, -3), axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take([2, 1], axis=1).sort_index(),
            pdf.take([2, 1], axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take([-1, -2], axis=1).sort_index(),
            pdf.take([-1, -2], axis=1).sort_index(),
        )

        # Checking the type of indices.
        self.assertRaises(TypeError, lambda: psdf.take(1))
        self.assertRaises(TypeError, lambda: psdf.take("1"))
        self.assertRaises(TypeError, lambda: psdf.take({1, 2}))
        self.assertRaises(TypeError, lambda: psdf.take({1: None, 2: None}))

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

    def test_truncate(self):
        pdf1 = pd.DataFrame(
            {
                "A": ["a", "b", "c", "d", "e", "f", "g"],
                "B": ["h", "i", "j", "k", "l", "m", "n"],
                "C": ["o", "p", "q", "r", "s", "t", "u"],
            },
            index=[-500, -20, -1, 0, 400, 550, 1000],
        )
        psdf1 = ps.from_pandas(pdf1)
        pdf2 = pd.DataFrame(
            {
                "A": ["a", "b", "c", "d", "e", "f", "g"],
                "B": ["h", "i", "j", "k", "l", "m", "n"],
                "C": ["o", "p", "q", "r", "s", "t", "u"],
            },
            index=[1000, 550, 400, 0, -1, -20, -500],
        )
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(psdf1.truncate(), pdf1.truncate())
        self.assert_eq(psdf1.truncate(before=-20), pdf1.truncate(before=-20))
        self.assert_eq(psdf1.truncate(after=400), pdf1.truncate(after=400))
        self.assert_eq(psdf1.truncate(copy=False), pdf1.truncate(copy=False))
        self.assert_eq(psdf1.truncate(-20, 400, copy=False), pdf1.truncate(-20, 400, copy=False))
        # The bug for these tests has been fixed in pandas 1.1.0.
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            self.assert_eq(psdf2.truncate(0, 550), pdf2.truncate(0, 550))
            self.assert_eq(psdf2.truncate(0, 550, copy=False), pdf2.truncate(0, 550, copy=False))
        else:
            expected_psdf = ps.DataFrame(
                {"A": ["b", "c", "d"], "B": ["i", "j", "k"], "C": ["p", "q", "r"]},
                index=[550, 400, 0],
            )
            self.assert_eq(psdf2.truncate(0, 550), expected_psdf)
            self.assert_eq(psdf2.truncate(0, 550, copy=False), expected_psdf)

        # axis = 1
        self.assert_eq(psdf1.truncate(axis=1), pdf1.truncate(axis=1))
        self.assert_eq(psdf1.truncate(before="B", axis=1), pdf1.truncate(before="B", axis=1))
        self.assert_eq(psdf1.truncate(after="A", axis=1), pdf1.truncate(after="A", axis=1))
        self.assert_eq(psdf1.truncate(copy=False, axis=1), pdf1.truncate(copy=False, axis=1))
        self.assert_eq(psdf2.truncate("B", "C", axis=1), pdf2.truncate("B", "C", axis=1))
        self.assert_eq(
            psdf1.truncate("B", "C", copy=False, axis=1),
            pdf1.truncate("B", "C", copy=False, axis=1),
        )

        # MultiIndex columns
        columns = pd.MultiIndex.from_tuples([("A", "Z"), ("B", "X"), ("C", "Z")])
        pdf1.columns = columns
        psdf1.columns = columns
        pdf2.columns = columns
        psdf2.columns = columns

        self.assert_eq(psdf1.truncate(), pdf1.truncate())
        self.assert_eq(psdf1.truncate(before=-20), pdf1.truncate(before=-20))
        self.assert_eq(psdf1.truncate(after=400), pdf1.truncate(after=400))
        self.assert_eq(psdf1.truncate(copy=False), pdf1.truncate(copy=False))
        self.assert_eq(psdf1.truncate(-20, 400, copy=False), pdf1.truncate(-20, 400, copy=False))
        # The bug for these tests has been fixed in pandas 1.1.0.
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            self.assert_eq(psdf2.truncate(0, 550), pdf2.truncate(0, 550))
            self.assert_eq(psdf2.truncate(0, 550, copy=False), pdf2.truncate(0, 550, copy=False))
        else:
            expected_psdf.columns = columns
            self.assert_eq(psdf2.truncate(0, 550), expected_psdf)
            self.assert_eq(psdf2.truncate(0, 550, copy=False), expected_psdf)
        # axis = 1
        self.assert_eq(psdf1.truncate(axis=1), pdf1.truncate(axis=1))
        self.assert_eq(psdf1.truncate(before="B", axis=1), pdf1.truncate(before="B", axis=1))
        self.assert_eq(psdf1.truncate(after="A", axis=1), pdf1.truncate(after="A", axis=1))
        self.assert_eq(psdf1.truncate(copy=False, axis=1), pdf1.truncate(copy=False, axis=1))
        self.assert_eq(psdf2.truncate("B", "C", axis=1), pdf2.truncate("B", "C", axis=1))
        self.assert_eq(
            psdf1.truncate("B", "C", copy=False, axis=1),
            pdf1.truncate("B", "C", copy=False, axis=1),
        )

        # Exceptions
        psdf = ps.DataFrame(
            {
                "A": ["a", "b", "c", "d", "e", "f", "g"],
                "B": ["h", "i", "j", "k", "l", "m", "n"],
                "C": ["o", "p", "q", "r", "s", "t", "u"],
            },
            index=[-500, 100, 400, 0, -1, 550, -20],
        )
        msg = "truncate requires a sorted index"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.truncate()

        psdf = ps.DataFrame(
            {
                "A": ["a", "b", "c", "d", "e", "f", "g"],
                "B": ["h", "i", "j", "k", "l", "m", "n"],
                "C": ["o", "p", "q", "r", "s", "t", "u"],
            },
            index=[-500, -20, -1, 0, 400, 550, 1000],
        )
        msg = "Truncate: -20 must be after 400"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.truncate(400, -20)
        msg = "Truncate: B must be after C"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.truncate("C", "B", axis=1)

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


class FrameReidexingTests(FrameReindexingMixin, ComparisonTestBase, SQLTestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_frame_reindexing import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
