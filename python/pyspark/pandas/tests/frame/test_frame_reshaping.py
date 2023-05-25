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

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.utils import name_like_string

from pyspark.testing.pandasutils import ComparisonTestBase
from pyspark.testing.sqlutils import SQLTestUtils


# This file contains test cases for 'Reshaping, sorting, transposing'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#reshaping-sorting-transposing
class FrameReshapingMixin:
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

    def test_explode(self):
        pdf = pd.DataFrame(
            {"A": [[-1.0, np.nan], [0.0, np.inf], [1.0, -np.inf]], "B": 1}, index=["a", "b", "c"]
        )
        pdf.index.name = "index"
        pdf.columns.name = "columns"
        psdf = ps.from_pandas(pdf)

        expected_result1, result1 = pdf.explode("A"), psdf.explode("A")
        expected_result2, result2 = pdf.explode("B"), psdf.explode("B")
        expected_result3, result3 = pdf.explode("A", ignore_index=True), psdf.explode(
            "A", ignore_index=True
        )

        self.assert_eq(result1, expected_result1, almost=True)
        self.assert_eq(result2, expected_result2)
        self.assert_eq(result1.index.name, expected_result1.index.name)
        self.assert_eq(result1.columns.name, expected_result1.columns.name)
        self.assert_eq(result3, expected_result3, almost=True)
        self.assert_eq(result3.index, expected_result3.index)

        self.assertRaises(TypeError, lambda: psdf.explode(["A", "B"]))

        # MultiIndex
        midx = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "b"), ("y", "c")], names=["index1", "index2"]
        )
        pdf.index = midx
        psdf = ps.from_pandas(pdf)

        expected_result1, result1 = pdf.explode("A"), psdf.explode("A")
        expected_result2, result2 = pdf.explode("B"), psdf.explode("B")
        expected_result3, result3 = pdf.explode("A", ignore_index=True), psdf.explode(
            "A", ignore_index=True
        )

        self.assert_eq(result1, expected_result1, almost=True)
        self.assert_eq(result2, expected_result2)
        self.assert_eq(result1.index.names, expected_result1.index.names)
        self.assert_eq(result1.columns.name, expected_result1.columns.name)
        self.assert_eq(result3, expected_result3, almost=True)
        self.assert_eq(result3.index, expected_result3.index)

        self.assertRaises(TypeError, lambda: psdf.explode(["A", "B"]))

        # MultiIndex columns
        columns = pd.MultiIndex.from_tuples([("A", "Z"), ("B", "X")], names=["column1", "column2"])
        pdf.columns = columns
        psdf.columns = columns

        expected_result1, result1 = pdf.explode(("A", "Z")), psdf.explode(("A", "Z"))
        expected_result2, result2 = pdf.explode(("B", "X")), psdf.explode(("B", "X"))
        expected_result3, result3 = pdf.A.explode("Z"), psdf.A.explode("Z")

        self.assert_eq(result1, expected_result1, almost=True)
        self.assert_eq(result2, expected_result2)
        self.assert_eq(result1.index.names, expected_result1.index.names)
        self.assert_eq(result1.columns.names, expected_result1.columns.names)
        self.assert_eq(result3, expected_result3, almost=True)

        self.assertRaises(TypeError, lambda: psdf.explode(["A", "B"]))
        self.assertRaises(ValueError, lambda: psdf.explode("A"))

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

    def test_squeeze(self):
        axises = [None, 0, 1, "rows", "index", "columns"]

        # Multiple columns
        pdf = pd.DataFrame([[1, 2], [3, 4]], columns=["a", "b"], index=["x", "y"])
        psdf = ps.from_pandas(pdf)
        for axis in axises:
            self.assert_eq(pdf.squeeze(axis), psdf.squeeze(axis))
        # Multiple columns with MultiIndex columns
        columns = pd.MultiIndex.from_tuples([("A", "Z"), ("B", "X")])
        pdf.columns = columns
        psdf.columns = columns
        for axis in axises:
            self.assert_eq(pdf.squeeze(axis), psdf.squeeze(axis))

        # Single column with single value
        pdf = pd.DataFrame([[1]], columns=["a"], index=["x"])
        psdf = ps.from_pandas(pdf)
        for axis in axises:
            self.assert_eq(pdf.squeeze(axis), psdf.squeeze(axis))
        # Single column with single value with MultiIndex column
        columns = pd.MultiIndex.from_tuples([("A", "Z")])
        pdf.columns = columns
        psdf.columns = columns
        for axis in axises:
            self.assert_eq(pdf.squeeze(axis), psdf.squeeze(axis))

        # Single column with multiple values
        pdf = pd.DataFrame([1, 2, 3, 4], columns=["a"])
        psdf = ps.from_pandas(pdf)
        for axis in axises:
            self.assert_eq(pdf.squeeze(axis), psdf.squeeze(axis))
        # Single column with multiple values with MultiIndex column
        pdf.columns = columns
        psdf.columns = columns
        for axis in axises:
            self.assert_eq(pdf.squeeze(axis), psdf.squeeze(axis))


class FrameReshapingTests(FrameReshapingMixin, ComparisonTestBase, SQLTestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_frame_reshaping import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
