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
import decimal
from datetime import datetime
from distutils.version import LooseVersion
import sys
import unittest
from io import StringIO
from typing import List

import numpy as np
import pandas as pd
from pandas.tseries.offsets import DateOffset
from pyspark import StorageLevel
from pyspark.ml.linalg import SparseVector
from pyspark.sql.types import StructType

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.frame import CachedDataFrame
from pyspark.testing.pandasutils import (
    have_tabulate,
    ComparisonTestBase,
    SPARK_CONF_ARROW_ENABLED,
    tabulate_requirement_message,
)
from pyspark.testing.sqlutils import SQLTestUtils


class DataFrameSlowTest(ComparisonTestBase, SQLTestUtils):
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

    def test_rank(self):
        pdf = pd.DataFrame(
            data={"col1": [1, 2, 3, 1], "col2": [3, 4, 3, 1]},
            columns=["col1", "col2"],
            index=np.random.rand(4),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.rank().sort_index(), psdf.rank().sort_index())
        self.assert_eq(pdf.rank().sum(), psdf.rank().sum())
        self.assert_eq(
            pdf.rank(ascending=False).sort_index(), psdf.rank(ascending=False).sort_index()
        )
        self.assert_eq(pdf.rank(method="min").sort_index(), psdf.rank(method="min").sort_index())
        self.assert_eq(pdf.rank(method="max").sort_index(), psdf.rank(method="max").sort_index())
        self.assert_eq(
            pdf.rank(method="first").sort_index(), psdf.rank(method="first").sort_index()
        )
        self.assert_eq(
            pdf.rank(method="dense").sort_index(), psdf.rank(method="dense").sort_index()
        )

        msg = "method must be one of 'average', 'min', 'max', 'first', 'dense'"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.rank(method="nothing")

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "col1"), ("y", "col2")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.rank().sort_index(), psdf.rank().sort_index())

        # non-numeric columns
        pdf = pd.DataFrame(
            data={"col1": [1, 2, 3, 1], "col2": ["a", "b", "c", "d"]},
            index=np.random.rand(4),
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.rank(numeric_only=True).sort_index(), psdf.rank(numeric_only=True).sort_index()
        )
        self.assert_eq(
            pdf.rank(numeric_only=False).sort_index(), psdf.rank(numeric_only=False).sort_index()
        )
        self.assert_eq(
            pdf.rank(numeric_only=None).sort_index(), psdf.rank(numeric_only=None).sort_index()
        )
        self.assert_eq(
            pdf[["col2"]].rank(numeric_only=True),
            psdf[["col2"]].rank(numeric_only=True),
        )

    def test_round(self):
        pdf = pd.DataFrame(
            {
                "A": [0.028208, 0.038683, 0.877076],
                "B": [0.992815, 0.645646, 0.149370],
                "C": [0.173891, 0.577595, 0.491027],
            },
            columns=["A", "B", "C"],
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)

        pser = pd.Series([1, 0, 2], index=["A", "B", "C"])
        psser = ps.Series([1, 0, 2], index=["A", "B", "C"])
        self.assert_eq(pdf.round(2), psdf.round(2))
        self.assert_eq(pdf.round({"A": 1, "C": 2}), psdf.round({"A": 1, "C": 2}))
        self.assert_eq(pdf.round({"A": 1, "D": 2}), psdf.round({"A": 1, "D": 2}))
        self.assert_eq(pdf.round(pser), psdf.round(psser))
        msg = "decimals must be an integer, a dict-like or a Series"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.round(1.5)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
        pdf.columns = columns
        psdf.columns = columns
        pser = pd.Series([1, 0, 2], index=columns)
        psser = ps.Series([1, 0, 2], index=columns)
        self.assert_eq(pdf.round(2), psdf.round(2))
        self.assert_eq(
            pdf.round({("X", "A"): 1, ("Y", "C"): 2}), psdf.round({("X", "A"): 1, ("Y", "C"): 2})
        )
        self.assert_eq(pdf.round({("X", "A"): 1, "Y": 2}), psdf.round({("X", "A"): 1, "Y": 2}))
        self.assert_eq(pdf.round(pser), psdf.round(psser))

        # non-string names
        pdf = pd.DataFrame(
            {
                10: [0.028208, 0.038683, 0.877076],
                20: [0.992815, 0.645646, 0.149370],
                30: [0.173891, 0.577595, 0.491027],
            },
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.round({10: 1, 30: 2}), psdf.round({10: 1, 30: 2}))

    def test_shift(self):
        pdf = pd.DataFrame(
            {
                "Col1": [10, 20, 15, 30, 45],
                "Col2": [13, 23, 18, 33, 48],
                "Col3": [17, 27, 22, 37, 52],
            },
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.shift(3), psdf.shift(3))
        self.assert_eq(pdf.shift().shift(-1), psdf.shift().shift(-1))
        self.assert_eq(pdf.shift().sum().astype(int), psdf.shift().sum())

        # Need the expected result since pandas 0.23 does not support `fill_value` argument.
        pdf1 = pd.DataFrame(
            {"Col1": [0, 0, 0, 10, 20], "Col2": [0, 0, 0, 13, 23], "Col3": [0, 0, 0, 17, 27]},
            index=pdf.index,
        )
        self.assert_eq(pdf1, psdf.shift(periods=3, fill_value=0))
        msg = "should be an int"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.shift(1.5)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "Col1"), ("x", "Col2"), ("y", "Col3")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.shift(3), psdf.shift(3))
        self.assert_eq(pdf.shift().shift(-1), psdf.shift().shift(-1))
        self.assert_eq(pdf.shift(0), psdf.shift(0))

    def test_diff(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.diff(), psdf.diff())
        self.assert_eq(pdf.diff().diff(-1), psdf.diff().diff(-1))
        self.assert_eq(pdf.diff().sum().astype(int), psdf.diff().sum())

        msg = "should be an int"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.diff(1.5)
        msg = 'axis should be either 0 or "index" currently.'
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.diff(axis=1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "Col1"), ("x", "Col2"), ("y", "Col3")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(pdf.diff(), psdf.diff())

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

    def test_ffill(self):
        idx = np.random.rand(6)
        pdf = pd.DataFrame(
            {
                "x": [np.nan, 2, 3, 4, np.nan, 6],
                "y": [1, 2, np.nan, 4, np.nan, np.nan],
                "z": [1, 2, 3, 4, np.nan, np.nan],
            },
            index=idx,
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.ffill(), pdf.ffill())
        self.assert_eq(psdf.ffill(limit=1), pdf.ffill(limit=1))

        pser = pdf.y
        psser = psdf.y

        psdf.ffill(inplace=True)
        pdf.ffill(inplace=True)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psser, pser)
        self.assert_eq(psser[idx[2]], pser[idx[2]])

    def test_bfill(self):
        idx = np.random.rand(6)
        pdf = pd.DataFrame(
            {
                "x": [np.nan, 2, 3, 4, np.nan, 6],
                "y": [1, 2, np.nan, 4, np.nan, np.nan],
                "z": [1, 2, 3, 4, np.nan, np.nan],
            },
            index=idx,
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.bfill(), pdf.bfill())
        self.assert_eq(psdf.bfill(limit=1), pdf.bfill(limit=1))

        pser = pdf.x
        psser = psdf.x

        psdf.bfill(inplace=True)
        pdf.bfill(inplace=True)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psser, pser)
        self.assert_eq(psser[idx[0]], pser[idx[0]])

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

    def test_pipe(self):
        psdf = ps.DataFrame(
            {"category": ["A", "A", "B"], "col1": [1, 2, 3], "col2": [4, 5, 6]},
            columns=["category", "col1", "col2"],
        )

        self.assertRaisesRegex(
            ValueError,
            "arg is both the pipe target and a keyword argument",
            lambda: psdf.pipe((lambda x: x, "arg"), arg="1"),
        )

    def test_transform(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 100,
                "b": [1.0, 1.0, 2.0, 3.0, 5.0, 8.0] * 100,
                "c": [1, 4, 9, 16, 25, 36] * 100,
            },
            columns=["a", "b", "c"],
            index=np.random.rand(600),
        )
        psdf = ps.DataFrame(pdf)
        self.assert_eq(
            psdf.transform(lambda x: x + 1).sort_index(),
            pdf.transform(lambda x: x + 1).sort_index(),
        )
        self.assert_eq(
            psdf.transform(lambda x, y: x + y, y=2).sort_index(),
            pdf.transform(lambda x, y: x + y, y=2).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.transform(lambda x: x + 1).sort_index(),
                pdf.transform(lambda x: x + 1).sort_index(),
            )
            self.assert_eq(
                psdf.transform(lambda x, y: x + y, y=1).sort_index(),
                pdf.transform(lambda x, y: x + y, y=1).sort_index(),
            )

        with self.assertRaisesRegex(AssertionError, "the first argument should be a callable"):
            psdf.transform(1)
        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psdf.transform(lambda x: x + 1, axis=1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.transform(lambda x: x + 1).sort_index(),
            pdf.transform(lambda x: x + 1).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.transform(lambda x: x + 1).sort_index(),
                pdf.transform(lambda x: x + 1).sort_index(),
            )

    def test_apply(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 100,
                "b": [1.0, 1.0, 2.0, 3.0, 5.0, 8.0] * 100,
                "c": [1, 4, 9, 16, 25, 36] * 100,
            },
            columns=["a", "b", "c"],
            index=np.random.rand(600),
        )
        psdf = ps.DataFrame(pdf)

        self.assert_eq(
            psdf.apply(lambda x: x + 1).sort_index(), pdf.apply(lambda x: x + 1).sort_index()
        )
        self.assert_eq(
            psdf.apply(lambda x, b: x + b, args=(1,)).sort_index(),
            pdf.apply(lambda x, b: x + b, args=(1,)).sort_index(),
        )
        self.assert_eq(
            psdf.apply(lambda x, b: x + b, b=1).sort_index(),
            pdf.apply(lambda x, b: x + b, b=1).sort_index(),
        )

        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.apply(lambda x: x + 1).sort_index(), pdf.apply(lambda x: x + 1).sort_index()
            )
            self.assert_eq(
                psdf.apply(lambda x, b: x + b, args=(1,)).sort_index(),
                pdf.apply(lambda x, b: x + b, args=(1,)).sort_index(),
            )
            self.assert_eq(
                psdf.apply(lambda x, b: x + b, b=1).sort_index(),
                pdf.apply(lambda x, b: x + b, b=1).sort_index(),
            )

        # returning a Series
        self.assert_eq(
            psdf.apply(lambda x: len(x), axis=1).sort_index(),
            pdf.apply(lambda x: len(x), axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.apply(lambda x, c: len(x) + c, axis=1, c=100).sort_index(),
            pdf.apply(lambda x, c: len(x) + c, axis=1, c=100).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.apply(lambda x: len(x), axis=1).sort_index(),
                pdf.apply(lambda x: len(x), axis=1).sort_index(),
            )
            self.assert_eq(
                psdf.apply(lambda x, c: len(x) + c, axis=1, c=100).sort_index(),
                pdf.apply(lambda x, c: len(x) + c, axis=1, c=100).sort_index(),
            )

        with self.assertRaisesRegex(AssertionError, "the first argument should be a callable"):
            psdf.apply(1)

        with self.assertRaisesRegex(TypeError, "The given function.*1 or 'column'; however"):

            def f1(_) -> ps.DataFrame[int]:
                pass

            psdf.apply(f1, axis=0)

        with self.assertRaisesRegex(TypeError, "The given function.*0 or 'index'; however"):

            def f2(_) -> ps.Series[int]:
                pass

            psdf.apply(f2, axis=1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.apply(lambda x: x + 1).sort_index(), pdf.apply(lambda x: x + 1).sort_index()
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.apply(lambda x: x + 1).sort_index(), pdf.apply(lambda x: x + 1).sort_index()
            )

        # returning a Series
        self.assert_eq(
            psdf.apply(lambda x: len(x), axis=1).sort_index(),
            pdf.apply(lambda x: len(x), axis=1).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.apply(lambda x: len(x), axis=1).sort_index(),
                pdf.apply(lambda x: len(x), axis=1).sort_index(),
            )

    def test_apply_with_type(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)

        def identify1(x) -> ps.DataFrame[int, int]:
            return x

        # Type hints set the default column names, and we use default index for
        # pandas API on Spark. Here we ignore both diff.
        actual = psdf.apply(identify1, axis=1)
        expected = pdf.apply(identify1, axis=1)
        self.assert_eq(sorted(actual["c0"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["c1"].to_numpy()), sorted(expected["b"].to_numpy()))

        def identify2(x) -> ps.DataFrame[slice("a", int), slice("b", int)]:  # noqa: F405
            return x

        actual = psdf.apply(identify2, axis=1)
        expected = pdf.apply(identify2, axis=1)
        self.assert_eq(sorted(actual["a"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["b"].to_numpy()), sorted(expected["b"].to_numpy()))

    def test_apply_batch(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 100,
                "b": [1.0, 1.0, 2.0, 3.0, 5.0, 8.0] * 100,
                "c": [1, 4, 9, 16, 25, 36] * 100,
            },
            columns=["a", "b", "c"],
            index=np.random.rand(600),
        )
        psdf = ps.DataFrame(pdf)

        self.assert_eq(
            psdf.pandas_on_spark.apply_batch(lambda pdf, a: pdf + a, args=(1,)).sort_index(),
            (pdf + 1).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.pandas_on_spark.apply_batch(lambda pdf: pdf + 1).sort_index(),
                (pdf + 1).sort_index(),
            )
            self.assert_eq(
                psdf.pandas_on_spark.apply_batch(lambda pdf, b: pdf + b, b=1).sort_index(),
                (pdf + 1).sort_index(),
            )

        with self.assertRaisesRegex(AssertionError, "the first argument should be a callable"):
            psdf.pandas_on_spark.apply_batch(1)

        with self.assertRaisesRegex(TypeError, "The given function.*frame as its type hints"):

            def f2(_) -> ps.Series[int]:
                pass

            psdf.pandas_on_spark.apply_batch(f2)

        with self.assertRaisesRegex(ValueError, "The given function should return a frame"):
            psdf.pandas_on_spark.apply_batch(lambda pdf: 1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.pandas_on_spark.apply_batch(lambda x: x + 1).sort_index(), (pdf + 1).sort_index()
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.pandas_on_spark.apply_batch(lambda x: x + 1).sort_index(),
                (pdf + 1).sort_index(),
            )

    def test_apply_batch_with_type(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)

        def identify1(x) -> ps.DataFrame[int, int]:
            return x

        # Type hints set the default column names, and we use default index for
        # pandas API on Spark. Here we ignore both diff.
        actual = psdf.pandas_on_spark.apply_batch(identify1)
        expected = pdf
        self.assert_eq(sorted(actual["c0"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["c1"].to_numpy()), sorted(expected["b"].to_numpy()))

        def identify2(x) -> ps.DataFrame[slice("a", int), slice("b", int)]:  # noqa: F405
            return x

        actual = psdf.pandas_on_spark.apply_batch(identify2)
        expected = pdf
        self.assert_eq(sorted(actual["a"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["b"].to_numpy()), sorted(expected["b"].to_numpy()))

        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [[e] for e in [4, 5, 6, 3, 2, 1, 0, 0, 0]]},
            index=np.random.rand(9),
        )
        psdf = ps.from_pandas(pdf)

        def identify3(x) -> ps.DataFrame[float, [int, List[int]]]:
            return x

        actual = psdf.pandas_on_spark.apply_batch(identify3)
        actual.columns = ["a", "b"]
        self.assert_eq(actual, pdf)

        # For NumPy typing, NumPy version should be 1.21+ and Python version should be 3.8+
        if sys.version_info >= (3, 8) and LooseVersion(np.__version__) >= LooseVersion("1.21"):
            import numpy.typing as ntp

            psdf = ps.from_pandas(pdf)

            def identify4(
                x,
            ) -> ps.DataFrame[float, [int, ntp.NDArray[int]]]:
                return x

            actual = psdf.pandas_on_spark.apply_batch(identify4)
            actual.columns = ["a", "b"]
            self.assert_eq(actual, pdf)

        arrays = [[1, 2, 3, 4, 5, 6, 7, 8, 9], ["a", "b", "c", "d", "e", "f", "g", "h", "i"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [[e] for e in [4, 5, 6, 3, 2, 1, 0, 0, 0]]},
            index=idx,
        )
        psdf = ps.from_pandas(pdf)

        def identify4(x) -> ps.DataFrame[[int, str], [int, List[int]]]:
            return x

        actual = psdf.pandas_on_spark.apply_batch(identify4)
        actual.index.names = ["number", "color"]
        actual.columns = ["a", "b"]
        self.assert_eq(actual, pdf)

        def identify5(
            x,
        ) -> ps.DataFrame[
            [("number", int), ("color", str)], [("a", int), ("b", List[int])]  # noqa: F405
        ]:
            return x

        actual = psdf.pandas_on_spark.apply_batch(identify5)
        self.assert_eq(actual, pdf)

    def test_transform_batch(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 100,
                "b": [1.0, 1.0, 2.0, 3.0, 5.0, 8.0] * 100,
                "c": [1, 4, 9, 16, 25, 36] * 100,
            },
            columns=["a", "b", "c"],
            index=np.random.rand(600),
        )
        psdf = ps.DataFrame(pdf)

        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda pdf: pdf.c + 1).sort_index(),
            (pdf.c + 1).sort_index(),
        )
        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda pdf, a: pdf + a, 1).sort_index(),
            (pdf + 1).sort_index(),
        )
        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda pdf, a: pdf.c + a, a=1).sort_index(),
            (pdf.c + 1).sort_index(),
        )

        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.pandas_on_spark.transform_batch(lambda pdf: pdf + 1).sort_index(),
                (pdf + 1).sort_index(),
            )
            self.assert_eq(
                psdf.pandas_on_spark.transform_batch(lambda pdf: pdf.b + 1).sort_index(),
                (pdf.b + 1).sort_index(),
            )
            self.assert_eq(
                psdf.pandas_on_spark.transform_batch(lambda pdf, a: pdf + a, 1).sort_index(),
                (pdf + 1).sort_index(),
            )
            self.assert_eq(
                psdf.pandas_on_spark.transform_batch(lambda pdf, a: pdf.c + a, a=1).sort_index(),
                (pdf.c + 1).sort_index(),
            )

        with self.assertRaisesRegex(AssertionError, "the first argument should be a callable"):
            psdf.pandas_on_spark.transform_batch(1)

        with self.assertRaisesRegex(ValueError, "The given function should return a frame"):
            psdf.pandas_on_spark.transform_batch(lambda pdf: 1)

        with self.assertRaisesRegex(
            ValueError, "transform_batch cannot produce aggregated results"
        ):
            psdf.pandas_on_spark.transform_batch(lambda pdf: pd.Series(1))

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda x: x + 1).sort_index(),
            (pdf + 1).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.pandas_on_spark.transform_batch(lambda x: x + 1).sort_index(),
                (pdf + 1).sort_index(),
            )

    def test_transform_batch_with_type(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)

        def identify1(x) -> ps.DataFrame[int, int]:
            return x

        # Type hints set the default column names, and we use default index for
        # pandas API on Spark. Here we ignore both diff.
        actual = psdf.pandas_on_spark.transform_batch(identify1)
        expected = pdf
        self.assert_eq(sorted(actual["c0"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["c1"].to_numpy()), sorted(expected["b"].to_numpy()))

        def identify2(x) -> ps.DataFrame[slice("a", int), slice("b", int)]:  # noqa: F405
            return x

        actual = psdf.pandas_on_spark.transform_batch(identify2)
        expected = pdf
        self.assert_eq(sorted(actual["a"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["b"].to_numpy()), sorted(expected["b"].to_numpy()))

    def test_transform_batch_same_anchor(self):
        psdf = ps.range(10)
        psdf["d"] = psdf.pandas_on_spark.transform_batch(lambda pdf: pdf.id + 1)
        self.assert_eq(
            psdf,
            pd.DataFrame({"id": list(range(10)), "d": list(range(1, 11))}, columns=["id", "d"]),
        )

        psdf = ps.range(10)

        def plus_one(pdf) -> ps.Series[np.int64]:
            return pdf.id + 1

        psdf["d"] = psdf.pandas_on_spark.transform_batch(plus_one)
        self.assert_eq(
            psdf,
            pd.DataFrame({"id": list(range(10)), "d": list(range(1, 11))}, columns=["id", "d"]),
        )

        psdf = ps.range(10)

        def plus_one(ser) -> ps.Series[np.int64]:
            return ser + 1

        psdf["d"] = psdf.id.pandas_on_spark.transform_batch(plus_one)
        self.assert_eq(
            psdf,
            pd.DataFrame({"id": list(range(10)), "d": list(range(1, 11))}, columns=["id", "d"]),
        )

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

    def test_to_spark(self):
        psdf = ps.from_pandas(self.pdf)

        with self.assertRaisesRegex(ValueError, "'index_col' cannot be overlapped"):
            psdf.to_spark(index_col="a")

        with self.assertRaisesRegex(ValueError, "length of index columns.*1.*3"):
            psdf.to_spark(index_col=["x", "y", "z"])

    def test_keys(self):
        pdf = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.keys(), pdf.keys())

    def test_quantile(self):
        pdf, psdf = self.df_pair

        self.assert_eq(psdf.quantile(0.5), pdf.quantile(0.5))
        self.assert_eq(psdf.quantile([0.25, 0.5, 0.75]), pdf.quantile([0.25, 0.5, 0.75]))

        self.assert_eq(psdf.loc[[]].quantile(0.5), pdf.loc[[]].quantile(0.5))
        self.assert_eq(
            psdf.loc[[]].quantile([0.25, 0.5, 0.75]), pdf.loc[[]].quantile([0.25, 0.5, 0.75])
        )

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psdf.quantile(0.5, axis=1)
        with self.assertRaisesRegex(TypeError, "accuracy must be an integer; however"):
            psdf.quantile(accuracy="a")
        with self.assertRaisesRegex(TypeError, "q must be a float or an array of floats;"):
            psdf.quantile(q="a")
        with self.assertRaisesRegex(TypeError, "q must be a float or an array of floats;"):
            psdf.quantile(q=["a"])
        with self.assertRaisesRegex(
            ValueError, r"percentiles should all be in the interval \[0, 1\]"
        ):
            psdf.quantile(q=[1.1])

        self.assert_eq(
            psdf.quantile(0.5, numeric_only=False), pdf.quantile(0.5, numeric_only=False)
        )
        self.assert_eq(
            psdf.quantile([0.25, 0.5, 0.75], numeric_only=False),
            pdf.quantile([0.25, 0.5, 0.75], numeric_only=False),
        )

        # multi-index column
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.quantile(0.5), pdf.quantile(0.5))
        self.assert_eq(psdf.quantile([0.25, 0.5, 0.75]), pdf.quantile([0.25, 0.5, 0.75]))

        pdf = pd.DataFrame({"x": ["a", "b", "c"]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.quantile(0.5), pdf.quantile(0.5))
        self.assert_eq(psdf.quantile([0.25, 0.5, 0.75]), pdf.quantile([0.25, 0.5, 0.75]))

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            psdf.quantile(0.5, numeric_only=False)
        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            psdf.quantile([0.25, 0.5, 0.75], numeric_only=False)

    def test_pct_change(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 2], "b": [4.0, 2.0, 3.0, 1.0], "c": [300, 200, 400, 200]},
            index=np.random.rand(4),
        )
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.pct_change(2), pdf.pct_change(2), check_exact=False)
        self.assert_eq(psdf.pct_change().sum(), pdf.pct_change().sum(), check_exact=False)

    def test_where(self):
        pdf, psdf = self.df_pair

        # pandas requires `axis` argument when the `other` is Series.
        # `axis` is not fully supported yet in pandas-on-Spark.
        self.assert_eq(
            psdf.where(psdf > 2, psdf.a + 10, axis=0), pdf.where(pdf > 2, pdf.a + 10, axis=0)
        )

        with self.assertRaisesRegex(TypeError, "type of cond must be a DataFrame or Series"):
            psdf.where(1)
        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psdf.where(psdf > 2, psdf.a + 10, axis=1)

    def test_mask(self):
        psdf = ps.from_pandas(self.pdf)

        with self.assertRaisesRegex(TypeError, "type of cond must be a DataFrame or Series"):
            psdf.mask(1)

    def test_query(self):
        pdf = pd.DataFrame({"A": range(1, 6), "B": range(10, 0, -2), "C": range(10, 5, -1)})
        psdf = ps.from_pandas(pdf)

        exprs = ("A > B", "A < C", "C == B")
        for expr in exprs:
            self.assert_eq(psdf.query(expr), pdf.query(expr))

        # test `inplace=True`
        for expr in exprs:
            dummy_psdf = psdf.copy()
            dummy_pdf = pdf.copy()

            pser = dummy_pdf.A
            psser = dummy_psdf.A
            dummy_pdf.query(expr, inplace=True)
            dummy_psdf.query(expr, inplace=True)

            self.assert_eq(dummy_psdf, dummy_pdf)
            self.assert_eq(psser, pser)

        # invalid values for `expr`
        invalid_exprs = (1, 1.0, (exprs[0],), [exprs[0]])
        for expr in invalid_exprs:
            with self.assertRaisesRegex(
                TypeError,
                "expr must be a string to be evaluated, {} given".format(type(expr).__name__),
            ):
                psdf.query(expr)

        # invalid values for `inplace`
        invalid_inplaces = (1, 0, "True", "False")
        for inplace in invalid_inplaces:
            with self.assertRaisesRegex(
                TypeError,
                'For argument "inplace" expected type bool, received type {}.'.format(
                    type(inplace).__name__
                ),
            ):
                psdf.query("a < b", inplace=inplace)

        # doesn't support for MultiIndex columns
        columns = pd.MultiIndex.from_tuples([("A", "Z"), ("B", "X"), ("C", "C")])
        psdf.columns = columns
        with self.assertRaisesRegex(TypeError, "Doesn't support for MultiIndex columns"):
            psdf.query("('A', 'Z') > ('B', 'X')")

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

    def test_axes(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.axes, psdf.axes)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.axes, psdf.axes)

    def test_udt(self):
        sparse_values = {0: 0.1, 1: 1.1}
        sparse_vector = SparseVector(len(sparse_values), sparse_values)
        pdf = pd.DataFrame({"a": [sparse_vector], "b": [10]})

        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf, pdf)

    def test_eval(self):
        pdf = pd.DataFrame({"A": range(1, 6), "B": range(10, 0, -2)})
        psdf = ps.from_pandas(pdf)

        # operation between columns (returns Series)
        self.assert_eq(pdf.eval("A + B"), psdf.eval("A + B"))
        self.assert_eq(pdf.eval("A + A"), psdf.eval("A + A"))
        # assignment (returns DataFrame)
        self.assert_eq(pdf.eval("C = A + B"), psdf.eval("C = A + B"))
        self.assert_eq(pdf.eval("A = A + A"), psdf.eval("A = A + A"))
        # operation between scalars (returns scalar)
        self.assert_eq(pdf.eval("1 + 1"), psdf.eval("1 + 1"))
        # complicated operations with assignment
        self.assert_eq(
            pdf.eval("B = A + B // (100 + 200) * (500 - B) - 10.5"),
            psdf.eval("B = A + B // (100 + 200) * (500 - B) - 10.5"),
        )

        # inplace=True (only support for assignment)
        pdf.eval("C = A + B", inplace=True)
        psdf.eval("C = A + B", inplace=True)
        self.assert_eq(pdf, psdf)
        pser = pdf.A
        psser = psdf.A
        pdf.eval("A = B + C", inplace=True)
        psdf.eval("A = B + C", inplace=True)
        self.assert_eq(pdf, psdf)
        # Skip due to pandas bug: https://github.com/pandas-dev/pandas/issues/47449
        if not (LooseVersion("1.4.0") <= LooseVersion(pd.__version__) <= LooseVersion("1.4.3")):
            self.assert_eq(pser, psser)

        # doesn't support for multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b"), ("z", "c")])
        psdf.columns = columns
        self.assertRaises(TypeError, lambda: psdf.eval("x.a + y.b"))

    @unittest.skipIf(not have_tabulate, tabulate_requirement_message)
    def test_to_markdown(self):
        pdf = pd.DataFrame(data={"animal_1": ["elk", "pig"], "animal_2": ["dog", "quetzal"]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.to_markdown(), psdf.to_markdown())

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

    def test_rfloordiv(self):
        pdf = pd.DataFrame(
            {"angles": [0, 3, 4], "degrees": [360, 180, 360]},
            index=["circle", "triangle", "rectangle"],
            columns=["angles", "degrees"],
        )
        psdf = ps.from_pandas(pdf)

        expected_result = pdf.rfloordiv(10)
        self.assert_eq(psdf.rfloordiv(10), expected_result)

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

    def test_mad(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 2, None, 4, np.nan],
                "B": [-0.1, 0.2, -0.3, np.nan, 0.5],
                "C": ["a", "b", "c", "d", "e"],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.mad(), pdf.mad())
        self.assert_eq(psdf.mad(axis=1), pdf.mad(axis=1))

        with self.assertRaises(ValueError):
            psdf.mad(axis=2)

        # MultiIndex columns
        columns = pd.MultiIndex.from_tuples([("A", "X"), ("A", "Y"), ("A", "Z")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.mad(), pdf.mad())
        self.assert_eq(psdf.mad(axis=1), pdf.mad(axis=1))

        pdf = pd.DataFrame({"A": [True, True, False, False], "B": [True, False, False, True]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.mad(), pdf.mad())
        self.assert_eq(psdf.mad(axis=1), pdf.mad(axis=1))

    def test_mode(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 2, None, 4, 5, 4, 2],
                "B": [-0.1, 0.2, -0.3, np.nan, 0.5, -0.1, -0.1],
                "C": ["d", "b", "c", "c", "e", "a", "a"],
                "D": [np.nan, np.nan, np.nan, np.nan, 0.1, -0.1, -0.1],
                "E": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.mode(), pdf.mode())
        self.assert_eq(psdf.mode(numeric_only=True), pdf.mode(numeric_only=True))
        self.assert_eq(psdf.mode(dropna=False), pdf.mode(dropna=False))

        # dataframe with single column
        for c in ["A", "B", "C", "D", "E"]:
            self.assert_eq(psdf[[c]].mode(), pdf[[c]].mode())

        with self.assertRaises(ValueError):
            psdf.mode(axis=2)

        def f(index, iterator):
            return ["3", "3", "3", "3", "4"] if index == 3 else ["0", "1", "2", "3", "4"]

        rdd = self.spark.sparkContext.parallelize(
            [
                1,
            ],
            4,
        ).mapPartitionsWithIndex(f)
        df = self.spark.createDataFrame(rdd, schema="string")
        psdf = df.pandas_api()
        self.assert_eq(psdf.mode(), psdf._to_pandas().mode())

    def test_abs(self):
        pdf = pd.DataFrame({"a": [-2, -1, 0, 1]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(abs(psdf), abs(pdf))
        self.assert_eq(np.abs(psdf), np.abs(pdf))

    def test_corrwith(self):
        df1 = ps.DataFrame(
            {"A": [1, np.nan, 7, 8], "B": [False, True, True, False], "C": [10, 4, 9, 3]}
        )
        df2 = df1[["A", "C"]]
        df3 = df1[["B", "C"]]
        self._test_corrwith(df1, df2)
        self._test_corrwith(df1, df3)
        self._test_corrwith((df1 + 1), df2.A)
        self._test_corrwith((df1 + 1), df3.B)
        self._test_corrwith((df1 + 1), (df2.C + 2))
        self._test_corrwith((df1 + 1), (df3.B + 2))

        with self.assertRaisesRegex(TypeError, "unsupported type"):
            df1.corrwith(123)
        with self.assertRaisesRegex(NotImplementedError, "only works for axis=0"):
            df1.corrwith(df1.A, axis=1)
        with self.assertRaisesRegex(ValueError, "Invalid method"):
            df1.corrwith(df1.A, method="cov")

        df_bool = ps.DataFrame({"A": [True, True, False, False], "B": [True, False, False, True]})
        self._test_corrwith(df_bool, df_bool.A)
        self._test_corrwith(df_bool, df_bool.B)

    def _test_corrwith(self, psdf, psobj):
        pdf = psdf._to_pandas()
        pobj = psobj._to_pandas()
        # There was a regression in pandas 1.5.0
        # when other is Series and method is "pearson" or "spearman", and fixed in pandas 1.5.1
        # Therefore, we only test the pandas 1.5.0 in different way.
        # See https://github.com/pandas-dev/pandas/issues/48826 for the reported issue,
        # and https://github.com/pandas-dev/pandas/pull/46174 for the initial PR that causes.
        if LooseVersion(pd.__version__) == LooseVersion("1.5.0") and isinstance(pobj, pd.Series):
            methods = ["kendall"]
        else:
            methods = ["pearson", "spearman", "kendall"]
        for method in methods:
            for drop in [True, False]:
                p_corr = pdf.corrwith(pobj, drop=drop, method=method)
                ps_corr = psdf.corrwith(psobj, drop=drop, method=method)
                self.assert_eq(p_corr.sort_index(), ps_corr.sort_index(), almost=True)

    def test_iteritems(self):
        pdf = pd.DataFrame(
            {"species": ["bear", "bear", "marsupial"], "population": [1864, 22000, 80000]},
            index=["panda", "polar", "koala"],
            columns=["species", "population"],
        )
        psdf = ps.from_pandas(pdf)

        for (p_name, p_items), (k_name, k_items) in zip(pdf.iteritems(), psdf.iteritems()):
            self.assert_eq(p_name, k_name)
            self.assert_eq(p_items, k_items)

    def test_tail(self):
        pdf = pd.DataFrame({"x": range(1000)})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.tail(), psdf.tail())
        self.assert_eq(pdf.tail(10), psdf.tail(10))
        self.assert_eq(pdf.tail(-990), psdf.tail(-990))
        self.assert_eq(pdf.tail(0), psdf.tail(0))
        self.assert_eq(pdf.tail(-1001), psdf.tail(-1001))
        self.assert_eq(pdf.tail(1001), psdf.tail(1001))
        self.assert_eq((pdf + 1).tail(), (psdf + 1).tail())
        self.assert_eq((pdf + 1).tail(10), (psdf + 1).tail(10))
        self.assert_eq((pdf + 1).tail(-990), (psdf + 1).tail(-990))
        self.assert_eq((pdf + 1).tail(0), (psdf + 1).tail(0))
        self.assert_eq((pdf + 1).tail(-1001), (psdf + 1).tail(-1001))
        self.assert_eq((pdf + 1).tail(1001), (psdf + 1).tail(1001))
        with self.assertRaisesRegex(TypeError, "bad operand type for unary -: 'str'"):
            psdf.tail("10")

    def test_last_valid_index(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, None], "b": [1.0, 2.0, 3.0, None], "c": [100, 200, 400, None]},
            index=["Q", "W", "E", "R"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.last_valid_index(), psdf.last_valid_index())
        self.assert_eq(pdf[[]].last_valid_index(), psdf[[]].last_valid_index())

        # MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.last_valid_index(), psdf.last_valid_index())

        # Empty DataFrame
        pdf = pd.Series([]).to_frame()
        psdf = ps.Series([]).to_frame()
        self.assert_eq(pdf.last_valid_index(), psdf.last_valid_index())

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

    def test_first_valid_index(self):
        pdf = pd.DataFrame(
            {"a": [None, 2, 3, 2], "b": [None, 2.0, 3.0, 1.0], "c": [None, 200, 400, 200]},
            index=["Q", "W", "E", "R"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.first_valid_index(), psdf.first_valid_index())
        self.assert_eq(pdf[[]].first_valid_index(), psdf[[]].first_valid_index())

        # MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.first_valid_index(), psdf.first_valid_index())

        # Empty DataFrame
        pdf = pd.Series([]).to_frame()
        psdf = ps.Series([]).to_frame()
        self.assert_eq(pdf.first_valid_index(), psdf.first_valid_index())

        pdf = pd.DataFrame(
            {"a": [None, 2, 3, 2], "b": [None, 2.0, 3.0, 1.0], "c": [None, 200, 400, 200]},
            index=[
                datetime(2021, 1, 1),
                datetime(2021, 2, 1),
                datetime(2021, 3, 1),
                datetime(2021, 4, 1),
            ],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.first_valid_index(), psdf.first_valid_index())

    def test_product(self):
        pdf = pd.DataFrame(
            {"A": [1, 2, 3, 4, 5], "B": [10, 20, 30, 40, 50], "C": ["a", "b", "c", "d", "e"]}
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index())

        # Named columns
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index())

        # MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index())

        # Named MultiIndex columns
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index())

        # No numeric columns
        pdf = pd.DataFrame({"key": ["a", "b", "c"], "val": ["x", "y", "z"]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index())

        # No numeric named columns
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), almost=True)

        # No numeric MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), almost=True)

        # No numeric named MultiIndex columns
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), almost=True)

        # All NaN columns
        pdf = pd.DataFrame(
            {
                "A": [np.nan, np.nan, np.nan, np.nan, np.nan],
                "B": [10, 20, 30, 40, 50],
                "C": ["a", "b", "c", "d", "e"],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), check_exact=False)

        # All NaN named columns
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), check_exact=False)

        # All NaN MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), check_exact=False)

        # All NaN named MultiIndex columns
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), check_exact=False)

    def test_from_dict(self):
        data = {"row_1": [3, 2, 1, 0], "row_2": [10, 20, 30, 40]}
        pdf = pd.DataFrame.from_dict(data)
        psdf = ps.DataFrame.from_dict(data)
        self.assert_eq(pdf, psdf)

        pdf = pd.DataFrame.from_dict(data, dtype="int8")
        psdf = ps.DataFrame.from_dict(data, dtype="int8")
        self.assert_eq(pdf, psdf)

        pdf = pd.DataFrame.from_dict(data, orient="index", columns=["A", "B", "C", "D"])
        psdf = ps.DataFrame.from_dict(data, orient="index", columns=["A", "B", "C", "D"])
        self.assert_eq(pdf, psdf)

    def test_pad(self):
        pdf = pd.DataFrame(
            {
                "A": [None, 3, None, None],
                "B": [2, 4, None, 3],
                "C": [None, None, None, 1],
                "D": [0, 1, 5, 4],
            },
            columns=["A", "B", "C", "D"],
        )
        psdf = ps.from_pandas(pdf)

        if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            self.assert_eq(pdf.pad(), psdf.pad())

            # Test `inplace=True`
            pdf.pad(inplace=True)
            psdf.pad(inplace=True)
            self.assert_eq(pdf, psdf)
        else:
            expected = ps.DataFrame(
                {
                    "A": [None, 3, 3, 3],
                    "B": [2.0, 4.0, 4.0, 3.0],
                    "C": [None, None, None, 1],
                    "D": [0, 1, 5, 4],
                },
                columns=["A", "B", "C", "D"],
            )
            self.assert_eq(expected, psdf.pad())

            # Test `inplace=True`
            psdf.pad(inplace=True)
            self.assert_eq(expected, psdf)

    def test_backfill(self):
        pdf = pd.DataFrame(
            {
                "A": [None, 3, None, None],
                "B": [2, 4, None, 3],
                "C": [None, None, None, 1],
                "D": [0, 1, 5, 4],
            },
            columns=["A", "B", "C", "D"],
        )
        psdf = ps.from_pandas(pdf)

        if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            self.assert_eq(pdf.backfill(), psdf.backfill())

            # Test `inplace=True`
            pdf.backfill(inplace=True)
            psdf.backfill(inplace=True)
            self.assert_eq(pdf, psdf)
        else:
            expected = ps.DataFrame(
                {
                    "A": [3.0, 3.0, None, None],
                    "B": [2.0, 4.0, 3.0, 3.0],
                    "C": [1.0, 1.0, 1.0, 1.0],
                    "D": [0, 1, 5, 4],
                },
                columns=["A", "B", "C", "D"],
            )
            self.assert_eq(expected, psdf.backfill())

            # Test `inplace=True`
            psdf.backfill(inplace=True)
            self.assert_eq(expected, psdf)

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

    def test_astype(self):
        psdf = self.psdf

        msg = "Only a column name can be used for the key in a dtype mappings argument."
        with self.assertRaisesRegex(KeyError, msg):
            psdf.astype({"c": float})

    def test_describe(self):
        pdf, psdf = self.df_pair

        # numeric columns
        self.assert_eq(psdf.describe(), pdf.describe())
        psdf.a += psdf.a
        pdf.a += pdf.a
        self.assert_eq(psdf.describe(), pdf.describe())

        # string columns
        psdf = ps.DataFrame({"A": ["a", "b", "b", "c"], "B": ["d", "e", "f", "f"]})
        pdf = psdf._to_pandas()
        self.assert_eq(psdf.describe(), pdf.describe().astype(str))
        psdf.A += psdf.A
        pdf.A += pdf.A
        self.assert_eq(psdf.describe(), pdf.describe().astype(str))

        # timestamp columns
        psdf = ps.DataFrame(
            {
                "A": [
                    pd.Timestamp("2020-10-20"),
                    pd.Timestamp("2021-06-02"),
                    pd.Timestamp("2021-06-02"),
                    pd.Timestamp("2022-07-11"),
                ],
                "B": [
                    pd.Timestamp("2021-11-20"),
                    pd.Timestamp("2023-06-02"),
                    pd.Timestamp("2026-07-11"),
                    pd.Timestamp("2026-07-11"),
                ],
            }
        )
        pdf = psdf._to_pandas()
        # NOTE: Set `datetime_is_numeric=True` for pandas:
        # FutureWarning: Treating datetime data as categorical rather than numeric in
        # `.describe` is deprecated and will be removed in a future version of pandas.
        # Specify `datetime_is_numeric=True` to silence this
        # warning and adopt the future behavior now.
        # NOTE: Compare the result except percentiles, since we use approximate percentile
        # so the result is different from pandas.
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pdf.describe(datetime_is_numeric=True)
                .astype(str)
                .loc[["count", "mean", "min", "max"]],
            )
        else:
            self.assert_eq(
                psdf.describe(),
                ps.DataFrame(
                    {
                        "A": [
                            "4",
                            "2021-07-16 18:00:00",
                            "2020-10-20 00:00:00",
                            "2020-10-20 00:00:00",
                            "2021-06-02 00:00:00",
                            "2021-06-02 00:00:00",
                            "2022-07-11 00:00:00",
                        ],
                        "B": [
                            "4",
                            "2024-08-02 18:00:00",
                            "2021-11-20 00:00:00",
                            "2021-11-20 00:00:00",
                            "2023-06-02 00:00:00",
                            "2026-07-11 00:00:00",
                            "2026-07-11 00:00:00",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max"],
                ),
            )

        # String & timestamp columns
        psdf = ps.DataFrame(
            {
                "A": ["a", "b", "b", "c"],
                "B": [
                    pd.Timestamp("2021-11-20"),
                    pd.Timestamp("2023-06-02"),
                    pd.Timestamp("2026-07-11"),
                    pd.Timestamp("2026-07-11"),
                ],
            }
        )
        pdf = psdf._to_pandas()
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pdf.describe(datetime_is_numeric=True)
                .astype(str)
                .loc[["count", "mean", "min", "max"]],
            )
            psdf.A += psdf.A
            pdf.A += pdf.A
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pdf.describe(datetime_is_numeric=True)
                .astype(str)
                .loc[["count", "mean", "min", "max"]],
            )
        else:
            expected_result = ps.DataFrame(
                {
                    "B": [
                        "4",
                        "2024-08-02 18:00:00",
                        "2021-11-20 00:00:00",
                        "2021-11-20 00:00:00",
                        "2023-06-02 00:00:00",
                        "2026-07-11 00:00:00",
                        "2026-07-11 00:00:00",
                    ]
                },
                index=["count", "mean", "min", "25%", "50%", "75%", "max"],
            )
            self.assert_eq(
                psdf.describe(),
                expected_result,
            )
            psdf.A += psdf.A
            self.assert_eq(
                psdf.describe(),
                expected_result,
            )

        # Numeric & timestamp columns
        psdf = ps.DataFrame(
            {
                "A": [1, 2, 2, 3],
                "B": [
                    pd.Timestamp("2021-11-20"),
                    pd.Timestamp("2023-06-02"),
                    pd.Timestamp("2026-07-11"),
                    pd.Timestamp("2026-07-11"),
                ],
            }
        )
        pdf = psdf._to_pandas()
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            pandas_result = pdf.describe(datetime_is_numeric=True)
            pandas_result.B = pandas_result.B.astype(str)
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pandas_result.loc[["count", "mean", "min", "max"]],
            )
            psdf.A += psdf.A
            pdf.A += pdf.A
            pandas_result = pdf.describe(datetime_is_numeric=True)
            pandas_result.B = pandas_result.B.astype(str)
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pandas_result.loc[["count", "mean", "min", "max"]],
            )
        else:
            self.assert_eq(
                psdf.describe(),
                ps.DataFrame(
                    {
                        "A": [4, 2, 1, 1, 2, 2, 3, 0.816497],
                        "B": [
                            "4",
                            "2024-08-02 18:00:00",
                            "2021-11-20 00:00:00",
                            "2021-11-20 00:00:00",
                            "2023-06-02 00:00:00",
                            "2026-07-11 00:00:00",
                            "2026-07-11 00:00:00",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max", "std"],
                ),
            )
            psdf.A += psdf.A
            self.assert_eq(
                psdf.describe(),
                ps.DataFrame(
                    {
                        "A": [4, 4, 2, 2, 4, 4, 6, 1.632993],
                        "B": [
                            "4",
                            "2024-08-02 18:00:00",
                            "2021-11-20 00:00:00",
                            "2021-11-20 00:00:00",
                            "2023-06-02 00:00:00",
                            "2026-07-11 00:00:00",
                            "2026-07-11 00:00:00",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max", "std"],
                ),
            )

        # Include None column
        psdf = ps.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)],
                "c": [None, None, None],
            }
        )
        pdf = psdf._to_pandas()
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            pandas_result = pdf.describe(datetime_is_numeric=True)
            pandas_result.b = pandas_result.b.astype(str)
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pandas_result.loc[["count", "mean", "min", "max"]],
            )
        else:
            self.assert_eq(
                psdf.describe(),
                ps.DataFrame(
                    {
                        "a": [3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 3.0, 1.0],
                        "b": [
                            "3",
                            "1970-01-01 00:00:00.000001",
                            "1970-01-01 00:00:00.000001",
                            "1970-01-01 00:00:00.000001",
                            "1970-01-01 00:00:00.000001",
                            "1970-01-01 00:00:00.000001",
                            "1970-01-01 00:00:00.000001",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max", "std"],
                ),
            )

        msg = r"Percentiles should all be in the interval \[0, 1\]"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.describe(percentiles=[1.1])

        psdf = ps.DataFrame()
        msg = "Cannot describe a DataFrame without columns"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.describe()

    def test_describe_empty(self):
        # Empty DataFrame
        psdf = ps.DataFrame(columns=["A", "B"])
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf.describe(),
            pdf.describe().astype(float),
        )

        # Explicit empty DataFrame numeric only
        psdf = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf[psdf.a != psdf.a].describe(),
            pdf[pdf.a != pdf.a].describe(),
        )

        # Explicit empty DataFrame string only
        psdf = ps.DataFrame({"a": ["a", "b", "c"], "b": ["q", "w", "e"]})
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf[psdf.a != psdf.a].describe(),
            pdf[pdf.a != pdf.a].describe().astype(float),
        )

        # Explicit empty DataFrame timestamp only
        psdf = ps.DataFrame(
            {
                "a": [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)],
                "b": [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)],
            }
        )
        pdf = psdf._to_pandas()
        # For timestamp type, we should convert NaT to None in pandas result
        # since pandas API on Spark doesn't support the NaT for object type.
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            pdf_result = pdf[pdf.a != pdf.a].describe(datetime_is_numeric=True)
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                pdf_result.where(pdf_result.notnull(), None).astype(str),
            )
        else:
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                ps.DataFrame(
                    {
                        "a": [
                            "0",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                        ],
                        "b": [
                            "0",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max"],
                ),
            )

        # Explicit empty DataFrame numeric & timestamp
        psdf = ps.DataFrame(
            {"a": [1, 2, 3], "b": [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)]}
        )
        pdf = psdf._to_pandas()
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            pdf_result = pdf[pdf.a != pdf.a].describe(datetime_is_numeric=True)
            pdf_result.b = pdf_result.b.where(pdf_result.b.notnull(), None).astype(str)
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                pdf_result,
            )
        else:
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                ps.DataFrame(
                    {
                        "a": [
                            0,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        ],
                        "b": [
                            "0",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max", "std"],
                ),
            )

        # Explicit empty DataFrame numeric & string
        psdf = ps.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf[psdf.a != psdf.a].describe(),
            pdf[pdf.a != pdf.a].describe(),
        )

        # Explicit empty DataFrame string & timestamp
        psdf = ps.DataFrame(
            {"a": ["a", "b", "c"], "b": [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)]}
        )
        pdf = psdf._to_pandas()
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            pdf_result = pdf[pdf.a != pdf.a].describe(datetime_is_numeric=True)
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                pdf_result.where(pdf_result.notnull(), None).astype(str),
            )
        else:
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                ps.DataFrame(
                    {
                        "b": [
                            "0",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max"],
                ),
            )

    def test_getitem_with_none_key(self):
        psdf = self.psdf

        with self.assertRaisesRegex(KeyError, "none key"):
            psdf[None]

    def test_iter_dataframe(self):
        pdf, psdf = self.df_pair

        for value_psdf, value_pdf in zip(psdf, pdf):
            self.assert_eq(value_psdf, value_pdf)

    def test_combine_first(self):
        pdf = pd.DataFrame(
            {("X", "A"): [None, 0], ("X", "B"): [4, None], ("Y", "C"): [3, 3], ("Y", "B"): [1, 1]}
        )
        pdf1, pdf2 = pdf["X"], pdf["Y"]
        psdf = ps.from_pandas(pdf)
        psdf1, psdf2 = psdf["X"], psdf["Y"]

        if LooseVersion(pd.__version__) >= LooseVersion("1.2.0"):
            self.assert_eq(pdf1.combine_first(pdf2), psdf1.combine_first(psdf2))
        else:
            # pandas < 1.2.0 returns unexpected dtypes,
            # please refer to https://github.com/pandas-dev/pandas/issues/28481 for details
            expected_pdf = pd.DataFrame({"A": [None, 0], "B": [4.0, 1.0], "C": [3, 3]})
            self.assert_eq(expected_pdf, psdf1.combine_first(psdf2))

    def test_multi_index_dtypes(self):
        # SPARK-36930: Support ps.MultiIndex.dtypes
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        pmidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        psmidx = ps.from_pandas(pmidx)

        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            self.assert_eq(psmidx.dtypes, pmidx.dtypes)
        else:
            expected = pd.Series([np.dtype("int64"), np.dtype("O")], index=["number", "color"])
            self.assert_eq(psmidx.dtypes, expected)

        # multiple labels
        pmidx = pd.MultiIndex.from_arrays(arrays, names=[("zero", "first"), ("one", "second")])
        psmidx = ps.from_pandas(pmidx)

        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            if LooseVersion(pd.__version__) not in (LooseVersion("1.4.1"), LooseVersion("1.4.2")):
                self.assert_eq(psmidx.dtypes, pmidx.dtypes)
        else:
            expected = pd.Series(
                [np.dtype("int64"), np.dtype("O")],
                index=pd.Index([("zero", "first"), ("one", "second")]),
            )
            self.assert_eq(psmidx.dtypes, expected)

    def test_multi_index_dtypes_not_unique_name(self):
        # Regression test for https://github.com/pandas-dev/pandas/issues/45174
        pmidx = pd.MultiIndex.from_arrays([[1], [2]], names=[1, 1])
        psmidx = ps.from_pandas(pmidx)

        if LooseVersion(pd.__version__) < LooseVersion("1.4"):
            expected = pd.Series(
                [np.dtype("int64"), np.dtype("int64")],
                index=[1, 1],
            )
            self.assert_eq(psmidx.dtypes, expected)
        else:
            self.assert_eq(psmidx.dtypes, pmidx.dtypes)

    def test_cov(self):
        # SPARK-36396: Implement DataFrame.cov

        # int
        pdf = pd.DataFrame([(1, 2), (0, 3), (2, 0), (1, 1)], columns=["a", "b"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(), psdf.cov(), almost=True)
        self.assert_eq(pdf.cov(min_periods=4), psdf.cov(min_periods=4), almost=True)
        self.assert_eq(pdf.cov(min_periods=5), psdf.cov(min_periods=5))

        # ddof
        with self.assertRaisesRegex(TypeError, "ddof must be integer"):
            psdf.cov(ddof="ddof")
        for ddof in [-1, 0, 2]:
            self.assert_eq(pdf.cov(ddof=ddof), psdf.cov(ddof=ddof), almost=True)
            self.assert_eq(
                pdf.cov(min_periods=4, ddof=ddof), psdf.cov(min_periods=4, ddof=ddof), almost=True
            )
            self.assert_eq(pdf.cov(min_periods=5, ddof=ddof), psdf.cov(min_periods=5, ddof=ddof))

        # bool
        pdf = pd.DataFrame(
            {
                "a": [1, np.nan, 3, 4],
                "b": [True, False, False, True],
                "c": [True, True, False, True],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(), psdf.cov(), almost=True)
        self.assert_eq(pdf.cov(min_periods=4), psdf.cov(min_periods=4), almost=True)
        self.assert_eq(pdf.cov(min_periods=5), psdf.cov(min_periods=5))

        # extension dtype
        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            numeric_dtypes = ["Int8", "Int16", "Int32", "Int64", "Float32", "Float64", "float"]
            boolean_dtypes = ["boolean", "bool"]
        else:
            numeric_dtypes = ["Int8", "Int16", "Int32", "Int64", "float"]
            boolean_dtypes = ["boolean", "bool"]

        sers = [pd.Series([1, 2, 3, None], dtype=dtype) for dtype in numeric_dtypes]
        sers += [pd.Series([True, False, True, None], dtype=dtype) for dtype in boolean_dtypes]
        sers.append(pd.Series([decimal.Decimal(1), decimal.Decimal(2), decimal.Decimal(3), None]))

        pdf = pd.concat(sers, axis=1)
        pdf.columns = [dtype for dtype in numeric_dtypes + boolean_dtypes] + ["decimal"]
        psdf = ps.from_pandas(pdf)

        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            self.assert_eq(pdf.cov(), psdf.cov(), almost=True)
            self.assert_eq(pdf.cov(min_periods=3), psdf.cov(min_periods=3), almost=True)
            self.assert_eq(pdf.cov(min_periods=4), psdf.cov(min_periods=4))
        else:
            test_types = [
                "Int8",
                "Int16",
                "Int32",
                "Int64",
                "float",
                "boolean",
                "bool",
            ]
            expected = pd.DataFrame(
                data=[
                    [1.0, 1.0, 1.0, 1.0, 1.0, 0.0000000, 0.0000000],
                    [1.0, 1.0, 1.0, 1.0, 1.0, 0.0000000, 0.0000000],
                    [1.0, 1.0, 1.0, 1.0, 1.0, 0.0000000, 0.0000000],
                    [1.0, 1.0, 1.0, 1.0, 1.0, 0.0000000, 0.0000000],
                    [1.0, 1.0, 1.0, 1.0, 1.0, 0.0000000, 0.0000000],
                    [0.0, 0.0, 0.0, 0.0, 0.0, 0.3333333, 0.3333333],
                    [0.0, 0.0, 0.0, 0.0, 0.0, 0.3333333, 0.3333333],
                ],
                index=test_types,
                columns=test_types,
            )
            self.assert_eq(expected, psdf.cov(), almost=True)

        # string column
        pdf = pd.DataFrame(
            [(1, 2, "a", 1), (0, 3, "b", 1), (2, 0, "c", 9), (1, 1, "d", 1)],
            columns=["a", "b", "c", "d"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(), psdf.cov(), almost=True)
        self.assert_eq(pdf.cov(min_periods=4), psdf.cov(min_periods=4), almost=True)
        self.assert_eq(pdf.cov(min_periods=5), psdf.cov(min_periods=5))

        # nan
        np.random.seed(42)
        pdf = pd.DataFrame(np.random.randn(20, 3), columns=["a", "b", "c"])
        pdf.loc[pdf.index[:5], "a"] = np.nan
        pdf.loc[pdf.index[5:10], "b"] = np.nan
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(min_periods=11), psdf.cov(min_periods=11), almost=True)
        self.assert_eq(pdf.cov(min_periods=10), psdf.cov(min_periods=10), almost=True)

        # return empty DataFrame
        pdf = pd.DataFrame([("1", "2"), ("0", "3"), ("2", "0"), ("1", "1")], columns=["a", "b"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(), psdf.cov())

    @unittest.skipIf(
        LooseVersion(pd.__version__) < LooseVersion("1.3.0"),
        "pandas support `Styler.to_latex` since 1.3.0",
    )
    def test_style(self):
        # Currently, the `style` function returns a pandas object `Styler` as it is,
        # processing only the number of rows declared in `compute.max_rows`.
        # So it's a bit vague to test, but we are doing minimal tests instead of not testing at all.
        pdf = pd.DataFrame(np.random.randn(10, 4), columns=["A", "B", "C", "D"])
        psdf = ps.from_pandas(pdf)

        def style_negative(v, props=""):
            return props if v < 0 else None

        def check_style():
            # If the value is negative, the text color will be displayed as red.
            pdf_style = pdf.style.applymap(style_negative, props="color:red;")
            psdf_style = psdf.style.applymap(style_negative, props="color:red;")

            # Test whether the same shape as pandas table is created including the color.
            self.assert_eq(pdf_style.to_latex(), psdf_style.to_latex())

        check_style()

        with ps.option_context("compute.max_rows", None):
            check_style()


if __name__ == "__main__":
    from pyspark.pandas.tests.test_dataframe_slow import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
