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
import inspect
from distutils.version import LooseVersion

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.exceptions import PandasNotImplementedError, DataError
from pyspark.pandas.missing.groupby import (
    MissingPandasLikeDataFrameGroupBy,
    MissingPandasLikeSeriesGroupBy,
)
from pyspark.pandas.groupby import is_multi_agg_with_relabel, SeriesGroupBy
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class GroupByTest(PandasOnSparkTestCase, TestUtils):
    @property
    def pdf(self):
        return pd.DataFrame(
            {
                "A": [1, 2, 1, 2],
                "B": [3.1, 4.1, 4.1, 3.1],
                "C": ["a", "b", "b", "a"],
                "D": [True, False, False, True],
            }
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_groupby_simple(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 6, 4, 4, 6, 4, 3, 7],
                "b": [4, 2, 7, 3, 3, 1, 1, 1, 2],
                "c": [4, 2, 7, 3, None, 1, 1, 1, 2],
                "d": list("abcdefght"),
                "e": [True, False, True, False, True, False, True, False, True],
            },
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )
        psdf = ps.from_pandas(pdf)

        for as_index in [True, False]:
            if as_index:

                def sort(df):
                    return df.sort_index()

            else:

                def sort(df):
                    return df.sort_values("a").reset_index(drop=True)

            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index).sum()),
                sort(pdf.groupby("a", as_index=as_index).sum()),
            )
            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index).b.sum()),
                sort(pdf.groupby("a", as_index=as_index).b.sum()),
            )
            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index)["b"].sum()),
                sort(pdf.groupby("a", as_index=as_index)["b"].sum()),
            )
            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index)[["b", "c"]].sum()),
                sort(pdf.groupby("a", as_index=as_index)[["b", "c"]].sum()),
            )
            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index)[[]].sum()),
                sort(pdf.groupby("a", as_index=as_index)[[]].sum()),
            )
            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index)["c"].sum()),
                sort(pdf.groupby("a", as_index=as_index)["c"].sum()),
            )

        self.assert_eq(
            psdf.groupby("a").a.sum().sort_index(), pdf.groupby("a").a.sum().sort_index()
        )
        self.assert_eq(
            psdf.groupby("a")["a"].sum().sort_index(), pdf.groupby("a")["a"].sum().sort_index()
        )
        self.assert_eq(
            psdf.groupby("a")[["a"]].sum().sort_index(), pdf.groupby("a")[["a"]].sum().sort_index()
        )
        self.assert_eq(
            psdf.groupby("a")[["a", "c"]].sum().sort_index(),
            pdf.groupby("a")[["a", "c"]].sum().sort_index(),
        )

        self.assert_eq(
            psdf.a.groupby(psdf.b).sum().sort_index(), pdf.a.groupby(pdf.b).sum().sort_index()
        )

        for axis in [0, "index"]:
            self.assert_eq(
                psdf.groupby("a", axis=axis).a.sum().sort_index(),
                pdf.groupby("a", axis=axis).a.sum().sort_index(),
            )
            self.assert_eq(
                psdf.groupby("a", axis=axis)["a"].sum().sort_index(),
                pdf.groupby("a", axis=axis)["a"].sum().sort_index(),
            )
            self.assert_eq(
                psdf.groupby("a", axis=axis)[["a"]].sum().sort_index(),
                pdf.groupby("a", axis=axis)[["a"]].sum().sort_index(),
            )
            self.assert_eq(
                psdf.groupby("a", axis=axis)[["a", "c"]].sum().sort_index(),
                pdf.groupby("a", axis=axis)[["a", "c"]].sum().sort_index(),
            )

            self.assert_eq(
                psdf.a.groupby(psdf.b, axis=axis).sum().sort_index(),
                pdf.a.groupby(pdf.b, axis=axis).sum().sort_index(),
            )

        self.assertRaises(ValueError, lambda: psdf.groupby("a", as_index=False).a)
        self.assertRaises(ValueError, lambda: psdf.groupby("a", as_index=False)["a"])
        self.assertRaises(ValueError, lambda: psdf.groupby("a", as_index=False)[["a"]])
        self.assertRaises(ValueError, lambda: psdf.groupby("a", as_index=False)[["a", "c"]])
        self.assertRaises(KeyError, lambda: psdf.groupby("z", as_index=False)[["a", "c"]])
        self.assertRaises(KeyError, lambda: psdf.groupby(["z"], as_index=False)[["a", "c"]])

        self.assertRaises(TypeError, lambda: psdf.a.groupby(psdf.b, as_index=False))

        self.assertRaises(NotImplementedError, lambda: psdf.groupby("a", axis=1))
        self.assertRaises(NotImplementedError, lambda: psdf.groupby("a", axis="columns"))
        self.assertRaises(ValueError, lambda: psdf.groupby("a", "b"))
        self.assertRaises(TypeError, lambda: psdf.a.groupby(psdf.a, psdf.b))

        # we can't use column name/names as a parameter `by` for `SeriesGroupBy`.
        self.assertRaises(KeyError, lambda: psdf.a.groupby(by="a"))
        self.assertRaises(KeyError, lambda: psdf.a.groupby(by=["a", "b"]))
        self.assertRaises(KeyError, lambda: psdf.a.groupby(by=("a", "b")))
        self.assertRaises(KeyError, lambda: psdf.a.groupby(by=[("a", "b")]))

        # we can't use DataFrame as a parameter `by` for `DataFrameGroupBy`/`SeriesGroupBy`.
        self.assertRaises(ValueError, lambda: psdf.groupby(psdf))
        self.assertRaises(ValueError, lambda: psdf.a.groupby(psdf))
        self.assertRaises(ValueError, lambda: psdf.a.groupby((psdf,)))

        with self.assertRaisesRegex(ValueError, "Grouper for 'list' not 1-dimensional"):
            psdf.groupby(by=[["a", "b"]])

        # non-string names
        pdf = pd.DataFrame(
            {
                10: [1, 2, 6, 4, 4, 6, 4, 3, 7],
                20: [4, 2, 7, 3, 3, 1, 1, 1, 2],
                30: [4, 2, 7, 3, None, 1, 1, 1, 2],
                40: list("abcdefght"),
            },
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )
        psdf = ps.from_pandas(pdf)

        for as_index in [True, False]:
            if as_index:

                def sort(df):
                    return df.sort_index()

            else:

                def sort(df):
                    return df.sort_values(10).reset_index(drop=True)

            self.assert_eq(
                sort(psdf.groupby(10, as_index=as_index).sum()),
                sort(pdf.groupby(10, as_index=as_index).sum()),
            )
            self.assert_eq(
                sort(psdf.groupby(10, as_index=as_index)[20].sum()),
                sort(pdf.groupby(10, as_index=as_index)[20].sum()),
            )
            self.assert_eq(
                sort(psdf.groupby(10, as_index=as_index)[[20, 30]].sum()),
                sort(pdf.groupby(10, as_index=as_index)[[20, 30]].sum()),
            )

    def test_groupby_multiindex_columns(self):
        pdf = pd.DataFrame(
            {
                (10, "a"): [1, 2, 6, 4, 4, 6, 4, 3, 7],
                (10, "b"): [4, 2, 7, 3, 3, 1, 1, 1, 2],
                (20, "c"): [4, 2, 7, 3, None, 1, 1, 1, 2],
                (30, "d"): list("abcdefght"),
            },
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby((10, "a")).sum().sort_index(), pdf.groupby((10, "a")).sum().sort_index()
        )
        self.assert_eq(
            psdf.groupby((10, "a"), as_index=False)
            .sum()
            .sort_values((10, "a"))
            .reset_index(drop=True),
            pdf.groupby((10, "a"), as_index=False)
            .sum()
            .sort_values((10, "a"))
            .reset_index(drop=True),
        )
        self.assert_eq(
            psdf.groupby((10, "a"))[[(20, "c")]].sum().sort_index(),
            pdf.groupby((10, "a"))[[(20, "c")]].sum().sort_index(),
        )

        # TODO: a pandas bug?
        #  expected = pdf.groupby((10, "a"))[(20, "c")].sum().sort_index()
        expected = pd.Series(
            [4.0, 2.0, 1.0, 4.0, 8.0, 2.0],
            name=(20, "c"),
            index=pd.Index([1, 2, 3, 4, 6, 7], name=(10, "a")),
        )

        self.assert_eq(psdf.groupby((10, "a"))[(20, "c")].sum().sort_index(), expected)

        if LooseVersion(pd.__version__) != LooseVersion("1.1.3") and LooseVersion(
            pd.__version__
        ) != LooseVersion("1.1.4"):
            self.assert_eq(
                psdf[(20, "c")].groupby(psdf[(10, "a")]).sum().sort_index(),
                pdf[(20, "c")].groupby(pdf[(10, "a")]).sum().sort_index(),
            )
        else:
            # Due to pandas bugs resolved in 1.0.4, re-introduced in 1.1.3 and resolved in 1.1.5
            self.assert_eq(psdf[(20, "c")].groupby(psdf[(10, "a")]).sum().sort_index(), expected)

    # TODO: All statistical functions should leverage this utility
    def _test_stat_func(self, func, check_exact=True):
        pdf, psdf = self.pdf, self.psdf
        for p_groupby_obj, ps_groupby_obj in [
            # Against DataFrameGroupBy
            (pdf.groupby("A"), psdf.groupby("A")),
            # Against DataFrameGroupBy with an aggregation column of string type
            (pdf.groupby("A")[["C"]], psdf.groupby("A")[["C"]]),
            # Against SeriesGroupBy
            (pdf.groupby("A")["B"], psdf.groupby("A")["B"]),
        ]:
            self.assert_eq(
                func(p_groupby_obj).sort_index(),
                func(ps_groupby_obj).sort_index(),
                check_exact=check_exact,
            )

    def test_basic_stat_funcs(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.var(), check_exact=False)

        pdf, psdf = self.pdf, self.psdf

        # Unlike pandas', the median in pandas-on-Spark is an approximated median based upon
        # approximate percentile computation because computing median across a large dataset
        # is extremely expensive.
        expected = ps.DataFrame({"B": [3.1, 3.1], "D": [0, 0]}, index=pd.Index([1, 2], name="A"))
        self.assert_eq(
            psdf.groupby("A").median().sort_index(),
            expected,
        )
        self.assert_eq(
            psdf.groupby("A").median(numeric_only=None).sort_index(),
            expected,
        )
        self.assert_eq(
            psdf.groupby("A").median(numeric_only=False).sort_index(),
            expected,
        )
        self.assert_eq(
            psdf.groupby("A")["B"].median().sort_index(),
            expected.B,
        )
        with self.assertRaises(TypeError):
            psdf.groupby("A")["C"].mean()

        with self.assertRaisesRegex(
            TypeError, "Unaccepted data types of aggregation columns; numeric or bool expected."
        ):
            psdf.groupby("A")[["C"]].std()

        with self.assertRaisesRegex(
            TypeError, "Unaccepted data types of aggregation columns; numeric or bool expected."
        ):
            psdf.groupby("A")[["C"]].sem()

        self.assert_eq(
            psdf.groupby("A").std().sort_index(),
            pdf.groupby("A").std().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby("A").sem().sort_index(),
            pdf.groupby("A").sem().sort_index(),
            check_exact=False,
        )

        # TODO: fix bug of `sum` and re-enable the test below
        # self._test_stat_func(lambda groupby_obj: groupby_obj.sum(), check_exact=False)
        self.assert_eq(
            psdf.groupby("A").sum().sort_index(),
            pdf.groupby("A").sum().sort_index(),
            check_exact=False,
        )

    def test_mean(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.mean())
        self._test_stat_func(lambda groupby_obj: groupby_obj.mean(numeric_only=None))
        self._test_stat_func(lambda groupby_obj: groupby_obj.mean(numeric_only=True))
        psdf = self.psdf
        with self.assertRaises(TypeError):
            psdf.groupby("A")["C"].mean()

    def test_quantile(self):
        dfs = [
            pd.DataFrame(
                [["a", 1], ["a", 2], ["a", 3], ["b", 1], ["b", 3], ["b", 5]], columns=["key", "val"]
            ),
            pd.DataFrame(
                [["a", True], ["a", True], ["a", False], ["b", True], ["b", True], ["b", False]],
                columns=["key", "val"],
            ),
        ]
        for df in dfs:
            psdf = ps.from_pandas(df)
            # q accept float and int between 0 and 1
            for i in [0, 0.1, 0.5, 1]:
                self.assert_eq(
                    df.groupby("key").quantile(q=i, interpolation="lower"),
                    psdf.groupby("key").quantile(q=i),
                    almost=True,
                )
                self.assert_eq(
                    df.groupby("key")["val"].quantile(q=i, interpolation="lower"),
                    psdf.groupby("key")["val"].quantile(q=i),
                    almost=True,
                )
            # raise ValueError when q not in [0, 1]
            with self.assertRaises(ValueError):
                psdf.groupby("key").quantile(q=1.1)
            with self.assertRaises(ValueError):
                psdf.groupby("key").quantile(q=-0.1)
            with self.assertRaises(ValueError):
                psdf.groupby("key").quantile(q=2)
            with self.assertRaises(ValueError):
                psdf.groupby("key").quantile(q=np.nan)
            # raise TypeError when q type mismatch
            with self.assertRaises(TypeError):
                psdf.groupby("key").quantile(q="0.1")
            # raise NotImplementedError when q is list like type
            with self.assertRaises(NotImplementedError):
                psdf.groupby("key").quantile(q=(0.1, 0.5))
            with self.assertRaises(NotImplementedError):
                psdf.groupby("key").quantile(q=[0.1, 0.5])

    def test_min(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.min())
        self._test_stat_func(lambda groupby_obj: groupby_obj.min(min_count=2))
        self._test_stat_func(lambda groupby_obj: groupby_obj.min(numeric_only=None))
        self._test_stat_func(lambda groupby_obj: groupby_obj.min(numeric_only=True))
        self._test_stat_func(lambda groupby_obj: groupby_obj.min(numeric_only=True, min_count=2))

    def test_max(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.max())
        self._test_stat_func(lambda groupby_obj: groupby_obj.max(min_count=2))
        self._test_stat_func(lambda groupby_obj: groupby_obj.max(numeric_only=None))
        self._test_stat_func(lambda groupby_obj: groupby_obj.max(numeric_only=True))
        self._test_stat_func(lambda groupby_obj: groupby_obj.max(numeric_only=True, min_count=2))

    def test_sum(self):
        pdf = pd.DataFrame(
            {
                "A": ["a", "a", "b", "a"],
                "B": [1, 2, 1, 2],
                "C": [-1.5, np.nan, -3.2, 0.1],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.groupby("A").sum().sort_index(), psdf.groupby("A").sum().sort_index())
        self.assert_eq(
            pdf.groupby("A").sum(min_count=2).sort_index(),
            psdf.groupby("A").sum(min_count=2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("A").sum(min_count=3).sort_index(),
            psdf.groupby("A").sum(min_count=3).sort_index(),
        )

    def test_mad(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.mad())

    def test_first(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.first())
        self._test_stat_func(lambda groupby_obj: groupby_obj.first(numeric_only=None))
        self._test_stat_func(lambda groupby_obj: groupby_obj.first(numeric_only=True))

        pdf = pd.DataFrame(
            {
                "A": [1, 2, 1, 2],
                "B": [-1.5, np.nan, -3.2, 0.1],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.groupby("A").first().sort_index(), psdf.groupby("A").first().sort_index()
        )
        self.assert_eq(
            pdf.groupby("A").first(min_count=1).sort_index(),
            psdf.groupby("A").first(min_count=1).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("A").first(min_count=2).sort_index(),
            psdf.groupby("A").first(min_count=2).sort_index(),
        )

    def test_last(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.last())
        self._test_stat_func(lambda groupby_obj: groupby_obj.last(numeric_only=None))
        self._test_stat_func(lambda groupby_obj: groupby_obj.last(numeric_only=True))

        pdf = pd.DataFrame(
            {
                "A": [1, 2, 1, 2],
                "B": [-1.5, np.nan, -3.2, 0.1],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.groupby("A").last().sort_index(), psdf.groupby("A").last().sort_index())
        self.assert_eq(
            pdf.groupby("A").last(min_count=1).sort_index(),
            psdf.groupby("A").last(min_count=1).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("A").last(min_count=2).sort_index(),
            psdf.groupby("A").last(min_count=2).sort_index(),
        )

    def test_nth(self):
        for n in [0, 1, 2, 128, -1, -2, -128]:
            self._test_stat_func(lambda groupby_obj: groupby_obj.nth(n))

        with self.assertRaisesRegex(NotImplementedError, "slice or list"):
            self.psdf.groupby("B").nth(slice(0, 2))
        with self.assertRaisesRegex(NotImplementedError, "slice or list"):
            self.psdf.groupby("B").nth([0, 1, -1])
        with self.assertRaisesRegex(TypeError, "Invalid index"):
            self.psdf.groupby("B").nth("x")

    def test_prod(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 2, 1, 2, 1],
                "B": [3.1, 4.1, 4.1, 3.1, 0.1],
                "C": ["a", "b", "b", "a", "c"],
                "D": [True, False, False, True, False],
                "E": [-1, -2, 3, -4, -2],
                "F": [-1.5, np.nan, -3.2, 0.1, 0],
                "G": [np.nan, np.nan, np.nan, np.nan, np.nan],
            }
        )
        psdf = ps.from_pandas(pdf)

        for n in [0, 1, 2, 128, -1, -2, -128]:
            self._test_stat_func(
                lambda groupby_obj: groupby_obj.prod(min_count=n), check_exact=False
            )
            self._test_stat_func(
                lambda groupby_obj: groupby_obj.prod(numeric_only=None, min_count=n),
                check_exact=False,
            )
            self._test_stat_func(
                lambda groupby_obj: groupby_obj.prod(numeric_only=True, min_count=n),
                check_exact=False,
            )
            self.assert_eq(
                pdf.groupby("A").prod(min_count=n).sort_index(),
                psdf.groupby("A").prod(min_count=n).sort_index(),
                almost=True,
            )

    def test_cumcount(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        for ascending in [True, False]:
            self.assert_eq(
                psdf.groupby("b").cumcount(ascending=ascending).sort_index(),
                pdf.groupby("b").cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(["a", "b"]).cumcount(ascending=ascending).sort_index(),
                pdf.groupby(["a", "b"]).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(["b"])["a"].cumcount(ascending=ascending).sort_index(),
                pdf.groupby(["b"])["a"].cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(["b"])[["a", "c"]].cumcount(ascending=ascending).sort_index(),
                pdf.groupby(["b"])[["a", "c"]].cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(psdf.b // 5).cumcount(ascending=ascending).sort_index(),
                pdf.groupby(pdf.b // 5).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(psdf.b // 5)["a"].cumcount(ascending=ascending).sort_index(),
                pdf.groupby(pdf.b // 5)["a"].cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby("b").cumcount(ascending=ascending).sum(),
                pdf.groupby("b").cumcount(ascending=ascending).sum(),
            )
            self.assert_eq(
                psdf.a.rename().groupby(psdf.b).cumcount(ascending=ascending).sort_index(),
                pdf.a.rename().groupby(pdf.b).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.a.groupby(psdf.b.rename()).cumcount(ascending=ascending).sort_index(),
                pdf.a.groupby(pdf.b.rename()).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.a.rename().groupby(psdf.b.rename()).cumcount(ascending=ascending).sort_index(),
                pdf.a.rename().groupby(pdf.b.rename()).cumcount(ascending=ascending).sort_index(),
            )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        for ascending in [True, False]:
            self.assert_eq(
                psdf.groupby(("x", "b")).cumcount(ascending=ascending).sort_index(),
                pdf.groupby(("x", "b")).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby([("x", "a"), ("x", "b")]).cumcount(ascending=ascending).sort_index(),
                pdf.groupby([("x", "a"), ("x", "b")]).cumcount(ascending=ascending).sort_index(),
            )

    def test_cummin(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cummin().sort_index(), pdf.groupby("b").cummin().sort_index()
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cummin().sort_index(),
            pdf.groupby(["a", "b"]).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cummin().sort_index(),
            pdf.groupby(["b"])["a"].cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cummin().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).cummin().sort_index(),
            pdf.groupby(pdf.b // 5).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].cummin().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").cummin().sum().sort_index(),
            pdf.groupby("b").cummin().sum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cummin().sort_index(),
            pdf.a.rename().groupby(pdf.b).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cummin().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cummin().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cummin().sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cummin().sort_index(),
            pdf.groupby(("x", "b")).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cummin().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cummin().sort_index(),
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cummin())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cummin())

    def test_cummax(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cummax().sort_index(), pdf.groupby("b").cummax().sort_index()
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cummax().sort_index(),
            pdf.groupby(["a", "b"]).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cummax().sort_index(),
            pdf.groupby(["b"])["a"].cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cummax().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).cummax().sort_index(),
            pdf.groupby(pdf.b // 5).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].cummax().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").cummax().sum().sort_index(),
            pdf.groupby("b").cummax().sum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cummax().sort_index(),
            pdf.a.rename().groupby(pdf.b).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cummax().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cummax().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cummax().sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cummax().sort_index(),
            pdf.groupby(("x", "b")).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cummax().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cummax().sort_index(),
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cummax())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cummax())

    def test_cumsum(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cumsum().sort_index(), pdf.groupby("b").cumsum().sort_index()
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cumsum().sort_index(),
            pdf.groupby(["a", "b"]).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cumsum().sort_index(),
            pdf.groupby(["b"])["a"].cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cumsum().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).cumsum().sort_index(),
            pdf.groupby(pdf.b // 5).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].cumsum().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").cumsum().sum().sort_index(),
            pdf.groupby("b").cumsum().sum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cumsum().sort_index(),
            pdf.a.rename().groupby(pdf.b).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cumsum().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cumsum().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cumsum().sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cumsum().sort_index(),
            pdf.groupby(("x", "b")).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cumsum().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cumsum().sort_index(),
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cumsum())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cumsum())

    def test_cumprod(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, -3, 4, -5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 0, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cumprod().sort_index(),
            pdf.groupby("b").cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cumprod().sort_index(),
            pdf.groupby(["a", "b"]).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cumprod().sort_index(),
            pdf.groupby(["b"])["a"].cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cumprod().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 3).cumprod().sort_index(),
            pdf.groupby(pdf.b // 3).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 3)["a"].cumprod().sort_index(),
            pdf.groupby(pdf.b // 3)["a"].cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby("b").cumprod().sum().sort_index(),
            pdf.groupby("b").cumprod().sum().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cumprod().sort_index(),
            pdf.a.rename().groupby(pdf.b).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cumprod().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cumprod().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cumprod().sort_index(),
            check_exact=False,
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cumprod().sort_index(),
            pdf.groupby(("x", "b")).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cumprod().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cumprod().sort_index(),
            check_exact=False,
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cumprod())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cumprod())

    def test_nsmallest(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
                "c": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
                "d": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
            },
            index=np.random.rand(9 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby(["a"])["b"].nsmallest(1).sort_values(),
            pdf.groupby(["a"])["b"].nsmallest(1).sort_values(),
        )
        self.assert_eq(
            psdf.groupby(["a"])["b"].nsmallest(2).sort_index(),
            pdf.groupby(["a"])["b"].nsmallest(2).sort_index(),
        )
        self.assert_eq(
            (psdf.b * 10).groupby(psdf.a).nsmallest(2).sort_index(),
            (pdf.b * 10).groupby(pdf.a).nsmallest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.rename().groupby(psdf.a).nsmallest(2).sort_index(),
            pdf.b.rename().groupby(pdf.a).nsmallest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.groupby(psdf.a.rename()).nsmallest(2).sort_index(),
            pdf.b.groupby(pdf.a.rename()).nsmallest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.rename().groupby(psdf.a.rename()).nsmallest(2).sort_index(),
            pdf.b.rename().groupby(pdf.a.rename()).nsmallest(2).sort_index(),
        )
        with self.assertRaisesRegex(ValueError, "nsmallest do not support multi-index now"):
            psdf.set_index(["a", "b"]).groupby(["c"])["d"].nsmallest(1)

    def test_nlargest(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
                "c": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
                "d": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
            },
            index=np.random.rand(9 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby(["a"])["b"].nlargest(1).sort_values(),
            pdf.groupby(["a"])["b"].nlargest(1).sort_values(),
        )
        self.assert_eq(
            psdf.groupby(["a"])["b"].nlargest(2).sort_index(),
            pdf.groupby(["a"])["b"].nlargest(2).sort_index(),
        )
        self.assert_eq(
            (psdf.b * 10).groupby(psdf.a).nlargest(2).sort_index(),
            (pdf.b * 10).groupby(pdf.a).nlargest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.rename().groupby(psdf.a).nlargest(2).sort_index(),
            pdf.b.rename().groupby(pdf.a).nlargest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.groupby(psdf.a.rename()).nlargest(2).sort_index(),
            pdf.b.groupby(pdf.a.rename()).nlargest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.rename().groupby(psdf.a.rename()).nlargest(2).sort_index(),
            pdf.b.rename().groupby(pdf.a.rename()).nlargest(2).sort_index(),
        )
        with self.assertRaisesRegex(ValueError, "nlargest do not support multi-index now"):
            psdf.set_index(["a", "b"]).groupby(["c"])["d"].nlargest(1)

    def test_fillna(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 2, 2] * 3,
                "B": [2, 4, None, 3] * 3,
                "C": [None, None, None, 1] * 3,
                "D": [0, 1, 5, 4] * 3,
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("A").fillna(0).sort_index(), pdf.groupby("A").fillna(0).sort_index()
        )
        self.assert_eq(
            psdf.groupby("A")["C"].fillna(0).sort_index(),
            pdf.groupby("A")["C"].fillna(0).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")[["C"]].fillna(0).sort_index(),
            pdf.groupby("A")[["C"]].fillna(0).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A").fillna(method="bfill").sort_index(),
            pdf.groupby("A").fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["C"].fillna(method="bfill").sort_index(),
            pdf.groupby("A")["C"].fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")[["C"]].fillna(method="bfill").sort_index(),
            pdf.groupby("A")[["C"]].fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A").fillna(method="ffill").sort_index(),
            pdf.groupby("A").fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["C"].fillna(method="ffill").sort_index(),
            pdf.groupby("A")["C"].fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")[["C"]].fillna(method="ffill").sort_index(),
            pdf.groupby("A")[["C"]].fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5).fillna(method="bfill").sort_index(),
            pdf.groupby(pdf.A // 5).fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5)["C"].fillna(method="bfill").sort_index(),
            pdf.groupby(pdf.A // 5)["C"].fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5)[["C"]].fillna(method="bfill").sort_index(),
            pdf.groupby(pdf.A // 5)[["C"]].fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5).fillna(method="ffill").sort_index(),
            pdf.groupby(pdf.A // 5).fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5)["C"].fillna(method="ffill").sort_index(),
            pdf.groupby(pdf.A // 5)["C"].fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5)[["C"]].fillna(method="ffill").sort_index(),
            pdf.groupby(pdf.A // 5)[["C"]].fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.C.rename().groupby(psdf.A).fillna(0).sort_index(),
            pdf.C.rename().groupby(pdf.A).fillna(0).sort_index(),
        )
        self.assert_eq(
            psdf.C.groupby(psdf.A.rename()).fillna(0).sort_index(),
            pdf.C.groupby(pdf.A.rename()).fillna(0).sort_index(),
        )
        self.assert_eq(
            psdf.C.rename().groupby(psdf.A.rename()).fillna(0).sort_index(),
            pdf.C.rename().groupby(pdf.A.rename()).fillna(0).sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Z", "D")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("X", "A")).fillna(0).sort_index(),
            pdf.groupby(("X", "A")).fillna(0).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(("X", "A")).fillna(method="bfill").sort_index(),
            pdf.groupby(("X", "A")).fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(("X", "A")).fillna(method="ffill").sort_index(),
            pdf.groupby(("X", "A")).fillna(method="ffill").sort_index(),
        )

    def test_ffill(self):
        idx = np.random.rand(4 * 3)
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 2, 2] * 3,
                "B": [2, 4, None, 3] * 3,
                "C": [None, None, None, 1] * 3,
                "D": [0, 1, 5, 4] * 3,
            },
            index=idx,
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("A").ffill().sort_index(), pdf.groupby("A").ffill().sort_index()
        )
        self.assert_eq(
            psdf.groupby("A")[["B"]].ffill().sort_index(),
            pdf.groupby("A")[["B"]].ffill().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["B"].ffill().sort_index(), pdf.groupby("A")["B"].ffill().sort_index()
        )
        self.assert_eq(
            psdf.groupby("A")["B"].ffill()[idx[6]], pdf.groupby("A")["B"].ffill()[idx[6]]
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Z", "D")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("X", "A")).ffill().sort_index(),
            pdf.groupby(("X", "A")).ffill().sort_index(),
        )

    def test_bfill(self):
        idx = np.random.rand(4 * 3)
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 2, 2] * 3,
                "B": [2, 4, None, 3] * 3,
                "C": [None, None, None, 1] * 3,
                "D": [0, 1, 5, 4] * 3,
            },
            index=idx,
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("A").bfill().sort_index(), pdf.groupby("A").bfill().sort_index()
        )
        self.assert_eq(
            psdf.groupby("A")[["B"]].bfill().sort_index(),
            pdf.groupby("A")[["B"]].bfill().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["B"].bfill().sort_index(),
            pdf.groupby("A")["B"].bfill().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["B"].bfill()[idx[6]], pdf.groupby("A")["B"].bfill()[idx[6]]
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Z", "D")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("X", "A")).bfill().sort_index(),
            pdf.groupby(("X", "A")).bfill().sort_index(),
        )

    def test_shift(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 2, 2, 3, 3] * 3,
                "b": [1, 1, 2, 2, 3, 4] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("a").shift().sort_index(), pdf.groupby("a").shift().sort_index()
        )
        # TODO: seems like a pandas' bug when fill_value is not None?
        # self.assert_eq(psdf.groupby(['a', 'b']).shift(periods=-1, fill_value=0).sort_index(),
        #                pdf.groupby(['a', 'b']).shift(periods=-1, fill_value=0).sort_index())
        self.assert_eq(
            psdf.groupby(["b"])["a"].shift().sort_index(),
            pdf.groupby(["b"])["a"].shift().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"])["c"].shift().sort_index(),
            pdf.groupby(["a", "b"])["c"].shift().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).shift().sort_index(),
            pdf.groupby(pdf.b // 5).shift().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].shift().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].shift().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).shift().sort_index(),
            pdf.a.rename().groupby(pdf.b).shift().sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).shift().sort_index(),
            pdf.a.groupby(pdf.b.rename()).shift().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).shift().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).shift().sort_index(),
        )

        self.assert_eq(psdf.groupby("a").shift().sum(), pdf.groupby("a").shift().sum().astype(int))
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).shift().sum(),
            pdf.a.rename().groupby(pdf.b).shift().sum(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "a")).shift().sort_index(),
            pdf.groupby(("x", "a")).shift().sort_index(),
        )
        # TODO: seems like a pandas' bug when fill_value is not None?
        # self.assert_eq(psdf.groupby([('x', 'a'), ('x', 'b')]).shift(periods=-1,
        #                                                            fill_value=0).sort_index(),
        #                pdf.groupby([('x', 'a'), ('x', 'b')]).shift(periods=-1,
        #                                                            fill_value=0).sort_index())

    def test_apply(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby("b").apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b").apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").apply(len).sort_index(),
            pdf.groupby("b").apply(len).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")["a"]
            .apply(lambda x, y, z: x + x.min() + y * z, 10, z=20)
            .sort_index(),
            pdf.groupby("b")["a"].apply(lambda x, y, z: x + x.min() + y * z, 10, z=20).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")[["a"]].apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b")[["a"]].apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"])
            .apply(lambda x, y, z: x + x.min() + y + z, 1, z=2)
            .sort_index(),
            pdf.groupby(["a", "b"]).apply(lambda x, y, z: x + x.min() + y + z, 1, z=2).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["c"].apply(lambda x: 1).sort_index(),
            pdf.groupby(["b"])["c"].apply(lambda x: 1).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["c"].apply(len).sort_index(),
            pdf.groupby(["b"])["c"].apply(len).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5).apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)["a"].apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)[["a"]].apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)[["a"]].apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)[["a"]].apply(len).sort_index(),
            pdf.groupby(pdf.b // 5)[["a"]].apply(len).sort_index(),
            almost=True,
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).apply(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b).apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
            pdf.a.groupby(pdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
        )

        with self.assertRaisesRegex(TypeError, "int object is not callable"):
            psdf.groupby("b").apply(1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).apply(lambda x: 1).sort_index(),
            pdf.groupby(("x", "b")).apply(lambda x: 1).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(("x", "b")).apply(len).sort_index(),
            pdf.groupby(("x", "b")).apply(len).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).apply(len).sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).apply(len).sort_index(),
        )

    def test_apply_without_shortcut(self):
        with option_context("compute.shortcut_limit", 0):
            self.test_apply()

    def test_apply_with_type_hint(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)

        def add_max1(x) -> ps.DataFrame[int, int, int]:
            return x + x.min()

        # Type hints set the default column names, and we use default index for
        # pandas API on Spark. Here we ignore both diff.
        actual = psdf.groupby("b").apply(add_max1).sort_index()
        expected = pdf.groupby("b").apply(add_max1).sort_index()
        self.assert_eq(sorted(actual["c0"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["c1"].to_numpy()), sorted(expected["b"].to_numpy()))
        self.assert_eq(sorted(actual["c2"].to_numpy()), sorted(expected["c"].to_numpy()))

        def add_max2(
            x,
        ) -> ps.DataFrame[slice("a", int), slice("b", int), slice("c", int)]:  # noqa: F405
            return x + x.min()

        actual = psdf.groupby("b").apply(add_max2).sort_index()
        expected = pdf.groupby("b").apply(add_max2).sort_index()
        self.assert_eq(sorted(actual["a"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["c"].to_numpy()), sorted(expected["c"].to_numpy()))
        self.assert_eq(sorted(actual["c"].to_numpy()), sorted(expected["c"].to_numpy()))

    def test_apply_negative(self):
        def func(_) -> ps.Series[int]:
            return pd.Series([1])

        with self.assertRaisesRegex(TypeError, "Series as a return type hint at frame groupby"):
            ps.range(10).groupby("id").apply(func)

    def test_apply_with_new_dataframe(self):
        pdf = pd.DataFrame(
            {"timestamp": [0.0, 0.5, 1.0, 0.0, 0.5], "car_id": ["A", "A", "A", "B", "B"]}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
            pdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
        )

        self.assert_eq(
            psdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
            pdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
        )

        # dataframe with 1000+ records
        pdf = pd.DataFrame(
            {
                "timestamp": [0.0, 0.5, 1.0, 0.0, 0.5] * 300,
                "car_id": ["A", "A", "A", "B", "B"] * 300,
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
            pdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
        )

        self.assert_eq(
            psdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
            pdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
        )

    def test_apply_infer_schema_without_shortcut(self):
        # SPARK-39054: Ensure infer schema accuracy in GroupBy.apply
        with option_context("compute.shortcut_limit", 0):
            dfs = (
                {"timestamp": [0.0], "car_id": ["A"]},
                {"timestamp": [0.0, 0.0], "car_id": ["A", "A"]},
            )
            func = lambda _: pd.DataFrame({"column": [0.0]})  # noqa: E731
            for df in dfs:
                pdf = pd.DataFrame(df)
                psdf = ps.from_pandas(pdf)
                self.assert_eq(
                    psdf.groupby("car_id").apply(func).sort_index(),
                    pdf.groupby("car_id").apply(func).sort_index(),
                )

    def test_apply_with_new_dataframe_without_shortcut(self):
        with option_context("compute.shortcut_limit", 0):
            self.test_apply_with_new_dataframe()

    def test_apply_key_handling(self):
        pdf = pd.DataFrame(
            {"d": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0], "v": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("d").apply(sum).sort_index(), pdf.groupby("d").apply(sum).sort_index()
        )

        with ps.option_context("compute.shortcut_limit", 1):
            self.assert_eq(
                psdf.groupby("d").apply(sum).sort_index(), pdf.groupby("d").apply(sum).sort_index()
            )

    def test_apply_with_side_effect(self):
        pdf = pd.DataFrame(
            {"d": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0], "v": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]}
        )
        psdf = ps.from_pandas(pdf)

        acc = ps.utils.default_session().sparkContext.accumulator(0)

        def sum_with_acc_frame(x) -> ps.DataFrame[np.float64, np.float64]:
            nonlocal acc
            acc += 1
            return np.sum(x)

        actual = psdf.groupby("d").apply(sum_with_acc_frame)
        actual.columns = ["d", "v"]
        self.assert_eq(
            actual._to_pandas().sort_index(),
            pdf.groupby("d").apply(sum).sort_index().reset_index(drop=True),
        )
        self.assert_eq(acc.value, 2)

        def sum_with_acc_series(x) -> np.float64:
            nonlocal acc
            acc += 1
            return np.sum(x)

        self.assert_eq(
            psdf.groupby("d")["v"].apply(sum_with_acc_series)._to_pandas().sort_index(),
            pdf.groupby("d")["v"].apply(sum).sort_index().reset_index(drop=True),
        )
        self.assert_eq(acc.value, 4)

    def test_apply_return_series(self):
        # SPARK-36907: Fix DataFrameGroupBy.apply without shortcut.
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").apply(lambda x: x.iloc[0]).sort_index(),
            pdf.groupby("b").apply(lambda x: x.iloc[0]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").apply(lambda x: x["a"]).sort_index(),
            pdf.groupby("b").apply(lambda x: x["a"]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b", "c"]).apply(lambda x: x.iloc[0]).sort_index(),
            pdf.groupby(["b", "c"]).apply(lambda x: x.iloc[0]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b", "c"]).apply(lambda x: x["a"]).sort_index(),
            pdf.groupby(["b", "c"]).apply(lambda x: x["a"]).sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).apply(lambda x: x.iloc[0]).sort_index(),
            pdf.groupby(("x", "b")).apply(lambda x: x.iloc[0]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(("x", "b")).apply(lambda x: x[("x", "a")]).sort_index(),
            pdf.groupby(("x", "b")).apply(lambda x: x[("x", "a")]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "b"), ("y", "c")]).apply(lambda x: x.iloc[0]).sort_index(),
            pdf.groupby([("x", "b"), ("y", "c")]).apply(lambda x: x.iloc[0]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "b"), ("y", "c")]).apply(lambda x: x[("x", "a")]).sort_index(),
            pdf.groupby([("x", "b"), ("y", "c")]).apply(lambda x: x[("x", "a")]).sort_index(),
        )

    def test_apply_return_series_without_shortcut(self):
        # SPARK-36907: Fix DataFrameGroupBy.apply without shortcut.
        with ps.option_context("compute.shortcut_limit", 2):
            self.test_apply_return_series()

    def test_apply_explicitly_infer(self):
        # SPARK-39317
        from pyspark.pandas.utils import SPARK_CONF_ARROW_ENABLED

        def plus_min(x):
            return x + x.min()

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            df = ps.DataFrame({"A": ["a", "a", "b"], "B": [1, 2, 3]}, columns=["A", "B"])
            g = df.groupby("A")
            g.apply(plus_min).sort_index()

    def test_transform(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby("b").transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b").transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")["a"].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b")["a"].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")[["a"]].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b")[["a"]].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(["a", "b"]).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["c"].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(["b"])["c"].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)["a"].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)[["a"]].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)[["a"]].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).transform(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
            pdf.a.groupby(pdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
        )
        with self.assertRaisesRegex(TypeError, "str object is not callable"):
            psdf.groupby("a").transform("sum")

        def udf(col) -> int:
            return col + 10

        with self.assertRaisesRegex(
            TypeError,
            "Expected the return type of this function to be of Series type, "
            "but found type ScalarType\\[LongType\\(\\)\\]",
        ):
            psdf.groupby("a").transform(udf)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(("x", "b")).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).transform(lambda x: x + x.min()).sort_index(),
        )

    def test_transform_without_shortcut(self):
        with option_context("compute.shortcut_limit", 0):
            self.test_transform()

    def test_filter(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby("b").filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")["a"].filter(lambda x: any(x == 2)).sort_index(),
            pdf.groupby("b")["a"].filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby("b")[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby(["a", "b"]).filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf["b"] // 5).filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby(pdf["b"] // 5).filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf["b"] // 5)["a"].filter(lambda x: any(x == 2)).sort_index(),
            pdf.groupby(pdf["b"] // 5)["a"].filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf["b"] // 5)[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby(pdf["b"] // 5)[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).filter(lambda x: any(x == 2)).sort_index(),
            pdf.a.rename().groupby(pdf.b).filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
            pdf.a.groupby(pdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
        )

        with self.assertRaisesRegex(TypeError, "int object is not callable"):
            psdf.groupby("b").filter(1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).filter(lambda x: any(x[("x", "a")] == 2)).sort_index(),
            pdf.groupby(("x", "b")).filter(lambda x: any(x[("x", "a")] == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")])
            .filter(lambda x: any(x[("x", "a")] == 2))
            .sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")])
            .filter(lambda x: any(x[("x", "a")] == 2))
            .sort_index(),
        )

    def test_idxmax(self):
        pdf = pd.DataFrame(
            {"a": [1, 1, 2, 2, 3] * 3, "b": [1, 2, 3, 4, 5] * 3, "c": [5, 4, 3, 2, 1] * 3}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.groupby(["a"]).idxmax().sort_index(), psdf.groupby(["a"]).idxmax().sort_index()
        )
        self.assert_eq(
            pdf.groupby(["a"]).idxmax(skipna=False).sort_index(),
            psdf.groupby(["a"]).idxmax(skipna=False).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(["a"])["b"].idxmax().sort_index(),
            psdf.groupby(["a"])["b"].idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).idxmax().sort_index(),
            psdf.b.rename().groupby(psdf.a).idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).idxmax().sort_index(),
            psdf.b.groupby(psdf.a.rename()).idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).idxmax().sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).idxmax().sort_index(),
        )

        with self.assertRaisesRegex(ValueError, "idxmax only support one-level index now"):
            psdf.set_index(["a", "b"]).groupby(["c"]).idxmax()

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            pdf.groupby(("x", "a")).idxmax().sort_index(),
            psdf.groupby(("x", "a")).idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.groupby(("x", "a")).idxmax(skipna=False).sort_index(),
            psdf.groupby(("x", "a")).idxmax(skipna=False).sort_index(),
        )

    def test_idxmin(self):
        pdf = pd.DataFrame(
            {"a": [1, 1, 2, 2, 3] * 3, "b": [1, 2, 3, 4, 5] * 3, "c": [5, 4, 3, 2, 1] * 3}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.groupby(["a"]).idxmin().sort_index(), psdf.groupby(["a"]).idxmin().sort_index()
        )
        self.assert_eq(
            pdf.groupby(["a"]).idxmin(skipna=False).sort_index(),
            psdf.groupby(["a"]).idxmin(skipna=False).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(["a"])["b"].idxmin().sort_index(),
            psdf.groupby(["a"])["b"].idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).idxmin().sort_index(),
            psdf.b.rename().groupby(psdf.a).idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).idxmin().sort_index(),
            psdf.b.groupby(psdf.a.rename()).idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).idxmin().sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).idxmin().sort_index(),
        )

        with self.assertRaisesRegex(ValueError, "idxmin only support one-level index now"):
            psdf.set_index(["a", "b"]).groupby(["c"]).idxmin()

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            pdf.groupby(("x", "a")).idxmin().sort_index(),
            psdf.groupby(("x", "a")).idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.groupby(("x", "a")).idxmin(skipna=False).sort_index(),
            psdf.groupby(("x", "a")).idxmin(skipna=False).sort_index(),
        )

    def test_head(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )
        psdf = ps.from_pandas(pdf)

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby("a").head(limit).sort_index(),
                psdf.groupby("a").head(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")["b"].head(limit).sort_index(),
                psdf.groupby("a")["b"].head(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")[["b"]].head(limit).sort_index(),
                psdf.groupby("a")[["b"]].head(limit).sort_index(),
            )

        self.assert_eq(
            pdf.groupby(pdf.a // 2).head(2).sort_index(),
            psdf.groupby(psdf.a // 2).head(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)["b"].head(2).sort_index(),
            psdf.groupby(psdf.a // 2)["b"].head(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)[["b"]].head(2).sort_index(),
            psdf.groupby(psdf.a // 2)[["b"]].head(2).sort_index(),
        )

        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).head(2).sort_index(),
            psdf.b.rename().groupby(psdf.a).head(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).head(2).sort_index(),
            psdf.b.groupby(psdf.a.rename()).head(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).head(2).sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).head(2).sort_index(),
        )

        # multi-index
        midx = pd.MultiIndex(
            [["x", "y"], ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]],
            [[0, 0, 0, 0, 0, 1, 1, 1, 1, 1], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]],
        )
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3],
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5],
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6],
            },
            columns=["a", "b", "c"],
            index=midx,
        )
        psdf = ps.from_pandas(pdf)

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby("a").head(limit).sort_index(),
                psdf.groupby("a").head(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")["b"].head(limit).sort_index(),
                psdf.groupby("a")["b"].head(limit).sort_index(),
            )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby(("x", "a")).head(limit).sort_index(),
                psdf.groupby(("x", "a")).head(limit).sort_index(),
            )

    def test_missing(self):
        psdf = ps.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9]})

        # DataFrameGroupBy functions
        missing_functions = inspect.getmembers(
            MissingPandasLikeDataFrameGroupBy, inspect.isfunction
        )
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*GroupBy.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.groupby("a"), name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*GroupBy.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.groupby("a"), name)()

        # SeriesGroupBy functions
        missing_functions = inspect.getmembers(MissingPandasLikeSeriesGroupBy, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*GroupBy.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.groupby(psdf.a), name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*GroupBy.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.groupby(psdf.a), name)()

        # DataFrameGroupBy properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeDataFrameGroupBy, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*GroupBy.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.groupby("a"), name)
        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*GroupBy.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.groupby("a"), name)

        # SeriesGroupBy properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeSeriesGroupBy, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*GroupBy.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.groupby(psdf.a), name)
        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*GroupBy.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.groupby(psdf.a), name)

    @staticmethod
    def test_is_multi_agg_with_relabel():

        assert is_multi_agg_with_relabel(a="max") is False
        assert is_multi_agg_with_relabel(a_min=("a", "max"), a_max=("a", "min")) is True

    def test_get_group(self):
        pdf = pd.DataFrame(
            [
                ("falcon", "bird", 389.0),
                ("parrot", "bird", 24.0),
                ("lion", "mammal", 80.5),
                ("monkey", "mammal", np.nan),
            ],
            columns=["name", "class", "max_speed"],
            index=[0, 2, 3, 1],
        )
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("class").get_group("bird"),
            pdf.groupby("class").get_group("bird"),
        )
        self.assert_eq(
            psdf.groupby("class")["name"].get_group("mammal"),
            pdf.groupby("class")["name"].get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby("class")[["name"]].get_group("mammal"),
            pdf.groupby("class")[["name"]].get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby(["class", "name"]).get_group(("mammal", "lion")),
            pdf.groupby(["class", "name"]).get_group(("mammal", "lion")),
        )
        self.assert_eq(
            psdf.groupby(["class", "name"])["max_speed"].get_group(("mammal", "lion")),
            pdf.groupby(["class", "name"])["max_speed"].get_group(("mammal", "lion")),
        )
        self.assert_eq(
            psdf.groupby(["class", "name"])[["max_speed"]].get_group(("mammal", "lion")),
            pdf.groupby(["class", "name"])[["max_speed"]].get_group(("mammal", "lion")),
        )
        self.assert_eq(
            (psdf.max_speed + 1).groupby(psdf["class"]).get_group("mammal"),
            (pdf.max_speed + 1).groupby(pdf["class"]).get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby("max_speed").get_group(80.5),
            pdf.groupby("max_speed").get_group(80.5),
        )

        self.assertRaises(KeyError, lambda: psdf.groupby("class").get_group("fish"))
        self.assertRaises(TypeError, lambda: psdf.groupby("class").get_group(["bird", "mammal"]))
        self.assertRaises(KeyError, lambda: psdf.groupby("class")["name"].get_group("fish"))
        self.assertRaises(
            TypeError, lambda: psdf.groupby("class")["name"].get_group(["bird", "mammal"])
        )
        self.assertRaises(
            KeyError, lambda: psdf.groupby(["class", "name"]).get_group(("lion", "mammal"))
        )
        self.assertRaises(ValueError, lambda: psdf.groupby(["class", "name"]).get_group(("lion",)))
        self.assertRaises(
            ValueError, lambda: psdf.groupby(["class", "name"]).get_group(("mammal",))
        )
        self.assertRaises(ValueError, lambda: psdf.groupby(["class", "name"]).get_group("mammal"))

        # MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("A", "name"), ("B", "class"), ("C", "max_speed")])
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby(("B", "class")).get_group("bird"),
            pdf.groupby(("B", "class")).get_group("bird"),
        )
        self.assert_eq(
            psdf.groupby(("B", "class"))[[("A", "name")]].get_group("mammal"),
            pdf.groupby(("B", "class"))[[("A", "name")]].get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby([("B", "class"), ("A", "name")]).get_group(("mammal", "lion")),
            pdf.groupby([("B", "class"), ("A", "name")]).get_group(("mammal", "lion")),
        )
        self.assert_eq(
            psdf.groupby([("B", "class"), ("A", "name")])[[("C", "max_speed")]].get_group(
                ("mammal", "lion")
            ),
            pdf.groupby([("B", "class"), ("A", "name")])[[("C", "max_speed")]].get_group(
                ("mammal", "lion")
            ),
        )
        self.assert_eq(
            (psdf[("C", "max_speed")] + 1).groupby(psdf[("B", "class")]).get_group("mammal"),
            (pdf[("C", "max_speed")] + 1).groupby(pdf[("B", "class")]).get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby(("C", "max_speed")).get_group(80.5),
            pdf.groupby(("C", "max_speed")).get_group(80.5),
        )

        self.assertRaises(KeyError, lambda: psdf.groupby(("B", "class")).get_group("fish"))
        self.assertRaises(
            TypeError, lambda: psdf.groupby(("B", "class")).get_group(["bird", "mammal"])
        )
        self.assertRaises(
            KeyError, lambda: psdf.groupby(("B", "class"))[("A", "name")].get_group("fish")
        )
        self.assertRaises(
            KeyError,
            lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group(("lion", "mammal")),
        )
        self.assertRaises(
            ValueError,
            lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group(("lion",)),
        )
        self.assertRaises(
            ValueError, lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group(("mammal",))
        )
        self.assertRaises(
            ValueError, lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group("mammal")
        )

    def test_median(self):
        psdf = ps.DataFrame(
            {
                "a": [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0],
                "b": [2.0, 3.0, 1.0, 4.0, 6.0, 9.0, 8.0, 10.0, 7.0, 5.0],
                "c": [3.0, 5.0, 2.0, 5.0, 1.0, 2.0, 6.0, 4.0, 3.0, 6.0],
            },
            columns=["a", "b", "c"],
            index=[7, 2, 4, 1, 3, 4, 9, 10, 5, 6],
        )
        # DataFrame
        expected_result = ps.DataFrame(
            {"b": [2.0, 8.0, 7.0], "c": [3.0, 2.0, 4.0]}, index=pd.Index([1.0, 2.0, 3.0], name="a")
        )
        self.assert_eq(expected_result, psdf.groupby("a").median().sort_index())
        # Series
        expected_result = ps.Series(
            [2.0, 8.0, 7.0], name="b", index=pd.Index([1.0, 2.0, 3.0], name="a")
        )
        self.assert_eq(expected_result, psdf.groupby("a")["b"].median().sort_index())

        with self.assertRaisesRegex(TypeError, "accuracy must be an integer; however"):
            psdf.groupby("a").median(accuracy="a")

    def test_tail(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )
        psdf = ps.from_pandas(pdf)

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby("a").tail(limit).sort_index(),
                psdf.groupby("a").tail(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")["b"].tail(limit).sort_index(),
                psdf.groupby("a")["b"].tail(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")[["b"]].tail(limit).sort_index(),
                psdf.groupby("a")[["b"]].tail(limit).sort_index(),
            )

        self.assert_eq(
            pdf.groupby(pdf.a // 2).tail(2).sort_index(),
            psdf.groupby(psdf.a // 2).tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)["b"].tail(2).sort_index(),
            psdf.groupby(psdf.a // 2)["b"].tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)[["b"]].tail(2).sort_index(),
            psdf.groupby(psdf.a // 2)[["b"]].tail(2).sort_index(),
        )

        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).tail(2).sort_index(),
            psdf.b.rename().groupby(psdf.a).tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).tail(2).sort_index(),
            psdf.b.groupby(psdf.a.rename()).tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).tail(2).sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).tail(2).sort_index(),
        )

        # multi-index
        midx = pd.MultiIndex(
            [["x", "y"], ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]],
            [[0, 0, 0, 0, 0, 1, 1, 1, 1, 1], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]],
        )
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3],
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5],
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6],
            },
            columns=["a", "b", "c"],
            index=midx,
        )
        psdf = ps.from_pandas(pdf)

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby("a").tail(limit).sort_index(),
                psdf.groupby("a").tail(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")["b"].tail(limit).sort_index(),
                psdf.groupby("a")["b"].tail(limit).sort_index(),
            )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby(("x", "a")).tail(limit).sort_index(),
                psdf.groupby(("x", "a")).tail(limit).sort_index(),
            )

    def test_ddof(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )
        psdf = ps.from_pandas(pdf)

        for ddof in [-1, 0, 1, 2, 3]:
            # std
            self.assert_eq(
                pdf.groupby("a").std(ddof=ddof).sort_index(),
                psdf.groupby("a").std(ddof=ddof).sort_index(),
                check_exact=False,
            )
            self.assert_eq(
                pdf.groupby("a")["b"].std(ddof=ddof).sort_index(),
                psdf.groupby("a")["b"].std(ddof=ddof).sort_index(),
                check_exact=False,
            )
            # var
            self.assert_eq(
                pdf.groupby("a").var(ddof=ddof).sort_index(),
                psdf.groupby("a").var(ddof=ddof).sort_index(),
                check_exact=False,
            )
            self.assert_eq(
                pdf.groupby("a")["b"].var(ddof=ddof).sort_index(),
                psdf.groupby("a")["b"].var(ddof=ddof).sort_index(),
                check_exact=False,
            )
            # sem
            self.assert_eq(
                pdf.groupby("a").sem(ddof=ddof).sort_index(),
                psdf.groupby("a").sem(ddof=ddof).sort_index(),
                check_exact=False,
            )
            self.assert_eq(
                pdf.groupby("a")["b"].sem(ddof=ddof).sort_index(),
                psdf.groupby("a")["b"].sem(ddof=ddof).sort_index(),
                check_exact=False,
            )

    def test_getitem(self):
        psdf = ps.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )

        self.assertTrue(isinstance(psdf.groupby("a")["b"], SeriesGroupBy))


if __name__ == "__main__":
    from pyspark.pandas.tests.test_groupby import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
