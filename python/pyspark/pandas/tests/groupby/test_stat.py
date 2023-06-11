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
from pyspark.testing.pandasutils import ComparisonTestBase
from pyspark.testing.sqlutils import SQLTestUtils


class GroupbyStatMixin:
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

    @unittest.skipIf(
        LooseVersion(pd.__version__) >= LooseVersion("2.0.0"),
        "TODO(SPARK-43554): Enable GroupByTests.test_basic_stat_funcs for pandas 2.0.0.",
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

    @unittest.skipIf(
        LooseVersion(pd.__version__) >= LooseVersion("2.0.0"),
        "TODO(SPARK-43706): Enable GroupByTests.test_mean " "for pandas 2.0.0.",
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

    @unittest.skipIf(
        LooseVersion(pd.__version__) >= LooseVersion("2.0.0"),
        "TODO(SPARK-43553): Enable GroupByTests.test_mad for pandas 2.0.0.",
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

    @unittest.skipIf(
        LooseVersion(pd.__version__) >= LooseVersion("2.0.0"),
        "TODO(SPARK-43552): Enable GroupByTests.test_nth for pandas 2.0.0.",
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

    @unittest.skipIf(
        LooseVersion(pd.__version__) >= LooseVersion("2.0.0"),
        "TODO(SPARK-43551): Enable GroupByTests.test_prod for pandas 2.0.0.",
    )
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


class GroupbyStatTests(GroupbyStatMixin, ComparisonTestBase, SQLTestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_stat import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
