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

import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.pandas.window import Expanding
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class ExpandingTest(PandasOnSparkTestCase, TestUtils):
    def _test_expanding_func(self, ps_func, pd_func=None):
        if not pd_func:
            pd_func = ps_func
        if isinstance(pd_func, str):
            pd_func = self.convert_str_to_lambda(pd_func)
        if isinstance(ps_func, str):
            ps_func = self.convert_str_to_lambda(ps_func)
        pser = pd.Series([1, 2, 3, 7, 9, 8], index=np.random.rand(6), name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(ps_func(psser.expanding(2)), pd_func(pser.expanding(2)), almost=True)
        self.assert_eq(ps_func(psser.expanding(2)), pd_func(pser.expanding(2)), almost=True)

        # Multiindex
        pser = pd.Series(
            [1, 2, 3], index=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z")])
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(ps_func(psser.expanding(2)), pd_func(pser.expanding(2)))

        pdf = pd.DataFrame(
            {"a": [1.0, 2.0, 3.0, 2.0], "b": [4.0, 2.0, 3.0, 1.0]}, index=np.random.rand(4)
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(ps_func(psdf.expanding(2)), pd_func(pdf.expanding(2)))
        self.assert_eq(ps_func(psdf.expanding(2)).sum(), pd_func(pdf.expanding(2)).sum())

        # Multiindex column
        columns = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(ps_func(psdf.expanding(2)), pd_func(pdf.expanding(2)))

    def test_expanding_error(self):
        with self.assertRaisesRegex(ValueError, "min_periods must be >= 0"):
            ps.range(10).expanding(-1)

        with self.assertRaisesRegex(
            TypeError, "psdf_or_psser must be a series or dataframe; however, got:.*int"
        ):
            Expanding(1, 2)

    def test_expanding_repr(self):
        self.assertEqual(repr(ps.range(10).expanding(5)), "Expanding [min_periods=5]")

    def test_expanding_count(self):
        self._test_expanding_func("count")

    def test_expanding_min(self):
        self._test_expanding_func("min")

    def test_expanding_max(self):
        self._test_expanding_func("max")

    def test_expanding_mean(self):
        self._test_expanding_func("mean")

    def test_expanding_quantile(self):
        self._test_expanding_func(lambda x: x.quantile(0.5), lambda x: x.quantile(0.5, "lower"))

    def test_expanding_sum(self):
        self._test_expanding_func("sum")

    def test_expanding_std(self):
        self._test_expanding_func("std")

    def test_expanding_var(self):
        self._test_expanding_func("var")

    def test_expanding_skew(self):
        self._test_expanding_func("skew")

    def test_expanding_kurt(self):
        self._test_expanding_func("kurt")

    def _test_groupby_expanding_func(self, ps_func, pd_func=None):
        if not pd_func:
            pd_func = ps_func
        if isinstance(pd_func, str):
            pd_func = self.convert_str_to_lambda(pd_func)
        if isinstance(ps_func, str):
            ps_func = self.convert_str_to_lambda(ps_func)
        pser = pd.Series([1, 2, 3, 2], index=np.random.rand(4), name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(
            ps_func(psser.groupby(psser).expanding(2)).sort_index(),
            pd_func(pser.groupby(pser).expanding(2)).sort_index(),
        )
        self.assert_eq(
            ps_func(psser.groupby(psser).expanding(2)).sum(),
            pd_func(pser.groupby(pser).expanding(2)).sum(),
        )

        # Multiindex
        pser = pd.Series(
            [1, 2, 3, 2],
            index=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z"), ("c", "z")]),
            name="a",
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(
            ps_func(psser.groupby(psser).expanding(2)).sort_index(),
            pd_func(pser.groupby(pser).expanding(2)).sort_index(),
        )

        pdf = pd.DataFrame({"a": [1.0, 2.0, 3.0, 2.0], "b": [4.0, 2.0, 3.0, 1.0]})
        psdf = ps.from_pandas(pdf)

        # The behavior of GroupBy.expanding is changed from pandas 1.3.
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            self.assert_eq(
                ps_func(psdf.groupby(psdf.a).expanding(2)).sort_index(),
                pd_func(pdf.groupby(pdf.a).expanding(2)).sort_index(),
            )
            self.assert_eq(
                ps_func(psdf.groupby(psdf.a).expanding(2)).sum(),
                pd_func(pdf.groupby(pdf.a).expanding(2)).sum(),
            )
            self.assert_eq(
                ps_func(psdf.groupby(psdf.a + 1).expanding(2)).sort_index(),
                pd_func(pdf.groupby(pdf.a + 1).expanding(2)).sort_index(),
            )
        else:
            self.assert_eq(
                ps_func(psdf.groupby(psdf.a).expanding(2)).sort_index(),
                pd_func(pdf.groupby(pdf.a).expanding(2)).drop("a", axis=1).sort_index(),
            )
            self.assert_eq(
                ps_func(psdf.groupby(psdf.a).expanding(2)).sum(),
                pd_func(pdf.groupby(pdf.a).expanding(2)).sum().drop("a"),
            )
            self.assert_eq(
                ps_func(psdf.groupby(psdf.a + 1).expanding(2)).sort_index(),
                pd_func(pdf.groupby(pdf.a + 1).expanding(2)).drop("a", axis=1).sort_index(),
            )

        self.assert_eq(
            ps_func(psdf.b.groupby(psdf.a).expanding(2)).sort_index(),
            pd_func(pdf.b.groupby(pdf.a).expanding(2)).sort_index(),
        )
        self.assert_eq(
            ps_func(psdf.groupby(psdf.a)["b"].expanding(2)).sort_index(),
            pd_func(pdf.groupby(pdf.a)["b"].expanding(2)).sort_index(),
        )
        self.assert_eq(
            ps_func(psdf.groupby(psdf.a)[["b"]].expanding(2)).sort_index(),
            pd_func(pdf.groupby(pdf.a)[["b"]].expanding(2)).sort_index(),
        )

        # Multiindex column
        columns = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        pdf.columns = columns
        psdf.columns = columns

        # The behavior of GroupBy.expanding is changed from pandas 1.3.
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            self.assert_eq(
                ps_func(psdf.groupby(("a", "x")).expanding(2)).sort_index(),
                pd_func(pdf.groupby(("a", "x")).expanding(2)).sort_index(),
            )

            self.assert_eq(
                ps_func(psdf.groupby([("a", "x"), ("a", "y")]).expanding(2)).sort_index(),
                pd_func(pdf.groupby([("a", "x"), ("a", "y")]).expanding(2)).sort_index(),
            )
        else:
            self.assert_eq(
                ps_func(psdf.groupby(("a", "x")).expanding(2)).sort_index(),
                pd_func(pdf.groupby(("a", "x")).expanding(2)).drop(("a", "x"), axis=1).sort_index(),
            )

            self.assert_eq(
                ps_func(psdf.groupby([("a", "x"), ("a", "y")]).expanding(2)).sort_index(),
                pd_func(pdf.groupby([("a", "x"), ("a", "y")]).expanding(2))
                .drop([("a", "x"), ("a", "y")], axis=1)
                .sort_index(),
            )

    def test_groupby_expanding_count(self):
        self._test_groupby_expanding_func("count")

    def test_groupby_expanding_min(self):
        self._test_groupby_expanding_func("min")

    def test_groupby_expanding_max(self):
        self._test_groupby_expanding_func("max")

    def test_groupby_expanding_mean(self):
        self._test_groupby_expanding_func("mean")

    def test_groupby_expanding_quantile(self):
        self._test_groupby_expanding_func(
            lambda x: x.quantile(0.5), lambda x: x.quantile(0.5, "lower")
        )

    def test_groupby_expanding_sum(self):
        self._test_groupby_expanding_func("sum")

    def test_groupby_expanding_std(self):
        self._test_groupby_expanding_func("std")

    def test_groupby_expanding_var(self):
        self._test_groupby_expanding_func("var")

    def test_groupby_expanding_skew(self):
        self._test_groupby_expanding_func("skew")

    def test_groupby_expanding_kurt(self):
        self._test_groupby_expanding_func("kurt")


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_expanding import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
