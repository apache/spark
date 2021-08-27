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
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils
from pyspark.pandas.window import Rolling


class RollingTest(PandasOnSparkTestCase, TestUtils):
    def test_rolling_error(self):
        with self.assertRaisesRegex(ValueError, "window must be >= 0"):
            ps.range(10).rolling(window=-1)
        with self.assertRaisesRegex(ValueError, "min_periods must be >= 0"):
            ps.range(10).rolling(window=1, min_periods=-1)

        with self.assertRaisesRegex(
            TypeError, "psdf_or_psser must be a series or dataframe; however, got:.*int"
        ):
            Rolling(1, 2)

    def _test_rolling_func(self, f):
        pser = pd.Series([1, 2, 3], index=np.random.rand(3), name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(getattr(psser.rolling(2), f)(), getattr(pser.rolling(2), f)())
        self.assert_eq(getattr(psser.rolling(2), f)().sum(), getattr(pser.rolling(2), f)().sum())

        # Multiindex
        pser = pd.Series(
            [1, 2, 3],
            index=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z")]),
            name="a",
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(getattr(psser.rolling(2), f)(), getattr(pser.rolling(2), f)())

        pdf = pd.DataFrame(
            {"a": [1.0, 2.0, 3.0, 2.0], "b": [4.0, 2.0, 3.0, 1.0]}, index=np.random.rand(4)
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(getattr(psdf.rolling(2), f)(), getattr(pdf.rolling(2), f)())
        self.assert_eq(getattr(psdf.rolling(2), f)().sum(), getattr(pdf.rolling(2), f)().sum())

        # Multiindex column
        columns = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(getattr(psdf.rolling(2), f)(), getattr(pdf.rolling(2), f)())

    def test_rolling_min(self):
        self._test_rolling_func("min")

    def test_rolling_max(self):
        self._test_rolling_func("max")

    def test_rolling_mean(self):
        self._test_rolling_func("mean")

    def test_rolling_sum(self):
        self._test_rolling_func("sum")

    def test_rolling_count(self):
        self._test_rolling_func("count")

    def test_rolling_std(self):
        self._test_rolling_func("std")

    def test_rolling_var(self):
        self._test_rolling_func("var")

    def _test_groupby_rolling_func(self, f):
        pser = pd.Series([1, 2, 3, 2], index=np.random.rand(4), name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(
            getattr(psser.groupby(psser).rolling(2), f)().sort_index(),
            getattr(pser.groupby(pser).rolling(2), f)().sort_index(),
        )
        self.assert_eq(
            getattr(psser.groupby(psser).rolling(2), f)().sum(),
            getattr(pser.groupby(pser).rolling(2), f)().sum(),
        )

        # Multiindex
        pser = pd.Series(
            [1, 2, 3, 2],
            index=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z"), ("c", "z")]),
            name="a",
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(
            getattr(psser.groupby(psser).rolling(2), f)().sort_index(),
            getattr(pser.groupby(pser).rolling(2), f)().sort_index(),
        )

        pdf = pd.DataFrame({"a": [1.0, 2.0, 3.0, 2.0], "b": [4.0, 2.0, 3.0, 1.0]})
        psdf = ps.from_pandas(pdf)

        # The behavior of GroupBy.rolling is changed from pandas 1.3.
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            self.assert_eq(
                getattr(psdf.groupby(psdf.a).rolling(2), f)().sort_index(),
                getattr(pdf.groupby(pdf.a).rolling(2), f)().sort_index(),
            )
            self.assert_eq(
                getattr(psdf.groupby(psdf.a).rolling(2), f)().sum(),
                getattr(pdf.groupby(pdf.a).rolling(2), f)().sum(),
            )
            self.assert_eq(
                getattr(psdf.groupby(psdf.a + 1).rolling(2), f)().sort_index(),
                getattr(pdf.groupby(pdf.a + 1).rolling(2), f)().sort_index(),
            )
        else:
            self.assert_eq(
                getattr(psdf.groupby(psdf.a).rolling(2), f)().sort_index(),
                getattr(pdf.groupby(pdf.a).rolling(2), f)().drop("a", axis=1).sort_index(),
            )
            self.assert_eq(
                getattr(psdf.groupby(psdf.a).rolling(2), f)().sum(),
                getattr(pdf.groupby(pdf.a).rolling(2), f)().sum().drop("a"),
            )
            self.assert_eq(
                getattr(psdf.groupby(psdf.a + 1).rolling(2), f)().sort_index(),
                getattr(pdf.groupby(pdf.a + 1).rolling(2), f)().drop("a", axis=1).sort_index(),
            )

        self.assert_eq(
            getattr(psdf.b.groupby(psdf.a).rolling(2), f)().sort_index(),
            getattr(pdf.b.groupby(pdf.a).rolling(2), f)().sort_index(),
        )
        self.assert_eq(
            getattr(psdf.groupby(psdf.a)["b"].rolling(2), f)().sort_index(),
            getattr(pdf.groupby(pdf.a)["b"].rolling(2), f)().sort_index(),
        )
        self.assert_eq(
            getattr(psdf.groupby(psdf.a)[["b"]].rolling(2), f)().sort_index(),
            getattr(pdf.groupby(pdf.a)[["b"]].rolling(2), f)().sort_index(),
        )

        # Multiindex column
        columns = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        pdf.columns = columns
        psdf.columns = columns

        # The behavior of GroupBy.rolling is changed from pandas 1.3.
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            self.assert_eq(
                getattr(psdf.groupby(("a", "x")).rolling(2), f)().sort_index(),
                getattr(pdf.groupby(("a", "x")).rolling(2), f)().sort_index(),
            )

            self.assert_eq(
                getattr(psdf.groupby([("a", "x"), ("a", "y")]).rolling(2), f)().sort_index(),
                getattr(pdf.groupby([("a", "x"), ("a", "y")]).rolling(2), f)().sort_index(),
            )
        else:
            self.assert_eq(
                getattr(psdf.groupby(("a", "x")).rolling(2), f)().sort_index(),
                getattr(pdf.groupby(("a", "x")).rolling(2), f)()
                .drop(("a", "x"), axis=1)
                .sort_index(),
            )

            self.assert_eq(
                getattr(psdf.groupby([("a", "x"), ("a", "y")]).rolling(2), f)().sort_index(),
                getattr(pdf.groupby([("a", "x"), ("a", "y")]).rolling(2), f)()
                .drop([("a", "x"), ("a", "y")], axis=1)
                .sort_index(),
            )

    def test_groupby_rolling_count(self):
        self._test_groupby_rolling_func("count")

    def test_groupby_rolling_min(self):
        self._test_groupby_rolling_func("min")

    def test_groupby_rolling_max(self):
        self._test_groupby_rolling_func("max")

    def test_groupby_rolling_mean(self):
        self._test_groupby_rolling_func("mean")

    def test_groupby_rolling_sum(self):
        self._test_groupby_rolling_func("sum")

    def test_groupby_rolling_std(self):
        # TODO: `std` now raise error in pandas 1.0.0
        self._test_groupby_rolling_func("std")

    def test_groupby_rolling_var(self):
        self._test_groupby_rolling_func("var")


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_rolling import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
