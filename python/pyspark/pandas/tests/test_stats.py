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

try:
    from pandas._testing import makeMissingDataframe
except ImportError:
    from pandas.util.testing import makeMissingDataframe

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.testing.pandasutils import PandasOnSparkTestCase, SPARK_CONF_ARROW_ENABLED
from pyspark.testing.sqlutils import SQLTestUtils


class StatsTest(PandasOnSparkTestCase, SQLTestUtils):
    def _test_stat_functions(self, pdf_or_pser, kdf_or_kser):
        functions = ["max", "min", "mean", "sum", "count"]
        for funcname in functions:
            self.assert_eq(getattr(kdf_or_kser, funcname)(), getattr(pdf_or_pser, funcname)())

        functions = ["std", "var", "product", "sem"]
        for funcname in functions:
            self.assert_eq(
                getattr(kdf_or_kser, funcname)(),
                getattr(pdf_or_pser, funcname)(),
                check_exact=False,
            )

        functions = ["std", "var", "sem"]
        for funcname in functions:
            self.assert_eq(
                getattr(kdf_or_kser, funcname)(ddof=0),
                getattr(pdf_or_pser, funcname)(ddof=0),
                check_exact=False,
            )

        # NOTE: To test skew, kurt, and median, just make sure they run.
        #       The numbers are different in spark and pandas.
        functions = ["skew", "kurt", "median"]
        for funcname in functions:
            getattr(kdf_or_kser, funcname)()

    def test_stat_functions(self):
        pdf = pd.DataFrame({"A": [1, 2, 3, 4], "B": [1, 2, 3, 4], "C": [1, np.nan, 3, np.nan]})
        kdf = ps.from_pandas(pdf)
        self._test_stat_functions(pdf.A, kdf.A)
        self._test_stat_functions(pdf, kdf)

        # empty
        self._test_stat_functions(pdf.A.loc[[]], kdf.A.loc[[]])
        self._test_stat_functions(pdf.loc[[]], kdf.loc[[]])

    def test_stat_functions_multiindex_column(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "B", "C"], columns=arrays)
        kdf = ps.from_pandas(pdf)
        self._test_stat_functions(pdf.A, kdf.A)
        self._test_stat_functions(pdf, kdf)

    def test_stat_functions_with_no_numeric_columns(self):
        pdf = pd.DataFrame(
            {
                "A": ["a", None, "c", "d", None, "f", "g"],
                "B": ["A", "B", "C", None, "E", "F", None],
            }
        )
        kdf = ps.from_pandas(pdf)

        self._test_stat_functions(pdf, kdf)

    def test_sum(self):
        pdf = pd.DataFrame({"a": [1, 2, 3, np.nan], "b": [0.1, np.nan, 0.3, np.nan]})
        kdf = ps.from_pandas(pdf)

        self.assert_eq(kdf.sum(), pdf.sum())
        self.assert_eq(kdf.sum(axis=1), pdf.sum(axis=1))
        self.assert_eq(kdf.sum(min_count=3), pdf.sum(min_count=3))
        self.assert_eq(kdf.sum(axis=1, min_count=1), pdf.sum(axis=1, min_count=1))
        self.assert_eq(kdf.loc[[]].sum(), pdf.loc[[]].sum())
        self.assert_eq(kdf.loc[[]].sum(min_count=1), pdf.loc[[]].sum(min_count=1))

        self.assert_eq(kdf["a"].sum(), pdf["a"].sum())
        self.assert_eq(kdf["a"].sum(min_count=3), pdf["a"].sum(min_count=3))
        self.assert_eq(kdf["b"].sum(min_count=3), pdf["b"].sum(min_count=3))
        self.assert_eq(kdf["a"].loc[[]].sum(), pdf["a"].loc[[]].sum())
        self.assert_eq(kdf["a"].loc[[]].sum(min_count=1), pdf["a"].loc[[]].sum(min_count=1))

    def test_product(self):
        pdf = pd.DataFrame(
            {"a": [1, -2, -3, np.nan], "b": [0.1, np.nan, -0.3, np.nan], "c": [10, 20, 0, -10]}
        )
        kdf = ps.from_pandas(pdf)

        self.assert_eq(kdf.product(), pdf.product(), check_exact=False)
        self.assert_eq(kdf.product(axis=1), pdf.product(axis=1))
        self.assert_eq(kdf.product(min_count=3), pdf.product(min_count=3), check_exact=False)
        self.assert_eq(kdf.product(axis=1, min_count=1), pdf.product(axis=1, min_count=1))
        self.assert_eq(kdf.loc[[]].product(), pdf.loc[[]].product())
        self.assert_eq(kdf.loc[[]].product(min_count=1), pdf.loc[[]].product(min_count=1))

        self.assert_eq(kdf["a"].product(), pdf["a"].product(), check_exact=False)
        self.assert_eq(
            kdf["a"].product(min_count=3), pdf["a"].product(min_count=3), check_exact=False
        )
        self.assert_eq(kdf["b"].product(min_count=3), pdf["b"].product(min_count=3))
        self.assert_eq(kdf["c"].product(min_count=3), pdf["c"].product(min_count=3))
        self.assert_eq(kdf["a"].loc[[]].product(), pdf["a"].loc[[]].product())
        self.assert_eq(kdf["a"].loc[[]].product(min_count=1), pdf["a"].loc[[]].product(min_count=1))

    def test_abs(self):
        pdf = pd.DataFrame(
            {
                "A": [1, -2, np.nan, -4, 5],
                "B": [1.0, -2, np.nan, -4, 5],
                "C": [-6.0, -7, -8, np.nan, 10],
                "D": ["a", "b", "c", "d", np.nan],
                "E": [True, np.nan, False, True, True],
            }
        )
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kdf.A.abs(), pdf.A.abs())
        self.assert_eq(kdf.B.abs(), pdf.B.abs())
        self.assert_eq(kdf.E.abs(), pdf.E.abs())
        # pandas' bug?
        # self.assert_eq(kdf[["B", "C", "E"]].abs(), pdf[["B", "C", "E"]].abs())
        self.assert_eq(kdf[["B", "C"]].abs(), pdf[["B", "C"]].abs())
        self.assert_eq(kdf[["E"]].abs(), pdf[["E"]].abs())

        with self.assertRaisesRegex(
            TypeError, "bad operand type for abs\\(\\): object \\(string\\)"
        ):
            kdf.abs()
        with self.assertRaisesRegex(
            TypeError, "bad operand type for abs\\(\\): object \\(string\\)"
        ):
            kdf.D.abs()

    def test_axis_on_dataframe(self):
        # The number of each count is intentionally big
        # because when data is small, it executes a shortcut.
        # Less than 'compute.shortcut_limit' will execute a shortcut
        # by using collected pandas dataframe directly.
        # now we set the 'compute.shortcut_limit' as 1000 explicitly
        with option_context("compute.shortcut_limit", 1000):
            pdf = pd.DataFrame(
                {
                    "A": [1, -2, 3, -4, 5] * 300,
                    "B": [1.0, -2, 3, -4, 5] * 300,
                    "C": [-6.0, -7, -8, -9, 10] * 300,
                    "D": [True, False, True, False, False] * 300,
                },
                index=range(10, 15001, 10),
            )
            kdf = ps.from_pandas(pdf)
            self.assert_eq(kdf.count(axis=1), pdf.count(axis=1))
            self.assert_eq(kdf.var(axis=1), pdf.var(axis=1))
            self.assert_eq(kdf.var(axis=1, ddof=0), pdf.var(axis=1, ddof=0))
            self.assert_eq(kdf.std(axis=1), pdf.std(axis=1))
            self.assert_eq(kdf.std(axis=1, ddof=0), pdf.std(axis=1, ddof=0))
            self.assert_eq(kdf.max(axis=1), pdf.max(axis=1))
            self.assert_eq(kdf.min(axis=1), pdf.min(axis=1))
            self.assert_eq(kdf.sum(axis=1), pdf.sum(axis=1))
            self.assert_eq(kdf.product(axis=1), pdf.product(axis=1))
            self.assert_eq(kdf.kurtosis(axis=1), pdf.kurtosis(axis=1))
            self.assert_eq(kdf.skew(axis=1), pdf.skew(axis=1))
            self.assert_eq(kdf.mean(axis=1), pdf.mean(axis=1))
            self.assert_eq(kdf.sem(axis=1), pdf.sem(axis=1))
            self.assert_eq(kdf.sem(axis=1, ddof=0), pdf.sem(axis=1, ddof=0))

            self.assert_eq(
                kdf.count(axis=1, numeric_only=True), pdf.count(axis=1, numeric_only=True)
            )
            self.assert_eq(kdf.var(axis=1, numeric_only=True), pdf.var(axis=1, numeric_only=True))
            self.assert_eq(
                kdf.var(axis=1, ddof=0, numeric_only=True),
                pdf.var(axis=1, ddof=0, numeric_only=True),
            )
            self.assert_eq(kdf.std(axis=1, numeric_only=True), pdf.std(axis=1, numeric_only=True))
            self.assert_eq(
                kdf.std(axis=1, ddof=0, numeric_only=True),
                pdf.std(axis=1, ddof=0, numeric_only=True),
            )
            self.assert_eq(
                kdf.max(axis=1, numeric_only=True), pdf.max(axis=1, numeric_only=True).astype(float)
            )
            self.assert_eq(
                kdf.min(axis=1, numeric_only=True), pdf.min(axis=1, numeric_only=True).astype(float)
            )
            self.assert_eq(
                kdf.sum(axis=1, numeric_only=True), pdf.sum(axis=1, numeric_only=True).astype(float)
            )
            self.assert_eq(
                kdf.product(axis=1, numeric_only=True),
                pdf.product(axis=1, numeric_only=True).astype(float),
            )
            self.assert_eq(
                kdf.kurtosis(axis=1, numeric_only=True), pdf.kurtosis(axis=1, numeric_only=True)
            )
            self.assert_eq(kdf.skew(axis=1, numeric_only=True), pdf.skew(axis=1, numeric_only=True))
            self.assert_eq(kdf.mean(axis=1, numeric_only=True), pdf.mean(axis=1, numeric_only=True))
            self.assert_eq(kdf.sem(axis=1, numeric_only=True), pdf.sem(axis=1, numeric_only=True))
            self.assert_eq(
                kdf.sem(axis=1, ddof=0, numeric_only=True),
                pdf.sem(axis=1, ddof=0, numeric_only=True),
            )

    def test_corr(self):
        # Disable arrow execution since corr() is using UDT internally which is not supported.
        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            # DataFrame
            # we do not handle NaNs for now
            pdf = makeMissingDataframe(0.3, 42).fillna(0)
            kdf = ps.from_pandas(pdf)

            self.assert_eq(kdf.corr(), pdf.corr(), check_exact=False)

            # Series
            pser_a = pdf.A
            pser_b = pdf.B
            kser_a = kdf.A
            kser_b = kdf.B

            self.assertAlmostEqual(kser_a.corr(kser_b), pser_a.corr(pser_b))
            self.assertRaises(TypeError, lambda: kser_a.corr(kdf))

            # multi-index columns
            columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Z", "D")])
            pdf.columns = columns
            kdf.columns = columns

            self.assert_eq(kdf.corr(), pdf.corr(), check_exact=False)

            # Series
            pser_xa = pdf[("X", "A")]
            pser_xb = pdf[("X", "B")]
            kser_xa = kdf[("X", "A")]
            kser_xb = kdf[("X", "B")]

            self.assert_eq(kser_xa.corr(kser_xb), pser_xa.corr(pser_xb), almost=True)

    def test_cov_corr_meta(self):
        # Disable arrow execution since corr() is using UDT internally which is not supported.
        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            pdf = pd.DataFrame(
                {
                    "a": np.array([1, 2, 3], dtype="i1"),
                    "b": np.array([1, 2, 3], dtype="i2"),
                    "c": np.array([1, 2, 3], dtype="i4"),
                    "d": np.array([1, 2, 3]),
                    "e": np.array([1.0, 2.0, 3.0], dtype="f4"),
                    "f": np.array([1.0, 2.0, 3.0]),
                    "g": np.array([True, False, True]),
                    "h": np.array(list("abc")),
                },
                index=pd.Index([1, 2, 3], name="myindex"),
            )
            kdf = ps.from_pandas(pdf)
            self.assert_eq(kdf.corr(), pdf.corr())

    def test_stats_on_boolean_dataframe(self):
        pdf = pd.DataFrame({"A": [True, False, True], "B": [False, False, True]})
        kdf = ps.from_pandas(pdf)

        self.assert_eq(kdf.min(), pdf.min())
        self.assert_eq(kdf.max(), pdf.max())
        self.assert_eq(kdf.count(), pdf.count())

        self.assert_eq(kdf.sum(), pdf.sum())
        self.assert_eq(kdf.product(), pdf.product())
        self.assert_eq(kdf.mean(), pdf.mean())

        self.assert_eq(kdf.var(), pdf.var(), check_exact=False)
        self.assert_eq(kdf.var(ddof=0), pdf.var(ddof=0), check_exact=False)
        self.assert_eq(kdf.std(), pdf.std(), check_exact=False)
        self.assert_eq(kdf.std(ddof=0), pdf.std(ddof=0), check_exact=False)
        self.assert_eq(kdf.sem(), pdf.sem(), check_exact=False)
        self.assert_eq(kdf.sem(ddof=0), pdf.sem(ddof=0), check_exact=False)

    def test_stats_on_boolean_series(self):
        pser = pd.Series([True, False, True])
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.min(), pser.min())
        self.assert_eq(kser.max(), pser.max())
        self.assert_eq(kser.count(), pser.count())

        self.assert_eq(kser.sum(), pser.sum())
        self.assert_eq(kser.product(), pser.product())
        self.assert_eq(kser.mean(), pser.mean())

        self.assert_eq(kser.var(), pser.var(), almost=True)
        self.assert_eq(kser.var(ddof=0), pser.var(ddof=0), almost=True)
        self.assert_eq(kser.std(), pser.std(), almost=True)
        self.assert_eq(kser.std(ddof=0), pser.std(ddof=0), almost=True)
        self.assert_eq(kser.sem(), pser.sem(), almost=True)
        self.assert_eq(kser.sem(ddof=0), pser.sem(ddof=0), almost=True)

    def test_stats_on_non_numeric_columns_should_be_discarded_if_numeric_only_is_true(self):
        pdf = pd.DataFrame({"i": [0, 1, 2], "b": [False, False, True], "s": ["x", "y", "z"]})
        kdf = ps.from_pandas(pdf)

        self.assert_eq(
            kdf[["i", "s"]].max(numeric_only=True), pdf[["i", "s"]].max(numeric_only=True)
        )
        self.assert_eq(
            kdf[["b", "s"]].max(numeric_only=True), pdf[["b", "s"]].max(numeric_only=True)
        )
        self.assert_eq(
            kdf[["i", "s"]].min(numeric_only=True), pdf[["i", "s"]].min(numeric_only=True)
        )
        self.assert_eq(
            kdf[["b", "s"]].min(numeric_only=True), pdf[["b", "s"]].min(numeric_only=True)
        )
        self.assert_eq(kdf.count(numeric_only=True), pdf.count(numeric_only=True))

        if LooseVersion(pd.__version__) >= LooseVersion("1.0.0"):
            self.assert_eq(kdf.sum(numeric_only=True), pdf.sum(numeric_only=True))
            self.assert_eq(kdf.product(numeric_only=True), pdf.product(numeric_only=True))
        else:
            self.assert_eq(kdf.sum(numeric_only=True), pdf.sum(numeric_only=True).astype(int))
            self.assert_eq(
                kdf.product(numeric_only=True), pdf.product(numeric_only=True).astype(int)
            )

        self.assert_eq(kdf.mean(numeric_only=True), pdf.mean(numeric_only=True))

        self.assert_eq(kdf.var(numeric_only=True), pdf.var(numeric_only=True), check_exact=False)
        self.assert_eq(
            kdf.var(ddof=0, numeric_only=True),
            pdf.var(ddof=0, numeric_only=True),
            check_exact=False,
        )
        self.assert_eq(kdf.std(numeric_only=True), pdf.std(numeric_only=True), check_exact=False)
        self.assert_eq(
            kdf.std(ddof=0, numeric_only=True),
            pdf.std(ddof=0, numeric_only=True),
            check_exact=False,
        )
        self.assert_eq(kdf.sem(numeric_only=True), pdf.sem(numeric_only=True), check_exact=False)
        self.assert_eq(
            kdf.sem(ddof=0, numeric_only=True),
            pdf.sem(ddof=0, numeric_only=True),
            check_exact=False,
        )

        self.assert_eq(len(kdf.median(numeric_only=True)), len(pdf.median(numeric_only=True)))
        self.assert_eq(len(kdf.kurtosis(numeric_only=True)), len(pdf.kurtosis(numeric_only=True)))
        self.assert_eq(len(kdf.skew(numeric_only=True)), len(pdf.skew(numeric_only=True)))

        self.assert_eq(
            len(kdf.quantile(q=0.5, numeric_only=True)), len(pdf.quantile(q=0.5, numeric_only=True))
        )
        self.assert_eq(
            len(kdf.quantile(q=[0.25, 0.5, 0.75], numeric_only=True)),
            len(pdf.quantile(q=[0.25, 0.5, 0.75], numeric_only=True)),
        )

    def test_numeric_only_unsupported(self):
        pdf = pd.DataFrame({"i": [0, 1, 2], "b": [False, False, True], "s": ["x", "y", "z"]})
        kdf = ps.from_pandas(pdf)

        if LooseVersion(pd.__version__) >= LooseVersion("1.0.0"):
            self.assert_eq(kdf.sum(numeric_only=True), pdf.sum(numeric_only=True))
            self.assert_eq(
                kdf[["i", "b"]].sum(numeric_only=False), pdf[["i", "b"]].sum(numeric_only=False)
            )
        else:
            self.assert_eq(kdf.sum(numeric_only=True), pdf.sum(numeric_only=True).astype(int))
            self.assert_eq(
                kdf[["i", "b"]].sum(numeric_only=False),
                pdf[["i", "b"]].sum(numeric_only=False).astype(int),
            )

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            kdf.sum(numeric_only=False)

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            kdf.s.sum()


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_stats import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
