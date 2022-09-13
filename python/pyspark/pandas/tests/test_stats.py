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
    def _test_stat_functions(self, pdf_or_pser, psdf_or_psser):
        functions = ["max", "min", "mean", "sum", "count"]
        for funcname in functions:
            self.assert_eq(getattr(psdf_or_psser, funcname)(), getattr(pdf_or_pser, funcname)())

        functions = ["std", "var", "product", "sem"]
        for funcname in functions:
            self.assert_eq(
                getattr(psdf_or_psser, funcname)(),
                getattr(pdf_or_pser, funcname)(),
                check_exact=False,
            )

        functions = ["std", "var", "sem"]
        for funcname in functions:
            self.assert_eq(
                getattr(psdf_or_psser, funcname)(ddof=0),
                getattr(pdf_or_pser, funcname)(ddof=0),
                check_exact=False,
            )

        # NOTE: To test skew, kurt, and median, just make sure they run.
        #       The numbers are different in spark and pandas.
        functions = ["skew", "kurt", "median"]
        for funcname in functions:
            getattr(psdf_or_psser, funcname)()

    def test_stat_functions(self):
        pdf = pd.DataFrame({"A": [1, 2, 3, 4], "B": [1, 2, 3, 4], "C": [1, np.nan, 3, np.nan]})
        psdf = ps.from_pandas(pdf)
        self._test_stat_functions(pdf.A, psdf.A)
        self._test_stat_functions(pdf, psdf)

        # empty
        self._test_stat_functions(pdf.A.loc[[]], psdf.A.loc[[]])
        self._test_stat_functions(pdf.loc[[]], psdf.loc[[]])

    def test_stat_functions_multiindex_column(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "B", "C"], columns=arrays)
        psdf = ps.from_pandas(pdf)
        self._test_stat_functions(pdf.A, psdf.A)
        self._test_stat_functions(pdf, psdf)

    def test_stat_functions_with_no_numeric_columns(self):
        pdf = pd.DataFrame(
            {
                "A": ["a", None, "c", "d", None, "f", "g"],
                "B": ["A", "B", "C", None, "E", "F", None],
            }
        )
        psdf = ps.from_pandas(pdf)

        self._test_stat_functions(pdf, psdf)

    def test_sum(self):
        pdf = pd.DataFrame({"a": [1, 2, 3, np.nan], "b": [0.1, np.nan, 0.3, np.nan]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.sum(), pdf.sum())
        self.assert_eq(psdf.sum(axis=1), pdf.sum(axis=1))
        self.assert_eq(psdf.sum(min_count=3), pdf.sum(min_count=3))
        self.assert_eq(psdf.sum(axis=1, min_count=1), pdf.sum(axis=1, min_count=1))
        self.assert_eq(psdf.loc[[]].sum(), pdf.loc[[]].sum())
        self.assert_eq(psdf.loc[[]].sum(min_count=1), pdf.loc[[]].sum(min_count=1))

        self.assert_eq(psdf["a"].sum(), pdf["a"].sum())
        self.assert_eq(psdf["a"].sum(min_count=3), pdf["a"].sum(min_count=3))
        self.assert_eq(psdf["b"].sum(min_count=3), pdf["b"].sum(min_count=3))
        self.assert_eq(psdf["a"].loc[[]].sum(), pdf["a"].loc[[]].sum())
        self.assert_eq(psdf["a"].loc[[]].sum(min_count=1), pdf["a"].loc[[]].sum(min_count=1))

    def test_product(self):
        pdf = pd.DataFrame(
            {"a": [1, -2, -3, np.nan], "b": [0.1, np.nan, -0.3, np.nan], "c": [10, 20, 0, -10]}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.product(), pdf.product(), check_exact=False)
        self.assert_eq(psdf.product(axis=1), pdf.product(axis=1))
        self.assert_eq(psdf.product(min_count=3), pdf.product(min_count=3), check_exact=False)
        self.assert_eq(psdf.product(axis=1, min_count=1), pdf.product(axis=1, min_count=1))
        self.assert_eq(psdf.loc[[]].product(), pdf.loc[[]].product())
        self.assert_eq(psdf.loc[[]].product(min_count=1), pdf.loc[[]].product(min_count=1))

        self.assert_eq(psdf["a"].product(), pdf["a"].product(), check_exact=False)
        self.assert_eq(
            psdf["a"].product(min_count=3), pdf["a"].product(min_count=3), check_exact=False
        )
        self.assert_eq(psdf["b"].product(min_count=3), pdf["b"].product(min_count=3))
        self.assert_eq(psdf["c"].product(min_count=3), pdf["c"].product(min_count=3))
        self.assert_eq(psdf["a"].loc[[]].product(), pdf["a"].loc[[]].product())
        self.assert_eq(
            psdf["a"].loc[[]].product(min_count=1), pdf["a"].loc[[]].product(min_count=1)
        )

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
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.A.abs(), pdf.A.abs())
        self.assert_eq(psdf.B.abs(), pdf.B.abs())
        self.assert_eq(psdf.E.abs(), pdf.E.abs())
        # pandas' bug?
        # self.assert_eq(psdf[["B", "C", "E"]].abs(), pdf[["B", "C", "E"]].abs())
        self.assert_eq(psdf[["B", "C"]].abs(), pdf[["B", "C"]].abs())
        self.assert_eq(psdf[["E"]].abs(), pdf[["E"]].abs())

        with self.assertRaisesRegex(
            TypeError, "bad operand type for abs\\(\\): object \\(string\\)"
        ):
            psdf.abs()
        with self.assertRaisesRegex(
            TypeError, "bad operand type for abs\\(\\): object \\(string\\)"
        ):
            psdf.D.abs()

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
            psdf = ps.from_pandas(pdf)
            self.assert_eq(psdf.count(axis=1), pdf.count(axis=1))
            self.assert_eq(psdf.var(axis=1), pdf.var(axis=1))
            self.assert_eq(psdf.var(axis=1, ddof=0), pdf.var(axis=1, ddof=0))
            self.assert_eq(psdf.std(axis=1), pdf.std(axis=1))
            self.assert_eq(psdf.std(axis=1, ddof=0), pdf.std(axis=1, ddof=0))
            self.assert_eq(psdf.max(axis=1), pdf.max(axis=1))
            self.assert_eq(psdf.min(axis=1), pdf.min(axis=1))
            self.assert_eq(psdf.sum(axis=1), pdf.sum(axis=1))
            self.assert_eq(psdf.product(axis=1), pdf.product(axis=1))
            self.assert_eq(psdf.kurtosis(axis=0), pdf.kurtosis(axis=0), almost=True)
            self.assert_eq(psdf.kurtosis(axis=1), pdf.kurtosis(axis=1))
            self.assert_eq(psdf.skew(axis=0), pdf.skew(axis=0), almost=True)
            self.assert_eq(psdf.skew(axis=1), pdf.skew(axis=1))
            self.assert_eq(psdf.mean(axis=1), pdf.mean(axis=1))
            self.assert_eq(psdf.sem(axis=1), pdf.sem(axis=1))
            self.assert_eq(psdf.sem(axis=1, ddof=0), pdf.sem(axis=1, ddof=0))

            self.assert_eq(
                psdf.count(axis=1, numeric_only=True), pdf.count(axis=1, numeric_only=True)
            )
            self.assert_eq(psdf.var(axis=1, numeric_only=True), pdf.var(axis=1, numeric_only=True))
            self.assert_eq(
                psdf.var(axis=1, ddof=0, numeric_only=True),
                pdf.var(axis=1, ddof=0, numeric_only=True),
            )
            self.assert_eq(psdf.std(axis=1, numeric_only=True), pdf.std(axis=1, numeric_only=True))
            self.assert_eq(
                psdf.std(axis=1, ddof=0, numeric_only=True),
                pdf.std(axis=1, ddof=0, numeric_only=True),
            )
            self.assert_eq(
                psdf.max(axis=1, numeric_only=True),
                pdf.max(axis=1, numeric_only=True).astype(float),
            )
            self.assert_eq(
                psdf.min(axis=1, numeric_only=True),
                pdf.min(axis=1, numeric_only=True).astype(float),
            )
            self.assert_eq(
                psdf.sum(axis=1, numeric_only=True),
                pdf.sum(axis=1, numeric_only=True).astype(float),
            )
            self.assert_eq(
                psdf.product(axis=1, numeric_only=True),
                pdf.product(axis=1, numeric_only=True).astype(float),
            )
            self.assert_eq(
                psdf.kurtosis(axis=0, numeric_only=True),
                pdf.kurtosis(axis=0, numeric_only=True),
                almost=True,
            )
            self.assert_eq(
                psdf.kurtosis(axis=1, numeric_only=True), pdf.kurtosis(axis=1, numeric_only=True)
            )
            self.assert_eq(
                psdf.skew(axis=0, numeric_only=True),
                pdf.skew(axis=0, numeric_only=True),
                almost=True,
            )
            self.assert_eq(
                psdf.skew(axis=1, numeric_only=True), pdf.skew(axis=1, numeric_only=True)
            )
            self.assert_eq(
                psdf.mean(axis=1, numeric_only=True), pdf.mean(axis=1, numeric_only=True)
            )
            self.assert_eq(psdf.sem(axis=1, numeric_only=True), pdf.sem(axis=1, numeric_only=True))
            self.assert_eq(
                psdf.sem(axis=1, ddof=0, numeric_only=True),
                pdf.sem(axis=1, ddof=0, numeric_only=True),
            )

    def test_skew_kurt_numerical_stability(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 1, 1, 1],
                "B": [1.0, np.nan, 4, 2, 5],
                "C": [-6.0, -7, np.nan, np.nan, 10],
                "D": [1.2, np.nan, np.nan, 9.8, np.nan],
                "E": [1, np.nan, np.nan, np.nan, np.nan],
                "F": [np.nan, np.nan, np.nan, np.nan, np.nan],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.skew(), pdf.skew(), almost=True)
        self.assert_eq(psdf.kurt(), pdf.kurt(), almost=True)

    def test_dataframe_corr(self):
        # existing 'test_corr' is mixed by df.corr and ser.corr, will delete 'test_corr'
        # when we have separate tests for df.corr and ser.corr
        pdf = makeMissingDataframe(0.3, 42)
        psdf = ps.from_pandas(pdf)

        with self.assertRaisesRegex(ValueError, "Invalid method"):
            psdf.corr("std")
        with self.assertRaisesRegex(NotImplementedError, "kendall for now"):
            psdf.corr("kendall")
        with self.assertRaisesRegex(TypeError, "Invalid min_periods type"):
            psdf.corr(min_periods="3")
        with self.assertRaisesRegex(NotImplementedError, "spearman for now"):
            psdf.corr(method="spearman", min_periods=3)

        self.assert_eq(psdf.corr(), pdf.corr(), check_exact=False)
        self.assert_eq(psdf.corr(min_periods=1), pdf.corr(min_periods=1), check_exact=False)
        self.assert_eq(psdf.corr(min_periods=3), pdf.corr(min_periods=3), check_exact=False)
        self.assert_eq(
            (psdf + 1).corr(min_periods=2), (pdf + 1).corr(min_periods=2), check_exact=False
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Z", "D")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.corr(), pdf.corr(), check_exact=False)
        self.assert_eq(psdf.corr(min_periods=1), pdf.corr(min_periods=1), check_exact=False)
        self.assert_eq(psdf.corr(min_periods=3), pdf.corr(min_periods=3), check_exact=False)
        self.assert_eq(
            (psdf + 1).corr(min_periods=2), (pdf + 1).corr(min_periods=2), check_exact=False
        )

    def test_corr(self):
        # Disable arrow execution since corr() is using UDT internally which is not supported.
        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            # DataFrame
            # we do not handle NaNs for now
            pdf = makeMissingDataframe(0.3, 42).fillna(0)
            psdf = ps.from_pandas(pdf)

            self.assert_eq(psdf.corr(), pdf.corr(), check_exact=False)

            # Series
            pser_a = pdf.A
            pser_b = pdf.B
            psser_a = psdf.A
            psser_b = psdf.B

            self.assertAlmostEqual(psser_a.corr(psser_b), pser_a.corr(pser_b))
            self.assertRaises(TypeError, lambda: psser_a.corr(psdf))

            # multi-index columns
            columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Z", "D")])
            pdf.columns = columns
            psdf.columns = columns

            self.assert_eq(psdf.corr(), pdf.corr(), check_exact=False)

            # Series
            pser_xa = pdf[("X", "A")]
            pser_xb = pdf[("X", "B")]
            psser_xa = psdf[("X", "A")]
            psser_xb = psdf[("X", "B")]

            self.assert_eq(psser_xa.corr(psser_xb), pser_xa.corr(pser_xb), almost=True)

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
            psdf = ps.from_pandas(pdf)
            self.assert_eq(psdf.corr(), pdf.corr(), check_exact=False)

    def test_stats_on_boolean_dataframe(self):
        pdf = pd.DataFrame({"A": [True, False, True], "B": [False, False, True]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.min(), pdf.min())
        self.assert_eq(psdf.max(), pdf.max())
        self.assert_eq(psdf.count(), pdf.count())

        self.assert_eq(psdf.sum(), pdf.sum())
        self.assert_eq(psdf.product(), pdf.product())
        self.assert_eq(psdf.mean(), pdf.mean())

        self.assert_eq(psdf.var(), pdf.var(), check_exact=False)
        self.assert_eq(psdf.var(ddof=0), pdf.var(ddof=0), check_exact=False)
        self.assert_eq(psdf.std(), pdf.std(), check_exact=False)
        self.assert_eq(psdf.std(ddof=0), pdf.std(ddof=0), check_exact=False)
        self.assert_eq(psdf.sem(), pdf.sem(), check_exact=False)
        self.assert_eq(psdf.sem(ddof=0), pdf.sem(ddof=0), check_exact=False)

    def test_stats_on_boolean_series(self):
        pser = pd.Series([True, False, True])
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.min(), pser.min())
        self.assert_eq(psser.max(), pser.max())
        self.assert_eq(psser.count(), pser.count())

        self.assert_eq(psser.sum(), pser.sum())
        self.assert_eq(psser.product(), pser.product())
        self.assert_eq(psser.mean(), pser.mean())

        self.assert_eq(psser.var(), pser.var(), almost=True)
        self.assert_eq(psser.var(ddof=0), pser.var(ddof=0), almost=True)
        self.assert_eq(psser.std(), pser.std(), almost=True)
        self.assert_eq(psser.std(ddof=0), pser.std(ddof=0), almost=True)
        self.assert_eq(psser.sem(), pser.sem(), almost=True)
        self.assert_eq(psser.sem(ddof=0), pser.sem(ddof=0), almost=True)

    def test_stats_on_non_numeric_columns_should_be_discarded_if_numeric_only_is_true(self):
        pdf = pd.DataFrame({"i": [0, 1, 2], "b": [False, False, True], "s": ["x", "y", "z"]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf[["i", "s"]].max(numeric_only=True), pdf[["i", "s"]].max(numeric_only=True)
        )
        self.assert_eq(
            psdf[["b", "s"]].max(numeric_only=True), pdf[["b", "s"]].max(numeric_only=True)
        )
        self.assert_eq(
            psdf[["i", "s"]].min(numeric_only=True), pdf[["i", "s"]].min(numeric_only=True)
        )
        self.assert_eq(
            psdf[["b", "s"]].min(numeric_only=True), pdf[["b", "s"]].min(numeric_only=True)
        )
        self.assert_eq(psdf.count(numeric_only=True), pdf.count(numeric_only=True))

        self.assert_eq(psdf.sum(numeric_only=True), pdf.sum(numeric_only=True))
        self.assert_eq(psdf.product(numeric_only=True), pdf.product(numeric_only=True))

        self.assert_eq(psdf.mean(numeric_only=True), pdf.mean(numeric_only=True))

        self.assert_eq(psdf.var(numeric_only=True), pdf.var(numeric_only=True), check_exact=False)
        self.assert_eq(
            psdf.var(ddof=0, numeric_only=True),
            pdf.var(ddof=0, numeric_only=True),
            check_exact=False,
        )
        self.assert_eq(psdf.std(numeric_only=True), pdf.std(numeric_only=True), check_exact=False)
        self.assert_eq(
            psdf.std(ddof=0, numeric_only=True),
            pdf.std(ddof=0, numeric_only=True),
            check_exact=False,
        )
        self.assert_eq(psdf.sem(numeric_only=True), pdf.sem(numeric_only=True), check_exact=False)
        self.assert_eq(
            psdf.sem(ddof=0, numeric_only=True),
            pdf.sem(ddof=0, numeric_only=True),
            check_exact=False,
        )

        self.assert_eq(len(psdf.median(numeric_only=True)), len(pdf.median(numeric_only=True)))
        self.assert_eq(len(psdf.kurtosis(numeric_only=True)), len(pdf.kurtosis(numeric_only=True)))
        self.assert_eq(len(psdf.skew(numeric_only=True)), len(pdf.skew(numeric_only=True)))

        # Boolean was excluded because of a behavior change in NumPy
        # https://github.com/numpy/numpy/pull/16273#discussion_r641264085 which pandas inherits
        # but this behavior is inconsistent in pandas context.
        # Boolean column in quantile tests are excluded for now.
        # TODO(SPARK-35555): track and match the behavior of quantile to pandas'
        pdf = pd.DataFrame({"i": [0, 1, 2], "s": ["x", "y", "z"]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            len(psdf.quantile(q=0.5, numeric_only=True)),
            len(pdf.quantile(q=0.5, numeric_only=True)),
        )
        self.assert_eq(
            len(psdf.quantile(q=[0.25, 0.5, 0.75], numeric_only=True)),
            len(pdf.quantile(q=[0.25, 0.5, 0.75], numeric_only=True)),
        )

    def test_numeric_only_unsupported(self):
        pdf = pd.DataFrame({"i": [0, 1, 2], "b": [False, False, True], "s": ["x", "y", "z"]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.sum(numeric_only=True), pdf.sum(numeric_only=True))
        self.assert_eq(
            psdf[["i", "b"]].sum(numeric_only=False), pdf[["i", "b"]].sum(numeric_only=False)
        )

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            psdf.sum(numeric_only=False)

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            psdf.s.sum()


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_stats import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
