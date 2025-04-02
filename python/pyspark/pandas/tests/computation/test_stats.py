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

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, SPARK_CONF_ARROW_ENABLED
from pyspark.testing.sqlutils import SQLTestUtils


class StatsTestsMixin:
    def _test_stat_functions(self, pdf_or_pser, psdf_or_psser):
        self.assert_eq(
            psdf_or_psser.count(),
            pdf_or_pser.count(),
            almost=True,
        )

        functions = ["max", "min", "mean", "sum"]
        for funcname in functions:
            self.assert_eq(
                getattr(psdf_or_psser, funcname)(),
                getattr(pdf_or_pser, funcname)(numeric_only=True),
            )

        functions = ["std", "var", "product", "sem"]
        for funcname in functions:
            self.assert_eq(
                getattr(psdf_or_psser, funcname)(),
                getattr(pdf_or_pser, funcname)(numeric_only=True),
                check_exact=False,
            )

        functions = ["std", "var", "sem"]
        for funcname in functions:
            self.assert_eq(
                getattr(psdf_or_psser, funcname)(ddof=0),
                getattr(pdf_or_pser, funcname)(ddof=0, numeric_only=True),
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
        self.assert_eq(psser.var(ddof=2), pser.var(ddof=2), almost=True)
        self.assert_eq(psser.std(), pser.std(), almost=True)
        self.assert_eq(psser.std(ddof=0), pser.std(ddof=0), almost=True)
        self.assert_eq(psser.std(ddof=2), pser.std(ddof=2), almost=True)
        self.assert_eq(psser.sem(), pser.sem(), almost=True)
        self.assert_eq(psser.sem(ddof=0), pser.sem(ddof=0), almost=True)
        self.assert_eq(psser.sem(ddof=2), pser.sem(ddof=2), almost=True)

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
        self.assert_eq(
            psdf.var(ddof=2, numeric_only=True),
            pdf.var(ddof=2, numeric_only=True),
            check_exact=False,
        )
        self.assert_eq(psdf.std(numeric_only=True), pdf.std(numeric_only=True), check_exact=False)
        self.assert_eq(
            psdf.std(ddof=0, numeric_only=True),
            pdf.std(ddof=0, numeric_only=True),
            check_exact=False,
        )
        self.assert_eq(
            psdf.std(ddof=2, numeric_only=True),
            pdf.std(ddof=2, numeric_only=True),
            check_exact=False,
        )
        self.assert_eq(psdf.sem(numeric_only=True), pdf.sem(numeric_only=True), check_exact=False)
        self.assert_eq(
            psdf.sem(ddof=0, numeric_only=True),
            pdf.sem(ddof=0, numeric_only=True),
            check_exact=False,
        )
        self.assert_eq(
            psdf.sem(ddof=2, numeric_only=True),
            pdf.sem(ddof=2, numeric_only=True),
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


class StatsTests(
    StatsTestsMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.computation.test_stats import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
