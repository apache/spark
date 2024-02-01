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

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class GenericFunctionsTestsMixin:
    def _test_stat_functions(self, stat_func):
        pdf = pd.DataFrame({"a": [np.nan, np.nan, np.nan], "b": [1, np.nan, 2], "c": [1, 2, 3]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(stat_func(pdf.a), stat_func(psdf.a))
        self.assert_eq(stat_func(pdf.b), stat_func(psdf.b))
        self.assert_eq(stat_func(pdf), stat_func(psdf))

    # Fix skew and kurtosis and re-enable tests below
    def test_stat_functions(self):
        self._test_stat_functions(lambda x: x.sum())
        self._test_stat_functions(lambda x: x.sum(skipna=False))
        self._test_stat_functions(lambda x: x.mean())
        self._test_stat_functions(lambda x: x.mean(skipna=False))
        self._test_stat_functions(lambda x: x.product())
        self._test_stat_functions(lambda x: x.product(skipna=False))
        self._test_stat_functions(lambda x: x.min())
        self._test_stat_functions(lambda x: x.min(skipna=False))
        self._test_stat_functions(lambda x: x.max())
        self._test_stat_functions(lambda x: x.max(skipna=False))
        self._test_stat_functions(lambda x: x.std())
        self._test_stat_functions(lambda x: x.std(skipna=False))
        self._test_stat_functions(lambda x: x.std(ddof=2))
        self._test_stat_functions(lambda x: x.var())
        self._test_stat_functions(lambda x: x.var(ddof=2))
        self._test_stat_functions(lambda x: x.sem())
        self._test_stat_functions(lambda x: x.sem(skipna=False))
        # self._test_stat_functions(lambda x: x.skew())
        self._test_stat_functions(lambda x: x.skew(skipna=False))

        # Test cases below return differently from pandas (either by design or to be fixed)
        pdf = pd.DataFrame({"a": [np.nan, np.nan, np.nan], "b": [1, np.nan, 2], "c": [1, 2, 3]})
        psdf = ps.from_pandas(pdf)

        with self.assertRaisesRegex(TypeError, "ddof must be integer"):
            psdf.std(ddof="ddof")
        with self.assertRaisesRegex(TypeError, "ddof must be integer"):
            psdf.a.std(ddof="ddof")

        with self.assertRaisesRegex(TypeError, "ddof must be integer"):
            psdf.var(ddof="ddof")
        with self.assertRaisesRegex(TypeError, "ddof must be integer"):
            psdf.a.var(ddof="ddof")

        self.assert_eq(pdf.a.median(), psdf.a.median())
        self.assert_eq(pdf.a.median(skipna=False), psdf.a.median(skipna=False))
        self.assert_eq(1.0, psdf.b.median())
        self.assert_eq(pdf.b.median(skipna=False), psdf.b.median(skipna=False))
        self.assert_eq(pdf.c.median(), psdf.c.median())

        self.assert_eq(pdf.a.kurtosis(skipna=False), psdf.a.kurtosis(skipna=False))
        self.assert_eq(pdf.a.kurtosis(), psdf.a.kurtosis())
        self.assert_eq(pdf.b.kurtosis(skipna=False), psdf.b.kurtosis(skipna=False))
        self.assert_eq(pdf.b.kurtosis(), psdf.b.kurtosis())
        self.assert_eq(pdf.c.kurtosis(), psdf.c.kurtosis())

    def test_prod_precision(self):
        pdf = pd.DataFrame(
            {
                "a": [np.nan, np.nan, np.nan, np.nan],
                "b": [1, np.nan, np.nan, -4],
                "c": [1, -2, 3, -4],
                "d": [55108, 55108, 55108, 55108],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.prod(), psdf.prod())
        self.assert_eq(pdf.prod(skipna=False), psdf.prod(skipna=False))
        self.assert_eq(pdf.prod(min_count=3), psdf.prod(min_count=3))
        self.assert_eq(pdf.prod(skipna=False, min_count=3), psdf.prod(skipna=False, min_count=3))


class GenericFunctionsTests(GenericFunctionsTestsMixin, PandasOnSparkTestCase, TestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_generic_functions import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
