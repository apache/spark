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

import datetime
import unittest

import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.utils import is_ansi_mode_test, ansi_mode_not_supported_message
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase


class ReverseTestsMixin:
    """Unit tests for arithmetic operations of numeric data types.

    A few test cases are disabled because pandas-on-Spark returns float64 whereas pandas
    returns float32.
    The underlying reason is the respective Spark operations return DoubleType always.
    """

    @property
    def float_pser(self):
        return pd.Series([1, 2, 3], dtype=float)

    @property
    def float_psser(self):
        return ps.from_pandas(self.float_pser)

    @unittest.skipIf(is_ansi_mode_test, ansi_mode_not_supported_message)
    def test_radd(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            self.assert_eq(1 + pser, 1 + psser, check_exact=False)
            # self.assert_eq(0.1 + pser, 0.1 + psser)
            self.assertRaises(TypeError, lambda: "x" + psser)
            self.assert_eq(True + pser, True + psser, check_exact=False)
            self.assert_eq(False + pser, False + psser, check_exact=False)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) + psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) + psser)

    @unittest.skipIf(is_ansi_mode_test, ansi_mode_not_supported_message)
    def test_rsub(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            self.assert_eq(1 - pser, 1 - psser, check_exact=False)
            # self.assert_eq(0.1 - pser, 0.1 - psser)
            self.assertRaises(TypeError, lambda: "x" - psser)
            self.assert_eq(True - pser, True - psser, check_exact=False)
            self.assert_eq(False - pser, False - psser, check_exact=False)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) - psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) - psser)

    @unittest.skipIf(is_ansi_mode_test, ansi_mode_not_supported_message)
    def test_rmul(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            self.assert_eq(1 * pser, 1 * psser, check_exact=False)
            # self.assert_eq(0.1 * pser, 0.1 * psser)
            self.assertRaises(TypeError, lambda: "x" * psser)
            self.assert_eq(True * pser, True * psser, check_exact=False)
            self.assert_eq(False * pser, False * psser, check_exact=False)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) * psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) * psser)

    def test_rtruediv(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            # self.assert_eq(5 / pser, 5 / psser)
            # self.assert_eq(0.1 / pser, 0.1 / psser)
            self.assertRaises(TypeError, lambda: "x" / psser)
            self.assert_eq((True / pser).astype(float), True / psser, check_exact=False)
            self.assert_eq((False / pser).astype(float), False / psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) / psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) / psser)

    def test_rfloordiv(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            # self.assert_eq(5 // pser, 5 // psser)
            # self.assert_eq(0.1 // pser, 0.1 // psser)
            self.assertRaises(TypeError, lambda: "x" // psser)
            self.assert_eq((True // pser).astype(float), True // psser)
            self.assert_eq((False // pser).astype(float), False // psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) // psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) // psser)

    def test_rpow(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            # self.assert_eq(1 ** pser, 1 ** psser)
            # self.assert_eq(0.1 ** pser, 0.1 ** psser)
            self.assertRaises(TypeError, lambda: "x" ** psser)
            self.assert_eq((True**pser).astype(float), True**psser)
            self.assert_eq((False**pser).astype(float), False**psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) ** psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) ** psser)

    @unittest.skipIf(is_ansi_mode_test, ansi_mode_not_supported_message)
    def test_rmod(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            self.assert_eq(1 % pser, 1 % psser, check_exact=False)
            # self.assert_eq(0.1 % pser, 0.1 % psser)
            self.assert_eq(True % pser, True % psser, check_exact=False)
            self.assert_eq(False % pser, False % psser, check_exact=False)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) % psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) % psser)


class ReverseTests(
    ReverseTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.data_type_ops.test_num_reverse import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
