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
from distutils.version import LooseVersion

import pandas as pd
import numpy as np

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.tests.data_type_ops.testing_utils import TestCasesUtils
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class NumOpsTest(PandasOnSparkTestCase, TestCasesUtils):
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

    def test_add(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(pser + pser, psser + psser)
            self.assert_eq(pser + 1, psser + 1)
            # self.assert_eq(pser + 0.1, psser + 0.1)
            self.assert_eq(pser + pser.astype(bool), psser + psser.astype(bool))
            self.assert_eq(pser + True, psser + True)
            self.assert_eq(pser + False, psser + False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser + self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser + self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser + self.non_numeric_pssers["date"])
                self.assertRaises(TypeError, lambda: psser + self.non_numeric_pssers["categorical"])
                self.assert_eq(
                    (psser + self.non_numeric_pssers["bool"]).sort_index(),
                    pser + self.non_numeric_psers["bool"],
                )

    def test_sub(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(pser - pser, psser - psser)
            self.assert_eq(pser - 1, psser - 1)
            # self.assert_eq(pser - 0.1, psser - 0.1)
            self.assert_eq(pser - pser.astype(bool), psser - psser.astype(bool))
            self.assert_eq(pser - True, psser - True)
            self.assert_eq(pser - False, psser - False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser - self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser - self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser - self.non_numeric_pssers["date"])
                self.assertRaises(TypeError, lambda: psser - self.non_numeric_pssers["categorical"])
                self.assert_eq(
                    (psser - self.non_numeric_pssers["bool"]).sort_index(),
                    pser - self.non_numeric_psers["bool"],
                )

    def test_mul(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(pser * pser, psser * psser)
            self.assert_eq(pser * pser.astype(bool), psser * psser.astype(bool))
            self.assert_eq(pser * True, psser * True)
            self.assert_eq(pser * False, psser * False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                if psser.dtype in [int, np.int32]:
                    self.assert_eq(
                        (psser * self.non_numeric_pssers["string"]).sort_index(),
                        pser * self.non_numeric_psers["string"],
                    )
                else:
                    self.assertRaises(TypeError, lambda: psser * self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser * self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser * self.non_numeric_pssers["date"])
                self.assertRaises(TypeError, lambda: psser * self.non_numeric_pssers["categorical"])
                self.assert_eq(
                    (psser * self.non_numeric_pssers["bool"]).sort_index(),
                    pser * self.non_numeric_psers["bool"],
                )

    def test_truediv(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            if psser.dtype in [float, int, np.int32]:
                self.assert_eq(pser / pser, psser / psser)
                self.assert_eq(pser / pser.astype(bool), psser / psser.astype(bool))
                self.assert_eq(pser / True, psser / True)
                self.assert_eq(pser / False, psser / False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser / self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser / self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser / self.non_numeric_pssers["date"])
                self.assertRaises(TypeError, lambda: psser / self.non_numeric_pssers["categorical"])
            self.assert_eq(
                (self.float_psser / self.non_numeric_pssers["bool"]).sort_index(),
                self.float_pser / self.non_numeric_psers["bool"],
            )

    def test_floordiv(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            if psser.dtype == float:
                self.assert_eq(pser // pser, psser // psser)
                self.assert_eq(pser // pser.astype(bool), psser // psser.astype(bool))
                self.assert_eq(pser // True, psser // True)
                self.assert_eq(pser // False, psser // False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser // self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser // self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser // self.non_numeric_pssers["date"])
                self.assertRaises(
                    TypeError, lambda: psser // self.non_numeric_pssers["categorical"]
                )
            if LooseVersion(pd.__version__) >= LooseVersion("0.25.3"):
                self.assert_eq(
                    (self.float_psser // self.non_numeric_pssers["bool"]).sort_index(),
                    self.float_pser // self.non_numeric_psers["bool"],
                )
            else:
                self.assert_eq(
                    (self.float_pser // self.non_numeric_psers["bool"]).sort_index(),
                    ps.Series([1.0, 2.0, np.inf]),
                )

    def test_mod(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(pser % pser, psser % psser)
            self.assert_eq(pser % pser.astype(bool), psser % psser.astype(bool))
            self.assert_eq(pser % True, psser % True)
            if psser.dtype in [int, np.int32]:
                self.assert_eq(ps.Series([np.nan, np.nan, np.nan], dtype=float), psser % False)
            else:
                self.assert_eq(
                    ps.Series([np.nan, np.nan, np.nan], dtype=psser.dtype), psser % False
                )

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser % self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser % self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser % self.non_numeric_pssers["date"])
                self.assertRaises(TypeError, lambda: psser % self.non_numeric_pssers["categorical"])
            self.assert_eq(
                (self.float_psser % self.non_numeric_pssers["bool"]).sort_index(),
                self.float_pser % self.non_numeric_psers["bool"],
            )

    def test_pow(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            if psser.dtype == float:
                self.assert_eq(pser ** pser, psser ** psser)
                self.assert_eq(pser ** pser.astype(bool), psser ** psser.astype(bool))
                self.assert_eq(pser ** True, psser ** True)
                self.assert_eq(pser ** False, psser ** False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser ** self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser ** self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser ** self.non_numeric_pssers["date"])
                self.assertRaises(
                    TypeError, lambda: psser ** self.non_numeric_pssers["categorical"]
                )
            self.assert_eq(
                (self.float_psser ** self.non_numeric_pssers["bool"]).sort_index(),
                self.float_pser ** self.non_numeric_psers["bool"],
            )

    def test_radd(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(1 + pser, 1 + psser)
            # self.assert_eq(0.1 + pser, 0.1 + psser)
            self.assertRaises(TypeError, lambda: "x" + psser)
            self.assert_eq(True + pser, True + psser)
            self.assert_eq(False + pser, False + psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) + psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) + psser)

    def test_rsub(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(1 - pser, 1 - psser)
            # self.assert_eq(0.1 - pser, 0.1 - psser)
            self.assertRaises(TypeError, lambda: "x" - psser)
            self.assert_eq(True - pser, True - psser)
            self.assert_eq(False - pser, False - psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) - psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) - psser)

    def test_rmul(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(1 * pser, 1 * psser)
            # self.assert_eq(0.1 * pser, 0.1 * psser)
            self.assertRaises(TypeError, lambda: "x" * psser)
            self.assert_eq(True * pser, True * psser)
            self.assert_eq(False * pser, False * psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) * psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) * psser)

    def test_rtruediv(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            # self.assert_eq(5 / pser, 5 / psser)
            # self.assert_eq(0.1 / pser, 0.1 / psser)
            self.assertRaises(TypeError, lambda: "x" / psser)
            self.assert_eq((True / pser).astype(float), True / psser, check_exact=False)
            self.assert_eq((False / pser).astype(float), False / psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) / psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) / psser)

    def test_rfloordiv(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            # self.assert_eq(5 // pser, 5 // psser)
            # self.assert_eq(0.1 // pser, 0.1 // psser)
            self.assertRaises(TypeError, lambda: "x" // psser)
            self.assert_eq((True // pser).astype(float), True // psser)
            self.assert_eq((False // pser).astype(float), False // psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) // psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) // psser)

    def test_rpow(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            # self.assert_eq(1 ** pser, 1 ** psser)
            # self.assert_eq(0.1 ** pser, 0.1 ** psser)
            self.assertRaises(TypeError, lambda: "x" ** psser)
            self.assert_eq((True ** pser).astype(float), True ** psser)
            self.assert_eq((False ** pser).astype(float), False ** psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) ** psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) ** psser)

    def test_rmod(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(1 % pser, 1 % psser)
            # self.assert_eq(0.1 % pser, 0.1 % psser)
            self.assert_eq(True % pser, True % psser)
            self.assert_eq(False % pser, False % psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) % psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) % psser)

    def test_and(self):
        psser = self.numeric_pssers[0]
        self.assertRaises(TypeError, lambda: psser & True)
        self.assertRaises(TypeError, lambda: psser & False)
        self.assertRaises(TypeError, lambda: psser & psser)

    def test_rand(self):
        psser = self.numeric_pssers[0]
        self.assertRaises(TypeError, lambda: True & psser)
        self.assertRaises(TypeError, lambda: False & psser)

    def test_or(self):
        psser = self.numeric_pssers[0]
        self.assertRaises(TypeError, lambda: psser | True)
        self.assertRaises(TypeError, lambda: psser | False)
        self.assertRaises(TypeError, lambda: psser | psser)

    def test_ror(self):
        psser = self.numeric_pssers[0]
        self.assertRaises(TypeError, lambda: True | psser)
        self.assertRaises(TypeError, lambda: False | psser)

    def test_from_to_pandas(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(pser, psser.to_pandas())
            self.assert_eq(ps.from_pandas(pser), psser)


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_num_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
