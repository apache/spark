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
import numpy as np

from pyspark.pandas.config import option_context
from pyspark.pandas.tests.data_type_ops.testing_utils import TestCasesUtils
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class NumOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    """Unit tests for arithmetic operations of numeric data types.

    A few test cases are disabled because pandas-on-Spark returns float64 whereas pandas
    returns float32.
    The underlying reason is the respective Spark operations return DoubleType always.
    """
    def test_add(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            self.assert_eq(pser + pser, kser + kser)
            self.assert_eq(pser + 1, kser + 1)
            # self.assert_eq(pser + 0.1, kser + 0.1)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, kser in self.numeric_pser_kser_pairs:
                self.assertRaises(TypeError, lambda: kser + self.non_numeric_ksers["string"])
                self.assertRaises(TypeError, lambda: kser + self.non_numeric_ksers["datetime"])
                self.assertRaises(TypeError, lambda: kser + self.non_numeric_ksers["date"])
                self.assertRaises(TypeError, lambda: kser + self.non_numeric_ksers["categorical"])
                self.assertRaises(TypeError, lambda: kser + self.non_numeric_ksers["bool"])

    def test_sub(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            self.assert_eq(pser - pser, kser - kser)
            self.assert_eq(pser - 1, kser - 1)
            # self.assert_eq(pser - 0.1, kser - 0.1)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, kser in self.numeric_pser_kser_pairs:
                self.assertRaises(TypeError, lambda: kser - self.non_numeric_ksers["string"])
                self.assertRaises(TypeError, lambda: kser - self.non_numeric_ksers["datetime"])
                self.assertRaises(TypeError, lambda: kser - self.non_numeric_ksers["date"])
                self.assertRaises(TypeError, lambda: kser - self.non_numeric_ksers["categorical"])
                self.assertRaises(TypeError, lambda: kser - self.non_numeric_ksers["bool"])

    def test_mul(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            self.assert_eq(pser * pser, kser * kser)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, kser in self.numeric_pser_kser_pairs:
                if kser.dtype in [int, np.int32]:
                    self.assert_eq(
                        (kser * self.non_numeric_ksers["string"]).sort_index(),
                        pser * self.non_numeric_psers["string"],
                    )
                else:
                    self.assertRaises(TypeError, lambda: kser * self.non_numeric_ksers["string"])
                self.assertRaises(TypeError, lambda: kser * self.non_numeric_ksers["datetime"])
                self.assertRaises(TypeError, lambda: kser * self.non_numeric_ksers["date"])
                self.assertRaises(TypeError, lambda: kser * self.non_numeric_ksers["categorical"])
                self.assertRaises(TypeError, lambda: kser * self.non_numeric_ksers["bool"])

    def test_truediv(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            if kser.dtype in [float, int, np.int32]:
                self.assert_eq(pser / pser, kser / kser)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, kser in self.numeric_pser_kser_pairs:
                self.assertRaises(TypeError, lambda: kser / self.non_numeric_ksers["string"])
                self.assertRaises(TypeError, lambda: kser / self.non_numeric_ksers["datetime"])
                self.assertRaises(TypeError, lambda: kser / self.non_numeric_ksers["date"])
                self.assertRaises(TypeError, lambda: kser / self.non_numeric_ksers["categorical"])
                self.assertRaises(TypeError, lambda: kser / self.non_numeric_ksers["bool"])

    def test_floordiv(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            if kser.dtype == float:
                self.assert_eq(pser // pser, kser // kser)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, kser in self.numeric_pser_kser_pairs:
                self.assertRaises(TypeError, lambda: kser // self.non_numeric_ksers["string"])
                self.assertRaises(TypeError, lambda: kser // self.non_numeric_ksers["datetime"])
                self.assertRaises(TypeError, lambda: kser // self.non_numeric_ksers["date"])
                self.assertRaises(TypeError, lambda: kser // self.non_numeric_ksers["categorical"])
                self.assertRaises(TypeError, lambda: kser // self.non_numeric_ksers["bool"])

    def test_mod(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            self.assert_eq(pser % pser, kser % kser)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, kser in self.numeric_pser_kser_pairs:
                self.assertRaises(TypeError, lambda: kser % self.non_numeric_ksers["string"])
                self.assertRaises(TypeError, lambda: kser % self.non_numeric_ksers["datetime"])
                self.assertRaises(TypeError, lambda: kser % self.non_numeric_ksers["date"])
                self.assertRaises(TypeError, lambda: kser % self.non_numeric_ksers["categorical"])
                self.assertRaises(TypeError, lambda: kser % self.non_numeric_ksers["bool"])

    def test_pow(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            if kser.dtype == float:
                self.assert_eq(pser ** pser, kser ** kser)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, kser in self.numeric_pser_kser_pairs:
                self.assertRaises(TypeError, lambda: kser ** self.non_numeric_ksers["string"])
                self.assertRaises(TypeError, lambda: kser ** self.non_numeric_ksers["datetime"])
                self.assertRaises(TypeError, lambda: kser ** self.non_numeric_ksers["date"])
                self.assertRaises(TypeError, lambda: kser ** self.non_numeric_ksers["categorical"])
                self.assertRaises(TypeError, lambda: kser ** self.non_numeric_ksers["bool"])

    def test_radd(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            self.assert_eq(1 + pser, 1 + kser)
            # self.assert_eq(0.1 + pser, 0.1 + kser)
            self.assertRaises(TypeError, lambda: "x" + kser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) + kser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) + kser)

    def test_rsub(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            self.assert_eq(1 - pser, 1 - kser)
            # self.assert_eq(0.1 - pser, 0.1 - kser)
            self.assertRaises(TypeError, lambda: "x" - kser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) - kser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) - kser)

    def test_rmul(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            self.assert_eq(1 * pser, 1 * kser)
            # self.assert_eq(0.1 * pser, 0.1 * kser)
            self.assertRaises(TypeError, lambda: "x" * kser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) * kser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) * kser)

    def test_rtruediv(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            # self.assert_eq(5 / pser, 5 / kser)
            # self.assert_eq(0.1 / pser, 0.1 / kser)
            self.assertRaises(TypeError, lambda: "x" + kser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) / kser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) / kser)

    def test_rfloordiv(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            # self.assert_eq(5 // pser, 5 // kser)
            # self.assert_eq(0.1 // pser, 0.1 // kser)
            self.assertRaises(TypeError, lambda: "x" // kser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) // kser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) // kser)

    def test_rpow(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            # self.assert_eq(1 ** pser, 1 ** kser)
            # self.assert_eq(0.1 ** pser, 0.1 ** kser)
            self.assertRaises(TypeError, lambda: "x" ** kser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) ** kser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) ** kser)

    def test_rmod(self):
        for pser, kser in self.numeric_pser_kser_pairs:
            self.assert_eq(1 % pser, 1 % kser)
            # self.assert_eq(0.1 % pser, 0.1 % kser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) % kser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) % kser)


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_string_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
