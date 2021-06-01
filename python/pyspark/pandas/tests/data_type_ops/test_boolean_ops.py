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


class BooleanOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    @property
    def pser(self):
        return pd.Series([True, True, False])

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def float_pser(self):
        return pd.Series([1, 2, 3], dtype=float)

    @property
    def float_psser(self):
        return ps.from_pandas(self.float_pser)

    def test_add(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser + 1, psser + 1)
        self.assert_eq(pser + 0.1, psser + 0.1)
        self.assert_eq(pser + pser.astype(int), psser + psser.astype(int))
        self.assertRaises(TypeError, lambda: psser + psser)
        self.assertRaises(TypeError, lambda: psser + True)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser + pser, (self.psser + psser).sort_index())

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser + psser)

    def test_sub(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser - 1, psser - 1)
        self.assert_eq(pser - 0.1, psser - 0.1)
        self.assert_eq(pser - pser.astype(int), psser - psser.astype(int))
        self.assertRaises(TypeError, lambda: psser - psser)
        self.assertRaises(TypeError, lambda: psser - True)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser - pser, (self.psser - psser).sort_index())

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser - psser)

    def test_mul(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser * 1, psser * 1)
        self.assert_eq(pser * 0.1, psser * 0.1)
        self.assert_eq(pser * pser.astype(int), psser * psser.astype(int))
        self.assertRaises(TypeError, lambda: psser * psser)
        self.assertRaises(TypeError, lambda: psser * True)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser * pser, (self.psser * psser).sort_index())

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser * psser)

    def test_truediv(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser / 1, psser / 1)
        self.assert_eq(pser / 0.1, psser / 0.1)
        self.assert_eq(pser / pser.astype(int), psser / psser.astype(int))
        self.assertRaises(TypeError, lambda: psser / psser)
        self.assertRaises(TypeError, lambda: psser / True)

        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser / self.float_pser, (self.psser / self.float_psser).sort_index())

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser / psser)

    def test_floordiv(self):
        pser = self.pser
        psser = self.psser

        # float is always returned in pandas-on-Spark
        self.assert_eq((pser // 1).astype("float"), psser // 1)

        # in pandas, 1 // 0.1 = 9.0; in pandas-on-Spark, 1 // 0.1 = 10.0
        # self.assert_eq(pser // 0.1, psser // 0.1)

        self.assert_eq(pser // pser.astype(int), psser // psser.astype(int))
        self.assertRaises(TypeError, lambda: psser // psser)
        self.assertRaises(TypeError, lambda: psser // True)

        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser // self.float_pser, (self.psser // self.float_psser).sort_index()
            )

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser // psser)

    def test_mod(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser % 1, psser % 1)
        self.assert_eq(pser % 0.1, psser % 0.1)
        self.assert_eq(pser % pser.astype(float), psser % psser.astype(float))
        self.assertRaises(TypeError, lambda: psser % psser)
        self.assertRaises(TypeError, lambda: psser % True)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser % pser, (self.psser % psser).sort_index())

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser % psser)

    def test_pow(self):
        pser = self.pser
        psser = self.psser
        # float is always returned in pandas-on-Spark
        self.assert_eq((pser ** 1).astype("float"), psser ** 1)
        self.assert_eq(pser ** 0.1, self.psser ** 0.1)
        self.assert_eq(pser ** pser.astype(float), psser ** psser.astype(float))
        self.assertRaises(TypeError, lambda: psser ** psser)
        self.assertRaises(TypeError, lambda: psser ** True)

        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser ** self.float_pser, (self.psser ** self.float_psser).sort_index()
            )

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser ** psser)

    def test_radd(self):
        self.assert_eq(1 + self.pser, 1 + self.psser)
        self.assert_eq(0.1 + self.pser, 0.1 + self.psser)
        self.assertRaises(TypeError, lambda: "x" + self.psser)
        self.assertRaises(TypeError, lambda: True + self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) + self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) + self.psser)

    def test_rsub(self):
        self.assert_eq(1 - self.pser, 1 - self.psser)
        self.assert_eq(0.1 - self.pser, 0.1 - self.psser)
        self.assertRaises(TypeError, lambda: "x" - self.psser)
        self.assertRaises(TypeError, lambda: True - self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) - self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) - self.psser)

    def test_rmul(self):
        self.assert_eq(1 * self.pser, 1 * self.psser)
        self.assert_eq(0.1 * self.pser, 0.1 * self.psser)
        self.assertRaises(TypeError, lambda: "x" * self.psser)
        self.assertRaises(TypeError, lambda: True * self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) * self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) * self.psser)

    def test_rtruediv(self):
        self.assert_eq(1 / self.pser, 1 / self.psser)
        self.assert_eq(0.1 / self.pser, 0.1 / self.psser)
        self.assertRaises(TypeError, lambda: "x" / self.psser)
        self.assertRaises(TypeError, lambda: True / self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) / self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) / self.psser)

    def test_rfloordiv(self):
        if LooseVersion(pd.__version__) >= LooseVersion("0.25.3"):
            self.assert_eq(1 // self.pser, 1 // self.psser)
            self.assert_eq(0.1 // self.pser, 0.1 // self.psser)
        else:
            self.assert_eq(1 // self.psser, ps.Series([1.0, 1.0, np.inf]))
            self.assert_eq(0.1 // self.psser, ps.Series([0.0, 0.0, np.inf]))
        self.assertRaises(TypeError, lambda: "x" + self.psser)
        self.assertRaises(TypeError, lambda: True + self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) // self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) // self.psser)

    def test_rpow(self):
        # float is returned always in pandas-on-Spark
        self.assert_eq((1 ** self.pser).astype(float), 1 ** self.psser)
        self.assert_eq(0.1 ** self.pser, 0.1 ** self.psser)
        self.assertRaises(TypeError, lambda: "x" ** self.psser)
        self.assertRaises(TypeError, lambda: True ** self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) ** self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) ** self.psser)

    def test_rmod(self):
        # 1 % False is 0.0 in pandas
        self.assert_eq(ps.Series([0, 0, None], dtype=float), 1 % self.psser)
        # 0.1 / True is 0.1 in pandas
        self.assert_eq(
            ps.Series([0.10000000000000009, 0.10000000000000009, None], dtype=float),
            0.1 % self.psser,
        )
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) % self.psser)
        self.assertRaises(TypeError, lambda: True % self.psser)


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_boolean_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
