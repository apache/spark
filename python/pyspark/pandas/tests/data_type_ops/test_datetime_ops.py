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
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.tests.data_type_ops.testing_utils import TestCasesUtils
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class DatetimeOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    @property
    def pser(self):
        return pd.Series(pd.date_range("1994-1-31 10:30:15", periods=3, freq="M"))

    @property
    def kser(self):
        return ps.from_pandas(self.pser)

    @property
    def some_datetime(self):
        return datetime.datetime(1994, 1, 31, 10, 30, 00)

    def test_add(self):
        self.assertRaises(TypeError, lambda: self.kser + "x")
        self.assertRaises(TypeError, lambda: self.kser + 1)
        self.assertRaises(TypeError, lambda: self.kser + self.some_datetime)

        with option_context("compute.ops_on_diff_frames", True):
            for kser in self.ksers:
                self.assertRaises(TypeError, lambda: self.kser + kser)

    def test_sub(self):
        self.assertRaises(TypeError, lambda: self.kser - "x")
        self.assertRaises(TypeError, lambda: self.kser - 1)
        self.assert_eq(
            (self.pser - self.some_datetime).dt.total_seconds().astype("int"),
            self.kser - self.some_datetime,
        )
        with option_context("compute.ops_on_diff_frames", True):
            for pser, kser in self.pser_kser_pairs:
                if pser.dtype == np.dtype("<M8[ns]"):
                    self.assert_eq(
                        (self.pser - pser).dt.total_seconds().astype("int"),
                        (self.kser - kser).sort_index(),
                    )
                else:
                    self.assertRaises(TypeError, lambda: self.kser - kser)

    def test_mul(self):
        self.assertRaises(TypeError, lambda: self.kser * "x")
        self.assertRaises(TypeError, lambda: self.kser * 1)
        self.assertRaises(TypeError, lambda: self.kser * self.some_datetime)

        with option_context("compute.ops_on_diff_frames", True):
            for kser in self.ksers:
                self.assertRaises(TypeError, lambda: self.kser * kser)

    def test_truediv(self):
        self.assertRaises(TypeError, lambda: self.kser / "x")
        self.assertRaises(TypeError, lambda: self.kser / 1)
        self.assertRaises(TypeError, lambda: self.kser / self.some_datetime)

        with option_context("compute.ops_on_diff_frames", True):
            for kser in self.ksers:
                self.assertRaises(TypeError, lambda: self.kser / kser)

    def test_floordiv(self):
        self.assertRaises(TypeError, lambda: self.kser // "x")
        self.assertRaises(TypeError, lambda: self.kser // 1)
        self.assertRaises(TypeError, lambda: self.kser // self.some_datetime)

        with option_context("compute.ops_on_diff_frames", True):
            for kser in self.ksers:
                self.assertRaises(TypeError, lambda: self.kser // kser)

    def test_mod(self):
        self.assertRaises(TypeError, lambda: self.kser % "x")
        self.assertRaises(TypeError, lambda: self.kser % 1)
        self.assertRaises(TypeError, lambda: self.kser % self.some_datetime)

        with option_context("compute.ops_on_diff_frames", True):
            for kser in self.ksers:
                self.assertRaises(TypeError, lambda: self.kser % kser)

    def test_pow(self):
        self.assertRaises(TypeError, lambda: self.kser ** "x")
        self.assertRaises(TypeError, lambda: self.kser ** 1)
        self.assertRaises(TypeError, lambda: self.kser ** self.some_datetime)

        with option_context("compute.ops_on_diff_frames", True):
            for kser in self.ksers:
                self.assertRaises(TypeError, lambda: self.kser ** kser)

    def test_radd(self):
        self.assertRaises(TypeError, lambda: "x" + self.kser)
        self.assertRaises(TypeError, lambda: 1 + self.kser)
        self.assertRaises(TypeError, lambda: self.some_datetime + self.kser)

    def test_rsub(self):
        self.assertRaises(TypeError, lambda: "x" - self.kser)
        self.assertRaises(TypeError, lambda: 1 - self.kser)
        self.assert_eq(
            (self.some_datetime - self.pser).dt.total_seconds().astype("int"),
            self.some_datetime - self.kser,
        )

    def test_rmul(self):
        self.assertRaises(TypeError, lambda: "x" * self.kser)
        self.assertRaises(TypeError, lambda: 1 * self.kser)
        self.assertRaises(TypeError, lambda: self.some_datetime * self.kser)

    def test_rtruediv(self):
        self.assertRaises(TypeError, lambda: "x" / self.kser)
        self.assertRaises(TypeError, lambda: 1 / self.kser)
        self.assertRaises(TypeError, lambda: self.some_datetime / self.kser)

    def test_rfloordiv(self):
        self.assertRaises(TypeError, lambda: "x" // self.kser)
        self.assertRaises(TypeError, lambda: 1 // self.kser)
        self.assertRaises(TypeError, lambda: self.some_datetime // self.kser)

    def test_rmod(self):
        self.assertRaises(TypeError, lambda: 1 % self.kser)
        self.assertRaises(TypeError, lambda: self.some_datetime % self.kser)

    def test_rpow(self):
        self.assertRaises(TypeError, lambda: "x" ** self.kser)
        self.assertRaises(TypeError, lambda: 1 ** self.kser)
        self.assertRaises(TypeError, lambda: self.some_datetime ** self.kser)


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_datetime_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
