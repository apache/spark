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

import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.tests.data_type_ops.testing_utils import TestCasesUtils
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class BinaryOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    @property
    def pser(self):
        return pd.Series([b"1", b"2", b"3"])

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def other_pser(self):
        return pd.Series([b"2", b"3", b"4"])

    @property
    def other_psser(self):
        return ps.from_pandas(self.other_pser)

    def test_add(self):
        psser = self.psser
        pser = self.pser
        self.assert_eq(psser + b"1", pser + b"1")
        self.assert_eq(psser + psser, pser + pser)
        self.assert_eq(psser + psser.astype("bytes"), pser + pser.astype("bytes"))
        self.assertRaises(TypeError, lambda: psser + "x")
        self.assertRaises(TypeError, lambda: psser + 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser + psser)
            self.assert_eq(self.psser + self.psser, self.pser + self.pser)

    def test_sub(self):
        self.assertRaises(TypeError, lambda: self.psser - "x")
        self.assertRaises(TypeError, lambda: self.psser - 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser - psser)

    def test_mul(self):
        self.assertRaises(TypeError, lambda: self.psser * "x")
        self.assertRaises(TypeError, lambda: self.psser * 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser * psser)

    def test_truediv(self):
        self.assertRaises(TypeError, lambda: self.psser / "x")
        self.assertRaises(TypeError, lambda: self.psser / 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser / psser)

    def test_floordiv(self):
        self.assertRaises(TypeError, lambda: self.psser // "x")
        self.assertRaises(TypeError, lambda: self.psser // 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser // psser)

    def test_mod(self):
        self.assertRaises(TypeError, lambda: self.psser % "x")
        self.assertRaises(TypeError, lambda: self.psser % 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser % psser)

    def test_pow(self):
        self.assertRaises(TypeError, lambda: self.psser ** "x")
        self.assertRaises(TypeError, lambda: self.psser ** 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser ** psser)

    def test_radd(self):
        self.assert_eq(b"1" + self.psser, b"1" + self.pser)
        self.assertRaises(TypeError, lambda: "x" + self.psser)
        self.assertRaises(TypeError, lambda: 1 + self.psser)

    def test_rsub(self):
        self.assertRaises(TypeError, lambda: "x" - self.psser)
        self.assertRaises(TypeError, lambda: 1 - self.psser)

    def test_rmul(self):
        self.assertRaises(TypeError, lambda: "x" * self.psser)
        self.assertRaises(TypeError, lambda: 2 * self.psser)

    def test_rtruediv(self):
        self.assertRaises(TypeError, lambda: "x" / self.psser)
        self.assertRaises(TypeError, lambda: 1 / self.psser)

    def test_rfloordiv(self):
        self.assertRaises(TypeError, lambda: "x" // self.psser)
        self.assertRaises(TypeError, lambda: 1 // self.psser)

    def test_rmod(self):
        self.assertRaises(TypeError, lambda: 1 % self.psser)

    def test_rpow(self):
        self.assertRaises(TypeError, lambda: "x" ** self.psser)
        self.assertRaises(TypeError, lambda: 1 ** self.psser)

    def test_and(self):
        self.assertRaises(TypeError, lambda: self.psser & True)
        self.assertRaises(TypeError, lambda: self.psser & False)
        self.assertRaises(TypeError, lambda: self.psser & self.psser)

    def test_rand(self):
        self.assertRaises(TypeError, lambda: True & self.psser)
        self.assertRaises(TypeError, lambda: False & self.psser)

    def test_or(self):
        self.assertRaises(TypeError, lambda: self.psser | True)
        self.assertRaises(TypeError, lambda: self.psser | False)
        self.assertRaises(TypeError, lambda: self.psser | self.psser)

    def test_ror(self):
        self.assertRaises(TypeError, lambda: True | self.psser)
        self.assertRaises(TypeError, lambda: False | self.psser)

    def test_from_to_pandas(self):
        data = [b"1", b"2", b"3"]
        pser = pd.Series(data)
        psser = ps.Series(data)
        self.assert_eq(pser, psser.to_pandas())
        self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        self.assert_eq(self.pser.isnull(), self.psser.isnull())

    def test_astype(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pd.Series(["1", "2", "3"]), psser.astype(str))
        self.assert_eq(pser.astype("category"), psser.astype("category"))
        cat_type = CategoricalDtype(categories=[b"2", b"3", b"1"])
        self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))

    def test_neg(self):
        self.assertRaises(TypeError, lambda: -self.psser)

    def test_abs(self):
        self.assertRaises(TypeError, lambda: abs(self.psser))

    def test_invert(self):
        self.assertRaises(TypeError, lambda: ~self.psser)

    def test_eq(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser == self.other_pser, (self.psser == self.other_psser).sort_index()
            )
            self.assert_eq(self.pser == self.pser, (self.psser == self.psser).sort_index())

    def test_ne(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser != self.other_pser, (self.psser != self.other_psser).sort_index()
            )
            self.assert_eq(self.pser != self.pser, (self.psser != self.psser).sort_index())

    def test_lt(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser < self.other_pser, (self.psser < self.other_psser).sort_index()
            )
            self.assert_eq(self.pser < self.pser, (self.psser < self.psser).sort_index())

    def test_le(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser <= self.other_pser, (self.psser <= self.other_psser).sort_index()
            )
            self.assert_eq(self.pser <= self.pser, (self.psser <= self.psser).sort_index())

    def test_gt(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser > self.other_pser, (self.psser > self.other_psser).sort_index()
            )
            self.assert_eq(self.pser > self.pser, (self.psser > self.psser).sort_index())

    def test_ge(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser >= self.other_pser, (self.psser >= self.other_psser).sort_index()
            )
            self.assert_eq(self.pser >= self.pser, (self.psser >= self.psser).sort_index())


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_binary_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
