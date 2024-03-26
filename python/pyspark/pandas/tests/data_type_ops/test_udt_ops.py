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

import pyspark.pandas as ps
from pyspark.ml.linalg import SparseVector
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase


class UDTOpsTestsMixin:
    @property
    def pser(self):
        sparse_values = {0: 0.1, 1: 1.1}
        return pd.Series([SparseVector(len(sparse_values), sparse_values)])

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def udt_pdf(self):
        sparse_values = {0: 0.2, 1: 1.0}
        psers = {
            "this": self.pser,
            "that": pd.Series([SparseVector(len(sparse_values), sparse_values)]),
        }
        return pd.concat(psers, axis=1)

    @property
    def udt_psdf(self):
        return ps.from_pandas(self.udt_pdf)

    def test_add(self):
        self.assertRaises(TypeError, lambda: self.psser + "x")
        self.assertRaises(TypeError, lambda: self.psser + 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser + psser)

    def test_sub(self):
        self.assertRaises(TypeError, lambda: self.psser - "x")
        self.assertRaises(TypeError, lambda: self.psser - 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser - psser)

    def test_mul(self):
        self.assertRaises(TypeError, lambda: self.psser * "x")
        self.assertRaises(TypeError, lambda: self.psser * 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser * psser)

    def test_truediv(self):
        self.assertRaises(TypeError, lambda: self.psser / "x")
        self.assertRaises(TypeError, lambda: self.psser / 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser / psser)

    def test_floordiv(self):
        self.assertRaises(TypeError, lambda: self.psser // "x")
        self.assertRaises(TypeError, lambda: self.psser // 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser // psser)

    def test_mod(self):
        self.assertRaises(TypeError, lambda: self.psser % "x")
        self.assertRaises(TypeError, lambda: self.psser % 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser % psser)

    def test_pow(self):
        self.assertRaises(TypeError, lambda: self.psser ** "x")
        self.assertRaises(TypeError, lambda: self.psser**1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser**psser)

    def test_radd(self):
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
        self.assertRaises(TypeError, lambda: 1**self.psser)

    def test_from_to_pandas(self):
        sparse_values = {0: 0.1, 1: 1.1}
        sparse_vector = SparseVector(len(sparse_values), sparse_values)
        pser = pd.Series([sparse_vector])
        psser = ps.Series([sparse_vector])
        self.assert_eq(pser, psser._to_pandas())
        self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        self.assert_eq(self.pser.isnull(), self.psser.isnull())

    def test_astype(self):
        self.assertRaises(TypeError, lambda: self.psser.astype(str))

    def test_neg(self):
        self.assertRaises(TypeError, lambda: -self.psser)

    def test_abs(self):
        self.assertRaises(TypeError, lambda: abs(self.psser))

    def test_invert(self):
        self.assertRaises(TypeError, lambda: ~self.psser)

    def test_eq(self):
        pdf, psdf = self.udt_pdf, self.udt_psdf
        self.assert_eq(pdf["this"] == pdf["this"], psdf["this"] == psdf["this"])
        self.assert_eq(pdf["this"] == pdf["that"], psdf["this"] == psdf["that"])

    def test_ne(self):
        pdf, psdf = self.udt_pdf, self.udt_psdf
        self.assert_eq(pdf["this"] != pdf["this"], psdf["this"] != psdf["this"])
        self.assert_eq(pdf["this"] != pdf["that"], psdf["this"] != psdf["that"])

    def test_lt(self):
        self.assertRaisesRegex(
            TypeError, "< can not be applied to", lambda: self.psser < self.psser
        )

    def test_le(self):
        self.assertRaisesRegex(
            TypeError, "<= can not be applied to", lambda: self.psser <= self.psser
        )

    def test_gt(self):
        self.assertRaisesRegex(
            TypeError, "> can not be applied to", lambda: self.psser > self.psser
        )

    def test_ge(self):
        self.assertRaisesRegex(
            TypeError, ">= can not be applied to", lambda: self.psser >= self.psser
        )


class UDTOpsTests(
    UDTOpsTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_udt_ops import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
