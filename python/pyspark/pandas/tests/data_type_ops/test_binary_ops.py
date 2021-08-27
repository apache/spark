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
    def byte_pdf(self):
        psers = {
            "this": self.pser,
            "that": pd.Series([b"2", b"3", b"4"]),
        }
        return pd.concat(psers, axis=1)

    @property
    def byte_psdf(self):
        return ps.from_pandas(self.byte_pdf)

    def test_add(self):
        byte_pdf, byte_psdf = self.byte_pdf, self.byte_psdf
        pser, psser = byte_pdf["this"], byte_psdf["this"]
        other_pser, other_psser = byte_pdf["that"], byte_psdf["that"]

        self.assert_eq(psser + b"1", pser + b"1")
        self.assert_eq(psser + psser, pser + pser)
        self.assert_eq(psser + psser.astype("bytes"), pser + pser.astype("bytes"))
        self.assertRaises(TypeError, lambda: psser + "x")
        self.assertRaises(TypeError, lambda: psser + 1)

        self.assert_eq(pser + pser, psser + psser)
        self.assert_eq(pser + other_pser, psser + other_psser)

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
        self.assertRaises(TypeError, lambda: self.psser ** 1)

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
        byte_pdf, byte_psdf = self.byte_pdf, self.byte_psdf
        self.assert_eq(byte_pdf["this"] == byte_pdf["that"], byte_psdf["this"] == byte_psdf["that"])
        self.assert_eq(byte_pdf["this"] == byte_pdf["this"], byte_psdf["this"] == byte_psdf["this"])

    def test_ne(self):
        byte_pdf, byte_psdf = self.byte_pdf, self.byte_psdf
        self.assert_eq(byte_pdf["this"] != byte_pdf["that"], byte_psdf["this"] != byte_psdf["that"])
        self.assert_eq(byte_pdf["this"] != byte_pdf["this"], byte_psdf["this"] != byte_psdf["this"])

    def test_lt(self):
        byte_pdf, byte_psdf = self.byte_pdf, self.byte_psdf
        self.assert_eq(byte_pdf["this"] < byte_pdf["that"], byte_psdf["this"] < byte_psdf["that"])
        self.assert_eq(byte_pdf["this"] < byte_pdf["this"], byte_psdf["this"] < byte_psdf["this"])

    def test_le(self):
        byte_pdf, byte_psdf = self.byte_pdf, self.byte_psdf
        self.assert_eq(byte_pdf["this"] <= byte_pdf["that"], byte_psdf["this"] <= byte_psdf["that"])
        self.assert_eq(byte_pdf["this"] <= byte_pdf["this"], byte_psdf["this"] <= byte_psdf["this"])

    def test_gt(self):
        byte_pdf, byte_psdf = self.byte_pdf, self.byte_psdf
        self.assert_eq(byte_pdf["this"] > byte_pdf["that"], byte_psdf["this"] > byte_psdf["that"])
        self.assert_eq(byte_pdf["this"] > byte_pdf["this"], byte_psdf["this"] > byte_psdf["this"])

    def test_ge(self):
        byte_pdf, byte_psdf = self.byte_pdf, self.byte_psdf
        self.assert_eq(byte_pdf["this"] >= byte_pdf["that"], byte_psdf["this"] >= byte_psdf["that"])
        self.assert_eq(byte_pdf["this"] >= byte_pdf["this"], byte_psdf["this"] >= byte_psdf["this"])


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_binary_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
