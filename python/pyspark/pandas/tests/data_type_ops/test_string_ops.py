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

import unittest

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.tests.data_type_ops.testing_utils import TestCasesUtils
from pyspark.pandas.typedef.typehints import extension_object_dtypes_available
from pyspark.testing.pandasutils import PandasOnSparkTestCase

if extension_object_dtypes_available:
    from pandas import StringDtype


class StringOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    @property
    def pser(self):
        return pd.Series(["x", "y", "z"])

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def other_pser(self):
        return pd.Series(["z", "y", "x"])

    @property
    def other_psser(self):
        return ps.from_pandas(self.other_pser)

    def test_add(self):
        self.assert_eq(self.pser + "x", self.psser + "x")
        self.assertRaises(TypeError, lambda: self.psser + 1)

        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser + self.non_numeric_psers["string"],
                (self.psser + self.non_numeric_pssers["string"]).sort_index(),
            )
            self.assertRaises(TypeError, lambda: self.psser + self.non_numeric_pssers["datetime"])
            self.assertRaises(TypeError, lambda: self.psser + self.non_numeric_pssers["date"])
            self.assertRaises(
                TypeError, lambda: self.psser + self.non_numeric_pssers["categorical"]
            )
            self.assertRaises(TypeError, lambda: self.psser + self.non_numeric_pssers["bool"])
            for psser in self.numeric_pssers:
                self.assertRaises(TypeError, lambda: self.psser + psser)

    def test_sub(self):
        self.assertRaises(TypeError, lambda: self.psser - "x")
        self.assertRaises(TypeError, lambda: self.psser - 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser - psser)

    def test_mul(self):
        self.assertRaises(TypeError, lambda: self.psser * "x")
        self.assert_eq(self.pser * 1, self.psser * 1)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.pser_psser_pairs:
                if psser.dtype in [np.int64, np.int32]:
                    self.assert_eq(self.pser * pser, (self.psser * psser).sort_index())
                else:
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
        self.assert_eq("x" + self.pser, "x" + self.psser)
        self.assertRaises(TypeError, lambda: 1 + self.psser)

    def test_rsub(self):
        self.assertRaises(TypeError, lambda: "x" - self.psser)
        self.assertRaises(TypeError, lambda: 1 - self.psser)

    def test_rmul(self):
        self.assertRaises(TypeError, lambda: "x" * self.psser)
        self.assert_eq(1 * self.pser, 1 * self.psser)

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
        data = ["x", "y", "z"]
        pser = pd.Series(data)
        psser = ps.Series(data)
        self.assert_eq(pser, psser.to_pandas())
        self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        self.assert_eq(self.pser.isnull(), self.psser.isnull())

    def test_astype(self):
        pser = pd.Series(["1", "2", "3"])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.astype(int), psser.astype(int))
        self.assert_eq(pser.astype(float), psser.astype(float))
        self.assert_eq(pser.astype(np.float32), psser.astype(np.float32))
        self.assert_eq(pser.astype(np.int32), psser.astype(np.int32))
        self.assert_eq(pser.astype(np.int16), psser.astype(np.int16))
        self.assert_eq(pser.astype(np.int8), psser.astype(np.int8))
        self.assert_eq(pser.astype(str), psser.astype(str))
        self.assert_eq(pser.astype(bool), psser.astype(bool))
        self.assert_eq(pser.astype("category"), psser.astype("category"))
        cat_type = CategoricalDtype(categories=["3", "1", "2"])
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


@unittest.skipIf(
    not extension_object_dtypes_available, "pandas extension object dtypes are not available"
)
class StringExtensionOpsTest(StringOpsTest, PandasOnSparkTestCase, TestCasesUtils):
    @property
    def pser(self):
        return pd.Series(["x", "y", "z", None], dtype="string")

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def other_pser(self):
        return pd.Series([None, "z", "y", "x"], dtype="string")

    @property
    def other_psser(self):
        return ps.from_pandas(self.other_pser)

    def test_radd(self):
        self.assert_eq("x" + self.pser, ("x" + self.psser).astype("string"))
        self.assertRaises(TypeError, lambda: 1 + self.psser)

    def test_mul(self):
        self.assertRaises(TypeError, lambda: self.psser * "x")
        self.assert_eq(self.pser * 1, self.psser * 1)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.pser_psser_pairs:
                if psser.dtype in [np.int32, np.int64]:
                    self.assert_eq(
                        ps.Series(["x", "yy", "zzz", None]).astype("string"),
                        (self.psser * psser).sort_index(),
                    )
                else:
                    self.assertRaises(TypeError, lambda: self.psser * psser)

    def test_from_to_pandas(self):
        data = ["x", "y", "z", None]
        pser = pd.Series(data, dtype="string")
        psser = ps.Series(data, dtype="string")
        self.assert_eq(pser, psser.to_pandas())
        self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        self.assert_eq(self.pser.isnull(), self.psser.isnull())

    def test_astype(self):
        pser = self.pser
        psser = self.psser

        # TODO(SPARK-35976): [x, y, z, <NA>] is returned in pandas
        self.assert_eq(["x", "y", "z", "None"], self.psser.astype(str).tolist())

        self.assert_eq(pser.astype("category"), psser.astype("category"))
        cat_type = CategoricalDtype(categories=["x", "y"])
        self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))
        for dtype in self.object_extension_dtypes:
            if dtype in ["string", StringDtype()]:
                self.check_extension(pser.astype(dtype), psser.astype(dtype))

    def test_eq(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.check_extension(
                self.pser == self.other_pser, (self.psser == self.other_psser).sort_index()
            )
            self.check_extension(self.pser == self.pser, (self.psser == self.psser).sort_index())

    def test_ne(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.check_extension(
                self.pser != self.other_pser, (self.psser != self.other_psser).sort_index()
            )
            self.check_extension(self.pser != self.pser, (self.psser != self.psser).sort_index())

    def test_lt(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.check_extension(
                self.pser < self.other_pser, (self.psser < self.other_psser).sort_index()
            )
            self.check_extension(self.pser < self.pser, (self.psser < self.psser).sort_index())

    def test_le(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.check_extension(
                self.pser <= self.other_pser, (self.psser <= self.other_psser).sort_index()
            )
            self.check_extension(self.pser <= self.pser, (self.psser <= self.psser).sort_index())

    def test_gt(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.check_extension(
                self.pser > self.other_pser, (self.psser > self.other_psser).sort_index()
            )
            self.check_extension(self.pser > self.pser, (self.psser > self.psser).sort_index())

    def test_ge(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.check_extension(
                self.pser >= self.other_pser, (self.psser >= self.other_psser).sort_index()
            )
            self.check_extension(self.pser >= self.pser, (self.psser >= self.psser).sort_index())


if __name__ == "__main__":

    from pyspark.pandas.tests.data_type_ops.test_string_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
