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

import decimal
import datetime

import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.tests.data_type_ops.testing_utils import TestCasesUtils
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class ComplexOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    @property
    def numeric_array_psers(self):
        return [
            pd.Series([[1, 2, 3]]),
            pd.Series([[0.1, 0.2, 0.3]]),
            pd.Series([[decimal.Decimal(1), decimal.Decimal(2), decimal.Decimal(3)]]),
        ]

    @property
    def non_numeric_array_psers(self):
        return {
            "string": pd.Series([["x", "y", "z"]]),
            "date": pd.Series(
                [[datetime.date(1994, 1, 1), datetime.date(1994, 1, 2), datetime.date(1994, 1, 3)]]
            ),
            "bool": pd.Series([[True, True, False]]),
        }

    @property
    def numeric_array_pssers(self):
        return [ps.from_pandas(pser) for pser in self.numeric_array_psers]

    @property
    def non_numeric_array_pssers(self):
        pssers = {}

        for k, v in self.non_numeric_array_psers.items():
            pssers[k] = ps.from_pandas(v)
        return pssers

    @property
    def psers(self):
        return self.numeric_array_psers + list(self.non_numeric_array_psers.values())

    @property
    def pssers(self):
        return self.numeric_array_pssers + list(self.non_numeric_array_pssers.values())

    @property
    def pser(self):
        return pd.Series([[1, 2, 3]])

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def other_pser(self):
        return pd.Series([[2, 3, 4]])

    @property
    def other_psser(self):
        return ps.from_pandas(self.other_pser)

    @property
    def struct_pser(self):
        return pd.Series([("x", 1)])

    @property
    def struct_psser(self):
        return ps.Index([("x", 1)]).to_series().reset_index(drop=True)

    def test_add(self):
        for pser, psser in zip(self.psers, self.pssers):
            self.assert_eq(pser + pser, psser + psser)

        with option_context("compute.ops_on_diff_frames", True):
            # Numeric array + Numeric array
            for pser1, psser1 in zip(self.numeric_array_psers, self.numeric_array_pssers):
                for pser2, psser2 in zip(self.numeric_array_psers, self.numeric_array_pssers):
                    self.assert_eq((pser1 + pser2).sort_values(), (psser1 + psser2).sort_values())

            # Non-numeric array + Non-numeric array
            self.assertRaises(
                TypeError,
                lambda: self.non_numeric_array_pssers["string"]
                + self.non_numeric_array_pssers["bool"],
            )
            self.assertRaises(
                TypeError,
                lambda: self.non_numeric_array_pssers["string"]
                + self.non_numeric_array_pssers["date"],
            )
            self.assertRaises(
                TypeError,
                lambda: self.non_numeric_array_pssers["bool"]
                + self.non_numeric_array_pssers["date"],
            )

            for data_type in self.non_numeric_array_psers.keys():
                self.assert_eq(
                    self.non_numeric_array_psers.get(data_type)
                    + self.non_numeric_array_psers.get(data_type),
                    self.non_numeric_array_pssers.get(data_type)
                    + self.non_numeric_array_pssers.get(data_type),
                )

            # Numeric array + Non-numeric array
            for numeric_ppser in self.numeric_array_pssers:
                for non_numeric_ppser in self.non_numeric_array_pssers.values():
                    self.assertRaises(TypeError, lambda: numeric_ppser + non_numeric_ppser)

    def test_sub(self):
        self.assertRaises(TypeError, lambda: self.psser - "x")
        self.assertRaises(TypeError, lambda: self.psser - 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser1 in self.pssers:
                for psser2 in self.pssers:
                    self.assertRaises(TypeError, lambda: psser1 - psser2)

    def test_mul(self):
        self.assertRaises(TypeError, lambda: self.psser * "x")
        self.assertRaises(TypeError, lambda: self.psser * 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser1 in self.pssers:
                for psser2 in self.pssers:
                    self.assertRaises(TypeError, lambda: psser1 * psser2)

    def test_truediv(self):
        self.assertRaises(TypeError, lambda: self.psser / "x")
        self.assertRaises(TypeError, lambda: self.psser / 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser1 in self.pssers:
                for psser2 in self.pssers:
                    self.assertRaises(TypeError, lambda: psser1 / psser2)

    def test_floordiv(self):
        self.assertRaises(TypeError, lambda: self.psser // "x")
        self.assertRaises(TypeError, lambda: self.psser // 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser1 in self.pssers:
                for psser2 in self.pssers:
                    self.assertRaises(TypeError, lambda: psser1 // psser2)

    def test_mod(self):
        self.assertRaises(TypeError, lambda: self.psser % "x")
        self.assertRaises(TypeError, lambda: self.psser % 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser1 in self.pssers:
                for psser2 in self.pssers:
                    self.assertRaises(TypeError, lambda: psser1 % psser2)

    def test_pow(self):
        self.assertRaises(TypeError, lambda: self.psser ** "x")
        self.assertRaises(TypeError, lambda: self.psser ** 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser1 in self.pssers:
                for psser2 in self.pssers:
                    self.assertRaises(TypeError, lambda: psser1 ** psser2)

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
        for pser, psser in zip(self.psers, self.pssers):
            self.assert_eq(pser, psser.to_pandas())
            self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        for pser, psser in zip(self.psers, self.pssers):
            self.assert_eq(pser.isnull(), psser.isnull())

    def test_astype(self):
        self.assert_eq(self.pser.astype(str), self.psser.astype(str))

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
            self.assert_eq(
                self.struct_pser == self.struct_pser,
                (self.struct_psser == self.struct_psser).sort_index(),
            )

    def test_ne(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser != self.other_pser, (self.psser != self.other_psser).sort_index()
            )
            self.assert_eq(self.pser != self.pser, (self.psser != self.psser).sort_index())
            self.assert_eq(
                self.struct_pser != self.struct_pser,
                (self.struct_psser != self.struct_psser).sort_index(),
            )

    def test_lt(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser < self.other_pser, (self.psser < self.other_psser).sort_index()
            )
            self.assert_eq(self.pser < self.pser, (self.psser < self.psser).sort_index())
            self.assert_eq(
                self.struct_pser < self.struct_pser,
                (self.struct_psser < self.struct_psser).sort_index(),
            )

    def test_le(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser <= self.other_pser, (self.psser <= self.other_psser).sort_index()
            )
            self.assert_eq(self.pser <= self.pser, (self.psser <= self.psser).sort_index())
            self.assert_eq(
                self.struct_pser <= self.struct_pser,
                (self.struct_psser <= self.struct_psser).sort_index(),
            )

    def test_gt(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser > self.other_pser, (self.psser > self.other_psser).sort_index()
            )
            self.assert_eq(self.pser > self.pser, (self.psser > self.psser).sort_index())
            self.assert_eq(
                self.struct_pser > self.struct_pser,
                (self.struct_psser > self.struct_psser).sort_index(),
            )

    def test_ge(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser >= self.other_pser, (self.psser >= self.other_psser).sort_index()
            )
            self.assert_eq(self.pser >= self.pser, (self.psser >= self.psser).sort_index())
            self.assert_eq(
                self.struct_pser >= self.struct_pser,
                (self.struct_psser >= self.struct_psser).sort_index(),
            )


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_complex_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
