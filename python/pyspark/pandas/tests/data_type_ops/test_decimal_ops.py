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

import decimal as d
import unittest

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.data_type_ops.num_ops import DecimalOps
from pyspark.pandas.tests.data_type_ops.testing_utils import TestCasesUtils
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class DecimalOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    @property
    def decimal_pser(self):
        return pd.Series([d.Decimal(1.0), d.Decimal(2.0), d.Decimal(np.nan)])

    @property
    def decimal_psser(self):
        return ps.from_pandas(self.decimal_pser)

    @property
    def other_decimal_pser(self):
        return pd.Series([d.Decimal(2.0), d.Decimal(1.0), d.Decimal(-3.0)])

    @property
    def other_decimal_psser(self):
        return ps.from_pandas(self.other_decimal_pser)

    @property
    def float_pser(self):
        return pd.Series([1, 2, np.nan], dtype=float)

    @property
    def float_psser(self):
        return ps.from_pandas(self.float_pser)

    def test_datatype_ops(self):
        self.assertIsInstance(self.decimal_psser._dtype_op, DecimalOps)
        self.assertEqual(self.decimal_psser._dtype_op.pretty_name, "decimal")

    def test_isnull(self):
        self.assertEqual(self.decimal_psser._dtype_op.pretty_name, "decimal")
        self.assertEqual(self.float_psser._dtype_op.pretty_name, "fractions")
        self.assert_eq(self.decimal_pser.isnull(), self.decimal_psser.isnull())
        self.assert_eq(self.float_pser.isnull(), self.float_psser.isnull())

    def test_neg(self):
        self.assert_eq(-self.other_decimal_pser, -self.other_decimal_psser)

    def test_abs(self):
        self.assert_eq(abs(self.other_decimal_pser), abs(self.other_decimal_psser))

    def test_invert(self):
        self.assertRaises(TypeError, lambda: ~self.decimal_psser)

    def test_eq(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.decimal_pser == self.other_decimal_pser,
                (self.decimal_psser == self.other_decimal_psser).sort_index(),
            )
            self.assert_eq(
                self.decimal_pser == self.decimal_pser,
                (self.decimal_psser == self.decimal_psser).sort_index(),
            )

    def test_ne(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.decimal_pser != self.other_decimal_pser,
                (self.decimal_psser != self.other_decimal_psser).sort_index(),
            )
            self.assert_eq(
                self.decimal_pser != self.decimal_pser,
                (self.decimal_psser != self.decimal_psser).sort_index(),
            )

    def test_lt(self):
        self.assertRaises(TypeError, lambda: self.decimal_psser < self.other_decimal_psser)

    def test_le(self):
        self.assertRaises(TypeError, lambda: self.decimal_psser <= self.other_decimal_psser)

    def test_gt(self):
        self.assertRaises(TypeError, lambda: self.decimal_psser > self.other_decimal_psser)

    def test_ge(self):
        self.assertRaises(TypeError, lambda: self.decimal_psser >= self.other_decimal_psser)


if __name__ == "__main__":

    from pyspark.pandas.tests.data_type_ops.test_decimal_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
