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

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


# This file contains test cases for SPARK-55818:
# "Decimal-float mixed arithmetic should always raise TypeError"
# https://issues.apache.org/jira/browse/SPARK-55818
class DecimalFloatArithmeticMixin:
    """
    Tests that float Series + decimal.Decimal scalar always raises TypeError,
    matching native pandas behavior, regardless of ANSI mode.
    """

    def test_float_add_decimal_raises(self):
        psser = ps.Series([1.0, 2.0, 3.0])
        with self.assertRaises(TypeError):
            psser + decimal.Decimal("1.5")

    def test_float_sub_decimal_raises(self):
        psser = ps.Series([1.0, 2.0, 3.0])
        with self.assertRaises(TypeError):
            psser - decimal.Decimal("1.5")

    def test_float_mul_decimal_raises(self):
        psser = ps.Series([1.0, 2.0, 3.0])
        with self.assertRaises(TypeError):
            psser * decimal.Decimal("2")

    def test_float_truediv_decimal_raises(self):
        psser = ps.Series([1.0, 2.0, 3.0])
        with self.assertRaises(TypeError):
            psser / decimal.Decimal("2")

    def test_float_floordiv_decimal_raises(self):
        psser = ps.Series([1.0, 2.0, 3.0])
        with self.assertRaises(TypeError):
            psser // decimal.Decimal("2")

    def test_float_mod_decimal_raises(self):
        psser = ps.Series([1.0, 2.0, 3.0])
        with self.assertRaises(TypeError):
            psser % decimal.Decimal("2")

    def test_radd_decimal_raises(self):
        psser = ps.Series([1.0, 2.0, 3.0])
        with self.assertRaises(TypeError):
            decimal.Decimal("1.5") + psser

    def test_rsub_decimal_raises(self):
        psser = ps.Series([1.0, 2.0, 3.0])
        with self.assertRaises(TypeError):
            decimal.Decimal("1.5") - psser

    def test_rmul_decimal_raises(self):
        psser = ps.Series([1.0, 2.0, 3.0])
        with self.assertRaises(TypeError):
            decimal.Decimal("2") * psser


class DecimalFloatArithmeticTests(DecimalFloatArithmeticMixin, PandasOnSparkTestCase):
    pass


if __name__ == "__main__":
    import unittest

    unittest.main()
