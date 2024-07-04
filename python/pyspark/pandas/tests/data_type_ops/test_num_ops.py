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

import pandas as pd
import numpy as np

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase
from pyspark.pandas.typedef.typehints import (
    extension_dtypes_available,
    extension_float_dtypes_available,
    extension_object_dtypes_available,
)
from pyspark.sql.types import DecimalType, IntegralType


class NumOpsTestsMixin:
    """Unit tests for arithmetic operations of numeric data types.

    A few test cases are disabled because pandas-on-Spark returns float64 whereas pandas
    returns float32.
    The underlying reason is the respective Spark operations return DoubleType always.
    """

    def test_and(self):
        psdf = self.psdf
        for col in self.numeric_df_cols:
            psser = psdf[col]
            self.assertRaises(TypeError, lambda: psser & True)
            self.assertRaises(TypeError, lambda: psser & False)
            self.assertRaises(TypeError, lambda: psser & psser)

    def test_rand(self):
        psdf = self.psdf
        for col in self.numeric_df_cols:
            psser = psdf[col]
            self.assertRaises(TypeError, lambda: True & psser)
            self.assertRaises(TypeError, lambda: False & psser)

    def test_or(self):
        psdf = self.psdf
        for col in self.numeric_df_cols:
            psser = psdf[col]
            self.assertRaises(TypeError, lambda: psser | True)
            self.assertRaises(TypeError, lambda: psser | False)
            self.assertRaises(TypeError, lambda: psser | psser)

    def test_ror(self):
        psdf = self.psdf
        for col in self.numeric_df_cols:
            psser = psdf[col]
            self.assertRaises(TypeError, lambda: True | psser)
            self.assertRaises(TypeError, lambda: False | psser)

    def test_xor(self):
        pdf, psdf = self.integral_pdf, self.integral_psdf
        pser, other_pser = pdf["this"], pdf["that"]
        psser, other_psser = psdf["this"], psdf["that"]

        self.assert_eq(pser ^ other_pser, psser ^ other_psser)
        self.assert_eq(pser ^ 2, psser ^ 2)
        self.assert_eq(pser ^ 3, psser ^ 3)
        self.assert_eq(pser ^ False, psser ^ False)
        self.assert_eq(pser ^ True, psser ^ True)

        with self.assertRaisesRegex(TypeError, "XOR can not be applied to given types."):
            psser ^ "a"
            psser ^ None

        with option_context("compute.ops_on_diff_frames", True):
            pser, other_pser = self.integral_pdf["this"], self.pdf["bool"]
            psser, other_psser = self.integral_psdf["this"], self.psdf["bool"]

            self.assert_eq(pser ^ other_pser, psser ^ other_psser)

    def test_rxor(self):
        pser, psser = self.pdf["int"], self.psdf["int"]
        self.assert_eq(True ^ pser, True ^ psser)
        self.assert_eq(False ^ pser, False ^ psser)
        self.assert_eq(1 ^ pser, 1 ^ psser)

    def test_from_to_pandas(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            self.assert_eq(pser, psser._to_pandas(), check_exact=False)
            self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            self.assert_eq(pdf[col].isnull(), psdf[col].isnull())

    def test_neg(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            self.assert_eq(-pdf[col], -psdf[col], check_exact=False)

    def test_abs(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            self.assert_eq(abs(pdf[col]), abs(psdf[col]), check_exact=False)

    def test_invert(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            if isinstance(psser.spark.data_type, IntegralType):
                self.assert_eq(~pser, ~psser)
            else:
                self.assertRaises(TypeError, lambda: ~psser)

    def test_eq(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            self.assert_eq(pdf[col] == pdf[col], psdf[col] == psdf[col])

    def test_ne(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            self.assert_eq(pdf[col] != pdf[col], psdf[col] != psdf[col])

    def test_lt(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            if isinstance(psser.spark.data_type, DecimalType):
                self.assertRaisesRegex(TypeError, "< can not be applied to", lambda: psser < psser)
            else:
                self.assert_eq(pser < pser, psser < psser)

    def test_le(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            if isinstance(psser.spark.data_type, DecimalType):
                self.assertRaisesRegex(
                    TypeError, "<= can not be applied to", lambda: psser <= psser
                )
            else:
                self.assert_eq(pser <= pser, psser <= psser)

    def test_gt(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            if isinstance(psser.spark.data_type, DecimalType):
                self.assertRaisesRegex(TypeError, "> can not be applied to", lambda: psser > psser)
            else:
                self.assert_eq(pser > pser, psser > psser)

    def test_ge(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            if isinstance(psser.spark.data_type, DecimalType):
                self.assertRaisesRegex(
                    TypeError, ">= can not be applied to", lambda: psser >= psser
                )
            else:
                self.assert_eq(pser >= pser, psser >= psser)


@unittest.skipIf(not extension_dtypes_available, "pandas extension dtypes are not available")
class IntegralExtensionOpsTest(OpsTestBase):
    @property
    def intergral_extension_psers(self):
        return [pd.Series([1, 2, 3, None], dtype=dtype) for dtype in self.integral_extension_dtypes]

    @property
    def intergral_extension_pssers(self):
        return [ps.from_pandas(pser) for pser in self.intergral_extension_psers]

    @property
    def intergral_extension_pser_psser_pairs(self):
        return zip(self.intergral_extension_psers, self.intergral_extension_pssers)

    def test_from_to_pandas(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            self.check_extension(pser, psser._to_pandas())
            self.check_extension(ps.from_pandas(pser), psser)

    def test_isnull(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            self.assert_eq(pser.isnull(), psser.isnull())

    def test_astype(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            for dtype in self.extension_dtypes:
                if dtype in self.string_extension_dtype:
                    self.check_extension(pser.astype(dtype), psser.astype(dtype))
                else:
                    self.check_extension(pser.astype(dtype), psser.astype(dtype))
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            self.assert_eq(pser.astype(float), psser.astype(float))
            self.assert_eq(pser.astype(np.float32), psser.astype(np.float32))
            with ps.option_context("compute.eager_check", True):
                self.assertRaisesRegex(
                    ValueError,
                    "Cannot convert integrals with missing values to bool",
                    lambda: psser.astype(bool),
                )
                self.assertRaisesRegex(
                    ValueError,
                    "Cannot convert integrals with missing values to integer",
                    lambda: psser.astype(int),
                )
                self.assertRaisesRegex(
                    ValueError,
                    "Cannot convert integrals with missing values to integer",
                    lambda: psser.astype(np.int32),
                )
            with ps.option_context("compute.eager_check", False):
                psser.astype(bool)
                psser.astype(int)
                psser.astype(np.int32)

    def test_neg(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            self.check_extension(-pser, -psser)

    def test_abs(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            self.check_extension(abs(pser), abs(psser))

    def test_invert(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            self.check_extension(~pser, ~psser)

    def test_eq(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.intergral_extension_pser_psser_pairs:
                self.check_extension(pser == pser, (psser == psser).sort_index())

    def test_ne(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.intergral_extension_pser_psser_pairs:
                self.check_extension(pser != pser, (psser != psser).sort_index())

    def test_lt(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.intergral_extension_pser_psser_pairs:
                self.check_extension(pser < pser, (psser < psser).sort_index())

    def test_le(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.intergral_extension_pser_psser_pairs:
                self.check_extension(pser <= pser, (psser <= psser).sort_index())

    def test_gt(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.intergral_extension_pser_psser_pairs:
                self.check_extension(pser > pser, (psser > psser).sort_index())

    def test_ge(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.intergral_extension_pser_psser_pairs:
                self.check_extension(pser >= pser, (psser >= psser).sort_index())

    def test_xor(self):
        for psser in self.intergral_extension_pssers:
            self.assertRaisesRegex(
                TypeError,
                "XOR can not be applied to given types.",
                lambda: psser ^ 1,
            )
            self.assertRaisesRegex(
                TypeError,
                "XOR can not be applied to given types.",
                lambda: psser ^ psser,
            )
            self.assertRaisesRegex(
                TypeError,
                "XOR can not be applied to given types.",
                lambda: psser ^ False,
            )

    def test_rxor(self):
        for psser in self.intergral_extension_pssers:
            self.assertRaisesRegex(
                TypeError,
                "XOR can not be applied to given types.",
                lambda: 1 ^ psser,
            )
            self.assertRaisesRegex(
                TypeError,
                "XOR can not be applied to given types.",
                lambda: False ^ psser,
            )


@unittest.skipIf(
    not extension_float_dtypes_available, "pandas extension float dtypes are not available"
)
class FractionalExtensionOpsTest(OpsTestBase):
    @property
    def fractional_extension_psers(self):
        return [
            pd.Series([0.1, 0.2, 0.3, None], dtype=dtype)
            for dtype in self.fractional_extension_dtypes
        ]

    @property
    def fractional_extension_pssers(self):
        return [ps.from_pandas(pser) for pser in self.fractional_extension_psers]

    @property
    def fractional_extension_pser_psser_pairs(self):
        return zip(self.fractional_extension_psers, self.fractional_extension_pssers)

    def test_from_to_pandas(self):
        for pser, psser in self.fractional_extension_pser_psser_pairs:
            self.check_extension(pser, psser._to_pandas())
            self.check_extension(ps.from_pandas(pser), psser)

    def test_isnull(self):
        for pser, psser in self.fractional_extension_pser_psser_pairs:
            self.assert_eq(pser.isnull(), psser.isnull())

    def test_astype(self):
        for pser, psser in self.fractional_extension_pser_psser_pairs:
            for dtype in self.extension_dtypes:
                self.check_extension(pser.astype(dtype), psser.astype(dtype))
        for pser, psser in self.fractional_extension_pser_psser_pairs:
            self.assert_eq(pser.astype(float), psser.astype(float))
            self.assert_eq(pser.astype("category"), psser.astype("category"))
            self.assert_eq(pser.astype(np.float32), psser.astype(np.float32))
            with ps.option_context("compute.eager_check", True):
                self.assertRaisesRegex(
                    ValueError,
                    "Cannot convert fractions with missing values to bool",
                    lambda: psser.astype(bool),
                )
                self.assertRaisesRegex(
                    ValueError,
                    "Cannot convert fractions with missing values to integer",
                    lambda: psser.astype(int),
                )
                self.assertRaisesRegex(
                    ValueError,
                    "Cannot convert fractions with missing values to integer",
                    lambda: psser.astype(np.int32),
                )
            with ps.option_context("compute.eager_check", False):
                psser.astype(bool)
                psser.astype(int)
                psser.astype(np.int32)

    def test_neg(self):
        # pandas raises "TypeError: bad operand type for unary -: 'FloatingArray'"
        for dtype in self.fractional_extension_dtypes:
            self.assert_eq(
                ps.Series([-0.1, -0.2, -0.3, None], dtype=dtype),
                -ps.Series([0.1, 0.2, 0.3, None], dtype=dtype),
            )

    def test_abs(self):
        for pser, psser in self.fractional_extension_pser_psser_pairs:
            self.check_extension(abs(pser), abs(psser))

    def test_invert(self):
        for psser in self.fractional_extension_pssers:
            self.assertRaises(TypeError, lambda: ~psser)

    def test_eq(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.fractional_extension_pser_psser_pairs:
                self.check_extension(pser == pser, (psser == psser).sort_index())

    def test_ne(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.fractional_extension_pser_psser_pairs:
                self.check_extension(pser != pser, (psser != psser).sort_index())

    def test_lt(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.fractional_extension_pser_psser_pairs:
                self.check_extension(pser < pser, (psser < psser).sort_index())

    def test_le(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.fractional_extension_pser_psser_pairs:
                self.check_extension(pser <= pser, (psser <= psser).sort_index())

    def test_gt(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.fractional_extension_pser_psser_pairs:
                self.check_extension(pser > pser, (psser > psser).sort_index())

    def test_ge(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.fractional_extension_pser_psser_pairs:
                self.check_extension(pser >= pser, (psser >= psser).sort_index())


class NumOpsTests(
    NumOpsTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.data_type_ops.test_num_ops import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
