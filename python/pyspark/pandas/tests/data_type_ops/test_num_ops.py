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
import numpy as np

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.utils import is_ansi_mode_test
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase
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
            ignore_null = self.ignore_null(col)
            self.assert_eq(pser, psser._to_pandas(), check_exact=False, ignore_null=ignore_null)
            self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            self.assert_eq(pdf[col].isnull(), psdf[col].isnull())

    def test_neg(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            ignore_null = self.ignore_null(col)
            self.assert_eq(-pdf[col], -psdf[col], check_exact=False, ignore_null=ignore_null)

    def test_abs(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            ignore_null = self.ignore_null(col)
            self.assert_eq(
                abs(pdf[col]), abs(psdf[col]), check_exact=False, ignore_null=ignore_null
            )

    def test_invert(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            if isinstance(psser.spark.data_type, IntegralType):
                self.assert_eq(~pser, ~psser)
            else:
                self.assertRaises(TypeError, lambda: ~psser)

    def test_comparison_dtype_compatibility(self):
        pdf = pd.DataFrame(
            {"int": [1, 2], "bool": [True, False], "float": [0.1, 0.2], "str": ["1", "2"]}
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf["int"] == pdf["bool"], psdf["int"] == psdf["bool"])
        self.assert_eq(pdf["bool"] == pdf["int"], psdf["bool"] == psdf["int"])
        self.assert_eq(pdf["int"] == pdf["float"], psdf["int"] == psdf["float"])
        if is_ansi_mode_test:  # TODO: match non-ansi behavior with pandas
            self.assert_eq(pdf["int"] == pdf["str"], psdf["int"] == psdf["str"])
        self.assert_eq(pdf["float"] == pdf["bool"], psdf["float"] == psdf["bool"])
        self.assert_eq(pdf["str"] == "x", psdf["str"] == "x")

    def test_eq(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            self.assert_eq(pdf[col] == pdf[col], psdf[col] == psdf[col])
            self.assert_eq(pdf[col] == np.nan, psdf[col] == np.nan)

    def test_ne(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            self.assert_eq(pdf[col] != pdf[col], psdf[col] != psdf[col])
            self.assert_eq(pdf[col] != np.nan, psdf[col] != np.nan)

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


class NumOpsTests(
    NumOpsTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
