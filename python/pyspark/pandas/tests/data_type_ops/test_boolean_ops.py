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
import unittest
from distutils.version import LooseVersion

import pandas as pd
import numpy as np
from pandas.api.types import CategoricalDtype

from pyspark import pandas as ps
from pyspark.pandas import option_context
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase
from pyspark.pandas.typedef.typehints import (
    extension_float_dtypes_available,
    extension_object_dtypes_available,
)


class BooleanOpsTest(OpsTestBase):
    @property
    def bool_pdf(self):
        return pd.DataFrame({"this": [True, False, True], "that": [False, True, True]})

    @property
    def bool_psdf(self):
        return ps.from_pandas(self.bool_pdf)

    def test_add(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        self.assert_eq(b_pser + 1, b_psser + 1)
        self.assert_eq(b_pser + 0.1, b_psser + 0.1)
        self.assert_eq(b_pser + b_pser.astype(int), b_psser + b_psser.astype(int))
        self.assert_eq(b_pser + b_pser, b_psser + b_psser)
        self.assert_eq(b_pser + True, b_psser + True)
        self.assert_eq(b_pser + False, b_psser + False)

        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            self.assert_eq(b_pser + pser, b_psser + psser)
        for col in self.non_numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            if col == "bool":
                self.assert_eq(b_pser + pser, b_psser + psser)
            else:
                self.assertRaises(TypeError, lambda: b_psser + psser)

    def test_sub(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        self.assert_eq(b_pser - 1, b_psser - 1)
        self.assert_eq(b_pser - 0.1, b_psser - 0.1)
        self.assert_eq(b_pser - b_pser.astype(int), b_psser - b_psser.astype(int))
        self.assertRaises(TypeError, lambda: b_psser - b_psser)
        self.assertRaises(TypeError, lambda: b_psser - True)

        for col in self.numeric_df_cols:
            self.assert_eq(b_pser - pdf[col], b_psser - psdf[col])

        for col in self.non_numeric_df_cols:
            self.assertRaises(TypeError, lambda: b_psser - psdf[col])

    def test_mul(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        self.assert_eq(b_pser * 1, b_psser * 1)
        self.assert_eq(b_pser * 0.1, b_psser * 0.1)
        self.assert_eq(b_pser * b_pser.astype(int), b_psser * b_psser.astype(int))
        self.assert_eq(b_pser * b_pser, b_psser * b_psser)
        self.assert_eq(b_pser * True, b_psser * True)
        self.assert_eq(b_pser * False, b_psser * False)

        for col in self.numeric_df_cols:
            self.assert_eq(b_pser * pdf[col], b_psser * psdf[col])

        for col in self.non_numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            if col == "bool":
                self.assert_eq(b_pser * pser, b_psser * psser)
            else:
                self.assertRaises(TypeError, lambda: b_psser * psser)

    def test_truediv(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        self.assert_eq(b_pser / 1, b_psser / 1)
        self.assert_eq(b_pser / 0.1, b_psser / 0.1)
        self.assert_eq(b_pser / b_pser.astype(int), b_psser / b_psser.astype(int))
        self.assertRaises(TypeError, lambda: b_psser / b_psser)
        self.assertRaises(TypeError, lambda: b_psser / True)

        self.assert_eq(b_pser / pdf["float"], b_psser / psdf["float"])

        for col in self.non_numeric_df_cols:
            self.assertRaises(TypeError, lambda: b_psser / psdf[col])

    def test_floordiv(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]

        # float is always returned in pandas-on-Spark
        self.assert_eq((b_pser // 1).astype("float"), b_psser // 1)

        # in pandas, 1 // 0.1 = 9.0; in pandas-on-Spark, 1 // 0.1 = 10.0
        # self.assert_eq(b_pser // 0.1, b_psser // 0.1)

        self.assert_eq(b_pser // b_pser.astype(int), b_psser // b_psser.astype(int))
        self.assertRaises(TypeError, lambda: b_psser // b_psser)
        self.assertRaises(TypeError, lambda: b_psser // True)

        self.assert_eq(b_pser // pdf["float"], b_psser // psdf["float"])

        for col in self.non_numeric_df_cols:
            self.assertRaises(TypeError, lambda: b_psser // psdf[col])

    def test_mod(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        self.assert_eq(b_pser % 1, b_psser % 1)
        self.assert_eq(b_pser % 0.1, b_psser % 0.1)
        self.assert_eq(b_pser % b_pser.astype(float), b_psser % b_psser.astype(float))
        self.assertRaises(TypeError, lambda: b_psser % b_psser)
        self.assertRaises(TypeError, lambda: b_psser % True)

        for col in self.numeric_df_cols:
            self.assert_eq(b_pser % pdf[col], b_psser % psdf[col])

        for col in self.non_numeric_df_cols:
            self.assertRaises(TypeError, lambda: b_psser % psdf[col])

    def test_pow(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        # float is always returned in pandas-on-Spark
        self.assert_eq((b_pser**1).astype("float"), b_psser**1)
        self.assert_eq(b_pser**0.1, b_psser**0.1)
        self.assert_eq(b_pser ** b_pser.astype(float), b_psser ** b_psser.astype(float))
        self.assertRaises(TypeError, lambda: b_psser**b_psser)
        self.assertRaises(TypeError, lambda: b_psser**True)

        self.assert_eq(b_pser % pdf["float"], b_psser % psdf["float"])
        for col in self.non_numeric_df_cols:
            self.assertRaises(TypeError, lambda: b_psser % psdf[col])

    def test_radd(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        self.assert_eq(1 + b_pser, 1 + b_psser)
        self.assert_eq(0.1 + b_pser, 0.1 + b_psser)
        self.assert_eq(True + b_pser, True + b_psser)
        self.assert_eq(False + b_pser, False + b_psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) + b_psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) + b_psser)

    def test_rsub(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        self.assert_eq(1 - b_pser, 1 - b_psser)
        self.assert_eq(0.1 - b_pser, 0.1 - b_psser)
        self.assertRaises(TypeError, lambda: "x" - b_psser)
        self.assertRaises(TypeError, lambda: True - b_psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) - b_psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) - b_psser)

    def test_rmul(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        self.assert_eq(1 * b_pser, 1 * b_psser)
        self.assert_eq(0.1 * b_pser, 0.1 * b_psser)
        self.assertRaises(TypeError, lambda: "x" * b_psser)
        self.assert_eq(True * b_pser, True * b_psser)
        self.assert_eq(False * b_pser, False * b_psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) * b_psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) * b_psser)

    def test_rtruediv(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        self.assert_eq(1 / b_pser, 1 / b_psser)
        self.assert_eq(0.1 / b_pser, 0.1 / b_psser)
        self.assertRaises(TypeError, lambda: "x" / b_psser)
        self.assertRaises(TypeError, lambda: True / b_psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) / b_psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) / b_psser)

    def test_rfloordiv(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        self.assert_eq(1 // b_pser, 1 // b_psser)
        self.assert_eq(0.1 // b_pser, 0.1 // b_psser)
        self.assertRaises(TypeError, lambda: "x" // b_psser)
        self.assertRaises(TypeError, lambda: True // b_psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) // b_psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) // b_psser)

    def test_rpow(self):
        pdf, psdf = self.pdf, self.psdf

        b_pser, b_psser = pdf["bool"], psdf["bool"]
        # float is returned always in pandas-on-Spark
        self.assert_eq((1**b_pser).astype(float), 1**b_psser)
        self.assert_eq(0.1**b_pser, 0.1**b_psser)
        self.assertRaises(TypeError, lambda: "x" ** b_psser)
        self.assertRaises(TypeError, lambda: True**b_psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) ** b_psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) ** b_psser)

    def test_rmod(self):
        psdf = self.psdf

        b_psser = psdf["bool"]
        # 1 % False is 0.0 in pandas
        self.assert_eq(pd.Series([0, 0, None], dtype=float, name="bool"), 1 % b_psser)
        # 0.1 / True is 0.1 in pandas
        self.assert_eq(
            pd.Series([0.10000000000000009, 0.10000000000000009, None], dtype=float, name="bool"),
            0.1 % b_psser,
        )
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) % b_psser)
        self.assertRaises(TypeError, lambda: True % b_psser)

    def test_and(self):
        pdf, psdf = self.bool_pdf, self.bool_psdf
        pser, other_pser = pdf["this"], pdf["that"]
        psser, other_psser = psdf["this"], psdf["that"]

        self.assert_eq(pser & True, psser & True)
        self.assert_eq(pser & False, psser & False)
        self.assert_eq(pser & pser, psser & psser)
        self.assert_eq(pser & other_pser, psser & other_psser)
        self.assert_eq(other_pser & pser, other_psser & psser)

    def test_rand(self):
        pser, psser = self.pdf["bool"], self.psdf["bool"]
        self.assert_eq(True & pser, True & psser)
        self.assert_eq(False & pser, False & psser)

    def test_or(self):
        pdf, psdf = self.bool_pdf, self.bool_psdf
        pser, other_pser = pdf["this"], pdf["that"]
        psser, other_psser = psdf["this"], psdf["that"]

        self.assert_eq(pser | True, psser | True)
        self.assert_eq(pser | False, psser | False)
        self.assert_eq(pser | pser, psser | psser)
        self.assert_eq(True | pser, True | psser)
        self.assert_eq(False | pser, False | psser)

        self.assert_eq(pser | other_pser, psser | other_psser)
        self.assert_eq(other_pser | pser, other_psser | psser)

    def test_ror(self):
        pser, psser = self.pdf["bool"], self.psdf["bool"]
        self.assert_eq(True | pser, True | psser)
        self.assert_eq(False | pser, False | psser)

    def test_xor(self):
        pdf, psdf = self.bool_pdf, self.bool_psdf
        pser, other_pser = pdf["this"], pdf["that"]
        psser, other_psser = psdf["this"], psdf["that"]

        self.assert_eq(pser ^ other_pser, psser ^ other_psser)
        self.assert_eq(pser ^ True, psser ^ True)
        self.assert_eq(pser ^ False, psser ^ False)
        self.assert_eq(pser ^ 2, psser ^ 2)
        self.assert_eq(pser ^ 99, psser ^ 99)

        with self.assertRaisesRegex(TypeError, "XOR can not be applied to given types."):
            psser ^ "a"

        with option_context("compute.ops_on_diff_frames", True):
            pser, other_pser = self.pdf["bool"], self.integral_pdf["this"]
            psser, other_psser = self.psdf["bool"], self.integral_psdf["this"]

            self.assert_eq(pser ^ other_pser, psser ^ other_psser)

    def test_rxor(self):
        pser, psser = self.pdf["bool"], self.psdf["bool"]
        self.assert_eq(True ^ pser, True ^ psser)
        self.assert_eq(False ^ pser, False ^ psser)
        self.assert_eq(1 ^ pser, 1 ^ psser)

    def test_isnull(self):
        self.assert_eq(self.pdf["bool"].isnull(), self.psdf["bool"].isnull())

    def test_astype(self):
        pser, psser = self.pdf["bool"], self.psdf["bool"]
        self.assert_eq(pser.astype(int), psser.astype(int))
        self.assert_eq(pser.astype(float), psser.astype(float))
        self.assert_eq(pser.astype(np.float32), psser.astype(np.float32))
        self.assert_eq(pser.astype(np.int32), psser.astype(np.int32))
        self.assert_eq(pser.astype(np.int16), psser.astype(np.int16))
        self.assert_eq(pser.astype(np.int8), psser.astype(np.int8))
        self.assert_eq(pser.astype(str), psser.astype(str))
        self.assert_eq(pser.astype(bool), psser.astype(bool))
        self.assert_eq(pser.astype("category"), psser.astype("category"))
        cat_type = CategoricalDtype(categories=[False, True])
        self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))

    def test_neg(self):
        self.assert_eq(-self.pdf["bool"], -self.psdf["bool"])

    def test_abs(self):
        self.assert_eq(abs(self.pdf["bool"]), abs(self.psdf["bool"]))

    def test_invert(self):
        self.assert_eq(~self.pdf["bool"], ~self.psdf["bool"])

    def test_eq(self):
        pdf, psdf = self.bool_pdf, self.bool_psdf
        pser, other_pser = pdf["this"], pdf["that"]
        psser, other_psser = psdf["this"], psdf["that"]
        self.assert_eq(pser == other_pser, psser == other_psser)
        self.assert_eq(pser == pser, psser == psser)

    def test_ne(self):
        pdf, psdf = self.bool_pdf, self.bool_psdf
        pser, other_pser = pdf["this"], pdf["that"]
        psser, other_psser = psdf["this"], psdf["that"]
        self.assert_eq(pser != other_pser, psser != other_psser)
        self.assert_eq(pser != pser, psser != psser)

    def test_lt(self):
        pdf, psdf = self.bool_pdf, self.bool_psdf
        pser, other_pser = pdf["this"], pdf["that"]
        psser, other_psser = psdf["this"], psdf["that"]
        self.assert_eq(pser < other_pser, psser < other_psser)
        self.assert_eq(pser < pser, psser < psser)

    def test_le(self):
        pdf, psdf = self.bool_pdf, self.bool_psdf
        pser, other_pser = pdf["this"], pdf["that"]
        psser, other_psser = psdf["this"], psdf["that"]
        self.assert_eq(pser <= other_pser, psser <= other_psser)
        self.assert_eq(pser <= pser, psser <= psser)

    def test_gt(self):
        pdf, psdf = self.bool_pdf, self.bool_psdf
        pser, other_pser = pdf["this"], pdf["that"]
        psser, other_psser = psdf["this"], psdf["that"]
        self.assert_eq(pser > other_pser, psser > other_psser)
        self.assert_eq(pser > pser, psser > psser)

    def test_ge(self):
        pdf, psdf = self.bool_pdf, self.bool_psdf
        pser, other_pser = pdf["this"], pdf["that"]
        psser, other_psser = psdf["this"], psdf["that"]
        self.assert_eq(pser >= other_pser, psser >= other_psser)
        self.assert_eq(pser >= pser, psser >= psser)


@unittest.skipIf(
    not extension_object_dtypes_available, "pandas extension object dtypes are not available"
)
class BooleanExtensionOpsTest(OpsTestBase):
    @property
    def boolean_pdf(self):
        return pd.DataFrame(
            {"this": [True, False, None], "that": [False, None, True]}, dtype="boolean"
        )

    @property
    def boolean_psdf(self):
        return ps.from_pandas(self.boolean_pdf)

    @property
    def boolean_numeric_pdf(self):
        return pd.concat([self.boolean_pdf, self.numeric_pdf], axis=1)

    @property
    def boolean_numeric_psdf(self):
        return ps.from_pandas(self.boolean_numeric_pdf)

    @property
    def boolean_non_numeric_pdf(self):
        return pd.concat([self.boolean_pdf, self.non_numeric_pdf], axis=1)

    @property
    def boolean_non_numeric_psdf(self):
        return ps.from_pandas(self.boolean_non_numeric_pdf)

    def test_add(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]
        self.check_extension(pser + 1, psser + 1)
        if extension_float_dtypes_available:
            self.check_extension(pser + 0.1, psser + 0.1)
        else:
            self.assert_eq(pser + 0.1, psser + 0.1)

        # In pandas, NA | True is NA, whereas NA | True is True in pandas-on-Spark
        self.check_extension(
            pd.Series([True, True, True], dtype="boolean", name=psser.name), psser + True
        )
        self.check_extension(pser + False, psser + False)
        self.check_extension(pser + pser, psser + psser)

        pdf, psdf = self.boolean_numeric_pdf, self.boolean_numeric_psdf
        for col in self.numeric_df_cols:
            self.assert_eq(pdf["this"] + pdf[col], psdf["this"] + psdf[col], almost=True)

        pdf, psdf = self.boolean_non_numeric_pdf, self.boolean_non_numeric_psdf
        for col in self.non_numeric_df_cols:
            if col == "bool":
                self.check_extension(pdf["this"] + pdf[col], psdf["this"] + psdf[col])
            else:
                self.assertRaises(TypeError, lambda: psdf["this"] + psdf[col])

    def test_sub(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]
        self.check_extension(pser - 1, psser - 1)
        if extension_float_dtypes_available:
            self.check_extension(pser - 0.1, psser - 0.1)
        else:
            self.assert_eq(pser - 0.1, psser - 0.1)
        self.assertRaises(TypeError, lambda: psser - psser)
        self.assertRaises(TypeError, lambda: psser - True)

        pdf, psdf = self.boolean_numeric_pdf, self.boolean_numeric_psdf
        for col in self.numeric_df_cols:
            self.assert_eq(pdf["this"] - pdf[col], psdf["this"] - psdf[col], almost=True)

        pdf, psdf = self.boolean_non_numeric_pdf, self.boolean_non_numeric_psdf
        for col in self.non_numeric_df_cols:
            self.assertRaises(TypeError, lambda: psdf["this"] - psdf[col])

    def test_mul(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]
        self.check_extension(pser * 1, psser * 1)
        if extension_float_dtypes_available:
            self.check_extension(pser * 0.1, psser * 0.1)
        else:
            self.assert_eq(pser * 0.1, psser * 0.1)

        # In pandas, NA & False is NA, whereas NA & False is False in pandas-on-Spark
        self.check_extension(pser * True, psser * True)
        self.check_extension(
            pd.Series([False, False, False], dtype="boolean", name=psser.name), psser * False
        )
        self.check_extension(pser * pser, psser * psser)

        pdf, psdf = self.boolean_numeric_pdf, self.boolean_numeric_psdf
        for col in self.numeric_df_cols:
            self.assert_eq(pdf["this"] * pdf[col], psdf["this"] * psdf[col], almost=True)

        pdf, psdf = self.boolean_non_numeric_pdf, self.boolean_non_numeric_psdf
        for col in self.non_numeric_df_cols:
            if col == "bool":
                self.check_extension(pdf["that"] * pdf[col], psdf["that"] * psdf[col])
            else:
                self.assertRaises(TypeError, lambda: psdf["this"] * psdf[col])

    def test_truediv(self):
        pdf, psdf = self.boolean_numeric_pdf, self.boolean_numeric_psdf
        pser, psser = pdf["this"], psdf["this"]
        if extension_float_dtypes_available:
            self.check_extension(pser / 1, psser / 1)
            self.check_extension(pser / 0.1, psser / 0.1)
        else:
            self.assert_eq(pser / 1, psser / 1)
            self.assert_eq(pser / 0.1, psser / 0.1)
        self.assertRaises(TypeError, lambda: psser / psser)
        self.assertRaises(TypeError, lambda: psser / True)

        self.assert_eq(
            pser / pdf["float"],
            psser / psdf["float"],
            almost=True,
        )
        psdf = self.boolean_non_numeric_psdf
        for col in self.non_numeric_df_cols:
            self.assertRaises(TypeError, lambda: psdf["this"] / psdf[col])

    def test_floordiv(self):
        pdf, psdf = self.boolean_numeric_pdf, self.boolean_numeric_psdf
        pser, psser = pdf["this"], psdf["this"]

        # float is always returned in pandas-on-Spark
        if extension_float_dtypes_available:
            self.check_extension((pser // 1).astype("Float64"), psser // 1)
        else:
            self.assert_eq((pser // 1).astype("float"), psser // 1)

        # in pandas, 1 // 0.1 = 9.0; in pandas-on-Spark, 1 // 0.1 = 10.0
        # self.assert_eq(pser // 0.1, psser // 0.1)

        self.assertRaises(TypeError, lambda: psser // psser)
        self.assertRaises(TypeError, lambda: psser // True)

        self.assert_eq(
            pser // pdf["float"],
            psser // psdf["float"],
            almost=True,
        )
        psdf = self.boolean_non_numeric_psdf
        for col in self.non_numeric_df_cols:
            self.assertRaises(TypeError, lambda: psdf["this"] // psdf[col])

    def test_mod(self):
        pdf, psdf = self.boolean_numeric_pdf, self.boolean_numeric_psdf
        pser, psser = pdf["this"], psdf["this"]
        self.check_extension(pser % 1, psser % 1)
        if extension_float_dtypes_available:
            self.check_extension(pser % 0.1, psser % 0.1)
        else:
            self.assert_eq(pser % 0.1, psser % 0.1)
        self.assertRaises(TypeError, lambda: psser % psser)
        self.assertRaises(TypeError, lambda: psser % True)

        pdf, psdf = self.boolean_numeric_pdf, self.boolean_numeric_psdf
        for col in self.numeric_df_cols:
            self.assert_eq(pdf["this"] % pdf[col], psdf["this"] % psdf[col], almost=True)

        psdf = self.boolean_non_numeric_psdf
        for col in self.non_numeric_df_cols:
            self.assertRaises(TypeError, lambda: psdf["this"] % psdf[col])

    def test_pow(self):
        pdf, psdf = self.boolean_numeric_pdf, self.boolean_numeric_psdf
        pser, psser = pdf["this"], psdf["this"]
        # float is always returned in pandas-on-Spark
        if extension_float_dtypes_available:
            self.check_extension((pser**1).astype("Float64"), psser**1)
            self.check_extension((pser**0.1).astype("Float64"), psser**0.1)
            self.check_extension(
                (pser ** pser.astype(float)).astype("Float64"), psser ** psser.astype(float)
            )
        else:
            self.assert_eq((pser**1).astype("float"), psser**1)
            self.assert_eq((pser**0.1).astype("float"), psser**0.1)
            self.assert_eq(
                (pser ** pser.astype(float)).astype("float"), psser ** psser.astype(float)
            )
        self.assertRaises(TypeError, lambda: psser**psser)
        self.assertRaises(TypeError, lambda: psser**True)

        self.assert_eq(
            pser ** pdf["float"],
            psser ** psdf["float"],
            almost=True,
        )
        psdf = self.boolean_non_numeric_psdf
        for col in self.non_numeric_df_cols:
            self.assertRaises(TypeError, lambda: psdf["this"] % psdf[col])

    def test_radd(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]

        self.check_extension(1 + pser, 1 + psser)
        if extension_float_dtypes_available:
            self.check_extension(0.1 + pser, 0.1 + psser)
        else:
            self.assert_eq(0.1 + pser, 0.1 + psser)
        self.assertRaises(TypeError, lambda: "x" + psser)

        # In pandas, NA | True is NA, whereas NA | True is True in pandas-on-Spark
        self.check_extension(
            ps.Series([True, True, True], dtype="boolean", name=psser.name), True + psser
        )
        self.check_extension(False + pser, False + psser)

        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) + psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) + psser)

    def test_rsub(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]
        self.check_extension(1 - pser, 1 - psser)
        if extension_float_dtypes_available:
            self.check_extension(0.1 - pser, 0.1 - psser)
        else:
            self.assert_eq(0.1 - pser, 0.1 - psser)
        self.assertRaises(TypeError, lambda: "x" - psser)
        self.assertRaises(TypeError, lambda: True - psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) - psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) - psser)

    def test_rmul(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]
        self.check_extension(1 * pser, 1 * psser)
        if extension_float_dtypes_available:
            self.check_extension(0.1 * pser, 0.1 * psser)
        else:
            self.assert_eq(0.1 * pser, 0.1 * psser)
        self.assertRaises(TypeError, lambda: "x" * psser)

        # In pandas, NA & False is NA, whereas NA & False is False in pandas-on-Spark
        self.check_extension(True * pser, True * psser)
        self.check_extension(
            pd.Series([False, False, False], dtype="boolean", name=psser.name), False * psser
        )

        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) * psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) * psser)

    def test_rtruediv(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]
        if extension_float_dtypes_available:
            self.check_extension(1 / pser, 1 / psser)
            self.check_extension(0.1 / pser, 0.1 / psser)
        else:
            self.assert_eq(1 / pser, 1 / psser)
            self.assert_eq(0.1 / pser, 0.1 / psser)
        self.assertRaises(TypeError, lambda: "x" / psser)
        self.assertRaises(TypeError, lambda: True / psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) / psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) / psser)

    def test_rfloordiv(self):
        psser = self.boolean_psdf["this"]
        self.assert_eq(
            pd.Series([1.0, np.inf, np.nan], name=psser.name), (1 // psser).astype(float)
        )
        self.assert_eq(
            pd.Series([0.0, np.inf, np.nan], name=psser.name), (0.1 // psser).astype(float)
        )
        self.assertRaises(TypeError, lambda: "x" // psser)
        self.assertRaises(TypeError, lambda: True // psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) // psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) // psser)

    def test_rpow(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]
        if extension_float_dtypes_available:
            self.check_extension(pd.Series([1, 1, 1], dtype="Float64", name=psser.name), 1**psser)
            self.check_extension((0.1**pser).astype("Float64"), 0.1**psser)
        else:
            self.assert_eq(pd.Series([1, 1, 1], dtype="float", name=psser.name), 1**psser)
            self.assert_eq((0.1**pser).astype("float"), 0.1**psser)
        self.assertRaises(TypeError, lambda: "x" ** psser)
        self.assertRaises(TypeError, lambda: True**psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) ** psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) ** psser)

    def test_rmod(self):
        psser = self.boolean_psdf["this"]
        self.check_extension(
            ps.Series([0, np.nan, np.nan], dtype="Int64", name=psser.name), 1 % psser
        )
        if extension_float_dtypes_available:
            self.check_extension(
                pd.Series([0.10000000000000009, np.nan, np.nan], dtype="Float64", name=psser.name),
                0.1 % psser,
            )
        else:
            self.assert_eq(
                pd.Series([0.10000000000000009, np.nan, np.nan], dtype="float", name=psser.name),
                0.1 % psser,
            )
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) % psser)
        self.assertRaises(TypeError, lambda: True % psser)

    def test_and(self):
        pdf, psdf = self.boolean_pdf, self.boolean_psdf
        pser, psser = pdf["this"], psdf["this"]
        other_pser, other_psser = pdf["that"], psdf["that"]
        self.check_extension(pser & True, psser & True)
        self.check_extension(pser & False, psser & False)
        self.check_extension(pser & pser, psser & psser)

        self.check_extension(pser & other_pser, psser & other_psser)
        self.check_extension(other_pser & pser, other_psser & psser)

    def test_rand(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]
        self.check_extension(True & pser, True & psser)
        self.check_extension(False & pser, False & psser)

    def test_or(self):
        pdf, psdf = self.boolean_pdf, self.boolean_psdf
        pser, psser = pdf["this"], psdf["this"]
        other_pser, other_psser = pdf["that"], psdf["that"]
        self.check_extension(pser | True, psser | True)
        self.check_extension(pser | False, psser | False)
        self.check_extension(pser | pser, psser | psser)

        self.check_extension(pser | other_pser, psser | other_psser)
        self.check_extension(other_pser | pser, other_psser | psser)

    def test_ror(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]
        self.check_extension(True | pser, True | psser)
        self.check_extension(False | pser, False | psser)

    def test_xor(self):
        pdf, psdf = self.boolean_pdf, self.boolean_psdf
        pser, psser = pdf["this"], psdf["this"]
        other_pser, other_psser = pdf["that"], psdf["that"]
        self.check_extension(pser ^ True, psser ^ True)
        self.check_extension(pser ^ False, psser ^ False)
        self.check_extension(pser ^ pser, psser ^ psser)

        self.check_extension(pser ^ other_pser, psser ^ other_psser)
        self.check_extension(other_pser ^ pser, other_psser ^ psser)
        with self.assertRaisesRegex(TypeError, "XOR can not be applied to given types."):
            psser ^ 2

    def test_rxor(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]
        self.check_extension(True | pser, True | psser)
        self.check_extension(False | pser, False | psser)
        with self.assertRaisesRegex(TypeError, "XOR can not be applied to given types."):
            1 ^ psser

    def test_from_to_pandas(self):
        data = [True, True, False, None]
        pser = pd.Series(data, dtype="boolean")
        psser = ps.Series(data, dtype="boolean")
        self.check_extension(pser, psser._to_pandas())
        self.check_extension(ps.from_pandas(pser), psser)

    def test_isnull(self):
        self.assert_eq(self.boolean_pdf["this"].isnull(), self.boolean_psdf["this"].isnull())

    def test_astype(self):
        pser, psser = self.boolean_pdf["this"], self.boolean_psdf["this"]

        self.assert_eq(pser.astype(str).tolist(), psser.astype(str).tolist())

        self.assert_eq(pser.astype("category"), psser.astype("category"))
        cat_type = CategoricalDtype(categories=[False, True])
        self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))
        for dtype in self.extension_dtypes:
            if dtype in self.fractional_extension_dtypes:
                # A pandas boolean extension series cannot be casted to fractional extension dtypes
                self.assert_eq([1.0, 0.0, np.nan], psser.astype(dtype).tolist())
            elif dtype in self.string_extension_dtype:
                if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
                    # Limit pandas version due to https://github.com/pandas-dev/pandas/issues/31204
                    self.check_extension(pser.astype(dtype), psser.astype(dtype))
            else:
                self.check_extension(pser.astype(dtype), psser.astype(dtype))

    def test_neg(self):
        self.assertRaises(TypeError, lambda: -self.boolean_psdf["this"])

    def test_abs(self):
        self.assertRaises(TypeError, lambda: abs(self.boolean_psdf["this"]))

    def test_invert(self):
        self.assertRaises(TypeError, lambda: ~self.boolean_psdf["this"])

    def test_eq(self):
        pdf, psdf = self.boolean_pdf, self.boolean_psdf
        pser, psser = pdf["this"], psdf["this"]
        other_pser, other_psser = pdf["that"], psdf["that"]
        self.check_extension(pser == other_pser, psser == other_psser)
        self.check_extension(pser == pser, psser == psser)

    def test_ne(self):
        pdf, psdf = self.boolean_pdf, self.boolean_psdf
        pser, psser = pdf["this"], psdf["this"]
        other_pser, other_psser = pdf["that"], psdf["that"]
        self.check_extension(pser != other_pser, psser != other_psser)
        self.check_extension(pser != pser, psser != psser)

    def test_lt(self):
        pdf, psdf = self.boolean_pdf, self.boolean_psdf
        pser, psser = pdf["this"], psdf["this"]
        other_pser, other_psser = pdf["that"], psdf["that"]
        self.check_extension(pser < other_pser, psser < other_psser)
        self.check_extension(pser < pser, psser < psser)

    def test_le(self):
        pdf, psdf = self.boolean_pdf, self.boolean_psdf
        pser, psser = pdf["this"], psdf["this"]
        other_pser, other_psser = pdf["that"], psdf["that"]
        self.check_extension(pser <= other_pser, psser <= other_psser)
        self.check_extension(pser <= pser, psser <= psser)

    def test_gt(self):
        pdf, psdf = self.boolean_pdf, self.boolean_psdf
        pser, psser = pdf["this"], psdf["this"]
        other_pser, other_psser = pdf["that"], psdf["that"]
        self.check_extension(pser > other_pser, psser > other_psser)
        self.check_extension(pser > pser, psser > psser)

    def test_ge(self):
        pdf, psdf = self.boolean_pdf, self.boolean_psdf
        pser, psser = pdf["this"], psdf["this"]
        other_pser, other_psser = pdf["that"], psdf["that"]
        self.check_extension(pser >= other_pser, psser >= other_psser)
        self.check_extension(pser >= pser, psser >= psser)


if __name__ == "__main__":
    from pyspark.pandas.tests.data_type_ops.test_boolean_ops import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
