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
from pyspark.pandas.config import option_context
from pyspark.pandas.tests.data_type_ops.testing_utils import TestCasesUtils
from pyspark.pandas.typedef.typehints import (
    extension_dtypes_available,
    extension_float_dtypes_available,
)
from pyspark.sql.types import DecimalType
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class NumOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    """Unit tests for arithmetic operations of numeric data types.

    A few test cases are disabled because pandas-on-Spark returns float64 whereas pandas
    returns float32.
    The underlying reason is the respective Spark operations return DoubleType always.
    """

    @property
    def float_pser(self):
        return pd.Series([1, 2, 3], dtype=float)

    @property
    def float_psser(self):
        return ps.from_pandas(self.float_pser)

    def test_add(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(pser + pser, psser + psser)
            self.assert_eq(pser + 1, psser + 1)
            # self.assert_eq(pser + 0.1, psser + 0.1)
            self.assert_eq(pser + pser.astype(bool), psser + psser.astype(bool))
            self.assert_eq(pser + True, psser + True)
            self.assert_eq(pser + False, psser + False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser + self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser + self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser + self.non_numeric_pssers["date"])
                self.assertRaises(TypeError, lambda: psser + self.non_numeric_pssers["categorical"])
                self.assert_eq(
                    (psser + self.non_numeric_pssers["bool"]).sort_index(),
                    pser + self.non_numeric_psers["bool"],
                )

    def test_sub(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(pser - pser, psser - psser)
            self.assert_eq(pser - 1, psser - 1)
            # self.assert_eq(pser - 0.1, psser - 0.1)
            self.assert_eq(pser - pser.astype(bool), psser - psser.astype(bool))
            self.assert_eq(pser - True, psser - True)
            self.assert_eq(pser - False, psser - False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser - self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser - self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser - self.non_numeric_pssers["date"])
                self.assertRaises(TypeError, lambda: psser - self.non_numeric_pssers["categorical"])
                self.assert_eq(
                    (psser - self.non_numeric_pssers["bool"]).sort_index(),
                    pser - self.non_numeric_psers["bool"],
                )

    def test_mul(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(pser * pser, psser * psser)
            self.assert_eq(pser * pser.astype(bool), psser * psser.astype(bool))
            self.assert_eq(pser * True, psser * True)
            self.assert_eq(pser * False, psser * False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                if psser.dtype in [int, np.int32]:
                    self.assert_eq(
                        (psser * self.non_numeric_pssers["string"]).sort_index(),
                        pser * self.non_numeric_psers["string"],
                    )
                else:
                    self.assertRaises(TypeError, lambda: psser * self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser * self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser * self.non_numeric_pssers["date"])
                self.assertRaises(TypeError, lambda: psser * self.non_numeric_pssers["categorical"])
                self.assert_eq(
                    (psser * self.non_numeric_pssers["bool"]).sort_index(),
                    pser * self.non_numeric_psers["bool"],
                )

    def test_truediv(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            if psser.dtype in [float, int, np.int32]:
                self.assert_eq(pser / pser, psser / psser)
                self.assert_eq(pser / pser.astype(bool), psser / psser.astype(bool))
                self.assert_eq(pser / True, psser / True)
                self.assert_eq(pser / False, psser / False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser / self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser / self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser / self.non_numeric_pssers["date"])
                self.assertRaises(TypeError, lambda: psser / self.non_numeric_pssers["categorical"])
            self.assert_eq(
                (self.float_psser / self.non_numeric_pssers["bool"]).sort_index(),
                self.float_pser / self.non_numeric_psers["bool"],
            )

    def test_floordiv(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            if psser.dtype == float:
                self.assert_eq(pser // pser, psser // psser)
                self.assert_eq(pser // pser.astype(bool), psser // psser.astype(bool))
                self.assert_eq(pser // True, psser // True)
                self.assert_eq(pser // False, psser // False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser // self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser // self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser // self.non_numeric_pssers["date"])
                self.assertRaises(
                    TypeError, lambda: psser // self.non_numeric_pssers["categorical"]
                )
            if LooseVersion(pd.__version__) >= LooseVersion("0.25.3"):
                self.assert_eq(
                    (self.float_psser // self.non_numeric_pssers["bool"]).sort_index(),
                    self.float_pser // self.non_numeric_psers["bool"],
                )
            else:
                self.assert_eq(
                    (self.float_pser // self.non_numeric_psers["bool"]).sort_index(),
                    ps.Series([1.0, 2.0, np.inf]),
                )

    def test_mod(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(pser % pser, psser % psser)
            self.assert_eq(pser % pser.astype(bool), psser % psser.astype(bool))
            self.assert_eq(pser % True, psser % True)
            if psser.dtype in [int, np.int32]:
                self.assert_eq(ps.Series([np.nan, np.nan, np.nan], dtype=float), psser % False)
            else:
                self.assert_eq(
                    ps.Series([np.nan, np.nan, np.nan], dtype=psser.dtype), psser % False
                )

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser % self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser % self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser % self.non_numeric_pssers["date"])
                self.assertRaises(TypeError, lambda: psser % self.non_numeric_pssers["categorical"])
            self.assert_eq(
                (self.float_psser % self.non_numeric_pssers["bool"]).sort_index(),
                self.float_pser % self.non_numeric_psers["bool"],
            )

    def test_pow(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            if psser.dtype == float:
                self.assert_eq(pser ** pser, psser ** psser)
                self.assert_eq(pser ** pser.astype(bool), psser ** psser.astype(bool))
                self.assert_eq(pser ** True, psser ** True)
                self.assert_eq(pser ** False, psser ** False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assertRaises(TypeError, lambda: psser ** self.non_numeric_pssers["string"])
                self.assertRaises(TypeError, lambda: psser ** self.non_numeric_pssers["datetime"])
                self.assertRaises(TypeError, lambda: psser ** self.non_numeric_pssers["date"])
                self.assertRaises(
                    TypeError, lambda: psser ** self.non_numeric_pssers["categorical"]
                )
            self.assert_eq(
                (self.float_psser ** self.non_numeric_pssers["bool"]).sort_index(),
                self.float_pser ** self.non_numeric_psers["bool"],
            )

    def test_radd(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(1 + pser, 1 + psser)
            # self.assert_eq(0.1 + pser, 0.1 + psser)
            self.assertRaises(TypeError, lambda: "x" + psser)
            self.assert_eq(True + pser, True + psser)
            self.assert_eq(False + pser, False + psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) + psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) + psser)

    def test_rsub(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(1 - pser, 1 - psser)
            # self.assert_eq(0.1 - pser, 0.1 - psser)
            self.assertRaises(TypeError, lambda: "x" - psser)
            self.assert_eq(True - pser, True - psser)
            self.assert_eq(False - pser, False - psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) - psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) - psser)

    def test_rmul(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(1 * pser, 1 * psser)
            # self.assert_eq(0.1 * pser, 0.1 * psser)
            self.assertRaises(TypeError, lambda: "x" * psser)
            self.assert_eq(True * pser, True * psser)
            self.assert_eq(False * pser, False * psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) * psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) * psser)

    def test_rtruediv(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            # self.assert_eq(5 / pser, 5 / psser)
            # self.assert_eq(0.1 / pser, 0.1 / psser)
            self.assertRaises(TypeError, lambda: "x" / psser)
            self.assert_eq((True / pser).astype(float), True / psser, check_exact=False)
            self.assert_eq((False / pser).astype(float), False / psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) / psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) / psser)

    def test_rfloordiv(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            # self.assert_eq(5 // pser, 5 // psser)
            # self.assert_eq(0.1 // pser, 0.1 // psser)
            self.assertRaises(TypeError, lambda: "x" // psser)
            self.assert_eq((True // pser).astype(float), True // psser)
            self.assert_eq((False // pser).astype(float), False // psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) // psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) // psser)

    def test_rpow(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            # self.assert_eq(1 ** pser, 1 ** psser)
            # self.assert_eq(0.1 ** pser, 0.1 ** psser)
            self.assertRaises(TypeError, lambda: "x" ** psser)
            self.assert_eq((True ** pser).astype(float), True ** psser)
            self.assert_eq((False ** pser).astype(float), False ** psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) ** psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) ** psser)

    def test_rmod(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(1 % pser, 1 % psser)
            # self.assert_eq(0.1 % pser, 0.1 % psser)
            self.assert_eq(True % pser, True % psser)
            self.assert_eq(False % pser, False % psser)
            self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) % psser)
            self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) % psser)

    def test_and(self):
        psser = self.numeric_pssers[0]
        self.assertRaises(TypeError, lambda: psser & True)
        self.assertRaises(TypeError, lambda: psser & False)
        self.assertRaises(TypeError, lambda: psser & psser)

    def test_rand(self):
        psser = self.numeric_pssers[0]
        self.assertRaises(TypeError, lambda: True & psser)
        self.assertRaises(TypeError, lambda: False & psser)

    def test_or(self):
        psser = self.numeric_pssers[0]
        self.assertRaises(TypeError, lambda: psser | True)
        self.assertRaises(TypeError, lambda: psser | False)
        self.assertRaises(TypeError, lambda: psser | psser)

    def test_ror(self):
        psser = self.numeric_pssers[0]
        self.assertRaises(TypeError, lambda: True | psser)
        self.assertRaises(TypeError, lambda: False | psser)

    def test_from_to_pandas(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(pser, psser.to_pandas())
            self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        for pser, psser in self.numeric_withnan_pser_psser_pairs:
            self.assert_eq(pser.isnull(), psser.isnull())

    def test_astype(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(pser.astype(int), psser.astype(int))
            self.assert_eq(pser.astype(float), psser.astype(float))
            self.assert_eq(pser.astype(np.float32), psser.astype(np.float32))
            self.assert_eq(pser.astype(np.int32), psser.astype(np.int32))
            self.assert_eq(pser.astype(np.int16), psser.astype(np.int16))
            self.assert_eq(pser.astype(np.int8), psser.astype(np.int8))
            self.assert_eq(pser.astype(str), psser.astype(str))
            self.assert_eq(pser.astype(bool), psser.astype(bool))
            self.assert_eq(pser.astype("category"), psser.astype("category"))
            cat_type = CategoricalDtype(categories=[2, 1, 3])
            self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))

    def test_neg(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(-pser, -psser)

    def test_abs(self):
        for pser, psser in self.numeric_pser_psser_pairs:
            self.assert_eq(abs(pser), abs(psser))

    def test_invert(self):
        for psser in self.numeric_pssers:
            if not isinstance(psser.spark.data_type, DecimalType):
                self.assertRaises(NotImplementedError, lambda: ~psser)
            else:
                self.assertRaises(TypeError, lambda: ~psser)

    def test_eq(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(pser == pser, (psser == psser).sort_index())

    def test_ne(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(pser != pser, (psser != psser).sort_index())

    def test_lt(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                if isinstance(psser.spark.data_type, DecimalType):
                    self.assertRaises(TypeError, lambda: psser < psser)
                else:
                    self.assert_eq(pser < pser, (psser < psser).sort_index())

    def test_le(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                if isinstance(psser.spark.data_type, DecimalType):
                    self.assertRaises(TypeError, lambda: psser <= psser)
                else:
                    self.assert_eq(pser <= pser, (psser <= psser).sort_index())

    def test_gt(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                if isinstance(psser.spark.data_type, DecimalType):
                    self.assertRaises(TypeError, lambda: psser > psser)
                else:
                    self.assert_eq(pser > pser, (psser > psser).sort_index())

    def test_ge(self):
        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                if isinstance(psser.spark.data_type, DecimalType):
                    self.assertRaises(TypeError, lambda: psser >= psser)
                else:
                    self.assert_eq(pser >= pser, (psser >= psser).sort_index())


@unittest.skipIf(not extension_dtypes_available, "pandas extension dtypes are not available")
class IntegralExtensionOpsTest(PandasOnSparkTestCase, TestCasesUtils):
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
            self.check_extension(pser, psser.to_pandas())
            self.check_extension(ps.from_pandas(pser), psser)

    def test_isnull(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            self.assert_eq(pser.isnull(), psser.isnull())

    def test_astype(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            for dtype in self.extension_dtypes:
                if dtype in self.string_extension_dtype:
                    if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
                        # Limit pandas version due to
                        # https://github.com/pandas-dev/pandas/issues/31204
                        self.check_extension(pser.astype(dtype), psser.astype(dtype))
                else:
                    self.check_extension(pser.astype(dtype), psser.astype(dtype))

    def test_neg(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            if LooseVersion(pd.__version__) < LooseVersion("1.1.3"):
                # pandas < 1.1.0: object dtype is returned after negation
                # pandas 1.1.1 and 1.1.2:
                #   a TypeError "bad operand type for unary -: 'IntegerArray'" is raised
                # Please refer to https://github.com/pandas-dev/pandas/issues/36063.
                self.check_extension(pd.Series([-1, -2, -3, None], dtype=pser.dtype), -psser)
            else:
                self.check_extension(-pser, -psser)

    def test_abs(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            self.check_extension(abs(pser), abs(psser))

    def test_invert(self):
        for psser in self.intergral_extension_pssers:
            self.assertRaises(NotImplementedError, lambda: ~psser)

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


@unittest.skipIf(
    not extension_float_dtypes_available, "pandas extension float dtypes are not available"
)
class FractionalExtensionOpsTest(PandasOnSparkTestCase, TestCasesUtils):
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
            self.check_extension(pser, psser.to_pandas())
            self.check_extension(ps.from_pandas(pser), psser)

    def test_isnull(self):
        for pser, psser in self.fractional_extension_pser_psser_pairs:
            self.assert_eq(pser.isnull(), psser.isnull())

    def test_astype(self):
        for pser, psser in self.fractional_extension_pser_psser_pairs:
            for dtype in self.extension_dtypes:
                self.check_extension(pser.astype(dtype), psser.astype(dtype))

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
            self.assertRaises(NotImplementedError, lambda: ~psser)

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


if __name__ == "__main__":
    from pyspark.pandas.tests.data_type_ops.test_num_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
