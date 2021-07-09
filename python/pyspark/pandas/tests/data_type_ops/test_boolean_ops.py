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
from pyspark.pandas.typedef.typehints import extension_object_dtypes_available
from pyspark.sql.types import BooleanType
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class BooleanOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    @property
    def pser(self):
        return pd.Series([True, True, False])

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def float_pser(self):
        return pd.Series([1, 2, 3], dtype=float)

    @property
    def float_psser(self):
        return ps.from_pandas(self.float_pser)

    @property
    def other_pser(self):
        return pd.Series([False, False, True])

    @property
    def other_psser(self):
        return ps.from_pandas(self.other_pser)

    def test_add(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser + 1, psser + 1)
        self.assert_eq(pser + 0.1, psser + 0.1)
        self.assert_eq(pser + pser.astype(int), psser + psser.astype(int))
        self.assert_eq(pser + pser, psser + psser)
        self.assert_eq(pser + True, psser + True)
        self.assert_eq(pser + False, psser + False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser + pser, (self.psser + psser).sort_index())

            for pser, psser in self.non_numeric_pser_psser_pairs:
                if isinstance(psser.spark.data_type, BooleanType):
                    self.assert_eq(self.pser + pser, (self.psser + psser).sort_index())
                else:
                    self.assertRaises(TypeError, lambda: self.psser + psser)

    def test_sub(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser - 1, psser - 1)
        self.assert_eq(pser - 0.1, psser - 0.1)
        self.assert_eq(pser - pser.astype(int), psser - psser.astype(int))
        self.assertRaises(TypeError, lambda: psser - psser)
        self.assertRaises(TypeError, lambda: psser - True)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser - pser, (self.psser - psser).sort_index())

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser - psser)

    def test_mul(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser * 1, psser * 1)
        self.assert_eq(pser * 0.1, psser * 0.1)
        self.assert_eq(pser * pser.astype(int), psser * psser.astype(int))
        self.assert_eq(pser * pser, psser * psser)
        self.assert_eq(pser * True, psser * True)
        self.assert_eq(pser * False, psser * False)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser * pser, (self.psser * psser).sort_index())

            for pser, psser in self.non_numeric_pser_psser_pairs:
                if isinstance(psser.spark.data_type, BooleanType):
                    self.assert_eq(self.pser * pser, (self.psser * psser).sort_index())
                else:
                    self.assertRaises(TypeError, lambda: self.psser * psser)

    def test_truediv(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser / 1, psser / 1)
        self.assert_eq(pser / 0.1, psser / 0.1)
        self.assert_eq(pser / pser.astype(int), psser / psser.astype(int))
        self.assertRaises(TypeError, lambda: psser / psser)
        self.assertRaises(TypeError, lambda: psser / True)

        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser / self.float_pser, (self.psser / self.float_psser).sort_index()
            )

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser / psser)

    def test_floordiv(self):
        pser = self.pser
        psser = self.psser

        # float is always returned in pandas-on-Spark
        self.assert_eq((pser // 1).astype("float"), psser // 1)

        # in pandas, 1 // 0.1 = 9.0; in pandas-on-Spark, 1 // 0.1 = 10.0
        # self.assert_eq(pser // 0.1, psser // 0.1)

        self.assert_eq(pser // pser.astype(int), psser // psser.astype(int))
        self.assertRaises(TypeError, lambda: psser // psser)
        self.assertRaises(TypeError, lambda: psser // True)

        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser // self.float_pser, (self.psser // self.float_psser).sort_index()
            )

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser // psser)

    def test_mod(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq(pser % 1, psser % 1)
        self.assert_eq(pser % 0.1, psser % 0.1)
        self.assert_eq(pser % pser.astype(float), psser % psser.astype(float))
        self.assertRaises(TypeError, lambda: psser % psser)
        self.assertRaises(TypeError, lambda: psser % True)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser % pser, (self.psser % psser).sort_index())

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser % psser)

    def test_pow(self):
        pser = self.pser
        psser = self.psser
        # float is always returned in pandas-on-Spark
        self.assert_eq((pser ** 1).astype("float"), psser ** 1)
        self.assert_eq(pser ** 0.1, self.psser ** 0.1)
        self.assert_eq(pser ** pser.astype(float), psser ** psser.astype(float))
        self.assertRaises(TypeError, lambda: psser ** psser)
        self.assertRaises(TypeError, lambda: psser ** True)

        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser ** self.float_pser, (self.psser ** self.float_psser).sort_index()
            )

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser ** psser)

    def test_radd(self):
        self.assert_eq(1 + self.pser, 1 + self.psser)
        self.assert_eq(0.1 + self.pser, 0.1 + self.psser)
        self.assert_eq(True + self.pser, True + self.psser)
        self.assert_eq(False + self.pser, False + self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) + self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) + self.psser)

    def test_rsub(self):
        self.assert_eq(1 - self.pser, 1 - self.psser)
        self.assert_eq(0.1 - self.pser, 0.1 - self.psser)
        self.assertRaises(TypeError, lambda: "x" - self.psser)
        self.assertRaises(TypeError, lambda: True - self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) - self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) - self.psser)

    def test_rmul(self):
        self.assert_eq(1 * self.pser, 1 * self.psser)
        self.assert_eq(0.1 * self.pser, 0.1 * self.psser)
        self.assertRaises(TypeError, lambda: "x" * self.psser)
        self.assert_eq(True * self.pser, True * self.psser)
        self.assert_eq(False * self.pser, False * self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) * self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) * self.psser)

    def test_rtruediv(self):
        self.assert_eq(1 / self.pser, 1 / self.psser)
        self.assert_eq(0.1 / self.pser, 0.1 / self.psser)
        self.assertRaises(TypeError, lambda: "x" / self.psser)
        self.assertRaises(TypeError, lambda: True / self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) / self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) / self.psser)

    def test_rfloordiv(self):
        if LooseVersion(pd.__version__) >= LooseVersion("0.25.3"):
            self.assert_eq(1 // self.pser, 1 // self.psser)
            self.assert_eq(0.1 // self.pser, 0.1 // self.psser)
        else:
            self.assert_eq(1 // self.psser, ps.Series([1.0, 1.0, np.inf]))
            self.assert_eq(0.1 // self.psser, ps.Series([0.0, 0.0, np.inf]))
        self.assertRaises(TypeError, lambda: "x" // self.psser)
        self.assertRaises(TypeError, lambda: True // self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) // self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) // self.psser)

    def test_rpow(self):
        # float is returned always in pandas-on-Spark
        self.assert_eq((1 ** self.pser).astype(float), 1 ** self.psser)
        self.assert_eq(0.1 ** self.pser, 0.1 ** self.psser)
        self.assertRaises(TypeError, lambda: "x" ** self.psser)
        self.assertRaises(TypeError, lambda: True ** self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) ** self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) ** self.psser)

    def test_rmod(self):
        # 1 % False is 0.0 in pandas
        self.assert_eq(ps.Series([0, 0, None], dtype=float), 1 % self.psser)
        # 0.1 / True is 0.1 in pandas
        self.assert_eq(
            ps.Series([0.10000000000000009, 0.10000000000000009, None], dtype=float),
            0.1 % self.psser,
        )
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) % self.psser)
        self.assertRaises(TypeError, lambda: True % self.psser)

    def test_and(self):
        pser = pd.Series([True, False, None], dtype="bool")
        psser = ps.from_pandas(pser)
        self.assert_eq(pser & True, psser & True)
        self.assert_eq(pser & False, psser & False)
        self.assert_eq(pser & pser, psser & psser)

        other_pser = pd.Series([False, None, True], dtype="bool")
        other_psser = ps.from_pandas(other_pser)
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(pser & other_pser, (psser & other_psser).sort_index())
            self.check_extension(
                pser & other_pser.astype("boolean"),
                (psser & other_psser.astype("boolean")).sort_index(),
            )
            self.assert_eq(other_pser & pser, (other_psser & psser).sort_index())

    def test_rand(self):
        pser = pd.Series([True, False, None], dtype="bool")
        psser = ps.from_pandas(pser)
        self.assert_eq(True & pser, True & psser)
        self.assert_eq(False & pser, False & psser)

    def test_or(self):
        pser = pd.Series([True, False, None], dtype="bool")
        psser = ps.from_pandas(pser)
        self.assert_eq(pser | True, psser | True)
        self.assert_eq(pser | False, psser | False)
        self.assert_eq(pser | pser, psser | psser)
        self.assert_eq(True | pser, True | psser)
        self.assert_eq(False | pser, False | psser)

        other_pser = pd.Series([False, None, True], dtype="bool")
        other_psser = ps.from_pandas(other_pser)
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(pser | other_pser, (psser | other_psser).sort_index())
            self.check_extension(
                pser | other_pser.astype("boolean"),
                (psser | other_psser.astype("boolean")).sort_index(),
            )
            self.assert_eq(other_pser | pser, (other_psser | psser).sort_index())

    def test_ror(self):
        pser = pd.Series([True, False, None], dtype="bool")
        psser = ps.from_pandas(pser)
        self.assert_eq(True | pser, True | psser)
        self.assert_eq(False | pser, False | psser)

    def test_isnull(self):
        self.assert_eq(self.pser.isnull(), self.psser.isnull())

    def test_astype(self):
        pser = self.pser
        psser = self.psser
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
        self.assert_eq(-self.pser, -self.psser)

    def test_abs(self):
        self.assert_eq(abs(self.pser), abs(self.psser))

    def test_invert(self):
        self.assert_eq(~self.pser, ~self.psser)

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
class BooleanExtensionOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    @property
    def pser(self):
        return pd.Series([True, False, None], dtype="boolean")

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def other_pser(self):
        return pd.Series([False, None, True], dtype="boolean")

    @property
    def other_psser(self):
        return ps.from_pandas(self.other_pser)

    @property
    def float_pser(self):
        return pd.Series([1, 2, 3], dtype=float)

    @property
    def float_psser(self):
        return ps.from_pandas(self.float_pser)

    def test_add(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq((pser + 1).astype(float), psser + 1)
        self.assert_eq((pser + 0.1).astype(float), psser + 0.1)

        # In pandas, NA | True is NA, whereas NA | True is True in pandas-on-Spark
        self.check_extension(ps.Series([True, True, True], dtype="boolean"), psser + True)
        self.check_extension(pser + False, psser + False)
        self.check_extension(pser + pser, psser + psser)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser + pser, (self.psser + psser).sort_index(), almost=True)
            for psser in self.non_numeric_pssers.values():
                if not isinstance(psser.spark.data_type, BooleanType):
                    self.assertRaises(TypeError, lambda: self.psser + psser)
            bool_pser = pd.Series([False, False, False])
            bool_psser = ps.from_pandas(bool_pser)
            self.check_extension(self.pser + bool_pser, (self.psser + bool_psser).sort_index())

    def test_sub(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq((pser - 1).astype(float), psser - 1)
        self.assert_eq((pser - 0.1).astype(float), psser - 0.1)
        self.assertRaises(TypeError, lambda: psser - psser)
        self.assertRaises(TypeError, lambda: psser - True)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser - pser, (self.psser - psser).sort_index(), almost=True)
            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser - psser)

    def test_mul(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq((pser * 1).astype(float), psser * 1)
        self.assert_eq((pser * 0.1).astype(float), psser * 0.1)

        # In pandas, NA & False is NA, whereas NA & False is False in pandas-on-Spark
        self.check_extension(pser * True, psser * True)
        self.check_extension(ps.Series([False, False, False], dtype="boolean"), psser * False)
        self.check_extension(pser * pser, psser * psser)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser * pser, (self.psser * psser).sort_index(), almost=True)
            for psser in self.non_numeric_pssers.values():
                if not isinstance(psser.spark.data_type, BooleanType):
                    self.assertRaises(TypeError, lambda: self.psser * psser)
            bool_pser = pd.Series([True, True, True])
            bool_psser = ps.from_pandas(bool_pser)
            self.check_extension(self.pser * bool_pser, (self.psser * bool_psser).sort_index())

    def test_truediv(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq((pser / 1).astype(float), psser / 1)
        self.assert_eq((pser / 0.1).astype(float), psser / 0.1)
        self.assertRaises(TypeError, lambda: psser / psser)
        self.assertRaises(TypeError, lambda: psser / True)

        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser / self.float_pser,
                (self.psser / self.float_psser).sort_index(),
                almost=True,
            )
            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser / psser)

    def test_floordiv(self):
        pser = self.pser
        psser = self.psser

        # float is always returned in pandas-on-Spark
        self.assert_eq((pser // 1).astype("float"), psser // 1)

        # in pandas, 1 // 0.1 = 9.0; in pandas-on-Spark, 1 // 0.1 = 10.0
        # self.assert_eq(pser // 0.1, psser // 0.1)

        self.assertRaises(TypeError, lambda: psser // psser)
        self.assertRaises(TypeError, lambda: psser // True)

        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser // self.float_pser,
                (self.psser // self.float_psser).sort_index(),
                almost=True,
            )
            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser // psser)

    def test_mod(self):
        pser = self.pser
        psser = self.psser
        self.assert_eq((pser % 1).astype(float), psser % 1)
        self.assert_eq((pser % 0.1).astype(float), psser % 0.1)
        self.assertRaises(TypeError, lambda: psser % psser)
        self.assertRaises(TypeError, lambda: psser % True)

        with option_context("compute.ops_on_diff_frames", True):
            for pser, psser in self.numeric_pser_psser_pairs:
                self.assert_eq(self.pser % pser, (self.psser % psser).sort_index(), almost=True)
            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser % psser)

    def test_pow(self):
        pser = self.pser
        psser = self.psser
        # float is always returned in pandas-on-Spark
        self.assert_eq((pser ** 1).astype("float"), psser ** 1)
        self.assert_eq((pser ** 0.1).astype("float"), self.psser ** 0.1)
        self.assert_eq((pser ** pser.astype(float)).astype("float"), psser ** psser.astype(float))
        self.assertRaises(TypeError, lambda: psser ** psser)
        self.assertRaises(TypeError, lambda: psser ** True)

        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser ** self.float_pser,
                (self.psser ** self.float_psser).sort_index(),
                almost=True,
            )

            for psser in self.non_numeric_pssers.values():
                self.assertRaises(TypeError, lambda: self.psser ** psser)

    def test_radd(self):
        self.assert_eq((1 + self.pser).astype(float), 1 + self.psser)
        self.assert_eq((0.1 + self.pser).astype(float), 0.1 + self.psser)
        self.assertRaises(TypeError, lambda: "x" + self.psser)

        # In pandas, NA | True is NA, whereas NA | True is True in pandas-on-Spark
        self.check_extension(ps.Series([True, True, True], dtype="boolean"), True + self.psser)
        self.check_extension(False + self.pser, False + self.psser)

        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) + self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) + self.psser)

    def test_rsub(self):
        self.assert_eq((1 - self.pser).astype(float), 1 - self.psser)
        self.assert_eq((0.1 - self.pser).astype(float), 0.1 - self.psser)
        self.assertRaises(TypeError, lambda: "x" - self.psser)
        self.assertRaises(TypeError, lambda: True - self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) - self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) - self.psser)

    def test_rmul(self):
        self.assert_eq((1 * self.pser).astype(float), 1 * self.psser)
        self.assert_eq((0.1 * self.pser).astype(float), 0.1 * self.psser)
        self.assertRaises(TypeError, lambda: "x" * self.psser)

        # In pandas, NA & False is NA, whereas NA & False is False in pandas-on-Spark
        self.check_extension(True * self.pser, True * self.psser)
        self.check_extension(ps.Series([False, False, False], dtype="boolean"), False * self.psser)

        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) * self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) * self.psser)

    def test_rtruediv(self):
        self.assert_eq((1 / self.pser).astype(float), 1 / self.psser)
        self.assert_eq((0.1 / self.pser).astype(float), 0.1 / self.psser)
        self.assertRaises(TypeError, lambda: "x" / self.psser)
        self.assertRaises(TypeError, lambda: True / self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) / self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) / self.psser)

    def test_rfloordiv(self):
        self.assert_eq((1 // self.psser).astype(float), ps.Series([1.0, np.inf, np.nan]))
        self.assert_eq((0.1 // self.psser).astype(float), ps.Series([0.0, np.inf, np.nan]))
        self.assertRaises(TypeError, lambda: "x" // self.psser)
        self.assertRaises(TypeError, lambda: True // self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) // self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) // self.psser)

    def test_rpow(self):
        self.assert_eq(1 ** self.psser, ps.Series([1, 1, 1], dtype=float))
        self.assert_eq((0.1 ** self.pser).astype(float), 0.1 ** self.psser)
        self.assertRaises(TypeError, lambda: "x" ** self.psser)
        self.assertRaises(TypeError, lambda: True ** self.psser)
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) ** self.psser)
        self.assertRaises(TypeError, lambda: datetime.datetime(1994, 1, 1) ** self.psser)

    def test_rmod(self):
        self.assert_eq(ps.Series([0, np.nan, np.nan], dtype=float), 1 % self.psser)
        self.assert_eq(
            ps.Series([0.10000000000000009, np.nan, np.nan], dtype=float),
            0.1 % self.psser,
        )
        self.assertRaises(TypeError, lambda: datetime.date(1994, 1, 1) % self.psser)
        self.assertRaises(TypeError, lambda: True % self.psser)

    def test_and(self):
        pser = self.pser
        psser = self.psser
        self.check_extension(pser & True, psser & True)
        self.check_extension(pser & False, psser & False)
        self.check_extension(pser & pser, psser & psser)

        with option_context("compute.ops_on_diff_frames", True):
            self.check_extension(pser & self.other_pser, (psser & self.other_psser).sort_index())
            self.check_extension(self.other_pser & pser, (self.other_psser & psser).sort_index())

    def test_rand(self):
        self.check_extension(True & self.pser, True & self.psser)
        self.check_extension(False & self.pser, False & self.psser)

    def test_or(self):
        pser = self.pser
        psser = self.psser
        self.check_extension(pser | True, psser | True)
        self.check_extension(pser | False, psser | False)
        self.check_extension(pser | pser, psser | psser)

        with option_context("compute.ops_on_diff_frames", True):
            self.check_extension(pser | self.other_pser, (psser | self.other_psser).sort_index())
            self.check_extension(self.other_pser | pser, (self.other_psser | psser).sort_index())

    def test_ror(self):
        self.check_extension(True | self.pser, True | self.psser)
        self.check_extension(False | self.pser, False | self.psser)

    def test_from_to_pandas(self):
        data = [True, True, False, None]
        pser = pd.Series(data, dtype="boolean")
        psser = ps.Series(data, dtype="boolean")
        self.check_extension(pser, psser.to_pandas())
        self.check_extension(ps.from_pandas(pser), psser)

    def test_isnull(self):
        self.assert_eq(self.pser.isnull(), self.psser.isnull())

    def test_astype(self):
        pser = self.pser
        psser = self.psser

        # TODO(SPARK-35976): [True, False, <NA>] is returned in pandas
        self.assert_eq(["True", "False", "None"], self.psser.astype(str).tolist())

        self.assert_eq(pser.astype("category"), psser.astype("category"))
        cat_type = CategoricalDtype(categories=[False, True])
        self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))
        for dtype in self.extension_dtypes:
            if dtype in self.fractional_extension_dtypes:
                # A pandas boolean extension series cannot be casted to fractional extension dtypes
                self.assert_eq([1.0, 0.0, np.nan], self.psser.astype(dtype).tolist())
            elif dtype in self.string_extension_dtype:
                if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
                    # Limit pandas version due to https://github.com/pandas-dev/pandas/issues/31204
                    self.check_extension(pser.astype(dtype), psser.astype(dtype))
            else:
                self.check_extension(pser.astype(dtype), psser.astype(dtype))

    def test_neg(self):
        self.assertRaises(TypeError, lambda: -self.psser)

    def test_abs(self):
        self.assertRaises(TypeError, lambda: abs(self.psser))

    def test_invert(self):
        self.assertRaises(TypeError, lambda: ~self.psser)

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
    from pyspark.pandas.tests.data_type_ops.test_boolean_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
