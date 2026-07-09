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
from pyspark.pandas.typedef.typehints import extension_float_dtypes_available


@unittest.skipIf(
    not extension_float_dtypes_available, "pandas extension float dtypes are not available"
)
class FractionalExtensionAstypeCmpOpsTestsMixin:
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


class FractionalExtensionAstypeCmpOpsTests(
    FractionalExtensionAstypeCmpOpsTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
