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

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase
from pyspark.pandas.typedef.typehints import extension_dtypes_available


@unittest.skipIf(not extension_dtypes_available, "pandas extension dtypes are not available")
class IntegralExtensionOpsTestsMixin:
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

    def test_neg(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            self.check_extension(-pser, -psser)

    def test_abs(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            self.check_extension(abs(pser), abs(psser))

    def test_invert(self):
        for pser, psser in self.intergral_extension_pser_psser_pairs:
            self.check_extension(~pser, ~psser)

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


class IntegralExtensionOpsTests(
    IntegralExtensionOpsTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
