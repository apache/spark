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
from pandas.api.types import CategoricalDtype

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase
from pyspark.pandas.typedef.typehints import (
    extension_dtypes_available,
    extension_float_dtypes_available,
    extension_object_dtypes_available,
)


class AsTypeTestsMixin:
    """Unit tests for arithmetic operations of numeric data types.

    A few test cases are disabled because pandas-on-Spark returns float64 whereas pandas
    returns float32.
    The underlying reason is the respective Spark operations return DoubleType always.
    """

    def test_astype(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]

            for int_type in [int, np.int32, np.int16, np.int8]:
                if not pser.hasnans:
                    self.assert_eq(pser.astype(int_type), psser.astype(int_type))
                else:
                    self.assertRaisesRegex(
                        ValueError,
                        "Cannot convert %s with missing "
                        "values to integer" % psser._dtype_op.pretty_name,
                        lambda: psser.astype(int_type),
                    )

            self.assert_eq(pser.astype(bool), psser.astype(bool))
            self.assert_eq(pser.astype(float), psser.astype(float))
            self.assert_eq(pser.astype(np.float32), psser.astype(np.float32))
            self.assert_eq(pser.astype(str), psser.astype(str))
            self.assert_eq(pser.astype("category"), psser.astype("category"))
            cat_type = CategoricalDtype(categories=[2, 1, 3])
            self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))
        if extension_object_dtypes_available and extension_float_dtypes_available:
            pser = pd.Series(pd.Categorical([1.0, 2.0, 3.0]), dtype=pd.Float64Dtype())
            psser = ps.from_pandas(pser)
            self.assert_eq(pser.astype(pd.BooleanDtype()), psser.astype(pd.BooleanDtype()))

    def test_astype_eager_check(self):
        psser = self.psdf["float_nan"]
        with ps.option_context("compute.eager_check", True), self.assertRaisesRegex(
            ValueError, "Cannot convert"
        ):
            psser.astype(int)
        with ps.option_context("compute.eager_check", False):
            psser.astype(int)

        psser = self.psdf["decimal_nan"]
        with ps.option_context("compute.eager_check", True), self.assertRaisesRegex(
            ValueError, "Cannot convert"
        ):
            psser.astype(int)
        with ps.option_context("compute.eager_check", False):
            psser.astype(int)


class AsTypeTests(
    AsTypeTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.data_type_ops.test_as_type import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
