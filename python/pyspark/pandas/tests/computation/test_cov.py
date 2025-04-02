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
import decimal

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class FrameCovMixin:
    def test_cov(self):
        # SPARK-36396: Implement DataFrame.cov

        # int
        pdf = pd.DataFrame([(1, 2), (0, 3), (2, 0), (1, 1)], columns=["a", "b"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(), psdf.cov(), almost=True)
        self.assert_eq(pdf.cov(min_periods=4), psdf.cov(min_periods=4), almost=True)
        self.assert_eq(pdf.cov(min_periods=5), psdf.cov(min_periods=5))

        # ddof
        with self.assertRaisesRegex(TypeError, "ddof must be integer"):
            psdf.cov(ddof="ddof")
        for ddof in [-1, 0, 2]:
            self.assert_eq(pdf.cov(ddof=ddof), psdf.cov(ddof=ddof), almost=True)
            self.assert_eq(
                pdf.cov(min_periods=4, ddof=ddof), psdf.cov(min_periods=4, ddof=ddof), almost=True
            )
            self.assert_eq(pdf.cov(min_periods=5, ddof=ddof), psdf.cov(min_periods=5, ddof=ddof))

        # bool
        pdf = pd.DataFrame(
            {
                "a": [1, np.nan, 3, 4],
                "b": [True, False, False, True],
                "c": [True, True, False, True],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(), psdf.cov(), almost=True)
        self.assert_eq(pdf.cov(min_periods=4), psdf.cov(min_periods=4), almost=True)
        self.assert_eq(pdf.cov(min_periods=5), psdf.cov(min_periods=5))

        # extension dtype
        numeric_dtypes = ["Int8", "Int16", "Int32", "Int64", "Float32", "Float64", "float"]
        boolean_dtypes = ["boolean", "bool"]

        sers = [pd.Series([1, 2, 3, None], dtype=dtype) for dtype in numeric_dtypes]
        sers += [pd.Series([True, False, True, None], dtype=dtype) for dtype in boolean_dtypes]
        sers.append(pd.Series([decimal.Decimal(1), decimal.Decimal(2), decimal.Decimal(3), None]))

        pdf = pd.concat(sers, axis=1)
        pdf.columns = [dtype for dtype in numeric_dtypes + boolean_dtypes] + ["decimal"]
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.cov(numeric_only=True), psdf.cov(), almost=True)

        # string column
        pdf = pd.DataFrame(
            [(1, 2, "a", 1), (0, 3, "b", 1), (2, 0, "c", 9), (1, 1, "d", 1)],
            columns=["a", "b", "c", "d"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(numeric_only=True), psdf.cov(), almost=True)
        self.assert_eq(
            pdf.cov(numeric_only=True, min_periods=4), psdf.cov(min_periods=4), almost=True
        )
        self.assert_eq(pdf.cov(numeric_only=True, min_periods=5), psdf.cov(min_periods=5))

        # nan
        np.random.seed(42)
        pdf = pd.DataFrame(np.random.randn(20, 3), columns=["a", "b", "c"])
        pdf.loc[pdf.index[:5], "a"] = np.nan
        pdf.loc[pdf.index[5:10], "b"] = np.nan
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(min_periods=11), psdf.cov(min_periods=11), almost=True)
        self.assert_eq(pdf.cov(min_periods=10), psdf.cov(min_periods=10), almost=True)

        # return empty DataFrame
        pdf = pd.DataFrame([("1", "2"), ("0", "3"), ("2", "0"), ("1", "1")], columns=["a", "b"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(numeric_only=True), psdf.cov())


class FrameCovTests(
    FrameCovMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_cov import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
