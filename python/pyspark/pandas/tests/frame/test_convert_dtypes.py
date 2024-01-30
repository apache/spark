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

import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class ConvertDtypesMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {
                "a": pd.Series([1, 2, 3], dtype=np.dtype("int32")),
                "b": pd.Series(["x", "y", "z"], dtype=np.dtype("O")),
                "c": pd.Series([True, False, np.nan], dtype=np.dtype("O")),
                "d": pd.Series(["h", "i", np.nan], dtype=np.dtype("O")),
                "e": pd.Series([10, np.nan, 20], dtype=np.dtype("float")),
                "f": pd.Series([np.nan, 100.5, 200], dtype=np.dtype("float")),
            }
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_disabled(self):
        with self.assertRaises(PandasNotImplementedError):
            self.psdf.infer_objects()

    def test_fallback(self):
        ps.set_option("compute.pandas_fallback", True)

        pdf2 = self.pdf.convert_dtypes()
        psdf2 = self.psdf.convert_dtypes()
        self.assert_eq(pdf2, psdf2)
        self.assert_eq(pdf2.dtypes, psdf2.dtypes)

        pdf2 = self.pdf.convert_dtypes(convert_string=False)
        psdf2 = self.psdf.convert_dtypes(convert_string=False)
        self.assert_eq(pdf2, psdf2)
        self.assert_eq(pdf2.dtypes, psdf2.dtypes)

        pdf2 = self.pdf.convert_dtypes(convert_integer=False)
        psdf2 = self.psdf.convert_dtypes(convert_integer=False)
        self.assert_eq(pdf2, psdf2)
        self.assert_eq(pdf2.dtypes, psdf2.dtypes)

        pdf2 = self.pdf.convert_dtypes(convert_boolean=False)
        psdf2 = self.psdf.convert_dtypes(convert_boolean=False)
        self.assert_eq(pdf2, psdf2)
        self.assert_eq(pdf2.dtypes, psdf2.dtypes)

        pdf2 = self.pdf.convert_dtypes(convert_floating=False)
        psdf2 = self.psdf.convert_dtypes(convert_floating=False)
        self.assert_eq(pdf2, psdf2)
        self.assert_eq(pdf2.dtypes, psdf2.dtypes)

        ps.reset_option("compute.pandas_fallback")


class ConvertDtypesTests(
    ConvertDtypesMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_convert_dtypes import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
