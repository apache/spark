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

import pyspark.pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class InferObjMixin:
    @property
    def pdf(self):
        df = pd.DataFrame({"A": ["a", 1, 2, 3]})
        return df.iloc[1:]

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_disabled(self):
        with self.assertRaises(PandasNotImplementedError):
            self.psdf.infer_objects()

    def test_fallback(self):
        ps.set_option("compute.pandas_fallback", True)

        pdf2 = self.pdf.infer_objects()
        psdf2 = self.psdf.infer_objects()
        self.assert_eq(pdf2, psdf2)
        self.assert_eq(pdf2.dtypes, psdf2.dtypes)

        ps.reset_option("compute.pandas_fallback")


class InferObjTests(
    InferObjMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_infer_objects import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
