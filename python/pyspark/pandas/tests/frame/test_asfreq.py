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


class AsFreqMixin:
    @property
    def pdf(self):
        index = pd.date_range("1/1/2000", periods=4, freq="min")
        series = pd.Series([0.0, None, 2.0, 3.0], index=index)
        return pd.DataFrame({"s": series})

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_disabled(self):
        with self.assertRaises(PandasNotImplementedError):
            self.psdf.asfreq(freq="30s")

    def test_fallback(self):
        ps.set_option("compute.pandas_fallback", True)

        self.assert_eq(self.pdf.asfreq(freq="30s"), self.psdf.asfreq(freq="30s"))
        self.assert_eq(
            self.pdf.asfreq(freq="30s", fill_value=9.0),
            self.psdf.asfreq(freq="30s", fill_value=9.0),
        )
        self.assert_eq(
            self.pdf.asfreq(freq="30s", method="bfill"),
            self.psdf.asfreq(freq="30s", method="bfill"),
        )

        # test with schema infered from partial dataset, len(pdf)==4
        ps.set_option("compute.shortcut_limit", 2)
        self.assert_eq(self.pdf.asfreq(freq="30s"), self.psdf.asfreq(freq="30s"))
        self.assert_eq(
            self.pdf.asfreq(freq="30s", fill_value=9.0),
            self.psdf.asfreq(freq="30s", fill_value=9.0),
        )
        self.assert_eq(
            self.pdf.asfreq(freq="30s", method="bfill"),
            self.psdf.asfreq(freq="30s", method="bfill"),
        )

        ps.reset_option("compute.shortcut_limit")
        ps.reset_option("compute.pandas_fallback")


class AsFreqTests(
    AsFreqMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_asfreq import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
