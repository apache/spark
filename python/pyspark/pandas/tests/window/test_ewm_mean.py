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
import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class EWMMeanMixin:
    def _test_ewm_func(self, f):
        pser = pd.Series([1, 2, 3], index=np.random.rand(3), name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(
            getattr(psser.ewm(com=0.2), f)(),
            getattr(pser.ewm(com=0.2), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psser.ewm(com=0.2), f)().sum(),
            getattr(pser.ewm(com=0.2), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psser.ewm(span=1.7), f)(),
            getattr(pser.ewm(span=1.7), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psser.ewm(span=1.7), f)().sum(),
            getattr(pser.ewm(span=1.7), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psser.ewm(halflife=0.5), f)(),
            getattr(pser.ewm(halflife=0.5), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psser.ewm(halflife=0.5), f)().sum(),
            getattr(pser.ewm(halflife=0.5), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psser.ewm(alpha=0.7), f)(),
            getattr(pser.ewm(alpha=0.7), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psser.ewm(alpha=0.7), f)().sum(),
            getattr(pser.ewm(alpha=0.7), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psser.ewm(alpha=0.7, min_periods=2), f)(),
            getattr(pser.ewm(alpha=0.7, min_periods=2), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psser.ewm(alpha=0.7, min_periods=2), f)().sum(),
            getattr(pser.ewm(alpha=0.7, min_periods=2), f)().sum(),
            almost=True,
        )

        pdf = pd.DataFrame(
            {"a": [1.0, 2.0, 3.0, 2.0], "b": [4.0, 2.0, 3.0, 1.0]}, index=np.random.rand(4)
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            getattr(psdf.ewm(com=0.2), f)(),
            getattr(pdf.ewm(com=0.2), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(com=0.2), f)().sum(),
            getattr(pdf.ewm(com=0.2), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(span=1.7), f)(),
            getattr(pdf.ewm(span=1.7), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(span=1.7), f)().sum(),
            getattr(pdf.ewm(span=1.7), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(halflife=0.5), f)(),
            getattr(pdf.ewm(halflife=0.5), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(halflife=0.5), f)().sum(),
            getattr(pdf.ewm(halflife=0.5), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7), f)(),
            getattr(pdf.ewm(alpha=0.7), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7), f)().sum(),
            getattr(pdf.ewm(alpha=0.7), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7, min_periods=2), f)(),
            getattr(pdf.ewm(alpha=0.7, min_periods=2), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7, min_periods=2), f)().sum(),
            getattr(pdf.ewm(alpha=0.7, min_periods=2), f)().sum(),
            almost=True,
        )

        pdf = pd.DataFrame(
            {
                "s1": [None, 2, 3, 4],
                "s2": [1, None, 3, 4],
                "s3": [1, 3, 4, 5],
                "s4": [1, 0, 3, 4],
                "s5": [None, None, 1, None],
                "s6": [None, None, None, None],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            getattr(psdf.ewm(com=0.2, ignore_na=True), f)(),
            getattr(pdf.ewm(com=0.2, ignore_na=True), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(com=0.2, ignore_na=True), f)().sum(),
            getattr(pdf.ewm(com=0.2, ignore_na=True), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(com=0.2, ignore_na=False), f)(),
            getattr(pdf.ewm(com=0.2, ignore_na=False), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(com=0.2, ignore_na=False), f)().sum(),
            getattr(pdf.ewm(com=0.2, ignore_na=False), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(span=1.7, ignore_na=True), f)(),
            getattr(pdf.ewm(span=1.7, ignore_na=True), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(span=1.7, ignore_na=True), f)().sum(),
            getattr(pdf.ewm(span=1.7, ignore_na=True), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(span=1.7, ignore_na=False), f)(),
            getattr(pdf.ewm(span=1.7, ignore_na=False), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(span=1.7, ignore_na=False), f)().sum(),
            getattr(pdf.ewm(span=1.7, ignore_na=False), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(halflife=0.5, ignore_na=True), f)(),
            getattr(pdf.ewm(halflife=0.5, ignore_na=True), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(halflife=0.5, ignore_na=True), f)().sum(),
            getattr(pdf.ewm(halflife=0.5, ignore_na=True), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(halflife=0.5, ignore_na=False), f)(),
            getattr(pdf.ewm(halflife=0.5, ignore_na=False), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(halflife=0.5, ignore_na=False), f)().sum(),
            getattr(pdf.ewm(halflife=0.5, ignore_na=False), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7, ignore_na=True), f)(),
            getattr(pdf.ewm(alpha=0.7, ignore_na=True), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7, ignore_na=True), f)().sum(),
            getattr(pdf.ewm(alpha=0.7, ignore_na=True), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7, ignore_na=False), f)(),
            getattr(pdf.ewm(alpha=0.7, ignore_na=False), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7, ignore_na=False), f)().sum(),
            getattr(pdf.ewm(alpha=0.7, ignore_na=False), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7, ignore_na=True, min_periods=2), f)(),
            getattr(pdf.ewm(alpha=0.7, ignore_na=True, min_periods=2), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7, ignore_na=True, min_periods=2), f)().sum(),
            getattr(pdf.ewm(alpha=0.7, ignore_na=True, min_periods=2), f)().sum(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7, ignore_na=False, min_periods=2), f)(),
            getattr(pdf.ewm(alpha=0.7, ignore_na=False, min_periods=2), f)(),
            almost=True,
        )
        self.assert_eq(
            getattr(psdf.ewm(alpha=0.7, ignore_na=False, min_periods=2), f)().sum(),
            getattr(pdf.ewm(alpha=0.7, ignore_na=False, min_periods=2), f)().sum(),
            almost=True,
        )

    def test_ewm_mean(self):
        self._test_ewm_func("mean")


class EWMMeanTests(
    EWMMeanMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.window.test_ewm_mean import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
