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
from pyspark.testing.sqlutils import SQLTestUtils


class FrameAxisMixin:
    def test_axis_on_dataframe(self):
        # The number of each count is intentionally big
        # because when data is small, it executes a shortcut.
        # Less than 'compute.shortcut_limit' will execute a shortcut
        # by using collected pandas dataframe directly.
        # now we set the 'compute.shortcut_limit' as 1000 explicitly
        with ps.option_context("compute.shortcut_limit", 1000):
            pdf = pd.DataFrame(
                {
                    "A": [1, -2, 3, -4, 5] * 300,
                    "B": [1.0, -2, 3, -4, 5] * 300,
                    "C": [-6.0, -7, -8, -9, 10] * 300,
                    "D": [True, False, True, False, False] * 300,
                },
                index=range(10, 15001, 10),
            )
            # TODO(SPARK-45228): Update `test_axis_on_dataframe` when Pandas regression is fixed
            # There is a regression in Pandas 2.1.0,
            # so we should manually cast to float until the regression is fixed.
            # See https://github.com/pandas-dev/pandas/issues/55194.
            pdf = pdf.astype(float)
            psdf = ps.from_pandas(pdf)
            self.assert_eq(psdf.count(axis=1), pdf.count(axis=1))
            self.assert_eq(psdf.var(axis=1), pdf.var(axis=1))
            self.assert_eq(psdf.var(axis=1, ddof=0), pdf.var(axis=1, ddof=0))
            self.assert_eq(psdf.std(axis=1), pdf.std(axis=1))
            self.assert_eq(psdf.std(axis=1, ddof=0), pdf.std(axis=1, ddof=0))
            self.assert_eq(psdf.max(axis=1), pdf.max(axis=1))
            self.assert_eq(psdf.min(axis=1), pdf.min(axis=1))
            self.assert_eq(psdf.sum(axis=1), pdf.sum(axis=1))
            self.assert_eq(psdf.product(axis=1), pdf.product(axis=1))
            self.assert_eq(psdf.kurtosis(axis=0), pdf.kurtosis(axis=0), almost=True)
            self.assert_eq(psdf.kurtosis(axis=1), pdf.kurtosis(axis=1))
            self.assert_eq(psdf.skew(axis=0), pdf.skew(axis=0), almost=True)
            self.assert_eq(psdf.skew(axis=1), pdf.skew(axis=1))
            self.assert_eq(psdf.mean(axis=1), pdf.mean(axis=1))
            self.assert_eq(psdf.sem(axis=1), pdf.sem(axis=1))
            self.assert_eq(psdf.sem(axis=1, ddof=0), pdf.sem(axis=1, ddof=0))

            self.assert_eq(
                psdf.count(axis=1, numeric_only=True), pdf.count(axis=1, numeric_only=True)
            )
            self.assert_eq(psdf.var(axis=1, numeric_only=True), pdf.var(axis=1, numeric_only=True))
            self.assert_eq(
                psdf.var(axis=1, ddof=0, numeric_only=True),
                pdf.var(axis=1, ddof=0, numeric_only=True),
            )
            self.assert_eq(psdf.std(axis=1, numeric_only=True), pdf.std(axis=1, numeric_only=True))
            self.assert_eq(
                psdf.std(axis=1, ddof=0, numeric_only=True),
                pdf.std(axis=1, ddof=0, numeric_only=True),
            )
            self.assert_eq(
                psdf.max(axis=1, numeric_only=True),
                pdf.max(axis=1, numeric_only=True).astype(float),
            )
            self.assert_eq(
                psdf.min(axis=1, numeric_only=True),
                pdf.min(axis=1, numeric_only=True).astype(float),
            )
            self.assert_eq(
                psdf.sum(axis=1, numeric_only=True),
                pdf.sum(axis=1, numeric_only=True).astype(float),
            )
            self.assert_eq(
                psdf.product(axis=1, numeric_only=True),
                pdf.product(axis=1, numeric_only=True).astype(float),
            )
            self.assert_eq(
                psdf.kurtosis(axis=0, numeric_only=True),
                pdf.kurtosis(axis=0, numeric_only=True),
                almost=True,
            )
            self.assert_eq(
                psdf.kurtosis(axis=1, numeric_only=True), pdf.kurtosis(axis=1, numeric_only=True)
            )
            self.assert_eq(
                psdf.skew(axis=0, numeric_only=True),
                pdf.skew(axis=0, numeric_only=True),
                almost=True,
            )
            self.assert_eq(
                psdf.skew(axis=1, numeric_only=True), pdf.skew(axis=1, numeric_only=True)
            )
            self.assert_eq(
                psdf.mean(axis=1, numeric_only=True), pdf.mean(axis=1, numeric_only=True)
            )
            self.assert_eq(psdf.sem(axis=1, numeric_only=True), pdf.sem(axis=1, numeric_only=True))
            self.assert_eq(
                psdf.sem(axis=1, ddof=0, numeric_only=True),
                pdf.sem(axis=1, ddof=0, numeric_only=True),
            )


class FrameAxisTests(
    FrameAxisMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_axis import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
