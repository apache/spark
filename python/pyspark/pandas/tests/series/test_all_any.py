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

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class SeriesAllAnyMixin:
    def test_all(self):
        for pser in [
            pd.Series([True, True], name="x"),
            pd.Series([True, False], name="x"),
            pd.Series([0, 1], name="x"),
            pd.Series([1, 2, 3], name="x"),
            pd.Series([np.nan, 0, 1], name="x"),
            pd.Series([np.nan, 1, 2, 3], name="x"),
            pd.Series([True, True, None], name="x"),
            pd.Series([True, False, None], name="x"),
            pd.Series([], name="x"),
            pd.Series([np.nan], name="x"),
            pd.Series([np.nan, np.nan], name="x"),
            pd.Series([None], name="x"),
            pd.Series([None, None], name="x"),
        ]:
            psser = ps.from_pandas(pser)
            self.assert_eq(psser.all(), pser.all())
            self.assert_eq(psser.all(skipna=False), pser.all(skipna=False))
            self.assert_eq(psser.all(skipna=True), pser.all(skipna=True))

        pser = pd.Series([1, 2, 3, 4], name="x")
        psser = ps.from_pandas(pser)

        self.assert_eq((psser % 2 == 0).all(), (pser % 2 == 0).all())

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psser.all(axis=1)

    def test_any(self):
        for pser in [
            pd.Series([False, False], name="x"),
            pd.Series([True, False], name="x"),
            pd.Series([0, 1], name="x"),
            pd.Series([1, 2, 3], name="x"),
            pd.Series([True, True, None], name="x"),
            pd.Series([True, False, None], name="x"),
            pd.Series([], name="x"),
            pd.Series([np.nan], name="x"),
        ]:
            psser = ps.from_pandas(pser)
            self.assert_eq(psser.any(), pser.any())

        pser = pd.Series([1, 2, 3, 4], name="x")
        psser = ps.from_pandas(pser)

        self.assert_eq((psser % 2 == 0).any(), (pser % 2 == 0).any())

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psser.any(axis=1)


class SeriesAllAnyTests(
    SeriesAllAnyMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_all_any import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
