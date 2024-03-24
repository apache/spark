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


class SeriesCumulativeMixin:
    def test_cummin(self):
        pser = pd.Series([1.0, None, 0.0, 4.0, 9.0])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cummin(), psser.cummin())
        self.assert_eq(pser.cummin(skipna=False), psser.cummin(skipna=False))
        self.assert_eq(pser.cummin().sum(), psser.cummin().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cummin(), psser.cummin())
        self.assert_eq(pser.cummin(skipna=False), psser.cummin(skipna=False))

    def test_cummax(self):
        pser = pd.Series([1.0, None, 0.0, 4.0, 9.0])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cummax(), psser.cummax())
        self.assert_eq(pser.cummax(skipna=False), psser.cummax(skipna=False))
        self.assert_eq(pser.cummax().sum(), psser.cummax().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cummax(), psser.cummax())
        self.assert_eq(pser.cummax(skipna=False), psser.cummax(skipna=False))

    def test_cumsum(self):
        pser = pd.Series([1.0, None, 0.0, 4.0, 9.0])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumsum(), psser.cumsum())
        self.assert_eq(pser.cumsum(skipna=False), psser.cumsum(skipna=False))
        self.assert_eq(pser.cumsum().sum(), psser.cumsum().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumsum(), psser.cumsum())
        self.assert_eq(pser.cumsum(skipna=False), psser.cumsum(skipna=False))

        # bool
        pser = pd.Series([True, True, False, True])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumsum().astype(int), psser.cumsum())
        self.assert_eq(pser.cumsum(skipna=False).astype(int), psser.cumsum(skipna=False))

        with self.assertRaisesRegex(TypeError, r"Could not convert object \(string\) to numeric"):
            ps.Series(["a", "b", "c", "d"]).cumsum()

    def test_cumprod(self):
        pser = pd.Series([1.0, None, 1.0, 4.0, 9.0])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), psser.cumprod(skipna=False))
        self.assert_eq(pser.cumprod().sum(), psser.cumprod().sum())

        # with integer type
        pser = pd.Series([1, 10, 1, 4, 9])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), psser.cumprod(skipna=False))
        self.assert_eq(pser.cumprod().sum(), psser.cumprod().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), psser.cumprod(skipna=False))

        # including zero
        pser = pd.Series([1, 2, 0, 3])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), psser.cumprod(skipna=False))

        # including negative values
        pser = pd.Series([1, -1, -2])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), psser.cumprod(skipna=False))

        # bool
        pser = pd.Series([True, True, False, True])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False).astype(int), psser.cumprod(skipna=False))

        with self.assertRaisesRegex(TypeError, r"Could not convert object \(string\) to numeric"):
            ps.Series(["a", "b", "c", "d"]).cumprod()


class SeriesCumulativeTests(
    SeriesCumulativeMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_cumulative import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
