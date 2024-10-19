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


class FrameCumulativeMixin:
    def _test_cummin(self, pdf, psdf):
        self.assert_eq(pdf.cummin(), psdf.cummin())
        self.assert_eq(pdf.cummin(skipna=False), psdf.cummin(skipna=False))
        self.assert_eq(pdf.cummin().sum(), psdf.cummin().sum())

    def test_cummin(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0], [5, None], [1.0, 0.0], [2.0, 4.0], [4.0, 9.0]],
            columns=list("AB"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cummin(pdf, psdf)

    def test_cummin_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cummin(pdf, psdf)

    def _test_cummax(self, pdf, psdf):
        self.assert_eq(pdf.cummax(), psdf.cummax())
        self.assert_eq(pdf.cummax(skipna=False), psdf.cummax(skipna=False))
        self.assert_eq(pdf.cummax().sum(), psdf.cummax().sum())

    def test_cummax(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0], [5, None], [1.0, 0.0], [2.0, 4.0], [4.0, 9.0]],
            columns=list("AB"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cummax(pdf, psdf)

    def test_cummax_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cummax(pdf, psdf)

    def _test_cumsum(self, pdf, psdf):
        self.assert_eq(pdf.cumsum(), psdf.cumsum())
        self.assert_eq(pdf.cumsum(skipna=False), psdf.cumsum(skipna=False))
        self.assert_eq(pdf.cumsum().sum(), psdf.cumsum().sum())

    def test_cumsum(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0], [5, None], [1.0, 0.0], [2.0, 4.0], [4.0, 9.0]],
            columns=list("AB"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cumsum(pdf, psdf)

    def test_cumsum_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cumsum(pdf, psdf)

    def _test_cumprod(self, pdf, psdf):
        self.assert_eq(pdf.cumprod(), psdf.cumprod(), almost=True)
        self.assert_eq(pdf.cumprod(skipna=False), psdf.cumprod(skipna=False), almost=True)
        self.assert_eq(pdf.cumprod().sum(), psdf.cumprod().sum(), almost=True)

    def test_cumprod(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0, 1], [5, None, 2], [1.0, -1.0, -3], [2.0, 0, 4], [4.0, 9.0, 5]],
            columns=list("ABC"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cumprod(pdf, psdf)

    def test_cumprod_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.rand(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cumprod(pdf, psdf)


class FrameCumulativeTests(
    FrameCumulativeMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_cumulative import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
