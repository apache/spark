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


class SeriesSortMixin:
    def test_sort_values(self):
        pdf = pd.DataFrame({"x": [1, 2, 3, 4, 5, None, 7]}, index=np.random.rand(7))
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x

        self.assert_eq(psser.sort_values(), pser.sort_values())
        self.assert_eq(psser.sort_values(ignore_index=True), pser.sort_values(ignore_index=True))
        self.assert_eq(psser.sort_values(ascending=False), pser.sort_values(ascending=False))
        self.assert_eq(
            psser.sort_values(na_position="first"), pser.sort_values(na_position="first")
        )

        self.assertRaises(ValueError, lambda: psser.sort_values(na_position="invalid"))

        # inplace
        # pandas raises an exception when the Series is derived from DataFrame
        psser.sort_values(inplace=True)
        self.assert_eq(psser, pser.sort_values())
        self.assert_eq(psdf, pdf)

        # pandas raises an exception when the Series is derived from DataFrame
        psser.sort_values(inplace=True, ascending=False, ignore_index=True)
        self.assert_eq(psser, pser.sort_values(ascending=False, ignore_index=True))
        self.assert_eq(psdf, pdf)

        pser = pdf.x.copy()
        psser = psdf.x.copy()

        psser.sort_values(inplace=True)
        pser.sort_values(inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

    def test_sort_index(self):
        pdf = pd.DataFrame({"x": [2, 1, np.nan]}, index=["b", "a", np.nan])
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x

        # Assert invalid parameters
        self.assertRaises(NotImplementedError, lambda: psser.sort_index(axis=1))
        self.assertRaises(NotImplementedError, lambda: psser.sort_index(kind="mergesort"))
        self.assertRaises(ValueError, lambda: psser.sort_index(na_position="invalid"))

        # Assert default behavior without parameters
        self.assert_eq(psser.sort_index(), pser.sort_index())
        # Assert sorting descending
        self.assert_eq(psser.sort_index(ascending=False), pser.sort_index(ascending=False))
        # Assert sorting NA indices first
        self.assert_eq(psser.sort_index(na_position="first"), pser.sort_index(na_position="first"))
        # Assert ignoring index
        self.assert_eq(psser.sort_index(ignore_index=True), pser.sort_index(ignore_index=True))

        # Assert sorting inplace
        # pandas sorts pdf.x by the index and update the column only
        # when the Series is derived from DataFrame.
        psser.sort_index(inplace=True)
        self.assert_eq(psser, pser.sort_index())
        self.assert_eq(psdf, pdf)

        # pandas sorts pdf.x by the index and update the column only
        # when the Series is derived from DataFrame.
        psser.sort_index(inplace=True, ascending=False, ignore_index=True)
        self.assert_eq(psser, pser.sort_index(ascending=False, ignore_index=True))
        self.assert_eq(psdf, pdf)

        pser = pdf.x.copy()
        psser = psdf.x.copy()

        psser.sort_index(inplace=True)
        pser.sort_index(inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        # Assert multi-indices
        pser = pd.Series(range(4), index=[["b", "b", "a", "a"], [1, 0, 1, 0]], name="0")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.sort_index(), pser.sort_index())
        self.assert_eq(psser.sort_index(level=[1, 0]), pser.sort_index(level=[1, 0]))

        self.assert_eq(psser.reset_index().sort_index(), pser.reset_index().sort_index())

    def test_searchsorted(self):
        pser1 = pd.Series([1, 2, 2, 3])

        index2 = pd.date_range("2018-04-09", periods=4, freq="2D")
        pser2 = pd.Series([1, 2, 3, 4], index=index2)

        index3 = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["index1", "index2"]
        )
        pser3 = pd.Series([1.0, 2.0, 3.0], index=index3, name="name")

        pser4 = pd.Series([])

        for pser in [pser1, pser2, pser3, pser4]:
            psser = ps.from_pandas(pser)
            for value in [0.5, 1, 2, 3.0, 4, 5]:
                for side in ["left", "right"]:
                    self.assert_eq(
                        pser.searchsorted(value, side=side),
                        psser.searchsorted(value, side=side),
                    )

        with self.assertRaisesRegex(ValueError, "Invalid side"):
            ps.from_pandas(pser1).searchsorted(1.1, side=[1, 2])
        with self.assertRaisesRegex(ValueError, "Invalid side"):
            ps.from_pandas(pser1).searchsorted(1.1, side="middle")


class SeriesSortTests(
    SeriesSortMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_sort import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
