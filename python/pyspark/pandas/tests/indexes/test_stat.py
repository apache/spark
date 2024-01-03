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


class StatMixin:
    def test_len(self):
        pidx = pd.Index(range(10000))
        psidx = ps.from_pandas(pidx)

        self.assert_eq(len(pidx), len(psidx))

        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])

        self.assert_eq(len(pidx), len(psidx))

    def test_abs(self):
        pidx = pd.Index([-2, -1, 0, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(abs(pidx), abs(psidx))
        self.assert_eq(np.abs(pidx), np.abs(psidx))

        psidx = ps.MultiIndex.from_tuples([(1, 2)], names=["level1", "level2"])
        with self.assertRaisesRegex(TypeError, "perform __abs__ with this index"):
            abs(psidx)

    def test_argmin(self):
        pidx = pd.Index([100, 50, 10, 20, 30, 60, 0, 50, 0, 100, 100, 100, 20, 0, 0])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.argmin(), psidx.argmin())

        # MultiIndex
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        with self.assertRaisesRegex(
            TypeError, "reduction operation 'argmin' not allowed for this dtype"
        ):
            psidx.argmin()

    def test_argmax(self):
        pidx = pd.Index([100, 50, 10, 20, 30, 60, 0, 50, 0, 100, 100, 100, 20, 0, 0])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.argmax(), psidx.argmax())

        # MultiIndex
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        with self.assertRaisesRegex(
            TypeError, "reduction operation 'argmax' not allowed for this dtype"
        ):
            psidx.argmax()

    def test_min(self):
        pidx = pd.Index([3, 2, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.min(), psidx.min())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.min(), psmidx.min())

        pidx = pd.DatetimeIndex(["2021-02-01", "2021-01-01", "2021-04-01", "2021-03-01"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.min(), psidx.min())

    def test_max(self):
        pidx = pd.Index([3, 2, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.max(), psidx.max())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.max(), psmidx.max())

        pidx = pd.DatetimeIndex(["2021-02-01", "2021-01-01", "2021-04-01", "2021-03-01"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.max(), psidx.max())

    def test_hasnans(self):
        # BooleanType
        pidx = pd.Index([True, False, True, True])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.hasnans, psidx.hasnans)

        pidx = pd.Index([True, False, np.nan, True])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.hasnans, psidx.hasnans)

        # TimestampType
        pser = pd.Series([pd.Timestamp("2020-07-30") for _ in range(3)])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

        pser = pd.Series([pd.Timestamp("2020-07-30"), np.nan, pd.Timestamp("2020-07-30")])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

        # empty
        pidx = pd.Index([])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.hasnans, psidx.hasnans)

        # Not supported for MultiIndex
        psmidx = ps.Index([("a", 1), ("b", 2)])
        self.assertRaises(NotImplementedError, lambda: psmidx.hasnans())

    def test_index_fillna(self):
        pidx = pd.Index([1, 2, None])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.fillna(0), psidx.fillna(0), almost=True)
        self.assert_eq(pidx.rename("name").fillna(0), psidx.rename("name").fillna(0), almost=True)

        with self.assertRaisesRegex(TypeError, "Unsupported type list"):
            psidx.fillna([1, 2])

    def test_multiindex_isna(self):
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])

        with self.assertRaisesRegex(NotImplementedError, "isna is not defined for MultiIndex"):
            psidx.isna()

        with self.assertRaisesRegex(NotImplementedError, "isna is not defined for MultiIndex"):
            psidx.isnull()

        with self.assertRaisesRegex(NotImplementedError, "notna is not defined for MultiIndex"):
            psidx.notna()

        with self.assertRaisesRegex(NotImplementedError, "notna is not defined for MultiIndex"):
            psidx.notnull()


class StatTests(
    StatMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_stat import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
