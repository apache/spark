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


class SeriesAsOfMixin:
    def test_asof(self):
        pser = pd.Series([1, 2, np.nan, 4], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.asof(20), pser.asof(20))
        self.assert_eq(psser.asof([5, 20]).sort_index(), pser.asof([5, 20]).sort_index())
        self.assert_eq(psser.asof(100), pser.asof(100))
        self.assert_eq(str(psser.asof(-100)), str(pser.asof(-100)))
        self.assert_eq(psser.asof([-100, 100]).sort_index(), pser.asof([-100, 100]).sort_index())

        # where cannot be an Index, Series or a DataFrame
        self.assertRaises(ValueError, lambda: psser.asof(ps.Index([-100, 100])))
        self.assertRaises(ValueError, lambda: psser.asof(ps.Series([-100, 100])))
        self.assertRaises(ValueError, lambda: psser.asof(ps.DataFrame({"A": [1, 2, 3]})))
        # asof is not supported for a MultiIndex
        pser.index = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c"), ("y", "d")])
        psser = ps.from_pandas(pser)
        self.assertRaises(ValueError, lambda: psser.asof(20))
        # asof requires a sorted index (More precisely, should be a monotonic increasing)
        psser = ps.Series([1, 2, np.nan, 4], index=[10, 30, 20, 40], name="Koalas")
        self.assertRaises(ValueError, lambda: psser.asof(20))
        psser = ps.Series([1, 2, np.nan, 4], index=[40, 30, 20, 10], name="Koalas")
        self.assertRaises(ValueError, lambda: psser.asof(20))

        pidx = pd.DatetimeIndex(["2013-12-31", "2014-01-02", "2014-01-03"])
        pser = pd.Series([1, 2, np.nan], index=pidx)
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.asof("2014-01-01"), pser.asof("2014-01-01"))
        self.assert_eq(psser.asof("2014-01-02"), pser.asof("2014-01-02"))
        self.assert_eq(str(psser.asof("1999-01-02")), str(pser.asof("1999-01-02")))

        # SPARK-37482: Skip check monotonic increasing for Series.asof with 'compute.eager_check'
        pser = pd.Series([1, 2, np.nan, 4], index=[10, 30, 20, 40])
        psser = ps.from_pandas(pser)

        with ps.option_context("compute.eager_check", False):
            self.assert_eq(psser.asof(20), 1.0)

        pser = pd.Series([1, 2, np.nan, 4], index=[40, 30, 20, 10])
        psser = ps.from_pandas(pser)

        with ps.option_context("compute.eager_check", False):
            self.assert_eq(psser.asof(20), 4.0)

        pser = pd.Series([2, 1, np.nan, 4], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof([5, 20]), pser.asof([5, 20]))

        pser = pd.Series([4, np.nan, np.nan, 2], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof([5, 100]), pser.asof([5, 100]))

        pser = pd.Series([np.nan, 4, 1, 2], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof([5, 35]), pser.asof([5, 35]))

        pser = pd.Series([2, 1, np.nan, 4], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof([25, 25]), pser.asof([25, 25]))

        pser = pd.Series([2, 1, np.nan, 4], index=["a", "b", "c", "d"], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof(["a", "d"]), pser.asof(["a", "d"]))

        pser = pd.Series(
            [2, 1, np.nan, 4],
            index=[
                pd.Timestamp(2020, 1, 1),
                pd.Timestamp(2020, 2, 2),
                pd.Timestamp(2020, 3, 3),
                pd.Timestamp(2020, 4, 4),
            ],
            name="Koalas",
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(
            psser.asof([pd.Timestamp(2020, 1, 1)]),
            pser.asof([pd.Timestamp(2020, 1, 1)]),
        )

        pser = pd.Series([2, np.nan, 1, 4], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof(np.nan), pser.asof(np.nan))
        self.assert_eq(psser.asof([np.nan, np.nan]), pser.asof([np.nan, np.nan]))
        self.assert_eq(psser.asof([10, np.nan]), pser.asof([10, np.nan]))


class SeriesAsOfTests(
    SeriesAsOfMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_as_of import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
