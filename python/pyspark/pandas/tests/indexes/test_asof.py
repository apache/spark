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
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class IndexesAsOfMixin:
    def test_asof(self):
        # Increasing values
        pidx = pd.Index(["2013-12-31", "2014-01-02", "2014-01-03"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        self.assert_eq(psidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        self.assert_eq(repr(psidx.asof("1999-01-02")), repr(pidx.asof("1999-01-02")))
        self.assert_eq(psidx.asof("2014-01-04"), pidx.asof("2014-01-04"))

        pidx = pd.DatetimeIndex(["2013-12-31", "2014-01-02", "2014-01-03"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        self.assert_eq(psidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        self.assert_eq(repr(psidx.asof("1999-01-02")), repr(pidx.asof("1999-01-02")))

        # Decreasing values
        pidx = pd.Index(["2014-01-03", "2014-01-02", "2013-12-31"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        self.assert_eq(psidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        self.assert_eq(psidx.asof("1999-01-02"), pidx.asof("1999-01-02"))
        self.assert_eq(repr(psidx.asof("2015-01-02")), repr(pidx.asof("2015-01-02")))

        pidx = pd.DatetimeIndex(["2014-01-03", "2014-01-02", "2013-12-31"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        self.assert_eq(psidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        self.assert_eq(psidx.asof("1999-01-02"), pidx.asof("1999-01-02"))
        self.assert_eq(repr(psidx.asof("2015-01-02")), repr(pidx.asof("2015-01-02")))
        self.assert_eq(psidx.asof("2014-01-01"), pd.Timestamp("2014-01-02 00:00:00"))
        self.assert_eq(psidx.asof("2014-01-02"), pd.Timestamp("2014-01-02 00:00:00"))
        self.assert_eq(psidx.asof("1999-01-02"), pd.Timestamp("2013-12-31 00:00:00"))
        self.assert_eq(repr(psidx.asof("2015-01-02")), repr(pd.NaT))

        # Not increasing, neither decreasing (ValueError)
        psidx = ps.Index(["2013-12-31", "2015-01-02", "2014-01-03"])
        self.assertRaises(ValueError, lambda: psidx.asof("2013-12-31"))

        psmidx = ps.MultiIndex.from_tuples([("a", "a"), ("a", "b"), ("a", "c")])
        self.assertRaises(NotImplementedError, lambda: psmidx.asof(("a", "b")))


class IndexesAsOfTests(
    IndexesAsOfMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_asof import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
