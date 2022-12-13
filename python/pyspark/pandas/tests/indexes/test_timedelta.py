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

from datetime import timedelta

import pandas as pd

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class TimedeltaIndexTest(PandasOnSparkTestCase, TestUtils):
    @property
    def pidx(self):
        return pd.TimedeltaIndex(
            [
                timedelta(days=1),
                timedelta(seconds=1),
                timedelta(microseconds=1),
                timedelta(milliseconds=1),
                timedelta(minutes=1),
                timedelta(hours=1),
                timedelta(weeks=1),
            ],
            name="x",
        )

    @property
    def neg_pidx(self):
        return pd.TimedeltaIndex(
            [
                timedelta(days=-1),
                timedelta(seconds=-1),
                timedelta(microseconds=-1),
                timedelta(milliseconds=-1),
                timedelta(minutes=-1),
                timedelta(hours=-1),
                timedelta(weeks=-1),
            ],
            name="x",
        )

    @property
    def psidx(self):
        return ps.from_pandas(self.pidx)

    @property
    def neg_psidx(self):
        return ps.from_pandas(self.neg_pidx)

    def test_timedelta_index(self):
        # Create TimedeltaIndex from constructor
        psidx = ps.TimedeltaIndex(
            [
                timedelta(days=1),
                timedelta(seconds=1),
                timedelta(microseconds=1),
                timedelta(milliseconds=1),
                timedelta(minutes=1),
                timedelta(hours=1),
                timedelta(weeks=1),
            ],
            name="x",
        )
        self.assert_eq(psidx, self.pidx)
        # Create TimedeltaIndex from Series
        self.assert_eq(
            ps.TimedeltaIndex(ps.Series([timedelta(days=1)])),
            pd.TimedeltaIndex(pd.Series([timedelta(days=1)])),
        )
        # Create TimedeltaIndex from Index
        self.assert_eq(
            ps.TimedeltaIndex(ps.Index([timedelta(days=1)])),
            pd.TimedeltaIndex(pd.Index([timedelta(days=1)])),
        )

        # ps.TimedeltaIndex(ps.Index([1, 2, 3]))
        with self.assertRaisesRegexp(TypeError, "Index.name must be a hashable type"):
            ps.TimedeltaIndex([timedelta(1), timedelta(microseconds=2)], name=[(1, 2)])
        with self.assertRaisesRegexp(
            TypeError, "Cannot perform 'all' with this index type: TimedeltaIndex"
        ):
            psidx.all()

    def test_properties(self):
        self.assert_eq(self.psidx.days, self.pidx.days)
        self.assert_eq(self.psidx.seconds, self.pidx.seconds)
        self.assert_eq(self.psidx.microseconds, self.pidx.microseconds)
        self.assert_eq(self.neg_psidx.days, self.neg_pidx.days)
        self.assert_eq(self.neg_psidx.seconds, self.neg_pidx.seconds)
        self.assert_eq(self.neg_psidx.microseconds, self.neg_pidx.microseconds)


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.indexes.test_timedelta import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
