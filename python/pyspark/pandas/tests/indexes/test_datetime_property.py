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

import datetime
import unittest

import pandas as pd

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class DatetimeIndexPropertyTestsMixin:
    @property
    def fixed_freqs(self):
        return [
            "D",
            "H",
            "T",  # min
            "S",
            "L",  # ms
            "U",  # us
            # 'N' not supported
        ]

    @property
    def non_fixed_freqs(self):
        return ["W", "Q"]

    @property
    def pidxs(self):
        return [
            pd.DatetimeIndex([0]),
            pd.DatetimeIndex(["2004-01-01", "2002-12-31", "2000-04-01"]),
        ] + [
            pd.date_range("2000-01-01", periods=3, freq=freq)
            for freq in (self.fixed_freqs + self.non_fixed_freqs)
        ]

    @property
    def psidxs(self):
        return [ps.from_pandas(pidx) for pidx in self.pidxs]

    @property
    def idx_pairs(self):
        return list(zip(self.psidxs, self.pidxs))

    def test_properties(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(psidx.year, pidx.year)
            self.assert_eq(psidx.month, pidx.month)
            self.assert_eq(psidx.day, pidx.day)
            self.assert_eq(psidx.hour, pidx.hour)
            self.assert_eq(psidx.minute, pidx.minute)
            self.assert_eq(psidx.second, pidx.second)
            self.assert_eq(psidx.microsecond, pidx.microsecond)
            self.assert_eq(psidx.dayofweek, pidx.dayofweek)
            self.assert_eq(psidx.weekday, pidx.weekday)
            self.assert_eq(psidx.dayofyear, pidx.dayofyear)
            self.assert_eq(psidx.quarter, pidx.quarter)
            self.assert_eq(psidx.daysinmonth, pidx.daysinmonth)
            self.assert_eq(psidx.days_in_month, pidx.days_in_month)
            self.assert_eq(psidx.is_month_start, pd.Index(pidx.is_month_start))
            self.assert_eq(psidx.is_month_end, pd.Index(pidx.is_month_end))
            self.assert_eq(psidx.is_quarter_start, pd.Index(pidx.is_quarter_start))
            self.assert_eq(psidx.is_quarter_end, pd.Index(pidx.is_quarter_end))
            self.assert_eq(psidx.is_year_start, pd.Index(pidx.is_year_start))
            self.assert_eq(psidx.is_year_end, pd.Index(pidx.is_year_end))
            self.assert_eq(psidx.is_leap_year, pd.Index(pidx.is_leap_year))
            self.assert_eq(psidx.day_of_year, pidx.day_of_year)
            self.assert_eq(psidx.day_of_week, pidx.day_of_week)

        # TODO(SPARK-42617): Support isocalendar.week and replace it.
        expected_results = [
            ps.Index([1]),
            ps.Index([1, 1, 13]),
            ps.Index([52, 52, 1]),
            ps.Index([52, 52, 52]),
            ps.Index([52, 52, 52]),
            ps.Index([52, 52, 52]),
            ps.Index([52, 52, 52]),
            ps.Index([52, 52, 52]),
            ps.Index([52, 1, 2]),
            ps.Index([13, 26, 39]),
        ]
        for psidx, expected_result in zip(self.psidxs, expected_results):
            self.assert_eq(psidx.week, expected_result)
            self.assert_eq(psidx.weekofyear, expected_result)


class DatetimeIndexPropertyTests(DatetimeIndexPropertyTestsMixin, PandasOnSparkTestCase, TestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.indexes.test_datetime_property import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
