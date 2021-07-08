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

from distutils.version import LooseVersion

import pandas as pd

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class DatetimeIndexTest(PandasOnSparkTestCase, TestUtils):
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

    def _disallow_nanoseconds(self, f):
        self.assertRaises(ValueError, lambda: f(freq="ns"))
        self.assertRaises(ValueError, lambda: f(freq="N"))

    def test_properties(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(psidx.year, pidx.year)
            self.assert_eq(psidx.month, pidx.month)
            self.assert_eq(psidx.day, pidx.day)
            self.assert_eq(psidx.hour, pidx.hour)
            self.assert_eq(psidx.minute, pidx.minute)
            self.assert_eq(psidx.second, pidx.second)
            self.assert_eq(psidx.microsecond, pidx.microsecond)
            self.assert_eq(psidx.week, pidx.week)
            self.assert_eq(psidx.weekofyear, pidx.weekofyear)
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

            if LooseVersion(pd.__version__) >= LooseVersion("1.2.0"):
                self.assert_eq(psidx.day_of_year, pidx.day_of_year)
                self.assert_eq(psidx.day_of_week, pidx.day_of_week)

    def test_ceil(self):
        for psidx, pidx in self.idx_pairs:
            for freq in self.fixed_freqs:
                self.assert_eq(psidx.ceil(freq), pidx.ceil(freq))

        self._disallow_nanoseconds(self.psidxs[0].ceil)

    def test_floor(self):
        for psidx, pidx in self.idx_pairs:
            for freq in self.fixed_freqs:
                self.assert_eq(psidx.floor(freq), pidx.floor(freq))

        self._disallow_nanoseconds(self.psidxs[0].floor)

    def test_round(self):
        for psidx, pidx in self.idx_pairs:
            for freq in self.fixed_freqs:
                self.assert_eq(psidx.round(freq), pidx.round(freq))

        self._disallow_nanoseconds(self.psidxs[0].round)

    def test_day_name(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(psidx.day_name(), pidx.day_name())

    def test_month_name(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(psidx.day_name(), pidx.day_name())

    def test_normalize(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(psidx.normalize(), pidx.normalize())

    def test_strftime(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(
                psidx.strftime(date_format="%B %d, %Y"), pidx.strftime(date_format="%B %d, %Y")
            )

    def test_indexer_between_time(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(
                psidx.indexer_between_time("00:00:00", "00:01:00").sort_values(),
                pd.Index(pidx.indexer_between_time("00:00:00", "00:01:00")),
            )

            self.assert_eq(
                psidx.indexer_between_time(
                    datetime.time(0, 0, 0), datetime.time(0, 1, 0)
                ).sort_values(),
                pd.Index(pidx.indexer_between_time(datetime.time(0, 0, 0), datetime.time(0, 1, 0))),
            )

            self.assert_eq(
                psidx.indexer_between_time("00:00:00", "00:01:00", True, False).sort_values(),
                pd.Index(pidx.indexer_between_time("00:00:00", "00:01:00", True, False)),
            )

            self.assert_eq(
                psidx.indexer_between_time("00:00:00", "00:01:00", False, True).sort_values(),
                pd.Index(pidx.indexer_between_time("00:00:00", "00:01:00", False, True)),
            )

            self.assert_eq(
                psidx.indexer_between_time("00:00:00", "00:01:00", False, False).sort_values(),
                pd.Index(pidx.indexer_between_time("00:00:00", "00:01:00", False, False)),
            )

            self.assert_eq(
                psidx.indexer_between_time("00:00:00", "00:01:00", True, True).sort_values(),
                pd.Index(pidx.indexer_between_time("00:00:00", "00:01:00", True, True)),
            )

    def test_indexer_at_time(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(
                psidx.indexer_at_time("00:00:00").sort_values(),
                pd.Index(pidx.indexer_at_time("00:00:00")),
            )

            self.assert_eq(
                psidx.indexer_at_time(datetime.time(0, 1, 0)).sort_values(),
                pd.Index(pidx.indexer_at_time(datetime.time(0, 1, 0))),
            )

            self.assert_eq(
                psidx.indexer_at_time("00:00:01").sort_values(),
                pd.Index(pidx.indexer_at_time("00:00:01")),
            )

        self.assertRaises(
            NotImplementedError,
            lambda: ps.DatetimeIndex([0]).indexer_at_time("00:00:00", asof=True),
        )

    def test_arithmetic_op_exceptions(self):
        for psidx, pidx in self.idx_pairs:
            py_datetime = pidx.to_pydatetime()
            for other in [1, 0.1, psidx, psidx.to_series().reset_index(drop=True), py_datetime]:
                expected_err_msg = "Addition can not be applied to datetimes."
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: psidx + other)
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: other + psidx)

                expected_err_msg = "Multiplication can not be applied to datetimes."
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: psidx * other)
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: other * psidx)

                expected_err_msg = "True division can not be applied to datetimes."
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: psidx / other)
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: other / psidx)

                expected_err_msg = "Floor division can not be applied to datetimes."
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: psidx // other)
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: other // psidx)

                expected_err_msg = "Modulo can not be applied to datetimes."
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: psidx % other)
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: other % psidx)

            expected_err_msg = "Datetime subtraction can only be applied to datetime series."

            for other in [1, 0.1]:
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: psidx - other)
                self.assertRaisesRegex(TypeError, expected_err_msg, lambda: other - psidx)

            self.assertRaisesRegex(TypeError, expected_err_msg, lambda: psidx - other)
            self.assertRaises(NotImplementedError, lambda: py_datetime - psidx)


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.indexes.test_datetime import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
