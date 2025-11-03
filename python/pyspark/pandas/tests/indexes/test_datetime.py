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

import pandas as pd

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class DatetimeIndexTestingFuncMixin:
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


class DatetimeIndexTestsMixin(DatetimeIndexTestingFuncMixin):
    def test_datetime_index(self):
        with self.assertRaisesRegex(TypeError, "Index.name must be a hashable type"):
            ps.DatetimeIndex(["2004-01-01", "2002-12-31", "2000-04-01"], name=[(1, 2)])
        with self.assertRaisesRegex(
            TypeError, "Cannot perform 'all' with this index type: DatetimeIndex"
        ):
            ps.DatetimeIndex(["2004-01-01", "2002-12-31", "2000-04-01"]).all()

    def test_day_name(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(psidx.day_name(), pidx.day_name())

    def test_month_name(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(psidx.month_name(), pidx.month_name())

    def test_normalize(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(psidx.normalize(), pidx.normalize())

    def test_strftime(self):
        for psidx, pidx in self.idx_pairs:
            self.assert_eq(
                psidx.strftime(date_format="%B %d, %Y"), pidx.strftime(date_format="%B %d, %Y")
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


class DatetimeIndexTests(
    DatetimeIndexTestsMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.indexes.test_datetime import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
