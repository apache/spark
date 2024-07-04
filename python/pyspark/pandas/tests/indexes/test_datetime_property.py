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
import numpy as np
import pandas as pd

from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.indexes.test_datetime import DatetimeIndexTestingFuncMixin


class DatetimeIndexPropertyTestsMixin(DatetimeIndexTestingFuncMixin):
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
            self.assert_eq(psidx.isocalendar().week, pidx.isocalendar().week.astype(np.int64))


class DatetimeIndexPropertyTests(
    DatetimeIndexPropertyTestsMixin,
    PandasOnSparkTestCase,
):
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
