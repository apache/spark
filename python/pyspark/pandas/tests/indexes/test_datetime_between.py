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

import pandas as pd

from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.indexes.test_datetime import DatetimeIndexTestingFuncMixin


class DatetimeIndexBetweenMixin(DatetimeIndexTestingFuncMixin):
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


class DatetimeIndexBetweenTests(
    DatetimeIndexBetweenMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.indexes.test_datetime_between import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
