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

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.indexes.test_datetime import DatetimeIndexTestingFuncMixin


class DatetimeIndexAtMixin(DatetimeIndexTestingFuncMixin):
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


class DatetimeIndexAtTests(
    DatetimeIndexAtMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.indexes.test_datetime_at import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
