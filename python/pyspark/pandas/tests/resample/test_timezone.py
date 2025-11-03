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
import os

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class ResampleTimezoneMixin:
    timezone = None

    @classmethod
    def setUpClass(cls):
        cls.timezone = os.environ.get("TZ", None)
        os.environ["TZ"] = "America/New_York"
        super(ResampleTimezoneMixin, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(ResampleTimezoneMixin, cls).tearDownClass()
        if cls.timezone is not None:
            os.environ["TZ"] = cls.timezone

    @property
    def pdf(self):
        np.random.seed(22)
        index = pd.date_range(start="2011-01-02", end="2022-05-01", freq="1D")
        return pd.DataFrame(np.random.rand(len(index), 2), index=index, columns=list("AB"))

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_series_resample_with_timezone(self):
        with self.sql_conf(
            {
                "spark.sql.session.timeZone": "Asia/Seoul",
                "spark.sql.timestampType": "TIMESTAMP_NTZ",
            }
        ):
            p_resample = self.pdf.resample(rule="1001H", closed="right", label="right")
            ps_resample = self.psdf.resample(rule="1001H", closed="right", label="right")
            self.assert_eq(
                p_resample.sum().sort_index(),
                ps_resample.sum().sort_index(),
                almost=True,
            )


class ResampleTimezoneTests(ResampleTimezoneMixin, PandasOnSparkTestCase, TestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.resample.test_timezone import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
