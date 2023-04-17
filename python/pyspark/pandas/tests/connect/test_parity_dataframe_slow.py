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

from pyspark import pandas as ps
from pyspark.pandas.tests.test_dataframe_slow import DataFrameSlowTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils


class DataFrameSlowParityTests(
    DataFrameSlowTestsMixin, PandasOnSparkTestUtils, ReusedConnectTestCase
):
    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_at_time(self):
        super().test_at_time()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_backfill(self):
        super().test_backfill()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_between_time(self):
        super().test_between_time()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_bfill(self):
        super().test_bfill()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_cache(self):
        super().test_cache()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_cov(self):
        super().test_cov()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_diff(self):
        super().test_diff()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_eval(self):
        super().test_eval()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_ffill(self):
        super().test_ffill()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_mode(self):
        super().test_mode()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_pad(self):
        super().test_pad()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_pct_change(self):
        super().test_pct_change()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_persist(self):
        super().test_persist()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_product(self):
        super().test_product()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_rank(self):
        super().test_rank()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_shift(self):
        super().test_shift()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_style(self):
        super().test_style()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udt(self):
        super().test_udt()


if __name__ == "__main__":
    from pyspark.pandas.tests.connect.test_parity_dataframe import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
