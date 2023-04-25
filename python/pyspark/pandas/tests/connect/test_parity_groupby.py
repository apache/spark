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

from pyspark.pandas.tests.test_groupby import GroupByTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils, TestUtils


class GroupByParityTests(
    GroupByTestsMixin, PandasOnSparkTestUtils, TestUtils, ReusedConnectTestCase
):
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_apply_with_side_effect(self):
        super().test_apply_with_side_effect()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_basic_stat_funcs(self):
        super().test_basic_stat_funcs()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_bfill(self):
        super().test_bfill()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_cumcount(self):
        super().test_cumcount()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_cummax(self):
        super().test_cummax()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_cummin(self):
        super().test_cummin()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_cumprod(self):
        super().test_cumprod()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_cumsum(self):
        super().test_cumsum()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_ddof(self):
        super().test_ddof()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_ffill(self):
        super().test_ffill()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_fillna(self):
        super().test_fillna()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_prod(self):
        super().test_prod()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_shift(self):
        super().test_shift()


if __name__ == "__main__":
    from pyspark.pandas.tests.connect.test_parity_groupby import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
