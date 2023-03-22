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

from pyspark.pandas.tests.test_expanding import ExpandingTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils, TestUtils


class ExpandingParityTests(
    ExpandingTestsMixin, PandasOnSparkTestUtils, TestUtils, ReusedConnectTestCase
):
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_expanding_count(self):
        super().test_expanding_count()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_expanding_kurt(self):
        super().test_expanding_kurt()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_expanding_max(self):
        super().test_expanding_max()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_expanding_mean(self):
        super().test_expanding_mean()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_expanding_min(self):
        super().test_expanding_min()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_expanding_quantile(self):
        super().test_expanding_quantile()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_expanding_skew(self):
        super().test_expanding_skew()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_expanding_std(self):
        super().test_expanding_std()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_expanding_sum(self):
        super().test_expanding_sum()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_expanding_var(self):
        super().test_expanding_var()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_groupby_expanding_count(self):
        super().test_groupby_expanding_count()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_groupby_expanding_kurt(self):
        super().test_groupby_expanding_kurt()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_groupby_expanding_max(self):
        super().test_groupby_expanding_max()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_groupby_expanding_mean(self):
        super().test_groupby_expanding_mean()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_groupby_expanding_min(self):
        super().test_groupby_expanding_min()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_groupby_expanding_quantile(self):
        super().test_groupby_expanding_quantile()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_groupby_expanding_skew(self):
        super().test_groupby_expanding_skew()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_groupby_expanding_std(self):
        super().test_groupby_expanding_std()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_groupby_expanding_sum(self):
        super().test_groupby_expanding_sum()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_groupby_expanding_var(self):
        super().test_groupby_expanding_var()


if __name__ == "__main__":
    from pyspark.pandas.tests.connect.test_parity_expanding import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
