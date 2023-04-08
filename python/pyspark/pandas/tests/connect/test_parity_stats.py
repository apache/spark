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

from pyspark.pandas.tests.test_stats import StatsTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils


class StatsParityTests(StatsTestsMixin, PandasOnSparkTestUtils, ReusedConnectTestCase):
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_axis_on_dataframe(self):
        super().test_axis_on_dataframe()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_product(self):
        super().test_product()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_skew_kurt_numerical_stability(self):
        super().test_skew_kurt_numerical_stability()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_stat_functions(self):
        super().test_stat_functions()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_stat_functions_multiindex_column(self):
        super().test_stat_functions_multiindex_column()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_stats_on_boolean_dataframe(self):
        super().test_stats_on_boolean_dataframe()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_stats_on_boolean_series(self):
        super().test_stats_on_boolean_series()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_stats_on_non_numeric_columns_should_be_discarded_if_numeric_only_is_true(self):
        super().test_stats_on_non_numeric_columns_should_be_discarded_if_numeric_only_is_true()


if __name__ == "__main__":
    from pyspark.pandas.tests.connect.test_parity_stats import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
