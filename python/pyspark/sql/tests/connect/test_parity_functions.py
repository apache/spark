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

from pyspark.sql import SparkSession
from pyspark.sql.tests.test_functions import FunctionsTestsMixin
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.sqlutils import ReusedSQLTestCase


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class FunctionsParityTests(ReusedSQLTestCase, FunctionsTestsMixin):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql.connect.session import SparkSession as RemoteSparkSession

        super(FunctionsParityTests, cls).setUpClass()
        cls._spark = cls.spark  # Assign existing Spark session to run the server
        # Sets the remote address. Now, we create a remote Spark Session.
        # Note that this is only allowed in testing.
        os.environ["SPARK_REMOTE"] = "sc://localhost"
        cls.spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
        assert isinstance(cls.spark, RemoteSparkSession)

    @classmethod
    def tearDownClass(cls):
        # TODO(SPARK-41529): Implement stop in RemoteSparkSession.
        #  Stop the regular Spark session (server) too.
        cls.spark = cls._spark
        super(FunctionsParityTests, cls).tearDownClass()
        del os.environ["SPARK_REMOTE"]

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_add_months_function(self):
        super().test_add_months_function()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_array_repeat(self):
        super().test_array_repeat()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_assert_true(self):
        super().test_assert_true()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_basic_functions(self):
        super().test_basic_functions()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_between_function(self):
        super().test_between_function()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_date_add_function(self):
        super().test_date_add_function()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_date_sub_function(self):
        super().test_date_sub_function()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_datetime_functions(self):
        super().test_datetime_functions()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_dayofweek(self):
        super().test_dayofweek()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_explode(self):
        super().test_explode()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_expr(self):
        super().test_expr()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_first_last_ignorenulls(self):
        super().test_first_last_ignorenulls()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_function_parity(self):
        super().test_function_parity()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_functions_broadcast(self):
        super().test_functions_broadcast()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_inline(self):
        super().test_inline()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_input_file_name_reset_for_rdd(self):
        super().test_input_file_name_reset_for_rdd()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_input_file_name_udf(self):
        super().test_input_file_name_udf()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_inverse_trig_functions(self):
        super().test_inverse_trig_functions()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_lit_list(self):
        super().test_lit_list()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_lit_np_scalar(self):
        super().test_lit_np_scalar()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_map_functions(self):
        super().test_map_functions()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_math_functions(self):
        super().test_math_functions()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_ndarray_input(self):
        super().test_ndarray_input()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_nested_higher_order_function(self):
        super().test_nested_higher_order_function()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_np_scalar_input(self):
        super().test_np_scalar_input()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_nth_value(self):
        super().test_nth_value()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_overlay(self):
        super().test_overlay()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_percentile_approx(self):
        super().test_percentile_approx()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_raise_error(self):
        super().test_raise_error()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_slice(self):
        super().test_slice()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_sorting_functions_with_column(self):
        super().test_sorting_functions_with_column()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_window_functions(self):
        super().test_window_functions()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_window_functions_cumulative_sum(self):
        super().test_window_functions_cumulative_sum()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_window_functions_without_partitionBy(self):
        super().test_window_functions_without_partitionBy()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_window_time(self):
        super().test_window_time()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_corr(self):
        super().test_corr()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_cov(self):
        super().test_cov()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_crosstab(self):
        super().test_crosstab()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_rand_functions(self):
        super().test_rand_functions()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_reciprocal_trig_functions(self):
        super().test_reciprocal_trig_functions()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_sampleby(self):
        super().test_sampleby()

    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_approxQuantile(self):
        super().test_approxQuantile()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_functions import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
