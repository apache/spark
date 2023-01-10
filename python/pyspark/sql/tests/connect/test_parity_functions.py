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

from pyspark.sql.tests.test_functions import FunctionsTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class FunctionsParityTests(FunctionsTestsMixin, ReusedConnectTestCase):
    # TODO(SPARK-41897): Parity in Error types between pyspark and connect functions
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_assert_true(self):
        super().test_assert_true()

    @unittest.skip("Spark Connect does not support Spark Context but the test depends on that.")
    def test_basic_functions(self):
        super().test_basic_functions()

    # TODO(SPARK-41847): DataFrame mapfield,structlist invalid type
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_explode(self):
        super().test_explode()

    @unittest.skip("Spark Connect does not support Spark Context but the test depends on that.")
    def test_function_parity(self):
        super().test_function_parity()

    @unittest.skip(
        "Spark Connect does not support Spark Context, _jdf but the test depends on that."
    )
    def test_functions_broadcast(self):
        super().test_functions_broadcast()

    @unittest.skip("Spark Connect does not support Spark Context but the test depends on that.")
    def test_input_file_name_reset_for_rdd(self):
        super().test_input_file_name_reset_for_rdd()

    # TODO(SPARK-41849): Implement DataFrameReader.text
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_input_file_name_udf(self):
        super().test_input_file_name_udf()

    # TODO(SPARK-41901): Parity in String representation of Column
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_inverse_trig_functions(self):
        super().test_inverse_trig_functions()

    # TODO(SPARK-41834): Implement SparkSession.conf
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_lit_list(self):
        super().test_lit_list()

    # TODO(SPARK-41900): support Data Type int8
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_lit_np_scalar(self):
        super().test_lit_np_scalar()

    # TODO(SPARK-41902): Fix String representation of maps created by `map_from_arrays`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_map_functions(self):
        super().test_map_functions()

    # TODO(SPARK-41903): Support data type ndarray
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_ndarray_input(self):
        super().test_ndarray_input()

    # TODO(SPARK-41902): Parity in String representation of higher_order_function's output
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_nested_higher_order_function(self):
        super().test_nested_higher_order_function()

    # TODO(SPARK-41900): support Data Type int8
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_np_scalar_input(self):
        super().test_np_scalar_input()

    # TODO(SPARK-41901): Parity in String representation of Column
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_overlay(self):
        super().test_overlay()

    # TODO(SPARK-41901): Parity in String representation of Column
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_percentile_approx(self):
        super().test_percentile_approx()

    # TODO(SPARK-41897): Parity in Error types between pyspark and connect functions
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_raise_error(self):
        super().test_raise_error()

    # Comparing column type of connect and pyspark
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_sorting_functions_with_column(self):
        super().test_sorting_functions_with_column()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_functions import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
