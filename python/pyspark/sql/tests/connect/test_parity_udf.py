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

from pyspark.testing.connectutils import should_test_connect

if should_test_connect:
    from pyspark import sql
    from pyspark.sql.connect.udf import UserDefinedFunction

    sql.udf.UserDefinedFunction = UserDefinedFunction

from pyspark.sql.tests.test_udf import BaseUDFTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.sql.types import IntegerType


class UDFParityTests(BaseUDFTestsMixin, ReusedConnectTestCase):
    @unittest.skip("Spark Connect does not support mapPartitions() but the test depends on it.")
    def test_worker_original_stdin_closed(self):
        super().test_worker_original_stdin_closed()

    @unittest.skip(
        "Spark Connect does not support reading from Hadoop RDD but the test depends on it."
    )
    def test_udf_with_input_file_name_for_hadooprdd(self):
        super().test_udf_with_input_file_name_for_hadooprdd()

    @unittest.skip("Spark Connect does not support accumulator but the test depends on it.")
    def test_same_accumulator_in_udfs(self):
        super().test_same_accumulator_in_udfs()

    @unittest.skip("Spark Connect does not support spark.conf but the test depends on it.")
    def test_udf_with_column_vector(self):
        super().test_udf_with_column_vector()

    @unittest.skip("Spark Connect does not support spark.conf but the test depends on it.")
    def test_udf_timestamp_ntz(self):
        super().test_udf_timestamp_ntz()

    @unittest.skip("Spark Connect does not support broadcast but the test depends on it.")
    def test_broadcast_in_udf(self):
        super().test_broadcast_in_udf()

    @unittest.skip("Spark Connect does not support sql_conf but the test depends on it.")
    def test_file_dsv2_with_udf_filter(self):
        super().test_file_dsv2_with_udf_filter()

    @unittest.skip("Spark Connect does not support sql_conf but the test depends on it.")
    def test_udf_in_join_condition(self):
        super().test_udf_in_join_condition()

    @unittest.skip("Spark Connect does not support cache() but the test depends on it.")
    def test_udf_cache(self):
        super().test_udf_cache()

    @unittest.skip("Requires JVM access.")
    def test_udf_defers_judf_initialization(self):
        super().test_udf_defers_judf_initialization()

    @unittest.skip("Requires JVM access.")
    def test_nondeterministic_udf3(self):
        super().test_nondeterministic_udf3()

    @unittest.skip("Requires JVM access.")
    def test_nondeterministic_udf_in_aggregate(self):
        super().test_nondeterministic_udf_in_aggregate()

    @unittest.skip("Requires JVM access.")
    def test_udf_registration_return_type_not_none(self):
        super().test_udf_registration_return_type_not_none()

    # TODO(SPARK-42263): implement `spark.catalog.registerFunction`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_worker_original_stdin_closed(self):
        super().test_worker_original_stdin_closed()

    # TODO(SPARK-42263): implement `spark.catalog.registerFunction`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_chained_udf(self):
        super().test_chained_udf()

    # TODO(SPARK-42263): implement `spark.catalog.registerFunction`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udf_without_arguments(self):
        super().test_udf_without_arguments()

    # TODO(SPARK-42263): implement `spark.catalog.registerFunction`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_multiple_udfs(self):
        super().test_multiple_udfs()

    # TODO(SPARK-42263): implement `spark.catalog.registerFunction`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_nondeterministic_udf2(self):
        super().test_nondeterministic_udf2()

    # TODO(SPARK-42263): implement `spark.catalog.registerFunction`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_single_udf_with_repeated_argument(self):
        super().test_single_udf_with_repeated_argument()

    # TODO(SPARK-42263): implement `spark.catalog.registerFunction`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udf(self):
        super().test_df()

    # TODO(SPARK-42263): implement `spark.catalog.registerFunction`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udf2(self):
        super().test_df2()

    # TODO(SPARK-42263): implement `spark.catalog.registerFunction`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udf3(self):
        super().test_df3()

    # TODO(SPARK-42263): implement `spark.catalog.registerFunction`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udf_registration_return_type_none(self):
        super().test_udf_registration_return_type_none()

    # TODO(SPARK-42263): implement `spark.catalog.registerFunction`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udf_with_array_type(self):
        super().test_udf_with_array_type()

    # TODO(SPARK-42210): implement `spark.udf`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_non_existed_udaf(self):
        super().test_non_existed_udaf()

    # TODO(SPARK-42210): implement `spark.udf`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_non_existed_udf(self):
        super().test_non_existed_udf()

    # TODO(SPARK-42210): implement `spark.udf`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_register_java_function(self):
        super().test_register_java_function()

    # TODO(SPARK-42210): implement `spark.udf`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_register_java_udaf(self):
        super().test_register_java_udaf()

    # TODO(SPARK-42210): implement `spark.udf`
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udf_in_left_outer_join_condition(self):
        super().test_udf_in_left_outer_join_condition()

    # TODO(SPARK-42269): support return type as a collection DataType in DDL strings
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udf_with_string_return_type(self):
        super().test_udf_with_string_return_type()

    # TODO(SPARK-41279): fix DataFrame.createTempView
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udf_in_subquery(self):
        super().test_udf_in_subquery()

    def test_udf_registration_returns_udf(self):
        df = self.spark.range(10)
        add_three = self.spark.udf.register("add_three", lambda x: x + 3, IntegerType())

        self.assertListEqual(
            df.selectExpr("add_three(id) AS plus_three").collect(),
            df.select(add_three("id").alias("plus_three")).collect(),
        )


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_udf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
