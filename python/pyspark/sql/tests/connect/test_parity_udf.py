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

if should_test_connect:  # test_udf_with_partial_function
    from pyspark import sql
    from pyspark.sql.connect.udf import UserDefinedFunction

    sql.udf.UserDefinedFunction = UserDefinedFunction

from pyspark.sql.tests.test_udf import BaseUDFTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.errors.exceptions import SparkConnectAnalysisException
from pyspark.sql.connect.functions import udf
from pyspark.sql.types import BooleanType


class UDFParityTests(BaseUDFTestsMixin, ReusedConnectTestCase):
    @unittest.skip("Relies on mapPartitions().")
    def test_worker_original_stdin_closed(self):
        super().test_worker_original_stdin_closed()

    @unittest.skip("Relies on registerFunction().")
    def test_worker_original_stdin_closed(self):
        super().test_worker_original_stdin_closed()

    @unittest.skip("Relies on read fomr Hadoop RDD.")
    def test_udf_with_input_file_name_for_hadooprdd(self):
        super().test_udf_with_input_file_name_for_hadooprdd()

    @unittest.skip("Relies on accumulator")
    def test_same_accumulator_in_udfs(self):
        super().test_same_accumulator_in_udfs()

    @unittest.skip("Relies on spark.conf")
    def test_udf_with_column_vector(self):
        super().test_udf_with_column_vector()

    @unittest.skip("Relies on spark.conf")
    def test_udf_timestamp_ntz(self):
        super().test_udf_timestamp_ntz()

    @unittest.skip("Relies on broadcast")
    def test_broadcast_in_udf(self):
        super().test_broadcast_in_udf()

    @unittest.skip("Relies on spark.catalog.registerFunction")
    def test_chained_udf(self):
        super().test_chained_udf()

    @unittest.skip("Relies on spark.catalog.registerFunction")
    def test_udf_without_arguments(self):
        super().test_udf_without_arguments()

    @unittest.skip("Relies on spark.catalog.registerFunction")
    def test_multiple_udfs(self):
        super().test_multiple_udfs()

    @unittest.skip("Relies on spark.catalog.registerFunction")
    def test_nondeterministic_udf2(self):
        super().test_nondeterministic_udf2()

    @unittest.skip("Relies on spark.catalog.registerFunction")
    def test_single_udf_with_repeated_argument(self):
        super().test_single_udf_with_repeated_argument()

    @unittest.skip("Relies on spark.catalog.registerFunction")
    def test_udf(self):
        super().test_df()

    @unittest.skip("Relies on spark.catalog.registerFunction")
    def test_udf2(self):
        super().test_df2()

    @unittest.skip("Relies on spark.catalog.registerFunction")
    def test_udf3(self):
        super().test_df3()

    @unittest.skip("Relies on spark.catalog.registerFunction")
    def test_udf_registration_return_type_none(self):
        super().test_udf_registration_return_type_none()

    @unittest.skip("Relies on spark.catalog.registerFunction")
    def test_udf_with_array_type(self):
        super().test_udf_with_array_type()

    @unittest.skip("Relies on sql_conf")
    def test_file_dsv2_with_udf_filter(self):
        super().test_file_dsv2_with_udf_filter()

    @unittest.skip("Relies on spark.udf")
    def test_non_existed_udaf(self):
        super().test_non_existed_udaf()

    @unittest.skip("Relies on spark.udf")
    def test_non_existed_udf(self):
        super().test_non_existed_udf()

    @unittest.skip("Relies on spark.udf")
    def test_udf_registration_returns_udf(self):
        super().test_udf_registration_returns_udf()

    @unittest.skip("Relies on cache()")
    def test_udf_cache(self):
        super().test_udf_cache()

    @unittest.skip("Relies on UserDefinedFunction._judf_placeholder")
    def test_udf_defers_judf_initialization(self):
        super().test_udf_defers_judf_initialization()

    @unittest.skip("Relies on left_outer join type.")
    def test_udf_in_left_outer_join_condition(self):
        super().test_udf_in_left_outer_join_condition()

    @unittest.skip("Relies on left_outer join type.")
    def test_udf_in_filter_on_top_of_outer_join(self):
        super().test_udf_in_filter_on_top_of_outer_join()

    @unittest.skip("Relies on sql_conf.")
    def test_udf_in_join_condition(self):
        super().test_udf_in_join_condition()

    @unittest.skip("Relies on df._jdf")
    def test_nondeterministic_udf3(self):
        super().test_nondeterministic_udf3()

    @unittest.skip("Relies on sc._jvm.org.apache.log4j")
    def test_nondeterministic_udf_in_aggregate(self):
        super().test_nondeterministic_udf_in_aggregate()

    @unittest.skip("Relies on sc._jvm.org.apache.log4j")
    def test_udf_registration_return_type_not_none(self):
        super().test_udf_registration_return_type_not_none()

    @unittest.skip("Parse array<double>")
    def test_udf_with_string_return_type(self):
        super().test_udf_with_string_return_type()

    # spark.range(1).filter(udf(lambda x: x)("id") >= 0).createTempView("v")
    @unittest.skip("SparkConnectGrpcException: requirement failed")
    def test_udf_in_subquery(self):
        super().test_udf_in_subquery()

    def test_udf_not_supported_in_join_condition(self):
        # test python udf is not supported in join type except inner join.
        left = self.spark.createDataFrame([(1, 1, 1), (2, 2, 2)], ["a", "a1", "a2"])
        right = self.spark.createDataFrame([(1, 1, 1), (1, 3, 1)], ["b", "b1", "b2"])
        f = udf(lambda a, b: a == b, BooleanType())

        def runWithJoinType(join_type, type_string):
            with self.assertRaisesRegex(
                SparkConnectAnalysisException,
                """Python UDF in the ON clause of a %s JOIN.""" % type_string,
            ):
                left.join(right, [f("a", "b"), left.a1 == right.b1], join_type).collect()

        runWithJoinType("full", "FULL OUTER")
        runWithJoinType("left", "LEFT OUTER")
        runWithJoinType("right", "RIGHT OUTER")
        runWithJoinType("leftanti", "LEFT ANTI")
        runWithJoinType("leftsemi", "LEFT SEMI")


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_udf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
