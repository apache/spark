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
    def test_udf_timestamp_ntz(self):
        super().test_udf_timestamp_ntz()

    @unittest.skip("Spark Connect does not support broadcast but the test depends on it.")
    def test_broadcast_in_udf(self):
        super().test_broadcast_in_udf()

    @unittest.skip("Requires JVM access.")
    def test_udf_defers_judf_initialization(self):
        super().test_udf_defers_judf_initialization()

    @unittest.skip("Requires JVM access.")
    def test_nondeterministic_udf3(self):
        super().test_nondeterministic_udf3()

    @unittest.skip("Spark Connect doesn't support RDD but the test depends on it.")
    def test_worker_original_stdin_closed(self):
        super().test_worker_original_stdin_closed()

    @unittest.skip("Spark Connect does not support SQLContext but the test depends on it.")
    def test_udf_on_sql_context(self):
        super().test_udf_on_sql_context()

    @unittest.skip("Spark Connect does not support SQLContext but the test depends on it.")
    def test_non_existed_udf_with_sql_context(self):
        super().test_non_existed_udf_with_sql_context()

    @unittest.skip("Spark Connect does not support SQLContext but the test depends on it.")
    def test_udf_registration_returns_udf_on_sql_context(self):
        super().test_udf_registration_returns_udf_on_sql_context()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_udf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
