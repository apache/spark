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

from pyspark.errors import PySparkException
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.connectutils import (
    ReusedConnectTestCase,
    should_test_connect,
    connect_requirement_message,
)

if should_test_connect:
    from pyspark.pipelines.block_connect_access import block_spark_connect_execution_and_analysis


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class BlockSparkConnectAccessTests(ReusedConnectTestCase):
    def test_create_dataframe_not_blocked(self):
        with block_spark_connect_execution_and_analysis():
            self.spark.createDataFrame([(1,)], ["id"])

    def test_schema_access_blocked(self):
        df = self.spark.createDataFrame([(1,)], ["id"])

        with block_spark_connect_execution_and_analysis():
            with self.assertRaises(PySparkException) as context:
                df.schema
            self.assertEqual(
                context.exception.getCondition(), "ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION"
            )

    def test_collect_blocked(self):
        df = self.spark.createDataFrame([(1,)], ["id"])

        with block_spark_connect_execution_and_analysis():
            with self.assertRaises(PySparkException) as context:
                df.collect()
            self.assertEqual(
                context.exception.getCondition(), "ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION"
            )


if __name__ == "__main__":
    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
