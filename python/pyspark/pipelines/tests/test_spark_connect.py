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

"""
Tests that run Pipelines against a Spark Connect server.
"""

import unittest

from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import (
    SparkConnectGraphElementRegistry,
)
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph,
    start_run,
    handle_pipeline_events,
)
from pyspark import pipelines as sdp
from pyspark.testing.connectutils import (
    ReusedConnectTestCase,
    should_test_connect,
    connect_requirement_message,
)


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectPipelinesTest(ReusedConnectTestCase):
    def test_dry_run(self):
        dataflow_graph_id = create_dataflow_graph(self.spark, None, None, None)
        registry = SparkConnectGraphElementRegistry(self.spark, dataflow_graph_id)

        with graph_element_registration_context(registry):

            @sdp.materialized_view
            def mv():
                return self.spark.range(1)

        result_iter = start_run(
            self.spark,
            dataflow_graph_id,
            full_refresh=None,
            refresh=None,
            full_refresh_all=False,
            dry=True,
        )
        handle_pipeline_events(result_iter)

    def test_dry_run_failure(self):
        dataflow_graph_id = create_dataflow_graph(self.spark, None, None, None)
        registry = SparkConnectGraphElementRegistry(self.spark, dataflow_graph_id)

        with graph_element_registration_context(registry):

            @sdp.table
            def st():
                # Invalid because a streaming query is expected
                return self.spark.range(1)

        result_iter = start_run(
            self.spark,
            dataflow_graph_id,
            full_refresh=None,
            refresh=None,
            full_refresh_all=False,
            dry=True,
        )
        with self.assertRaises(AnalysisException) as context:
            handle_pipeline_events(result_iter)
        self.assertIn(
            "INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE", str(context.exception)
        )


if __name__ == "__main__":
    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
