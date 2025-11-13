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

from pyspark.testing.connectutils import (
    ReusedConnectTestCase,
    should_test_connect,
    connect_requirement_message,
)

if should_test_connect:
    from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class AddPipelineAnalysisContextTests(ReusedConnectTestCase):
    def test_add_pipeline_analysis_context_with_flow_name(self):
        with add_pipeline_analysis_context(self.spark, "test_dataflow_graph_id", "test_flow_name"):
            import pyspark.sql.connect.proto as pb2

            thread_local_extensions = self.spark.client.thread_local.user_context_extensions
            self.assertEqual(len(thread_local_extensions), 1)
            # Extension is stored as (id, extension), unpack the extension
            _extension_id, extension = thread_local_extensions[0]
            context = pb2.PipelineAnalysisContext()
            extension.Unpack(context)
            self.assertEqual(context.dataflow_graph_id, "test_dataflow_graph_id")
            self.assertEqual(context.flow_name, "test_flow_name")
        thread_local_extensions_after = self.spark.client.thread_local.user_context_extensions
        self.assertEqual(len(thread_local_extensions_after), 0)

    def test_add_pipeline_analysis_context_without_flow_name(self):
        with add_pipeline_analysis_context(self.spark, "test_dataflow_graph_id", None):
            import pyspark.sql.connect.proto as pb2

            thread_local_extensions = self.spark.client.thread_local.user_context_extensions
            self.assertEqual(len(thread_local_extensions), 1)
            # Extension is stored as (id, extension), unpack the extension
            _extension_id, extension = thread_local_extensions[0]
            context = pb2.PipelineAnalysisContext()
            extension.Unpack(context)
            self.assertEqual(context.dataflow_graph_id, "test_dataflow_graph_id")
            # Empty string means no flow name
            self.assertEqual(context.flow_name, "")
        thread_local_extensions_after = self.spark.client.thread_local.user_context_extensions
        self.assertEqual(len(thread_local_extensions_after), 0)

    def test_nested_add_pipeline_analysis_context(self):
        import pyspark.sql.connect.proto as pb2

        with add_pipeline_analysis_context(self.spark, "test_dataflow_graph_id_1", flow_name=None):
            with add_pipeline_analysis_context(
                self.spark, "test_dataflow_graph_id_2", flow_name="test_flow_name"
            ):
                thread_local_extensions = self.spark.client.thread_local.user_context_extensions
                self.assertEqual(len(thread_local_extensions), 2)
                # Extension is stored as (id, extension), unpack the extensions
                _, extension_1 = thread_local_extensions[0]
                context_1 = pb2.PipelineAnalysisContext()
                extension_1.Unpack(context_1)
                self.assertEqual(context_1.dataflow_graph_id, "test_dataflow_graph_id_1")
                self.assertEqual(context_1.flow_name, "")
                _, extension_2 = thread_local_extensions[1]
                context_2 = pb2.PipelineAnalysisContext()
                extension_2.Unpack(context_2)
                self.assertEqual(context_2.dataflow_graph_id, "test_dataflow_graph_id_2")
                self.assertEqual(context_2.flow_name, "test_flow_name")
            thread_local_extensions_after_1 = self.spark.client.thread_local.user_context_extensions
            self.assertEqual(len(thread_local_extensions_after_1), 1)
            _, extension_3 = thread_local_extensions_after_1[0]
            context_3 = pb2.PipelineAnalysisContext()
            extension_3.Unpack(context_3)
            self.assertEqual(context_3.dataflow_graph_id, "test_dataflow_graph_id_1")
            self.assertEqual(context_3.flow_name, "")
        thread_local_extensions_after_2 = self.spark.client.thread_local.user_context_extensions
        self.assertEqual(len(thread_local_extensions_after_2), 0)


if __name__ == "__main__":
    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
