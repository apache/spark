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

from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.utils import have_graphviz, graphviz_requirement_message


class SparkConnectDataFrameDebug(ReusedConnectTestCase):
    def test_df_debug_basics(self):
        df = self.spark.range(100).repartition(10).groupBy("id").count()
        x = df.collect()  # noqa: F841
        ei = df.executionInfo

        root, graph = ei.metrics.extract_graph()
        self.assertIn(root, graph, "The root must be rooted in the graph")

    def test_df_quey_execution_empty_before_execution(self):
        df = self.spark.range(100).repartition(10).groupBy("id").count()
        ei = df.executionInfo
        self.assertIsNone(ei, "The query execution must be None before the action is executed")

    def test_df_query_execution_with_writes(self):
        df = self.spark.range(100).repartition(10).groupBy("id").count()
        df.write.save("/tmp/test_df_query_execution_with_writes", format="json", mode="overwrite")
        ei = df.executionInfo
        self.assertIsNotNone(
            ei, "The query execution must be None after the write action is executed"
        )

    def test_query_execution_text_format(self):
        df = self.spark.range(100).repartition(10).groupBy("id").count()
        df.collect()
        self.assertIn("HashAggregate", df.executionInfo.metrics.toText())

        # Different execution mode.
        df = self.spark.range(100).repartition(10).groupBy("id").count()
        df.toPandas()
        self.assertIn("HashAggregate", df.executionInfo.metrics.toText())

    @unittest.skipIf(not have_graphviz, graphviz_requirement_message)
    def test_df_query_execution_metrics_to_dot(self):
        df = self.spark.range(100).repartition(10).groupBy("id").count()
        x = df.collect()  # noqa: F841
        ei = df.executionInfo

        dot = ei.metrics.toDot()
        source = dot.source
        self.assertIsNotNone(dot, "The dot representation must not be None")
        self.assertGreater(len(source), 0, "The dot representation must not be empty")
        self.assertIn("digraph", source, "The dot representation must contain the digraph keyword")
        self.assertIn("Metrics", source, "The dot representation must contain the Metrics keyword")


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_df_debug import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
