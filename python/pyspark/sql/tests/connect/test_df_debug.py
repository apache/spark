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
import os
import unittest

from pyspark.sql.metrics import DataDebugOp
from pyspark.testing.connectutils import (
    should_test_connect,
    have_graphviz,
    graphviz_requirement_message,
    ReusedConnectTestCase,
)

if should_test_connect:
    from pyspark.sql.connect.dataframe import DataFrame


class SparkConnectDataFrameDebug(ReusedConnectTestCase):
    def test_df_debug_basics(self):
        df: DataFrame = self.spark.range(100).repartition(10).groupBy("id").count()
        x = df.collect()  # noqa: F841
        ei = df.executionInfo

        root, graph = ei.metrics.extract_graph()
        self.assertIn(root, graph, "The root must be rooted in the graph")

    def test_df_quey_execution_empty_before_execution(self):
        df: DataFrame = self.spark.range(100).repartition(10).groupBy("id").count()
        ei = df.executionInfo
        self.assertIsNone(ei, "The query execution must be None before the action is executed")

    def test_df_query_execution_with_writes(self):
        df: DataFrame = self.spark.range(100).repartition(10).groupBy("id").count()
        df.write.save("/tmp/test_df_query_execution_with_writes", format="json", mode="overwrite")
        ei = df.executionInfo
        self.assertIsNotNone(
            ei, "The query execution must be None after the write action is executed"
        )

    def test_query_execution_text_format(self):
        df: DataFrame = self.spark.range(100).repartition(10).groupBy("id").count()
        df.collect()
        self.assertIn("HashAggregate", df.executionInfo.metrics.toText())

        # Different execution mode.
        df: DataFrame = self.spark.range(100).repartition(10).groupBy("id").count()
        df.toPandas()
        self.assertIn("HashAggregate", df.executionInfo.metrics.toText())

    @unittest.skipIf(not have_graphviz, graphviz_requirement_message)
    def test_df_query_execution_metrics_to_dot(self):
        df: DataFrame = self.spark.range(100).repartition(10).groupBy("id").count()
        df.collect()
        ei = df.executionInfo

        dot = ei.metrics.toDot()
        source = dot.source
        self.assertIsNotNone(dot, "The dot representation must not be None")
        self.assertGreater(len(source), 0, "The dot representation must not be empty")
        self.assertIn("digraph", source, "The dot representation must contain the digraph keyword")
        self.assertIn("Metrics", source, "The dot representation must contain the Metrics keyword")

    def test_query_with_debug(self):
        self.assertIn("SPARK_TESTING", os.environ, "SPARK_TESTING must be set to run this test")

        df: DataFrame = self.spark.range(100).repartition(10).groupBy("id").count()
        df = df.debug()
        df.collect()
        ei = df.executionInfo
        self.assertIsNotNone(
            ei.observations, "Observations must be present when debug has been used"
        )

        # Check that the call site contains this function in the top stack element
        observations = ei.observations

        # THe stack should contain call_site_stack (0), df.debug() (1), test_query_with_debug (2)
        stack = observations[0]._call_site[2]
        self.assertIn(
            "test_query_with_debug",
            stack,
            (
                "The call site must contain this function: stack "
                ";".join(observations[0]._call_site)
            ),
        )

    def test_query_with_debug_and_plan_id_order(self):
        a: DataFrame = self.spark.range(100).repartition(10).groupBy("id").count()
        a = a.debug()
        b: DataFrame = self.spark.range(1000)
        b = b.filter(b.id < 10).debug()
        c: DataFrame = a.join(b, "id")
        c = c.debug()
        c.collect()

        ei = c.executionInfo
        self.assertIsNotNone(ei, "The query execution must be set after the action is executed")
        self.assertEqual(
            len(ei.observations),
            3,
            "The number of observations must be equal to the number of debug",
        )

        # Check the count values for all observations
        o1 = ei.observations[0]
        o2 = ei.observations[1]
        o3 = ei.observations[2]

        self.assertEqual(o1.get["count_values"], 100, "The count values must be 100")
        self.assertEqual(o2.get["count_values"], 10, "The count values must be 10 after filter")
        self.assertEqual(o3.get["count_values"], 10, "The count values must be 10 after join")

    def test_query_with_debug_and_other_ops(self):
        a: DataFrame = self.spark.range(100).repartition(10).groupBy("id").count()
        b = a.debug(
            DataDebugOp.count_distinct_values("id"),
            DataDebugOp.max_value("id"),
            DataDebugOp.min_value("id"),
        )
        b.collect()
        ei = b.executionInfo
        self.assertIsNotNone(ei, "The query execution must be set after the action is executed")
        self.assertEqual(
            len(ei.observations),
            1,
            "The number of observations must be equal to the number of debug observations",
        )

        o1 = ei.observations[0]  # count_values
        self.assertEqual(
            4, len(o1.get.values()), "Four metrics were collected (count, min, max. distinct)"
        )
        self.assertEqual(o1.get["count_values"], 100, "The count values must be 100")
        self.assertGreater(
            o1.get["count_distinct_values_id"],
            90,
            "The approx count distinct values must be roughly",
        )
        self.assertEqual(o1.get["max_value_id"], 99, "The max value must be 99")
        self.assertEqual(o1.get["min_value_id"], 0, "The min value must be 0")


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_df_debug import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
