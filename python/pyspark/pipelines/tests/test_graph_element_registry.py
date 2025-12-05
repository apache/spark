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
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark import pipelines as dp
from pyspark.pipelines.output import Sink
from pyspark.pipelines.tests.local_graph_element_registry import LocalGraphElementRegistry
from typing import cast


class GraphElementRegistryTest(unittest.TestCase):
    def test_graph_element_registry(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):

            @dp.materialized_view
            def mv():
                raise NotImplementedError()

            @dp.table
            def st():
                raise NotImplementedError()

            dp.create_streaming_table("st2")

            @dp.append_flow(target="st2")
            def flow1():
                raise NotImplementedError()

            @dp.append_flow(target="st2")
            def flow2():
                raise NotImplementedError()

            dp.create_sink(
                name="sink",
                format="parquet",
                options={
                    "key1": "value1",
                },
            )

        self.assertEqual(len(registry.outputs), 4)
        self.assertEqual(len(registry.flows), 4)

        mv_obj = registry.outputs[0]
        self.assertEqual(mv_obj.name, "mv")
        assert mv_obj.source_code_location.filename.endswith("test_graph_element_registry.py")

        mv_flow_obj = registry.flows[0]
        self.assertEqual(mv_flow_obj.name, "mv")
        self.assertEqual(mv_flow_obj.target, "mv")
        assert mv_flow_obj.source_code_location.filename.endswith("test_graph_element_registry.py")

        st_obj = registry.outputs[1]
        self.assertEqual(st_obj.name, "st")
        assert st_obj.source_code_location.filename.endswith("test_graph_element_registry.py")

        st_flow_obj = registry.flows[1]
        self.assertEqual(st_flow_obj.name, "st")
        self.assertEqual(st_flow_obj.target, "st")
        assert mv_flow_obj.source_code_location.filename.endswith("test_graph_element_registry.py")

        st2_obj = registry.outputs[2]
        self.assertEqual(st2_obj.name, "st2")
        assert st2_obj.source_code_location.filename.endswith("test_graph_element_registry.py")

        st2_flow1_obj = registry.flows[2]
        self.assertEqual(st2_flow1_obj.name, "flow1")
        self.assertEqual(st2_flow1_obj.target, "st2")
        assert mv_flow_obj.source_code_location.filename.endswith("test_graph_element_registry.py")

        st2_flow1_obj = registry.flows[3]
        self.assertEqual(st2_flow1_obj.name, "flow2")
        self.assertEqual(st2_flow1_obj.target, "st2")
        assert mv_flow_obj.source_code_location.filename.endswith("test_graph_element_registry.py")

        sink_obj = cast(Sink, registry.outputs[3])
        self.assertEqual(sink_obj.name, "sink")
        self.assertEqual(sink_obj.format, "parquet")
        self.assertEqual(sink_obj.options["key1"], "value1")
        assert sink_obj.source_code_location.filename.endswith("test_graph_element_registry.py")

    def test_definition_without_graph_element_registry(self):
        for decorator in [dp.table, dp.temporary_view, dp.materialized_view]:
            with self.assertRaises(PySparkException) as context:

                @decorator
                def a():
                    raise NotImplementedError()

            self.assertEqual(
                context.exception.getCondition(),
                "GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE",
            )

        with self.assertRaises(PySparkException) as context:
            dp.create_streaming_table("st")

        self.assertEqual(
            context.exception.getCondition(),
            "GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE",
        )

        with self.assertRaises(PySparkException) as context:

            @dp.append_flow(target="st")
            def b():
                raise NotImplementedError()

        self.assertEqual(
            context.exception.getCondition(),
            "GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE",
        )


if __name__ == "__main__":
    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
