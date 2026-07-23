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
from typing import cast

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.sql import Column
from pyspark.testing.connectutils import (
    should_test_connect,
    connect_requirement_message,
)

if should_test_connect:
    from pyspark import pipelines as dp
    from pyspark.pipelines.graph_element_registry import graph_element_registration_context
    from pyspark.pipelines.flow import AutoCdcFlow
    from pyspark.pipelines.tests.local_graph_element_registry import LocalGraphElementRegistry
    from pyspark.sql.connect.functions.builtin import col, expr


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class AutoCdcFlowConstructionTest(unittest.TestCase):
    def test_create_auto_cdc_flow(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("target")
            dp.create_auto_cdc_flow(
                target="target",
                source="source",
                keys=[col("key")],
                sequence_by=expr("seq"),
            )

        self.assertEqual(len(registry.outputs), 1)
        self.assertEqual(len(registry.auto_cdc_flows), 1)

        flow = cast(AutoCdcFlow, registry.auto_cdc_flows[0])
        self.assertEqual(flow.target, "target")
        self.assertEqual(flow.source, "source")

        # When name is not specified, it inherits the target's name at construction time.
        self.assertEqual(flow.name, "target")
        self.assertIsNone(flow.stored_as_scd_type)
        self.assertIsNone(flow.apply_as_deletes)
        assert flow.source_code_location.filename.endswith("test_auto_cdc_flow.py")

    def test_create_auto_cdc_flow_with_all_args(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("tgt")
            dp.create_auto_cdc_flow(
                target="tgt",
                source="src",
                keys=[col("id")],
                sequence_by=expr("ts"),
                apply_as_deletes=expr("op = 'DELETE'"),
                column_list=[col("id"), col("val")],
                stored_as_scd_type=1,
                name="my_flow",
            )

        flow = cast(AutoCdcFlow, registry.auto_cdc_flows[0])
        self.assertEqual(flow.name, "my_flow")
        self.assertEqual(flow.stored_as_scd_type, 1)

    def test_create_auto_cdc_flow_with_string_args(self):
        # Verify that string forms of column / expression arguments are normalized to
        # PySpark Columns, equivalent to passing col(...) / expr(...) directly.
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("tgt")
            dp.create_auto_cdc_flow(
                target="tgt",
                source="src",
                keys=["id"],
                sequence_by="ts",
                apply_as_deletes="op = 'DELETE'",
                column_list=["id", "val"],
            )

        flow = cast(AutoCdcFlow, registry.auto_cdc_flows[0])
        for k in flow.keys:
            self.assertIsInstance(k, Column)
        self.assertIsInstance(flow.sequence_by, Column)
        self.assertIsInstance(flow.apply_as_deletes, Column)
        assert flow.column_list is not None
        for c in flow.column_list:
            self.assertIsInstance(c, Column)

    def test_create_auto_cdc_flow_stored_as_scd_type_string(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("t")
            dp.create_auto_cdc_flow(
                target="t",
                source="s",
                keys=[col("k")],
                sequence_by=expr("seq"),
                stored_as_scd_type="1",
            )

        flow = cast(AutoCdcFlow, registry.auto_cdc_flows[0])
        self.assertEqual(flow.stored_as_scd_type, "1")

    def test_create_auto_cdc_flow_stored_as_scd_type_2(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("t")
            dp.create_auto_cdc_flow(
                target="t",
                source="s",
                keys=[col("k")],
                sequence_by=expr("seq"),
                stored_as_scd_type=2,
            )

        flow = cast(AutoCdcFlow, registry.auto_cdc_flows[0])
        self.assertEqual(flow.stored_as_scd_type, 2)

    def test_create_auto_cdc_flow_stored_as_scd_type_2_string(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("t")
            dp.create_auto_cdc_flow(
                target="t",
                source="s",
                keys=[col("k")],
                sequence_by=expr("seq"),
                stored_as_scd_type="2",
            )

        flow = cast(AutoCdcFlow, registry.auto_cdc_flows[0])
        self.assertEqual(flow.stored_as_scd_type, "2")

    def test_create_auto_cdc_flow_with_track_history_column_list(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("t")
            dp.create_auto_cdc_flow(
                target="t",
                source="s",
                keys=[col("k")],
                sequence_by=expr("seq"),
                stored_as_scd_type=2,
                track_history_column_list=["val"],
            )

        flow = cast(AutoCdcFlow, registry.auto_cdc_flows[0])
        assert flow.track_history_column_list is not None
        self.assertEqual(len(flow.track_history_column_list), 1)
        self.assertIsInstance(flow.track_history_column_list[0], Column)
        self.assertIsNone(flow.track_history_except_column_list)

    def test_create_auto_cdc_flow_with_track_history_except_column_list(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("t")
            dp.create_auto_cdc_flow(
                target="t",
                source="s",
                keys=[col("k")],
                sequence_by=expr("seq"),
                stored_as_scd_type=2,
                track_history_except_column_list=["op", "seq"],
            )

        flow = cast(AutoCdcFlow, registry.auto_cdc_flows[0])
        assert flow.track_history_except_column_list is not None
        self.assertEqual(len(flow.track_history_except_column_list), 2)
        self.assertIsNone(flow.track_history_column_list)

    def test_create_auto_cdc_flow_invalid_scd_type(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("t")
            with self.assertRaises(PySparkTypeError) as ctx:
                dp.create_auto_cdc_flow(
                    target="t",
                    source="s",
                    keys=[col("k")],
                    sequence_by=expr("seq"),
                    stored_as_scd_type=3,  # type: ignore[arg-type]
                )
            self.assertEqual(ctx.exception.getCondition(), "NOT_EXPECTED_TYPE")

    def test_create_auto_cdc_flow_track_history_requires_scd2(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("t")
            with self.assertRaises(PySparkValueError) as ctx:
                dp.create_auto_cdc_flow(
                    target="t",
                    source="s",
                    keys=[col("k")],
                    sequence_by=expr("seq"),
                    stored_as_scd_type=1,
                    track_history_column_list=["val"],
                )
            self.assertEqual(ctx.exception.getCondition(), "INVALID_MULTIPLE_ARGUMENT_CONDITIONS")

    def test_create_auto_cdc_flow_empty_track_history_list_is_accepted_without_scd2(self):
        # An empty list serializes identically to an omitted one (an unset repeated field), so it
        # is a no-op and must not trip the "track history requires SCD2" check.
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("t")
            dp.create_auto_cdc_flow(
                target="t",
                source="s",
                keys=[col("k")],
                sequence_by=expr("seq"),
                track_history_column_list=[],
                track_history_except_column_list=[],
            )

        flow = cast(AutoCdcFlow, registry.auto_cdc_flows[0])
        self.assertEqual(flow.track_history_column_list, [])
        self.assertEqual(flow.track_history_except_column_list, [])

    def test_create_auto_cdc_flow_with_except_column_list(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("tgt")
            dp.create_auto_cdc_flow(
                target="tgt",
                source="src",
                keys=[col("id")],
                sequence_by=expr("ts"),
                except_column_list=["op", "ts"],
            )

        flow = cast(AutoCdcFlow, registry.auto_cdc_flows[0])
        self.assertIsNone(flow.column_list)
        assert flow.except_column_list is not None
        self.assertEqual(len(flow.except_column_list), 2)
        for c in flow.except_column_list:
            self.assertIsInstance(c, Column)

    def test_create_auto_cdc_flow_rejects_both_column_list_and_except(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("tgt")
            with self.assertRaises(PySparkValueError) as ctx:
                dp.create_auto_cdc_flow(
                    target="tgt",
                    source="src",
                    keys=[col("id")],
                    sequence_by=expr("ts"),
                    column_list=["id"],
                    except_column_list=["op"],
                )
            self.assertEqual(ctx.exception.getCondition(), "CANNOT_SET_TOGETHER")

    def test_create_auto_cdc_flow_rejects_both_track_history_column_list_and_except(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("tgt")
            with self.assertRaises(PySparkValueError) as ctx:
                dp.create_auto_cdc_flow(
                    target="tgt",
                    source="src",
                    keys=[col("id")],
                    sequence_by=expr("ts"),
                    stored_as_scd_type=2,
                    track_history_column_list=["val"],
                    track_history_except_column_list=["op"],
                )
            self.assertEqual(ctx.exception.getCondition(), "CANNOT_SET_TOGETHER")

    def test_create_auto_cdc_flow_empty_column_lists_not_treated_as_both_set(self):
        # Empty lists are no-ops (they serialize to unset repeated fields), so an empty include
        # list alongside a non-empty except list must not trip the mutual-exclusivity check.
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("tgt")
            dp.create_auto_cdc_flow(
                target="tgt",
                source="src",
                keys=[col("id")],
                sequence_by=expr("ts"),
                column_list=[],
                except_column_list=["op"],
            )

        flow = cast(AutoCdcFlow, registry.auto_cdc_flows[0])
        self.assertEqual(flow.column_list, [])
        assert flow.except_column_list is not None
        self.assertEqual(len(flow.except_column_list), 1)

    def test_create_auto_cdc_flow_rejects_non_str_target(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("tgt")
            with self.assertRaises(PySparkTypeError) as ctx:
                dp.create_auto_cdc_flow(
                    target=123,  # type: ignore[arg-type]
                    source="src",
                    keys=[col("id")],
                    sequence_by=expr("ts"),
                )
            self.assertEqual(ctx.exception.getCondition(), "NOT_EXPECTED_TYPE")

    def test_create_auto_cdc_flow_rejects_invalid_key_element(self):
        registry = LocalGraphElementRegistry()
        with graph_element_registration_context(registry):
            dp.create_streaming_table("tgt")
            with self.assertRaises(PySparkTypeError) as ctx:
                dp.create_auto_cdc_flow(
                    target="tgt",
                    source="src",
                    keys=[123],  # type: ignore[list-item]
                    sequence_by=expr("ts"),
                )
            self.assertEqual(ctx.exception.getCondition(), "NOT_EXPECTED_TYPE")

    def test_create_auto_cdc_flow_without_registry(self):
        with self.assertRaises(PySparkRuntimeError) as context:
            dp.create_auto_cdc_flow(
                target="t",
                source="s",
                keys=["k"],
                sequence_by="seq",
            )

        self.assertEqual(
            context.exception.getCondition(),
            "GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE",
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
