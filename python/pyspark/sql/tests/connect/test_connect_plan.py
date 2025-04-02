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
import uuid
import datetime
import decimal
import math

from pyspark.testing.connectutils import (
    PlanOnlyTestFixture,
    should_test_connect,
    connect_requirement_message,
)
from pyspark.errors import PySparkValueError

if should_test_connect:
    import pyspark.sql.connect.proto as proto
    from pyspark.sql.connect.column import Column
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect.plan import WriteOperation, Read
    from pyspark.sql.connect.readwriter import DataFrameReader
    from pyspark.sql.connect.expressions import LiteralExpression
    from pyspark.sql.connect.functions import col, lit, max, min, sum
    from pyspark.sql.connect.types import pyspark_types_to_proto_types
    from pyspark.sql.types import (
        StringType,
        StructType,
        StructField,
        IntegerType,
        MapType,
        ArrayType,
        DoubleType,
    )


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectPlanTests(PlanOnlyTestFixture):
    """These test cases exercise the interface to the proto plan
    generation but do not call Spark."""

    def test_sql_project(self):
        plan = self.connect.sql("SELECT 1")._plan.to_proto(self.connect)
        self.assertEqual(plan.root.sql.query, "SELECT 1")

    def test_simple_project(self):
        plan = self.connect.readTable(table_name=self.tbl_name)._plan.to_proto(self.connect)
        self.assertIsNotNone(plan.root, "Root relation must be set")
        self.assertIsNotNone(plan.root.read)

    def test_join_using_columns(self):
        left_input = self.connect.readTable(table_name=self.tbl_name)
        right_input = self.connect.readTable(table_name=self.tbl_name)
        plan = left_input.join(other=right_input, on="join_column")._plan.to_proto(self.connect)
        self.assertEqual(len(plan.root.join.using_columns), 1)

        plan2 = left_input.join(other=right_input, on=["col1", "col2"])._plan.to_proto(self.connect)
        self.assertEqual(len(plan2.root.join.using_columns), 2)

    def test_join_condition(self):
        left_input = self.connect.readTable(table_name=self.tbl_name)
        right_input = self.connect.readTable(table_name=self.tbl_name)
        plan = left_input.join(
            other=right_input, on=left_input.name == right_input.name
        )._plan.to_proto(self.connect)
        self.assertIsNotNone(plan.root.join.join_condition)
        plan = left_input.join(
            other=right_input,
            on=[left_input.name == right_input.name, left_input.age == right_input.age],
        )._plan.to_proto(self.connect)
        self.assertIsNotNone(plan.root.join.join_condition)

    def test_crossjoin(self):
        # SPARK-41227: Test CrossJoin
        left_input = self.connect.readTable(table_name=self.tbl_name)
        right_input = self.connect.readTable(table_name=self.tbl_name)
        crossJoin_plan = left_input.crossJoin(other=right_input)._plan.to_proto(self.connect)
        join_plan = left_input.join(other=right_input, how="cross")._plan.to_proto(self.connect)
        self.assertEqual(
            crossJoin_plan.root.join.left.read.named_table,
            join_plan.root.join.left.read.named_table,
        )
        self.assertEqual(
            crossJoin_plan.root.join.right.read.named_table,
            join_plan.root.join.right.read.named_table,
        )
        self.assertEqual(
            crossJoin_plan.root.join.join_type,
            join_plan.root.join.join_type,
        )

    def test_filter(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.filter(df.col_name > 3)._plan.to_proto(self.connect)
        self.assertIsNotNone(plan.root.filter)
        self.assertTrue(
            isinstance(
                plan.root.filter.condition.unresolved_function, proto.Expression.UnresolvedFunction
            )
        )
        self.assertEqual(plan.root.filter.condition.unresolved_function.function_name, ">")
        self.assertEqual(len(plan.root.filter.condition.unresolved_function.arguments), 2)

    def test_filter_with_string_expr(self):
        """SPARK-41297: filter supports SQL expression"""
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.filter("id < 10")._plan.to_proto(self.connect)
        self.assertIsNotNone(plan.root.filter)
        self.assertIsNotNone(plan.root.filter.condition.expression_string)
        self.assertEqual(plan.root.filter.condition.expression_string.expression, "id < 10")

    def test_fill_na(self):
        # SPARK-41128: Test fill na
        df = self.connect.readTable(table_name=self.tbl_name)

        plan = df.fillna(value=1)._plan.to_proto(self.connect)
        self.assertEqual(len(plan.root.fill_na.values), 1)
        self.assertEqual(plan.root.fill_na.values[0].long, 1)
        self.assertEqual(plan.root.fill_na.cols, [])

        plan = df.na.fill(value="xyz")._plan.to_proto(self.connect)
        self.assertEqual(len(plan.root.fill_na.values), 1)
        self.assertEqual(plan.root.fill_na.values[0].string, "xyz")
        self.assertEqual(plan.root.fill_na.cols, [])

        plan = df.na.fill(value="xyz", subset=["col_a", "col_b"])._plan.to_proto(self.connect)
        self.assertEqual(len(plan.root.fill_na.values), 1)
        self.assertEqual(plan.root.fill_na.values[0].string, "xyz")
        self.assertEqual(plan.root.fill_na.cols, ["col_a", "col_b"])

        plan = df.na.fill(value=True, subset=("col_a", "col_b", "col_c"))._plan.to_proto(
            self.connect
        )
        self.assertEqual(len(plan.root.fill_na.values), 1)
        self.assertEqual(plan.root.fill_na.values[0].boolean, True)
        self.assertEqual(plan.root.fill_na.cols, ["col_a", "col_b", "col_c"])

        plan = df.fillna({"col_a": 1.5, "col_b": "abc"})._plan.to_proto(self.connect)
        self.assertEqual(len(plan.root.fill_na.values), 2)
        self.assertEqual(plan.root.fill_na.values[0].double, 1.5)
        self.assertEqual(plan.root.fill_na.values[1].string, "abc")
        self.assertEqual(plan.root.fill_na.cols, ["col_a", "col_b"])

    def test_drop_na(self):
        # SPARK-41148: Test drop na
        df = self.connect.readTable(table_name=self.tbl_name)

        plan = df.dropna()._plan.to_proto(self.connect)
        self.assertEqual(plan.root.drop_na.cols, [])
        self.assertEqual(plan.root.drop_na.HasField("min_non_nulls"), False)

        plan = df.na.drop(thresh=2, subset=("col_a", "col_b"))._plan.to_proto(self.connect)
        self.assertEqual(plan.root.drop_na.cols, ["col_a", "col_b"])
        self.assertEqual(plan.root.drop_na.min_non_nulls, 2)

        plan = df.dropna(how="all", subset="col_c")._plan.to_proto(self.connect)
        self.assertEqual(plan.root.drop_na.cols, ["col_c"])
        self.assertEqual(plan.root.drop_na.min_non_nulls, 1)

    def test_replace(self):
        # SPARK-41315: Test replace
        df = self.connect.readTable(table_name=self.tbl_name)

        plan = df.replace(10, 20)._plan.to_proto(self.connect)
        self.assertEqual(plan.root.replace.cols, [])
        self.assertEqual(plan.root.replace.replacements[0].old_value.double, 10.0)
        self.assertEqual(plan.root.replace.replacements[0].new_value.double, 20.0)

        plan = df.na.replace((1, 2, 3), (4, 5, 6), subset=("col_a", "col_b"))._plan.to_proto(
            self.connect
        )
        self.assertEqual(plan.root.replace.cols, ["col_a", "col_b"])
        self.assertEqual(plan.root.replace.replacements[0].old_value.double, 1.0)
        self.assertEqual(plan.root.replace.replacements[0].new_value.double, 4.0)
        self.assertEqual(plan.root.replace.replacements[1].old_value.double, 2.0)
        self.assertEqual(plan.root.replace.replacements[1].new_value.double, 5.0)
        self.assertEqual(plan.root.replace.replacements[2].old_value.double, 3.0)
        self.assertEqual(plan.root.replace.replacements[2].new_value.double, 6.0)

        plan = df.replace(["Alice", "Bob"], ["A", "B"], subset="col_x")._plan.to_proto(self.connect)
        self.assertEqual(plan.root.replace.cols, ["col_x"])
        self.assertEqual(plan.root.replace.replacements[0].old_value.string, "Alice")
        self.assertEqual(plan.root.replace.replacements[0].new_value.string, "A")
        self.assertEqual(plan.root.replace.replacements[1].old_value.string, "Bob")
        self.assertEqual(plan.root.replace.replacements[1].new_value.string, "B")

    def test_unpivot(self):
        df = self.connect.readTable(table_name=self.tbl_name)

        plan = (
            df.filter(df.col_name > 3)
            .unpivot(["id"], ["name"], "variable", "value")
            ._plan.to_proto(self.connect)
        )
        self.assertTrue(all(isinstance(c, proto.Expression) for c in plan.root.unpivot.ids))
        self.assertEqual(plan.root.unpivot.ids[0].unresolved_attribute.unparsed_identifier, "id")
        self.assertEqual(plan.root.unpivot.HasField("values"), True)
        self.assertTrue(
            all(isinstance(c, proto.Expression) for c in plan.root.unpivot.values.values)
        )
        self.assertEqual(
            plan.root.unpivot.values.values[0].unresolved_attribute.unparsed_identifier, "name"
        )
        self.assertEqual(plan.root.unpivot.variable_column_name, "variable")
        self.assertEqual(plan.root.unpivot.value_column_name, "value")

        plan = (
            df.filter(df.col_name > 3)
            .unpivot(["id"], None, "variable", "value")
            ._plan.to_proto(self.connect)
        )
        self.assertTrue(len(plan.root.unpivot.ids) == 1)
        self.assertTrue(all(isinstance(c, proto.Expression) for c in plan.root.unpivot.ids))
        self.assertEqual(plan.root.unpivot.ids[0].unresolved_attribute.unparsed_identifier, "id")
        self.assertEqual(plan.root.unpivot.HasField("values"), False)
        self.assertEqual(plan.root.unpivot.variable_column_name, "variable")
        self.assertEqual(plan.root.unpivot.value_column_name, "value")

    def test_melt(self):
        df = self.connect.readTable(table_name=self.tbl_name)

        plan = (
            df.filter(df.col_name > 3)
            .melt(["id"], ["name"], "variable", "value")
            ._plan.to_proto(self.connect)
        )
        self.assertTrue(all(isinstance(c, proto.Expression) for c in plan.root.unpivot.ids))
        self.assertEqual(plan.root.unpivot.ids[0].unresolved_attribute.unparsed_identifier, "id")
        self.assertEqual(plan.root.unpivot.HasField("values"), True)
        self.assertTrue(
            all(isinstance(c, proto.Expression) for c in plan.root.unpivot.values.values)
        )
        self.assertEqual(
            plan.root.unpivot.values.values[0].unresolved_attribute.unparsed_identifier, "name"
        )
        self.assertEqual(plan.root.unpivot.variable_column_name, "variable")
        self.assertEqual(plan.root.unpivot.value_column_name, "value")

        plan = (
            df.filter(df.col_name > 3)
            .melt(["id"], [], "variable", "value")
            ._plan.to_proto(self.connect)
        )
        self.assertTrue(len(plan.root.unpivot.ids) == 1)
        self.assertTrue(all(isinstance(c, proto.Expression) for c in plan.root.unpivot.ids))
        self.assertEqual(plan.root.unpivot.ids[0].unresolved_attribute.unparsed_identifier, "id")
        self.assertEqual(plan.root.unpivot.HasField("values"), True)
        self.assertTrue(len(plan.root.unpivot.values.values) == 0)
        self.assertEqual(plan.root.unpivot.variable_column_name, "variable")
        self.assertEqual(plan.root.unpivot.value_column_name, "value")

    def test_random_split(self):
        # SPARK-41440: test randomSplit(weights, seed).
        from typing import List

        df = self.connect.readTable(table_name=self.tbl_name)

        def checkRelations(relations: List["DataFrame"]):
            self.assertTrue(len(relations) == 3)

            plan = relations[0]._plan.to_proto(self.connect)
            self.assertEqual(plan.root.sample.lower_bound, 0.0)
            self.assertEqual(plan.root.sample.upper_bound, 0.16666666666666666)
            self.assertEqual(plan.root.sample.with_replacement, False)
            self.assertEqual(plan.root.sample.HasField("seed"), True)
            self.assertEqual(plan.root.sample.deterministic_order, True)

            plan = relations[1]._plan.to_proto(self.connect)
            self.assertEqual(plan.root.sample.lower_bound, 0.16666666666666666)
            self.assertEqual(plan.root.sample.upper_bound, 0.5)
            self.assertEqual(plan.root.sample.with_replacement, False)
            self.assertEqual(plan.root.sample.HasField("seed"), True)
            self.assertEqual(plan.root.sample.deterministic_order, True)

            plan = relations[2]._plan.to_proto(self.connect)
            self.assertEqual(plan.root.sample.lower_bound, 0.5)
            self.assertEqual(plan.root.sample.upper_bound, 1.0)
            self.assertEqual(plan.root.sample.with_replacement, False)
            self.assertEqual(plan.root.sample.HasField("seed"), True)
            self.assertEqual(plan.root.sample.deterministic_order, True)

        relations = df.filter(df.col_name > 3).randomSplit([1.0, 2.0, 3.0], 1)
        checkRelations(relations)

        relations = df.filter(df.col_name > 3).randomSplit([1.0, 2.0, 3.0])
        checkRelations(relations)

    def test_observe(self):
        # SPARK-41527: test DataFrame.observe()
        df = self.connect.readTable(table_name=self.tbl_name)

        plan = (
            df.filter(df.col_name > 3)
            .observe("my_metric", min("id"), max("id"), sum("id"))
            ._plan.to_proto(self.connect)
        )
        self.assertEqual(plan.root.collect_metrics.name, "my_metric")
        self.assertTrue(
            all(isinstance(c, proto.Expression) for c in plan.root.collect_metrics.metrics)
        )
        self.assertEqual(
            plan.root.collect_metrics.metrics[0].unresolved_function.function_name, "min"
        )
        self.assertTrue(
            len(plan.root.collect_metrics.metrics[0].unresolved_function.arguments) == 1
        )
        self.assertTrue(
            all(
                isinstance(c, proto.Expression)
                for c in plan.root.collect_metrics.metrics[0].unresolved_function.arguments
            )
        )
        self.assertEqual(
            plan.root.collect_metrics.metrics[0]
            .unresolved_function.arguments[0]
            .unresolved_attribute.unparsed_identifier,
            "id",
        )

        from pyspark.sql.connect.observation import Observation

        class MockDF(DataFrame):
            def __new__(cls, df: DataFrame) -> "DataFrame":
                self = object.__new__(cls)
                self.__init__(df)  # type: ignore[misc]
                return self

            def __init__(self, df: DataFrame):
                super().__init__(df._plan, df._session)

            @property
            def isStreaming(self) -> bool:
                return False

        plan = (
            MockDF(df.filter(df.col_name > 3))
            .observe(Observation("my_metric"), min("id"), max("id"), sum("id"))
            ._plan.to_proto(self.connect)
        )
        self.assertEqual(plan.root.collect_metrics.name, "my_metric")
        self.assertTrue(
            all(isinstance(c, proto.Expression) for c in plan.root.collect_metrics.metrics)
        )
        self.assertEqual(
            plan.root.collect_metrics.metrics[0].unresolved_function.function_name, "min"
        )
        self.assertTrue(
            len(plan.root.collect_metrics.metrics[0].unresolved_function.arguments) == 1
        )
        self.assertTrue(
            all(
                isinstance(c, proto.Expression)
                for c in plan.root.collect_metrics.metrics[0].unresolved_function.arguments
            )
        )
        self.assertEqual(
            plan.root.collect_metrics.metrics[0]
            .unresolved_function.arguments[0]
            .unresolved_attribute.unparsed_identifier,
            "id",
        )

    def test_summary(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.filter(df.col_name > 3).summary()._plan.to_proto(self.connect)
        self.assertEqual(plan.root.summary.statistics, [])

        plan = (
            df.filter(df.col_name > 3)
            .summary("count", "mean", "stddev", "min", "25%")
            ._plan.to_proto(self.connect)
        )
        self.assertEqual(
            plan.root.summary.statistics,
            ["count", "mean", "stddev", "min", "25%"],
        )

    def test_describe(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.filter(df.col_name > 3).describe()._plan.to_proto(self.connect)
        self.assertEqual(plan.root.describe.cols, [])

        plan = df.filter(df.col_name > 3).describe("col_a", "col_b")._plan.to_proto(self.connect)
        self.assertEqual(
            plan.root.describe.cols,
            ["col_a", "col_b"],
        )

    def test_crosstab(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.filter(df.col_name > 3).crosstab("col_a", "col_b")._plan.to_proto(self.connect)
        self.assertEqual(plan.root.crosstab.col1, "col_a")
        self.assertEqual(plan.root.crosstab.col2, "col_b")

        plan = df.stat.crosstab("col_a", "col_b")._plan.to_proto(self.connect)
        self.assertEqual(plan.root.crosstab.col1, "col_a")
        self.assertEqual(plan.root.crosstab.col2, "col_b")

    def test_freqItems(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = (
            df.filter(df.col_name > 3).freqItems(["col_a", "col_b"], 1)._plan.to_proto(self.connect)
        )
        self.assertEqual(plan.root.freq_items.cols, ["col_a", "col_b"])
        self.assertEqual(plan.root.freq_items.support, 1)
        plan = df.filter(df.col_name > 3).freqItems(["col_a", "col_b"])._plan.to_proto(self.connect)
        self.assertEqual(plan.root.freq_items.cols, ["col_a", "col_b"])
        self.assertEqual(plan.root.freq_items.support, 0.01)

        plan = df.stat.freqItems(["col_a", "col_b"], 1)._plan.to_proto(self.connect)
        self.assertEqual(plan.root.freq_items.cols, ["col_a", "col_b"])
        self.assertEqual(plan.root.freq_items.support, 1)
        plan = df.stat.freqItems(["col_a", "col_b"])._plan.to_proto(self.connect)
        self.assertEqual(plan.root.freq_items.cols, ["col_a", "col_b"])
        self.assertEqual(plan.root.freq_items.support, 0.01)

    def test_limit(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        limit_plan = df.limit(10)._plan.to_proto(self.connect)
        self.assertEqual(limit_plan.root.limit.limit, 10)

    def test_offset(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        offset_plan = df.offset(10)._plan.to_proto(self.connect)
        self.assertEqual(offset_plan.root.offset.offset, 10)

    def test_sample(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.filter(df.col_name > 3).sample(fraction=0.3)._plan.to_proto(self.connect)
        self.assertEqual(plan.root.sample.lower_bound, 0.0)
        self.assertEqual(plan.root.sample.upper_bound, 0.3)
        self.assertEqual(plan.root.sample.with_replacement, False)
        self.assertEqual(plan.root.sample.HasField("seed"), True)
        self.assertEqual(plan.root.sample.deterministic_order, False)

        plan = (
            df.filter(df.col_name > 3)
            .sample(withReplacement=True, fraction=0.4, seed=-1)
            ._plan.to_proto(self.connect)
        )
        self.assertEqual(plan.root.sample.lower_bound, 0.0)
        self.assertEqual(plan.root.sample.upper_bound, 0.4)
        self.assertEqual(plan.root.sample.with_replacement, True)
        self.assertEqual(plan.root.sample.seed, -1)
        self.assertEqual(plan.root.sample.deterministic_order, False)

    def test_sort(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.filter(df.col_name > 3).sort("col_a", "col_b")._plan.to_proto(self.connect)
        self.assertEqual(
            [f.child.unresolved_attribute.unparsed_identifier for f in plan.root.sort.order],
            ["col_a", "col_b"],
        )
        self.assertEqual(plan.root.sort.is_global, True)
        self.assertEqual(
            plan.root.sort.order[0].direction,
            proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
        )
        self.assertEqual(
            plan.root.sort.order[0].direction,
            proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST,
        )
        self.assertEqual(
            plan.root.sort.order[1].direction,
            proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
        )
        self.assertEqual(
            plan.root.sort.order[1].direction,
            proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST,
        )

        plan = df.filter(df.col_name > 3).orderBy("col_a", "col_b")._plan.to_proto(self.connect)
        self.assertEqual(
            [f.child.unresolved_attribute.unparsed_identifier for f in plan.root.sort.order],
            ["col_a", "col_b"],
        )
        self.assertEqual(plan.root.sort.is_global, True)

        plan = (
            df.filter(df.col_name > 3)
            .sortWithinPartitions(df.col_a.desc(), df.col_b.asc())
            ._plan.to_proto(self.connect)
        )
        self.assertEqual(
            [f.child.unresolved_attribute.unparsed_identifier for f in plan.root.sort.order],
            ["col_a", "col_b"],
        )
        self.assertEqual(plan.root.sort.is_global, False)
        self.assertEqual(
            plan.root.sort.order[0].direction,
            proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING,
        )
        self.assertEqual(
            plan.root.sort.order[0].direction,
            proto.Expression.SortOrder.NullOrdering.SORT_NULLS_LAST,
        )
        self.assertEqual(
            plan.root.sort.order[1].direction,
            proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
        )
        self.assertEqual(
            plan.root.sort.order[1].direction,
            proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST,
        )

    def test_drop(self):
        # SPARK-41169: test drop
        df = self.connect.readTable(table_name=self.tbl_name)

        plan = df.filter(df.col_name > 3).drop("col_a", "col_b")._plan.to_proto(self.connect)
        self.assertEqual(
            plan.root.drop.column_names,
            ["col_a", "col_b"],
        )

        plan = df.filter(df.col_name > 3).drop(df.col_x, "col_b")._plan.to_proto(self.connect)
        self.assertEqual(
            [f.unresolved_attribute.unparsed_identifier for f in plan.root.drop.columns],
            ["col_x"],
        )
        self.assertEqual(
            plan.root.drop.column_names,
            ["col_b"],
        )

    def test_deduplicate(self):
        df = self.connect.readTable(table_name=self.tbl_name)

        distinct_plan = df.distinct()._plan.to_proto(self.connect)
        self.assertTrue(distinct_plan.root.deduplicate.HasField("input"), "input must be set")

        self.assertEqual(distinct_plan.root.deduplicate.all_columns_as_keys, True)
        self.assertEqual(len(distinct_plan.root.deduplicate.column_names), 0)

        deduplicate_on_all_columns_plan = df.dropDuplicates()._plan.to_proto(self.connect)
        self.assertEqual(deduplicate_on_all_columns_plan.root.deduplicate.all_columns_as_keys, True)
        self.assertEqual(len(deduplicate_on_all_columns_plan.root.deduplicate.column_names), 0)

        deduplicate_on_all_columns_plan = df.drop_duplicates()._plan.to_proto(self.connect)
        self.assertEqual(deduplicate_on_all_columns_plan.root.deduplicate.all_columns_as_keys, True)
        self.assertEqual(len(deduplicate_on_all_columns_plan.root.deduplicate.column_names), 0)

        deduplicate_on_subset_columns_plan = df.dropDuplicates(["name", "height"])._plan.to_proto(
            self.connect
        )
        self.assertEqual(
            deduplicate_on_subset_columns_plan.root.deduplicate.all_columns_as_keys, False
        )
        self.assertEqual(len(deduplicate_on_subset_columns_plan.root.deduplicate.column_names), 2)

    def test_relation_alias(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.alias("table_alias")._plan.to_proto(self.connect)
        self.assertEqual(plan.root.subquery_alias.alias, "table_alias")
        self.assertIsNotNone(plan.root.subquery_alias.input)

    def test_range(self):
        plan = self.connect.range(start=10, end=20, step=3, num_partitions=4)._plan.to_proto(
            self.connect
        )
        self.assertEqual(plan.root.range.start, 10)
        self.assertEqual(plan.root.range.end, 20)
        self.assertEqual(plan.root.range.step, 3)
        self.assertEqual(plan.root.range.num_partitions, 4)

        plan = self.connect.range(start=10, end=20)._plan.to_proto(self.connect)
        self.assertEqual(plan.root.range.start, 10)
        self.assertEqual(plan.root.range.end, 20)
        self.assertEqual(plan.root.range.step, 1)
        self.assertFalse(plan.root.range.HasField("num_partitions"))

    def test_datasource_read(self):
        reader = DataFrameReader(self.connect)
        df = reader.load(path="test_path", format="text", schema="id INT", op1="opv", op2="opv2")
        plan = df._plan.to_proto(self.connect)
        data_source = plan.root.read.data_source
        self.assertEqual(data_source.format, "text")
        self.assertEqual(data_source.schema, "id INT")
        self.assertEqual(len(data_source.options), 2)
        self.assertEqual(data_source.options.get("op1"), "opv")
        self.assertEqual(data_source.options.get("op2"), "opv2")
        self.assertEqual(len(data_source.paths), 1)
        self.assertEqual(data_source.paths[0], "test_path")

    def test_all_the_plans(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        df = df.select(df.col1).filter(df.col2 == 2).sort(df.col3.asc())
        plan = df._plan.to_proto(self.connect)
        self.assertIsNotNone(plan.root, "Root relation must be set")
        self.assertIsNotNone(plan.root.read)

    def test_union(self):
        df1 = self.connect.readTable(table_name=self.tbl_name)
        df2 = self.connect.readTable(table_name=self.tbl_name)
        plan1 = df1.union(df2)._plan.to_proto(self.connect)
        self.assertTrue(plan1.root.set_op.is_all)
        self.assertEqual(proto.SetOperation.SET_OP_TYPE_UNION, plan1.root.set_op.set_op_type)
        plan2 = df1.union(df2)._plan.to_proto(self.connect)
        self.assertTrue(plan2.root.set_op.is_all)
        self.assertEqual(proto.SetOperation.SET_OP_TYPE_UNION, plan2.root.set_op.set_op_type)
        plan3 = df1.unionByName(df2, True)._plan.to_proto(self.connect)
        self.assertTrue(plan3.root.set_op.by_name)
        self.assertEqual(proto.SetOperation.SET_OP_TYPE_UNION, plan3.root.set_op.set_op_type)

    def test_subtract(self):
        # SPARK-41453: test `subtract` API for Python client.
        df1 = self.connect.readTable(table_name=self.tbl_name)
        df2 = self.connect.readTable(table_name=self.tbl_name)
        plan1 = df1.subtract(df2)._plan.to_proto(self.connect)
        self.assertTrue(not plan1.root.set_op.is_all)
        self.assertEqual(proto.SetOperation.SET_OP_TYPE_EXCEPT, plan1.root.set_op.set_op_type)

    def test_except(self):
        # SPARK-41010: test `except` API for Python client.
        df1 = self.connect.readTable(table_name=self.tbl_name)
        df2 = self.connect.readTable(table_name=self.tbl_name)
        plan1 = df1.exceptAll(df2)._plan.to_proto(self.connect)
        self.assertTrue(plan1.root.set_op.is_all)
        self.assertEqual(proto.SetOperation.SET_OP_TYPE_EXCEPT, plan1.root.set_op.set_op_type)

    def test_intersect(self):
        # SPARK-41010: test `intersect` API for Python client.
        df1 = self.connect.readTable(table_name=self.tbl_name)
        df2 = self.connect.readTable(table_name=self.tbl_name)
        plan1 = df1.intersect(df2)._plan.to_proto(self.connect)
        self.assertFalse(plan1.root.set_op.is_all)
        self.assertEqual(proto.SetOperation.SET_OP_TYPE_INTERSECT, plan1.root.set_op.set_op_type)
        plan2 = df1.intersectAll(df2)._plan.to_proto(self.connect)
        self.assertTrue(plan2.root.set_op.is_all)
        self.assertEqual(proto.SetOperation.SET_OP_TYPE_INTERSECT, plan2.root.set_op.set_op_type)

    def test_coalesce_and_repartition(self):
        # SPARK-41026: test Coalesce and Repartition API in Python client.
        df = self.connect.readTable(table_name=self.tbl_name)
        plan1 = df.coalesce(10)._plan.to_proto(self.connect)
        self.assertEqual(10, plan1.root.repartition.num_partitions)
        self.assertFalse(plan1.root.repartition.shuffle)
        plan2 = df.repartition(20)._plan.to_proto(self.connect)
        self.assertTrue(plan2.root.repartition.shuffle)

        with self.assertRaises(PySparkValueError) as pe:
            df.coalesce(-1)._plan.to_proto(self.connect)

        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_POSITIVE",
            messageParameters={"arg_name": "numPartitions", "arg_value": "-1"},
        )

        with self.assertRaises(PySparkValueError) as pe:
            df.repartition(-1)._plan.to_proto(self.connect)

        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_POSITIVE",
            messageParameters={"arg_name": "numPartitions", "arg_value": "-1"},
        )

    def test_repartition_by_expression(self):
        # SPARK-41354: test dataframe.repartition(expressions)
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.repartition(10, "col_a", "col_b")._plan.to_proto(self.connect)
        self.assertEqual(10, plan.root.repartition_by_expression.num_partitions)
        self.assertEqual(
            [
                f.unresolved_attribute.unparsed_identifier
                for f in plan.root.repartition_by_expression.partition_exprs
            ],
            ["col_a", "col_b"],
        )

    def test_repartition_by_range(self):
        # SPARK-41354: test dataframe.repartitionByRange(expressions)
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.repartitionByRange(10, "col_a", "col_b")._plan.to_proto(self.connect)
        self.assertEqual(10, plan.root.repartition_by_expression.num_partitions)
        self.assertEqual(
            [
                f.sort_order.child.unresolved_attribute.unparsed_identifier
                for f in plan.root.repartition_by_expression.partition_exprs
            ],
            ["col_a", "col_b"],
        )

    def test_to(self):
        # SPARK-41464: test `to` API in Python client.
        df = self.connect.readTable(table_name=self.tbl_name)

        schema = StructType(
            [
                StructField("col1", IntegerType(), True),
                StructField("col2", StringType(), True),
                StructField("map1", MapType(StringType(), IntegerType(), True), True),
                StructField("array1", ArrayType(IntegerType(), True), True),
            ]
        )
        new_plan = df.to(schema)._plan.to_proto(self.connect)
        self.assertEqual(pyspark_types_to_proto_types(schema), new_plan.root.to_schema.schema)

    def test_write_operation(self):
        wo = WriteOperation(self.connect.readTable("name")._plan)
        wo.mode = "overwrite"
        wo.source = "parquet"

        p = wo.command(None)
        self.assertIsNotNone(p)
        self.assertFalse(p.write_operation.HasField("path"))
        self.assertFalse(p.write_operation.HasField("table"))

        wo.path = "path"
        p = wo.command(None)
        self.assertIsNotNone(p)
        self.assertTrue(p.write_operation.HasField("path"))
        self.assertFalse(p.write_operation.HasField("table"))

        wo.path = None
        wo.table_name = "table"
        wo.table_save_method = "save_as_table"
        p = wo.command(None)
        self.assertFalse(p.write_operation.HasField("path"))
        self.assertTrue(p.write_operation.HasField("table"))

        wo.bucket_cols = ["a", "b", "c"]
        p = wo.command(None)
        self.assertFalse(p.write_operation.HasField("bucket_by"))

        wo.num_buckets = 10
        p = wo.command(None)
        self.assertTrue(p.write_operation.HasField("bucket_by"))

        # Unsupported save mode
        wo.mode = "unknown"
        with self.assertRaises(ValueError):
            wo.command(None)

    def test_column_regexp(self):
        # SPARK-41438: test colRegex
        df = self.connect.readTable(table_name=self.tbl_name)
        col = df.colRegex("col_name")
        self.assertIsInstance(col, Column)
        self.assertEqual("Column<'UnresolvedRegex(col_name)'>", str(col))

        col_plan = col.to_plan(self.session.client)
        self.assertIsNotNone(col_plan)
        self.assertEqual(col_plan.unresolved_regex.col_name, "col_name")

    def test_print(self):
        # SPARK-41717: test print
        self.assertEqual(
            self.connect.sql("SELECT 1")._plan.print().strip(),
            "<SQL query='SELECT 1', args='None', named_args='None', views='None'>",
        )
        self.assertEqual(
            self.connect.range(1, 10)._plan.print().strip(),
            "<Range start='1', end='10', step='1', num_partitions='None'>",
        )

    def test_repr(self):
        # SPARK-41717: test __repr_html__
        self.assertIn("query: SELECT 1", self.connect.sql("SELECT 1")._plan._repr_html_().strip())

        expected = (
            "<b>Range</b><br/>",
            "start: 1 <br/>",
            "end: 10 <br/>",
            "step: 1 <br/>",
            "num_partitions: None <br/>",
        )
        actual = self.connect.range(1, 10)._plan._repr_html_().strip()
        for line in expected:
            self.assertIn(line, actual)

    def test_select_with_columns_and_strings(self):
        df = self.connect.with_plan(Read("table"))
        self.assertIsNotNone(df.select(col("name"))._plan.to_proto(self.connect))
        self.assertIsNotNone(df.select("name"))
        self.assertIsNotNone(df.select("name", "name2"))
        self.assertIsNotNone(df.select(col("name"), col("name2")))
        self.assertIsNotNone(df.select(col("name"), "name2"))
        self.assertIsNotNone(df.select("*"))

    def test_join_with_join_type(self):
        df_left = self.connect.with_plan(Read("table"))
        df_right = self.connect.with_plan(Read("table"))
        for join_type_str, join_type in [
            (None, proto.Join.JoinType.JOIN_TYPE_INNER),
            ("inner", proto.Join.JoinType.JOIN_TYPE_INNER),
            ("outer", proto.Join.JoinType.JOIN_TYPE_FULL_OUTER),
            ("full", proto.Join.JoinType.JOIN_TYPE_FULL_OUTER),
            ("fullouter", proto.Join.JoinType.JOIN_TYPE_FULL_OUTER),
            ("full_outer", proto.Join.JoinType.JOIN_TYPE_FULL_OUTER),
            ("left", proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER),
            ("leftouter", proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER),
            ("left_outer", proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER),
            ("right", proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER),
            ("rightouter", proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER),
            ("right_outer", proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER),
            ("semi", proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI),
            ("leftsemi", proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI),
            ("left_semi", proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI),
            ("anti", proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI),
            ("leftanti", proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI),
            ("left_anti", proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI),
            ("cross", proto.Join.JoinType.JOIN_TYPE_CROSS),
        ]:
            joined_df = df_left.join(df_right, on=col("name"), how=join_type_str)._plan.to_proto(
                self.connect
            )
            self.assertEqual(joined_df.root.join.join_type, join_type)

    def test_simple_column_expressions(self):
        df = self.connect.with_plan(Read("table"))

        c1 = df.col_name
        self.assertIsInstance(c1, Column)
        c2 = df["col_name"]
        self.assertIsInstance(c2, Column)
        c3 = col("col_name")
        self.assertIsInstance(c3, Column)

        # All Protos should be identical
        cp1 = c1.to_plan(None)
        cp2 = c2.to_plan(None)
        cp3 = c3.to_plan(None)

        self.assertIsNotNone(cp1)
        self.assertEqual(cp1, cp2)
        self.assertEqual(
            cp2.unresolved_attribute.unparsed_identifier,
            cp3.unresolved_attribute.unparsed_identifier,
        )
        self.assertTrue(cp2.unresolved_attribute.HasField("plan_id"))
        self.assertFalse(cp3.unresolved_attribute.HasField("plan_id"))

    def test_null_literal(self):
        null_lit = lit(None)
        null_lit_p = null_lit.to_plan(None)
        self.assertEqual(null_lit_p.literal.HasField("null"), True)

    def test_binary_literal(self):
        val = b"binary\0\0asas"
        bin_lit = lit(val)
        bin_lit_p = bin_lit.to_plan(None)
        self.assertEqual(bin_lit_p.literal.binary, val)

    def test_uuid_literal(self):
        val = uuid.uuid4()
        with self.assertRaises(TypeError):
            lit(val)

    def test_column_literals(self):
        df = self.connect.with_plan(Read("table"))
        lit_df = df.select(lit(10))
        self.assertIsNotNone(lit_df._plan.to_proto(None))

        self.assertIsNotNone(lit(10).to_plan(None))
        plan = lit(10).to_plan(None)
        self.assertIs(plan.literal.integer, 10)

        plan = lit(1 << 33).to_plan(None)
        self.assertEqual(plan.literal.long, 1 << 33)

    def test_numeric_literal_types(self):
        int_lit = lit(10)
        float_lit = lit(10.1)
        decimal_lit = lit(decimal.Decimal(99))

        self.assertIsNotNone(int_lit.to_plan(None))
        self.assertIsNotNone(float_lit.to_plan(None))
        self.assertIsNotNone(decimal_lit.to_plan(None))

    def test_float_nan_inf(self):
        na_lit = lit(float("nan"))
        self.assertIsNotNone(na_lit.to_plan(None))

        inf_lit = lit(float("inf"))
        self.assertIsNotNone(inf_lit.to_plan(None))

        inf_lit = lit(float("-inf"))
        self.assertIsNotNone(inf_lit.to_plan(None))

    def test_datetime_literal_types(self):
        """Test the different timestamp, date, and timedelta types."""
        datetime_lit = lit(datetime.datetime.now())

        p = datetime_lit.to_plan(None)
        self.assertIsNotNone(datetime_lit.to_plan(None))
        self.assertGreater(p.literal.timestamp, 10000000000000)

        date_lit = lit(datetime.date.today())
        time_delta = lit(datetime.timedelta(days=1, seconds=2, microseconds=3))

        self.assertIsNotNone(date_lit.to_plan(None))
        self.assertIsNotNone(time_delta.to_plan(None))
        # (24 * 3600 + 2) * 1000000 + 3
        self.assertEqual(86402000003, time_delta.to_plan(None).literal.day_time_interval)

    def test_list_to_literal(self):
        """Test conversion of lists to literals"""
        empty_list = []
        single_type = [1, 2, 3, 4]
        multi_type = ["ooo", 1, "asas", 2.3]

        empty_list_lit = lit(empty_list)
        single_type_lit = lit(single_type)
        multi_type_lit = lit(multi_type)

        p = empty_list_lit.to_plan(None)
        self.assertIsNotNone(p)

        p = single_type_lit.to_plan(None)
        self.assertIsNotNone(p)

        p = multi_type_lit.to_plan(None)
        self.assertIsNotNone(p)

    def test_column_alias(self) -> None:
        # SPARK-40809: Support for Column Aliases
        col0 = col("a").alias("martin")
        self.assertEqual("Column<'a AS martin'>", str(col0))

        col0 = col("a").alias("martin", metadata={"pii": True})
        plan = col0.to_plan(self.session.client)
        self.assertIsNotNone(plan)
        self.assertEqual(plan.alias.metadata, '{"pii": true}')

    def test_column_expressions(self):
        """Test a more complex combination of expressions and their translation into
        the protobuf structure."""
        df = self.connect.with_plan(Read("table"))

        expr = lit(10) < lit(10)
        expr_plan = expr.to_plan(None)
        self.assertIsNotNone(expr_plan.unresolved_function)
        self.assertEqual(expr_plan.unresolved_function.function_name, "<")

        expr = df.id % lit(10) == lit(10)
        expr_plan = expr.to_plan(None)
        self.assertIsNotNone(expr_plan.unresolved_function)
        self.assertEqual(expr_plan.unresolved_function.function_name, "==")

        lit_fun = expr_plan.unresolved_function.arguments[1]
        self.assertIsInstance(lit_fun, proto.Expression)
        self.assertIsInstance(lit_fun.literal, proto.Expression.Literal)
        self.assertEqual(lit_fun.literal.integer, 10)

        mod_fun = expr_plan.unresolved_function.arguments[0]
        self.assertIsInstance(mod_fun, proto.Expression)
        self.assertIsInstance(mod_fun.unresolved_function, proto.Expression.UnresolvedFunction)
        self.assertEqual(len(mod_fun.unresolved_function.arguments), 2)
        self.assertIsInstance(mod_fun.unresolved_function.arguments[0], proto.Expression)
        self.assertIsInstance(
            mod_fun.unresolved_function.arguments[0].unresolved_attribute,
            proto.Expression.UnresolvedAttribute,
        )
        self.assertEqual(
            mod_fun.unresolved_function.arguments[0].unresolved_attribute.unparsed_identifier, "id"
        )

    def test_literal_expression_with_arrays(self):
        l0 = LiteralExpression._from_value(["x", "y", "z"]).to_plan(None).literal
        self.assertTrue(l0.array.element_type.HasField("string"))
        self.assertEqual(len(l0.array.elements), 3)
        self.assertEqual(l0.array.elements[0].string, "x")
        self.assertEqual(l0.array.elements[1].string, "y")
        self.assertEqual(l0.array.elements[2].string, "z")

        l1 = LiteralExpression._from_value([3, -3]).to_plan(None).literal
        self.assertTrue(l1.array.element_type.HasField("integer"))
        self.assertEqual(len(l1.array.elements), 2)
        self.assertEqual(l1.array.elements[0].integer, 3)
        self.assertEqual(l1.array.elements[1].integer, -3)

        l2 = LiteralExpression._from_value([float("nan"), -3.0, 0.0]).to_plan(None).literal
        self.assertTrue(l2.array.element_type.HasField("double"))
        self.assertEqual(len(l2.array.elements), 3)
        self.assertTrue(math.isnan(l2.array.elements[0].double))
        self.assertEqual(l2.array.elements[1].double, -3.0)
        self.assertEqual(l2.array.elements[2].double, 0.0)

        l3 = LiteralExpression._from_value([[3, 4], [5, 6, 7]]).to_plan(None).literal
        self.assertTrue(l3.array.element_type.HasField("array"))
        self.assertTrue(l3.array.element_type.array.element_type.HasField("integer"))
        self.assertEqual(len(l3.array.elements), 2)
        self.assertEqual(len(l3.array.elements[0].array.elements), 2)
        self.assertEqual(len(l3.array.elements[1].array.elements), 3)

        l4 = (
            LiteralExpression._from_value([[float("inf"), 0.4], [0.5, float("nan")], []])
            .to_plan(None)
            .literal
        )
        self.assertTrue(l4.array.element_type.HasField("array"))
        self.assertTrue(l4.array.element_type.array.element_type.HasField("double"))
        self.assertEqual(len(l4.array.elements), 3)
        self.assertEqual(len(l4.array.elements[0].array.elements), 2)
        self.assertEqual(len(l4.array.elements[1].array.elements), 2)
        self.assertEqual(len(l4.array.elements[2].array.elements), 0)

    def test_literal_to_any_conversion(self):
        for value in [
            b"binary\0\0asas",
            True,
            False,
            0,
            12,
            -1,
            0.0,
            1.234567,
            decimal.Decimal(0.0),
            decimal.Decimal(1.234567),
            "sss",
            datetime.date(2022, 12, 13),
            datetime.datetime.now(),
            datetime.timedelta(1, 2, 3),
            [1, 2, 3, 4, 5, 6],
            [-1.0, 2.0, 3.0],
            ["x", "y", "z"],
            [[1.0, 2.0, 3.0], [4.0, 5.0], [6.0]],
        ]:
            lit = LiteralExpression._from_value(value)
            proto_lit = lit.to_plan(None).literal
            value2 = LiteralExpression._to_value(proto_lit)
            self.assertEqual(value, value2)

        with self.assertRaises(AssertionError):
            lit = LiteralExpression._from_value(1.234567)
            proto_lit = lit.to_plan(None).literal
            LiteralExpression._to_value(proto_lit, StringType())

        with self.assertRaises(AssertionError):
            lit = LiteralExpression._from_value("1.234567")
            proto_lit = lit.to_plan(None).literal
            LiteralExpression._to_value(proto_lit, DoubleType())

        with self.assertRaises(AssertionError):
            # build a array<string> proto literal, but with incorrect elements
            proto_lit = proto.Expression().literal
            proto_lit.array.element_type.CopyFrom(pyspark_types_to_proto_types(StringType()))
            proto_lit.array.elements.append(
                LiteralExpression("string", StringType()).to_plan(None).literal
            )
            proto_lit.array.elements.append(
                LiteralExpression(1.234, DoubleType()).to_plan(None).literal
            )

            LiteralExpression._to_value(proto_lit, DoubleType)


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_plan import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
