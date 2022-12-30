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
    PlanOnlyTestFixture,
    should_test_connect,
    connect_requirement_message,
)

if should_test_connect:
    import pyspark.sql.connect.proto as proto
    from pyspark.sql.connect.column import Column
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect.plan import WriteOperation
    from pyspark.sql.connect.readwriter import DataFrameReader
    from pyspark.sql.connect.function_builder import UserDefinedFunction, udf
    from pyspark.sql.connect.types import pyspark_types_to_proto_types
    from pyspark.sql.types import (
        StringType,
        StructType,
        StructField,
        IntegerType,
        MapType,
        ArrayType,
    )


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectTestsPlanOnly(PlanOnlyTestFixture):
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
        self.assertEqual(crossJoin_plan, join_plan)

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
        self.assertTrue(all(isinstance(c, proto.Expression) for c in plan.root.unpivot.values))
        self.assertEqual(
            plan.root.unpivot.values[0].unresolved_attribute.unparsed_identifier, "name"
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
        self.assertTrue(len(plan.root.unpivot.values) == 0)
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
        self.assertTrue(all(isinstance(c, proto.Expression) for c in plan.root.unpivot.values))
        self.assertEqual(
            plan.root.unpivot.values[0].unresolved_attribute.unparsed_identifier, "name"
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
        self.assertTrue(len(plan.root.unpivot.values) == 0)
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
            self.assertEqual(plan.root.sample.force_stable_sort, True)

            plan = relations[1]._plan.to_proto(self.connect)
            self.assertEqual(plan.root.sample.lower_bound, 0.16666666666666666)
            self.assertEqual(plan.root.sample.upper_bound, 0.5)
            self.assertEqual(plan.root.sample.with_replacement, False)
            self.assertEqual(plan.root.sample.HasField("seed"), True)
            self.assertEqual(plan.root.sample.force_stable_sort, True)

            plan = relations[2]._plan.to_proto(self.connect)
            self.assertEqual(plan.root.sample.lower_bound, 0.5)
            self.assertEqual(plan.root.sample.upper_bound, 1.0)
            self.assertEqual(plan.root.sample.with_replacement, False)
            self.assertEqual(plan.root.sample.HasField("seed"), True)
            self.assertEqual(plan.root.sample.force_stable_sort, True)

        relations = df.filter(df.col_name > 3).randomSplit([1.0, 2.0, 3.0], 1)
        checkRelations(relations)

        relations = df.filter(df.col_name > 3).randomSplit([1.0, 2.0, 3.0])
        checkRelations(relations)

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
        self.assertEqual(plan.root.sample.HasField("seed"), False)
        self.assertEqual(plan.root.sample.force_stable_sort, False)

        plan = (
            df.filter(df.col_name > 3)
            .sample(withReplacement=True, fraction=0.4, seed=-1)
            ._plan.to_proto(self.connect)
        )
        self.assertEqual(plan.root.sample.lower_bound, 0.0)
        self.assertEqual(plan.root.sample.upper_bound, 0.4)
        self.assertEqual(plan.root.sample.with_replacement, True)
        self.assertEqual(plan.root.sample.seed, -1)
        self.assertEqual(plan.root.sample.force_stable_sort, False)

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
            [f.unresolved_attribute.unparsed_identifier for f in plan.root.drop.cols],
            ["col_a", "col_b"],
        )

        plan = df.filter(df.col_name > 3).drop(df.col_x, "col_b")._plan.to_proto(self.connect)
        self.assertEqual(
            [f.unresolved_attribute.unparsed_identifier for f in plan.root.drop.cols],
            ["col_x", "col_b"],
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
        self.assertEqual(len(data_source.options), 3)
        self.assertEqual(data_source.options.get("path"), "test_path")
        self.assertEqual(data_source.options.get("op1"), "opv")
        self.assertEqual(data_source.options.get("op2"), "opv2")

    def test_simple_udf(self):
        u = udf(lambda x: "Martin", StringType())
        self.assertIsNotNone(u)
        expr = u("ThisCol", "ThatCol", "OtherCol")
        self.assertTrue(isinstance(expr, Column))
        self.assertTrue(isinstance(expr._expr, UserDefinedFunction))
        u_plan = expr.to_plan(self.connect)
        self.assertIsNotNone(u_plan)

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

        with self.assertRaises(ValueError) as context:
            df.coalesce(-1)._plan.to_proto(self.connect)
        self.assertTrue("numPartitions must be positive" in str(context.exception))

        with self.assertRaises(ValueError) as context:
            df.repartition(-1)._plan.to_proto(self.connect)
        self.assertTrue("numPartitions must be positive" in str(context.exception))

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

    def test_unsupported_functions(self):
        # SPARK-41225: Disable unsupported functions.
        df = self.connect.readTable(table_name=self.tbl_name)
        for f in (
            "rdd",
            "unpersist",
            "cache",
            "persist",
            "withWatermark",
            "observe",
            "foreach",
            "foreachPartition",
            "toLocalIterator",
            "checkpoint",
            "localCheckpoint",
            "_repr_html_",
            "semanticHash",
            "sameSemantics",
        ):
            with self.assertRaises(NotImplementedError):
                getattr(df, f)()

    def test_write_operation(self):
        wo = WriteOperation(self.connect.readTable("name")._plan)
        wo.mode = "overwrite"
        wo.source = "parquet"

        # Missing path or table name.
        with self.assertRaises(AssertionError):
            wo.command(None)

        wo.path = "path"
        p = wo.command(None)
        self.assertIsNotNone(p)
        self.assertTrue(p.write_operation.HasField("path"))
        self.assertFalse(p.write_operation.HasField("table_name"))

        wo.path = None
        wo.table_name = "table"
        p = wo.command(None)
        self.assertFalse(p.write_operation.HasField("path"))
        self.assertTrue(p.write_operation.HasField("table_name"))

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
            self.connect.sql("SELECT 1")._plan.print().strip(), "<SQL query='SELECT 1'>"
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


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_plan_only import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
