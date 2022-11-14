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
from typing import cast
import unittest

from pyspark.testing.connectutils import PlanOnlyTestFixture
from pyspark.testing.sqlutils import have_pandas, pandas_requirement_message

if have_pandas:
    import pyspark.sql.connect.proto as proto
    from pyspark.sql.connect.readwriter import DataFrameReader
    from pyspark.sql.connect.function_builder import UserDefinedFunction, udf
    from pyspark.sql.types import StringType


@unittest.skipIf(not have_pandas, cast(str, pandas_requirement_message))
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

    def test_filter(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.filter(df.col_name > 3)._plan.to_proto(self.connect)
        self.assertIsNotNone(plan.root.filter)
        self.assertTrue(
            isinstance(
                plan.root.filter.condition.unresolved_function, proto.Expression.UnresolvedFunction
            )
        )
        self.assertEqual(plan.root.filter.condition.unresolved_function.parts, [">"])
        self.assertEqual(len(plan.root.filter.condition.unresolved_function.arguments), 2)

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

        plan = (
            df.filter(df.col_name > 3)
            .sample(withReplacement=True, fraction=0.4, seed=-1)
            ._plan.to_proto(self.connect)
        )
        self.assertEqual(plan.root.sample.lower_bound, 0.0)
        self.assertEqual(plan.root.sample.upper_bound, 0.4)
        self.assertEqual(plan.root.sample.with_replacement, True)
        self.assertEqual(plan.root.sample.seed, -1)

    def test_sort(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.filter(df.col_name > 3).sort("col_a", "col_b")._plan.to_proto(self.connect)
        self.assertEqual(
            [
                f.expression.unresolved_attribute.unparsed_identifier
                for f in plan.root.sort.sort_fields
            ],
            ["col_a", "col_b"],
        )
        self.assertEqual(plan.root.sort.is_global, True)

        plan = (
            df.filter(df.col_name > 3)
            .sortWithinPartitions("col_a", "col_b")
            ._plan.to_proto(self.connect)
        )
        self.assertEqual(
            [
                f.expression.unresolved_attribute.unparsed_identifier
                for f in plan.root.sort.sort_fields
            ],
            ["col_a", "col_b"],
        )
        self.assertEqual(plan.root.sort.is_global, False)

    def test_deduplicate(self):
        df = self.connect.readTable(table_name=self.tbl_name)

        distinct_plan = df.distinct()._plan.to_proto(self.connect)
        self.assertEqual(distinct_plan.root.deduplicate.all_columns_as_keys, True)
        self.assertEqual(len(distinct_plan.root.deduplicate.column_names), 0)

        deduplicate_on_all_columns_plan = df.dropDuplicates()._plan.to_proto(self.connect)
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
        self.assertTrue(isinstance(expr, UserDefinedFunction))
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


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_plan_only import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
