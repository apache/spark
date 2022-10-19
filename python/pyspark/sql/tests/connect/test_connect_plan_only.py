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

from pyspark.testing.connectutils import PlanOnlyTestFixture
import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.function_builder import UserDefinedFunction, udf
from pyspark.sql.types import StringType


class SparkConnectTestsPlanOnly(PlanOnlyTestFixture):
    """These test cases exercise the interface to the proto plan
    generation but do not call Spark."""

    def test_simple_project(self):
        plan = self.connect.readTable(table_name=self.tbl_name)._plan.to_proto(self.connect)
        self.assertIsNotNone(plan.root, "Root relation must be set")
        self.assertIsNotNone(plan.root.read)

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

    def test_relation_alias(self):
        df = self.connect.readTable(table_name=self.tbl_name)
        plan = df.alias("table_alias")._plan.to_proto(self.connect)
        self.assertEqual(plan.root.common.alias, "table_alias")

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


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_plan_only import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
