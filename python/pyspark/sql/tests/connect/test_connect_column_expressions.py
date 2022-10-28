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
    from pyspark.sql.connect.proto import Expression as ProtoExpression
    import pyspark.sql.connect as c
    import pyspark.sql.connect.plan as p
    import pyspark.sql.connect.column as col
    import pyspark.sql.connect.functions as fun


@unittest.skipIf(not have_pandas, cast(str, pandas_requirement_message))
class SparkConnectColumnExpressionSuite(PlanOnlyTestFixture):
    def test_simple_column_expressions(self):
        df = c.DataFrame.withPlan(p.Read("table"))

        c1 = df.col_name
        self.assertIsInstance(c1, col.ColumnRef)
        c2 = df["col_name"]
        self.assertIsInstance(c2, col.ColumnRef)
        c3 = fun.col("col_name")
        self.assertIsInstance(c3, col.ColumnRef)

        # All Protos should be identical
        cp1 = c1.to_plan(None)
        cp2 = c2.to_plan(None)
        cp3 = c3.to_plan(None)

        self.assertIsNotNone(cp1)
        self.assertEqual(cp1, cp2)
        self.assertEqual(cp2, cp3)

    def test_column_literals(self):
        df = c.DataFrame.withPlan(p.Read("table"))
        lit_df = df.select(fun.lit(10))
        self.assertIsNotNone(lit_df._plan.to_proto(None))

        self.assertIsNotNone(fun.lit(10).to_plan(None))
        plan = fun.lit(10).to_plan(None)
        self.assertIs(plan.literal.i32, 10)

    def test_column_expressions(self):
        """Test a more complex combination of expressions and their translation into
        the protobuf structure."""
        df = c.DataFrame.withPlan(p.Read("table"))

        expr = fun.lit(10) < fun.lit(10)
        expr_plan = expr.to_plan(None)
        self.assertIsNotNone(expr_plan.unresolved_function)
        self.assertEqual(expr_plan.unresolved_function.parts[0], "<")

        expr = df.id % fun.lit(10) == fun.lit(10)
        expr_plan = expr.to_plan(None)
        self.assertIsNotNone(expr_plan.unresolved_function)
        self.assertEqual(expr_plan.unresolved_function.parts[0], "==")

        lit_fun = expr_plan.unresolved_function.arguments[1]
        self.assertIsInstance(lit_fun, ProtoExpression)
        self.assertIsInstance(lit_fun.literal, ProtoExpression.Literal)
        self.assertEqual(lit_fun.literal.i32, 10)

        mod_fun = expr_plan.unresolved_function.arguments[0]
        self.assertIsInstance(mod_fun, ProtoExpression)
        self.assertIsInstance(mod_fun.unresolved_function, ProtoExpression.UnresolvedFunction)
        self.assertEqual(len(mod_fun.unresolved_function.arguments), 2)
        self.assertIsInstance(mod_fun.unresolved_function.arguments[0], ProtoExpression)
        self.assertIsInstance(
            mod_fun.unresolved_function.arguments[0].unresolved_attribute,
            ProtoExpression.UnresolvedAttribute,
        )
        self.assertEqual(
            mod_fun.unresolved_function.arguments[0].unresolved_attribute.unparsed_identifier, "id"
        )


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_connect_column_expressions import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
