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

from pyspark.sql.connect import DataFrame
from pyspark.sql.connect.plan import Read
from pyspark.sql.connect.function_builder import UserDefinedFunction, udf
from pyspark.sql.tests.connect.utils.spark_connect_test_utils import PlanOnlyTestFixture
from pyspark.sql.types import StringType


class SparkConnectTestsPlanOnly(PlanOnlyTestFixture):
    """These test cases exercise the interface to the proto plan
    generation but do not call Spark."""

    def test_simple_project(self):
        def read_table(x):
            return DataFrame.withPlan(Read(x), self.connect)

        self.connect.set_hook("readTable", read_table)

        plan = self.connect.readTable(self.tbl_name)._plan.collect(self.connect)
        self.assertIsNotNone(plan.root, "Root relation must be set")
        self.assertIsNotNone(plan.root.read)

    def test_simple_udf(self):
        def udf_mock(*args, **kwargs):
            return "internal_name"

        self.connect.set_hook("register_udf", udf_mock)

        u = udf(lambda x: "Martin", StringType())
        self.assertIsNotNone(u)
        expr = u("ThisCol", "ThatCol", "OtherCol")
        self.assertTrue(isinstance(expr, UserDefinedFunction))
        u_plan = expr.to_plan(self.connect)
        assert u_plan is not None

    def test_all_the_plans(self):
        def read_table(x):
            return DataFrame.withPlan(Read(x), self.connect)

        self.connect.set_hook("readTable", read_table)

        df = self.connect.readTable(self.tbl_name)
        df = df.select(df.col1).filter(df.col2 == 2).sort(df.col3.asc())
        plan = df._plan.collect(self.connect)
        self.assertIsNotNone(plan.root, "Root relation must be set")
        self.assertIsNotNone(plan.root.read)


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_plan_only import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
