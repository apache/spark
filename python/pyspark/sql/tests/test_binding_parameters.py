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

from pyspark.sql import SparkSession
from pyspark.testing.sqlutils import ReusedSQLTestCase


class BindingParametersTests(ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super(BindingParametersTests, cls).setUpClass()
        cls.spark = SparkSession.builder.getOrCreate()
        assert cls.spark is not None
        assert cls.spark._jvm.SparkSession.getDefaultSession().isDefined()

    def test_wrapping_plan_in_limit_node(self):
        # Test the following Scala equivalent
        # val df = spark.sql("EXECUTE IMMEDIATE 'SELECT SUM(c1) num_sum FROM VALUES (?), (?) AS t(c1) ' USING 5, 6;")
        # val analyzedPlan = Limit(Literal.create(100), df.queryExecution.logical)
        # spark.sessionState.analyzer.executeAndCheck(analyzedPlan, df.queryExecution.tracker)
        sqlText = """EXECUTE IMMEDIATE 'SELECT SUM(c1) num_sum FROM VALUES (?), (?) AS t(c1) ' USING 5, 6;"""
        df = self.spark.sql(sqlText)
        jvm = self.spark._jvm
        assert jvm is not None

        # Binds to jvm.org.apache.spark.sql.catalyst.expressions.Literal.create(), a version that takes one input.
        boundMethodLiteralCreate = getattr(
            getattr(getattr(jvm.org.apache.spark.sql.catalyst.expressions, "Literal$"), "MODULE$"),
            "$anonfun$create$2",
        )

        # Binds Limit singleton.
        limitObj = getattr(
            getattr(jvm.org.apache.spark.sql.catalyst.plans.logical, "Limit$"), "MODULE$"
        )

        # It is not possible to call Limit singleton's constructor, calling apply() built-in equivalent.
        analyzedplan = limitObj.apply(
            boundMethodLiteralCreate(100),  # Equivalent to Literal.create(100)
            df._jdf.queryExecution().logical(),
        )
        self.spark._jsparkSession.sessionState().analyzer().executeAndCheck(
            analyzedplan, df._jdf.queryExecution().tracker()
        )

        self.assertEqual(self.spark.sql(sqlText).collect()[0][0], 11)
        self.assertEqual(self.spark.sql(sqlText).head().num_sum, 11)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_binding_parameters import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
