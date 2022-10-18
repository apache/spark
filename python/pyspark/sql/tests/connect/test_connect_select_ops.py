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
from pyspark.testing.connectutils import PlanOnlyTestFixture
from pyspark.sql.connect import DataFrame
from pyspark.sql.connect.functions import col
from pyspark.sql.connect.plan import Read, InputValidationError
import pyspark.sql.connect.proto as proto


class SparkConnectToProtoSuite(PlanOnlyTestFixture):
    def test_select_with_literal(self):
        df = DataFrame.withPlan(Read("table"))
        self.assertIsNotNone(df.select(col("name"))._plan.collect())
        self.assertRaises(InputValidationError, df.select, "name")

    def test_join_with_join_type(self):
        df_left = DataFrame.withPlan(Read("table"))
        df_right = DataFrame.withPlan(Read("table"))
        for (join_type_str, join_type) in [
            (None, proto.Join.JoinType.JOIN_TYPE_INNER),
            ("inner", proto.Join.JoinType.JOIN_TYPE_INNER),
            ("outer", proto.Join.JoinType.JOIN_TYPE_FULL_OUTER),
            ("leftouter", proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER),
            ("rightouter", proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER),
            ("leftanti", proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI),
            ("leftsemi", proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI),
        ]:
            joined_df = df_left.join(df_right, on=col("name"), how=join_type_str)._plan.collect()
            self.assertEqual(joined_df.root.join.join_type, join_type)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_connect_select_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
