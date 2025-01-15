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

from pyspark.sql import functions as sf
from pyspark.sql.tests.test_subquery import SubqueryTestsMixin
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.connectutils import ReusedConnectTestCase


class SubqueryParityTests(SubqueryTestsMixin, ReusedConnectTestCase):
    def test_scalar_subquery_with_missing_outer_reference(self):
        with self.tempView("l", "r"):
            self.df1.createOrReplaceTempView("l")
            self.df2.createOrReplaceTempView("r")

            assertDataFrameEqual(
                self.spark.table("l").select(
                    "a",
                    (
                        self.spark.table("r")
                        .where(sf.col("c") == sf.col("a"))
                        .select(sf.sum("d"))
                        .scalar()
                    ),
                ),
                self.spark.sql("""SELECT a, (SELECT sum(d) FROM r WHERE c = a) FROM l"""),
            )

    def test_subquery_in_unpivot(self):
        self.check_subquery_in_unpivot(None, None)


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_parity_subquery import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
