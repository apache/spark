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

from pyspark.testing.sqlutils import ReusedSQLTestCase


class ConfTests(ReusedSQLTestCase):

    def test_conf(self):
        spark = self.spark
        spark.conf.set("bogo", "sipeo")
        self.assertEqual(spark.conf.get("bogo"), "sipeo")
        spark.conf.set("bogo", "ta")
        self.assertEqual(spark.conf.get("bogo"), "ta")
        self.assertEqual(spark.conf.get("bogo", "not.read"), "ta")
        self.assertEqual(spark.conf.get("not.set", "ta"), "ta")
        self.assertRaisesRegexp(Exception, "not.set", lambda: spark.conf.get("not.set"))
        spark.conf.unset("bogo")
        self.assertEqual(spark.conf.get("bogo", "colombia"), "colombia")

        self.assertEqual(spark.conf.get("hyukjin", None), None)

        # This returns 'STATIC' because it's the default value of
        # 'spark.sql.sources.partitionOverwriteMode', and `defaultValue` in
        # `spark.conf.get` is unset.
        self.assertEqual(spark.conf.get("spark.sql.sources.partitionOverwriteMode"), "STATIC")

        # This returns None because 'spark.sql.sources.partitionOverwriteMode' is unset, but
        # `defaultValue` in `spark.conf.get` is set to None.
        self.assertEqual(spark.conf.get("spark.sql.sources.partitionOverwriteMode", None), None)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_conf import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
