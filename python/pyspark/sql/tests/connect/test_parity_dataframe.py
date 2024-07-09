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

from pyspark.sql.tests.test_dataframe import DataFrameTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class DataFrameParityTests(DataFrameTestsMixin, ReusedConnectTestCase):
    def test_help_command(self):
        df = self.spark.createDataFrame(data=[{"foo": "bar"}, {"foo": "baz"}])
        super().check_help_command(df)

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_toDF_with_schema_string(self):
        super().test_toDF_with_schema_string()

    @unittest.skip("Spark Connect does not support mergeInto at the moment.")
    def test_df_merge_into(self):
        super().test_df_merge_into()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_dataframe import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
