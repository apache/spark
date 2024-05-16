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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


class DataFrameParityTests(DataFrameTestsMixin, ReusedConnectTestCase):
    def test_help_command(self):
        df = self.spark.createDataFrame(data=[{"foo": "bar"}, {"foo": "baz"}])
        super().check_help_command(df)

    def test_cached_property_is_copied(self):
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )
        # Create some dummy data
        data = [
            (1, "Alice", 30, "New York"),
            (2, "Bob", 25, "San Francisco"),
            (3, "Cathy", 29, "Los Angeles"),
            (4, "David", 35, "Chicago"),
        ]
        df = self.spark.createDataFrame(data, schema)
        df_columns = df.columns
        assert len(df.columns) == 4
        for col in ["id", "name"]:
            df_columns.remove(col)
        assert len(df.columns) == 4

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_toDF_with_schema_string(self):
        super().test_toDF_with_schema_string()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_dataframe import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
