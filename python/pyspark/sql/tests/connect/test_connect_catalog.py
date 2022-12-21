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

from pyspark.sql import SparkSession
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.utils import ReusedPySparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils

if should_test_connect:
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectCatalogTestCase(PandasOnSparkTestCase, ReusedPySparkTestCase, SQLTestUtils):
    """Parent test fixture class for all Spark Connect Catalog API related
    test cases."""

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        # Create the new Spark Session
        cls.spark = SparkSession(cls.sc)
        cls.spark.sql(
            """
        CREATE DATABASE IF NOT EXISTS customer_db COMMENT
        'This is customer database' LOCATION '/user'
        WITH DBPROPERTIES (ID=001, Name='John');
        """
        )

        # Setup Remote Spark Session
        cls.connect = RemoteSparkSession.builder.remote().getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.sql("DROP DATABASE IF EXISTS customer_db")
        ReusedPySparkTestCase.tearDownClass()


class SparkConnectCatalogTests(SparkConnectCatalogTestCase):
    """Test cases for all Spark Connect Catalog API related
    test cases."""

    def test_list_databases(self):
        ret = self.connect.catalog.listDatabases()
        expected = ["customer_db", "default"]
        self.assertEqual(len(ret), 2)
        self.assertTrue(ret[0].name in expected)
        self.assertTrue(ret[1].name in expected)
