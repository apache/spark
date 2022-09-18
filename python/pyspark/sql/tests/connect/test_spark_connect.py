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
from typing import Any
import uuid
import unittest
import tempfile

from pyspark.sql import SparkSession, Row
from pyspark.sql.connect.client import RemoteSparkSession
from pyspark.sql.connect.function_builder import udf
from pyspark.testing.utils import ReusedPySparkTestCase


class SparkConnectSQLTestCase(ReusedPySparkTestCase):
    """Parent test fixture class for all Spark Connect related
    test cases."""

    @classmethod
    def setUpClass(cls: Any) -> None:
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        cls.hive_available = True
        # Create the new Spark Session
        cls.spark = SparkSession(cls.sc)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.df = cls.sc.parallelize(cls.testData).toDF()

        # Load test data
        cls.spark_connect_test_data()

    @classmethod
    def spark_connect_test_data(cls: Any) -> None:
        # Setup Remote Spark Session
        cls.tbl_name = f"tbl{uuid.uuid4()}".replace("-", "")
        cls.connect = RemoteSparkSession(user_id="test_user")
        df = cls.spark.createDataFrame([(x, f"{x}") for x in range(100)], ["id", "name"])
        # Since we might create multiple Spark sessions, we need to creata global temporary view
        # that is specifically maintained in the "global_temp" schema.
        df.write.saveAsTable(cls.tbl_name)


class SparkConnectTests(SparkConnectSQLTestCase):
    def test_simple_read(self) -> None:
        """Tests that we can access the Spark Connect GRPC service locally."""
        df = self.connect.read.table(self.tbl_name)
        data = df.limit(10).collect()
        # Check that the limit is applied
        assert len(data.index) == 10

    def test_simple_udf(self) -> None:
        def conv_udf(x) -> str:
            return "Martin"

        u = udf(conv_udf)
        df = self.connect.read.table(self.tbl_name)
        result = df.select(u(df.id)).collect()
        assert result is not None

    def test_simple_explain_string(self) -> None:
        df = self.connect.read.table(self.tbl_name).limit(10)
        result = df.explain()
        assert len(result) > 0


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_spark_connect import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
