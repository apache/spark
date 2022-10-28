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
import unittest
import shutil
import tempfile
from pyspark.testing.sqlutils import have_pandas

if have_pandas:
    import pandas

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, LongType, StringType

if have_pandas:
    from pyspark.sql.connect.client import RemoteSparkSession
    from pyspark.sql.connect.function_builder import udf
    from pyspark.sql.connect.functions import lit
from pyspark.sql.dataframe import DataFrame
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.utils import ReusedPySparkTestCase


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectSQLTestCase(ReusedPySparkTestCase):
    """Parent test fixture class for all Spark Connect related
    test cases."""

    if have_pandas:
        connect: RemoteSparkSession
    tbl_name: str
    df_text: "DataFrame"

    @classmethod
    def setUpClass(cls: Any):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        cls.hive_available = True
        # Create the new Spark Session
        cls.spark = SparkSession(cls.sc)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.testDataStr = [Row(key=str(i)) for i in range(100)]
        cls.df = cls.sc.parallelize(cls.testData).toDF()
        cls.df_text = cls.sc.parallelize(cls.testDataStr).toDF()

        cls.tbl_name = "test_connect_basic_table_1"

        # Cleanup test data
        cls.spark_connect_clean_up_test_data()
        # Load test data
        cls.spark_connect_load_test_data()

    @classmethod
    def tearDownClass(cls: Any) -> None:
        cls.spark_connect_clean_up_test_data()

    @classmethod
    def spark_connect_load_test_data(cls: Any):
        # Setup Remote Spark Session
        cls.connect = RemoteSparkSession(user_id="test_user")
        df = cls.spark.createDataFrame([(x, f"{x}") for x in range(100)], ["id", "name"])
        # Since we might create multiple Spark sessions, we need to creata global temporary view
        # that is specifically maintained in the "global_temp" schema.
        df.write.saveAsTable(cls.tbl_name)

    @classmethod
    def spark_connect_clean_up_test_data(cls: Any) -> None:
        cls.spark.sql("DROP TABLE IF EXISTS {}".format(cls.tbl_name))


class SparkConnectTests(SparkConnectSQLTestCase):
    def test_simple_read(self):
        df = self.connect.read.table(self.tbl_name)
        data = df.limit(10).toPandas()
        # Check that the limit is applied
        self.assertEqual(len(data.index), 10)

    def test_simple_udf(self):
        def conv_udf(x) -> str:
            return "Martin"

        u = udf(conv_udf)
        df = self.connect.read.table(self.tbl_name)
        result = df.select(u(df.id)).toPandas()
        self.assertIsNotNone(result)

    def test_simple_explain_string(self):
        df = self.connect.read.table(self.tbl_name).limit(10)
        result = df.explain()
        self.assertGreater(len(result), 0)

    def test_schema(self):
        schema = self.connect.read.table(self.tbl_name).schema()
        self.assertEqual(
            StructType(
                [StructField("id", LongType(), True), StructField("name", StringType(), True)]
            ),
            schema,
        )

    def test_simple_binary_expressions(self):
        """Test complex expression"""
        df = self.connect.read.table(self.tbl_name)
        pd = df.select(df.id).where(df.id % lit(30) == lit(0)).sort(df.id.asc()).toPandas()
        self.assertEqual(len(pd.index), 4)

        res = pandas.DataFrame(data={"id": [0, 30, 60, 90]})
        self.assert_(pd.equals(res), f"{pd.to_string()} != {res.to_string()}")

    def test_limit_offset(self):
        df = self.connect.read.table(self.tbl_name)
        pd = df.limit(10).offset(1).toPandas()
        self.assertEqual(9, len(pd.index))
        pd2 = df.offset(98).limit(10).toPandas()
        self.assertEqual(2, len(pd2.index))

    def test_simple_datasource_read(self) -> None:
        writeDf = self.df_text
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        writeDf.write.text(tmpPath)

        readDf = self.connect.read.format("text").schema("id STRING").load(path=tmpPath)
        expectResult = writeDf.collect()
        pandasResult = readDf.toPandas()
        if pandasResult is None:
            self.assertTrue(False, "Empty pandas dataframe")
        else:
            actualResult = pandasResult.values.tolist()
            self.assertEqual(len(expectResult), len(actualResult))


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_basic import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
