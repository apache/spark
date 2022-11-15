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

import grpc  # type: ignore
from grpc._channel import _MultiThreadedRendezvous  # type: ignore

from pyspark.testing.sqlutils import have_pandas, SQLTestUtils

if have_pandas:
    import pandas

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, LongType, StringType

if have_pandas:
    from pyspark.sql.connect.client import RemoteSparkSession, ChannelBuilder
    from pyspark.sql.connect.function_builder import udf
    from pyspark.sql.connect.functions import lit
from pyspark.sql.dataframe import DataFrame
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.utils import ReusedPySparkTestCase


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectSQLTestCase(ReusedPySparkTestCase, SQLTestUtils):
    """Parent test fixture class for all Spark Connect related
    test cases."""

    if have_pandas:
        connect: RemoteSparkSession
    tbl_name: str
    tbl_name_empty: str
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
        cls.tbl_name_empty = "test_connect_basic_table_empty"

        # Cleanup test data
        cls.spark_connect_clean_up_test_data()
        # Load test data
        cls.spark_connect_load_test_data()

    @classmethod
    def tearDownClass(cls: Any) -> None:
        cls.spark_connect_clean_up_test_data()
        ReusedPySparkTestCase.tearDownClass()

    @classmethod
    def spark_connect_load_test_data(cls: Any):
        # Setup Remote Spark Session
        cls.connect = RemoteSparkSession(userId="test_user")
        df = cls.spark.createDataFrame([(x, f"{x}") for x in range(100)], ["id", "name"])
        # Since we might create multiple Spark sessions, we need to create global temporary view
        # that is specifically maintained in the "global_temp" schema.
        df.write.saveAsTable(cls.tbl_name)
        empty_table_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
            ]
        )
        emptyRDD = cls.spark.sparkContext.emptyRDD()
        empty_df = cls.spark.createDataFrame(emptyRDD, empty_table_schema)
        empty_df.write.saveAsTable(cls.tbl_name_empty)

    @classmethod
    def spark_connect_clean_up_test_data(cls: Any) -> None:
        cls.spark.sql("DROP TABLE IF EXISTS {}".format(cls.tbl_name))
        cls.spark.sql("DROP TABLE IF EXISTS {}".format(cls.tbl_name_empty))


class SparkConnectTests(SparkConnectSQLTestCase):
    def test_simple_read(self):
        df = self.connect.read.table(self.tbl_name)
        data = df.limit(10).toPandas()
        # Check that the limit is applied
        self.assertEqual(len(data.index), 10)

    def test_columns(self):
        # SPARK-41036: test `columns` API for python client.
        columns = self.connect.read.table(self.tbl_name).columns
        self.assertEqual(["id", "name"], columns)

    def test_collect(self):
        df = self.connect.read.table(self.tbl_name)
        data = df.limit(10).collect()
        self.assertEqual(len(data), 10)
        # Check Row has schema column names.
        self.assertTrue("name" in data[0])
        self.assertTrue("id" in data[0])

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

    def test_sql(self):
        pdf = self.connect.sql("SELECT 1").toPandas()
        self.assertEqual(1, len(pdf.index))

    def test_head(self):
        # SPARK-41002: test `head` API in Python Client
        df = self.connect.read.table(self.tbl_name)
        self.assertIsNotNone(len(df.head()))
        self.assertIsNotNone(len(df.head(1)))
        self.assertIsNotNone(len(df.head(5)))
        df2 = self.connect.read.table(self.tbl_name_empty)
        self.assertIsNone(df2.head())

    def test_first(self):
        # SPARK-41002: test `first` API in Python Client
        df = self.connect.read.table(self.tbl_name)
        self.assertIsNotNone(len(df.first()))
        df2 = self.connect.read.table(self.tbl_name_empty)
        self.assertIsNone(df2.first())

    def test_take(self) -> None:
        # SPARK-41002: test `take` API in Python Client
        df = self.connect.read.table(self.tbl_name)
        self.assertEqual(5, len(df.take(5)))
        df2 = self.connect.read.table(self.tbl_name_empty)
        self.assertEqual(0, len(df2.take(5)))

    def test_subquery_alias(self) -> None:
        # SPARK-40938: test subquery alias.
        plan_text = self.connect.read.table(self.tbl_name).alias("special_alias").explain()
        self.assertTrue("special_alias" in plan_text)

    def test_range(self):
        self.assertTrue(
            self.connect.range(start=0, end=10)
            .toPandas()
            .equals(self.spark.range(start=0, end=10).toPandas())
        )
        self.assertTrue(
            self.connect.range(start=0, end=10, step=3)
            .toPandas()
            .equals(self.spark.range(start=0, end=10, step=3).toPandas())
        )
        self.assertTrue(
            self.connect.range(start=0, end=10, step=3, numPartitions=2)
            .toPandas()
            .equals(self.spark.range(start=0, end=10, step=3, numPartitions=2).toPandas())
        )

    def test_create_global_temp_view(self):
        # SPARK-41127: test global temp view creation.
        with self.tempView("view_1"):
            self.connect.sql("SELECT 1 AS X LIMIT 0").createGlobalTempView("view_1")
            self.connect.sql("SELECT 2 AS X LIMIT 1").createOrReplaceGlobalTempView("view_1")
            self.assertTrue(self.spark.catalog.tableExists("global_temp.view_1"))

            # Test when creating a view which is alreayd exists but
            self.assertTrue(self.spark.catalog.tableExists("global_temp.view_1"))
            with self.assertRaises(_MultiThreadedRendezvous):
                self.connect.sql("SELECT 1 AS X LIMIT 0").createGlobalTempView("view_1")

    def test_fill_na(self):
        # SPARK-41128: Test fill na
        query = """
            SELECT * FROM VALUES
            (false, 1, NULL), (false, NULL, 2.0), (NULL, 3, 3.0)
            AS tab(a, b, c)
            """
        # +-----+----+----+
        # |    a|   b|   c|
        # +-----+----+----+
        # |false|   1|null|
        # |false|null| 2.0|
        # | null|   3| 3.0|
        # +-----+----+----+

        self.assertTrue(
            self.connect.sql(query)
            .fillna(True)
            .toPandas()
            .equals(self.spark.sql(query).fillna(True).toPandas())
        )
        self.assertTrue(
            self.connect.sql(query)
            .fillna(2)
            .toPandas()
            .equals(self.spark.sql(query).fillna(2).toPandas())
        )
        self.assertTrue(
            self.connect.sql(query)
            .fillna(2, ["a", "b"])
            .toPandas()
            .equals(self.spark.sql(query).fillna(2, ["a", "b"]).toPandas())
        )
        self.assertTrue(
            self.connect.sql(query)
            .na.fill({"a": True, "b": 2})
            .toPandas()
            .equals(self.spark.sql(query).na.fill({"a": True, "b": 2}).toPandas())
        )

    def test_empty_dataset(self):
        # SPARK-41005: Test arrow based collection with empty dataset.
        self.assertTrue(
            self.connect.sql("SELECT 1 AS X LIMIT 0")
            .toPandas()
            .equals(self.spark.sql("SELECT 1 AS X LIMIT 0").toPandas())
        )
        pdf = self.connect.sql("SELECT 1 AS X LIMIT 0").toPandas()
        self.assertEqual(0, len(pdf))  # empty dataset
        self.assertEqual(1, len(pdf.columns))  # one column
        self.assertEqual("X", pdf.columns[0])

    def test_session(self):
        self.assertEqual(self.connect, self.connect.sql("SELECT 1").sparkSession())

    def test_show(self):
        # SPARK-41111: Test the show method
        show_str = self.connect.sql("SELECT 1 AS X, 2 AS Y")._show_string()
        # +---+---+
        # |  X|  Y|
        # +---+---+
        # |  1|  2|
        # +---+---+
        expected = "+---+---+\n|  X|  Y|\n+---+---+\n|  1|  2|\n+---+---+\n"
        self.assertEqual(show_str, expected)

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


class ChannelBuilderTests(ReusedPySparkTestCase):
    def test_invalid_connection_strings(self):
        invalid = [
            "scc://host:12",
            "http://host",
            "sc:/host:1234/path",
            "sc://host/path",
            "sc://host/;parm1;param2",
        ]
        for i in invalid:
            self.assertRaises(AttributeError, ChannelBuilder, i)

    def test_sensible_defaults(self):
        chan = ChannelBuilder("sc://host")
        self.assertFalse(chan.secure, "Default URL is not secure")

        chan = ChannelBuilder("sc://host/;token=abcs")
        self.assertTrue(chan.secure, "specifying a token must set the channel to secure")

        chan = ChannelBuilder("sc://host/;use_ssl=abcs")
        self.assertFalse(chan.secure, "Garbage in, false out")

    def test_valid_channel_creation(self):
        chan = ChannelBuilder("sc://host").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

        # Sets up a channel without tokens because ssl is not used.
        chan = ChannelBuilder("sc://host/;use_ssl=true;token=abc").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

        chan = ChannelBuilder("sc://host/;use_ssl=true").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

    def test_channel_properties(self):
        chan = ChannelBuilder("sc://host/;use_ssl=true;token=abc;param1=120%2021")
        self.assertEqual("host:15002", chan.endpoint)
        self.assertEqual(True, chan.secure)
        self.assertEqual("120 21", chan.get("param1"))

    def test_metadata(self):
        chan = ChannelBuilder("sc://host/;use_ssl=true;token=abc;param1=120%2021;x-my-header=abcd")
        md = chan.metadata()
        self.assertEqual([("param1", "120 21"), ("x-my-header", "abcd")], md)


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_basic import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
