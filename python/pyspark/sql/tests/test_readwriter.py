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

import os
import shutil
import tempfile

from pyspark.sql.functions import col
from pyspark.sql.readwriter import DataFrameWriterV2
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.testing.sqlutils import ReusedSQLTestCase


class ReadwriterTests(ReusedSQLTestCase):

    def test_save_and_load(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.write.json(tmpPath)
        actual = self.spark.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        schema = StructType([StructField("value", StringType(), True)])
        actual = self.spark.read.json(tmpPath, schema)
        self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))

        df.write.json(tmpPath, "overwrite")
        actual = self.spark.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        df.write.save(format="json", mode="overwrite", path=tmpPath,
                      noUse="this options will not be used in save.")
        actual = self.spark.read.load(format="json", path=tmpPath,
                                      noUse="this options will not be used in load.")
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        defaultDataSourceName = self.spark.conf.get("spark.sql.sources.default",
                                                    "org.apache.spark.sql.parquet")
        self.spark.sql("SET spark.sql.sources.default=org.apache.spark.sql.json")
        actual = self.spark.read.load(path=tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.spark.sql("SET spark.sql.sources.default=" + defaultDataSourceName)

        csvpath = os.path.join(tempfile.mkdtemp(), 'data')
        df.write.option('quote', None).format('csv').save(csvpath)

        shutil.rmtree(tmpPath)

    def test_save_and_load_builder(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.write.json(tmpPath)
        actual = self.spark.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        schema = StructType([StructField("value", StringType(), True)])
        actual = self.spark.read.json(tmpPath, schema)
        self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))

        df.write.mode("overwrite").json(tmpPath)
        actual = self.spark.read.json(tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        df.write.mode("overwrite").options(noUse="this options will not be used in save.")\
                .option("noUse", "this option will not be used in save.")\
                .format("json").save(path=tmpPath)
        actual =\
            self.spark.read.format("json")\
                           .load(path=tmpPath, noUse="this options will not be used in load.")
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

        defaultDataSourceName = self.spark.conf.get("spark.sql.sources.default",
                                                    "org.apache.spark.sql.parquet")
        self.spark.sql("SET spark.sql.sources.default=org.apache.spark.sql.json")
        actual = self.spark.read.load(path=tmpPath)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.spark.sql("SET spark.sql.sources.default=" + defaultDataSourceName)

        shutil.rmtree(tmpPath)

    def test_bucketed_write(self):
        data = [
            (1, "foo", 3.0), (2, "foo", 5.0),
            (3, "bar", -1.0), (4, "bar", 6.0),
        ]
        df = self.spark.createDataFrame(data, ["x", "y", "z"])

        def count_bucketed_cols(names, table="pyspark_bucket"):
            """Given a sequence of column names and a table name
            query the catalog and return number o columns which are
            used for bucketing
            """
            cols = self.spark.catalog.listColumns(table)
            num = len([c for c in cols if c.name in names and c.isBucket])
            return num

        with self.table("pyspark_bucket"):
            # Test write with one bucketing column
            df.write.bucketBy(3, "x").mode("overwrite").saveAsTable("pyspark_bucket")
            self.assertEqual(count_bucketed_cols(["x"]), 1)
            self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

            # Test write two bucketing columns
            df.write.bucketBy(3, "x", "y").mode("overwrite").saveAsTable("pyspark_bucket")
            self.assertEqual(count_bucketed_cols(["x", "y"]), 2)
            self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

            # Test write with bucket and sort
            df.write.bucketBy(2, "x").sortBy("z").mode("overwrite").saveAsTable("pyspark_bucket")
            self.assertEqual(count_bucketed_cols(["x"]), 1)
            self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

            # Test write with a list of columns
            df.write.bucketBy(3, ["x", "y"]).mode("overwrite").saveAsTable("pyspark_bucket")
            self.assertEqual(count_bucketed_cols(["x", "y"]), 2)
            self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

            # Test write with bucket and sort with a list of columns
            (df.write.bucketBy(2, "x")
                .sortBy(["y", "z"])
                .mode("overwrite").saveAsTable("pyspark_bucket"))
            self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

            # Test write with bucket and sort with multiple columns
            (df.write.bucketBy(2, "x")
                .sortBy("y", "z")
                .mode("overwrite").saveAsTable("pyspark_bucket"))
            self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

    def test_insert_into(self):
        df = self.spark.createDataFrame([("a", 1), ("b", 2)], ["C1", "C2"])
        with self.table("test_table"):
            df.write.saveAsTable("test_table")
            self.assertEqual(2, self.spark.sql("select * from test_table").count())

            df.write.insertInto("test_table")
            self.assertEqual(4, self.spark.sql("select * from test_table").count())

            df.write.mode("overwrite").insertInto("test_table")
            self.assertEqual(2, self.spark.sql("select * from test_table").count())

            df.write.insertInto("test_table", True)
            self.assertEqual(2, self.spark.sql("select * from test_table").count())

            df.write.insertInto("test_table", False)
            self.assertEqual(4, self.spark.sql("select * from test_table").count())

            df.write.mode("overwrite").insertInto("test_table", False)
            self.assertEqual(6, self.spark.sql("select * from test_table").count())


class ReadwriterV2Tests(ReusedSQLTestCase):
    def test_api(self):
        df = self.df
        writer = df.writeTo("testcat.t")
        self.assertIsInstance(writer, DataFrameWriterV2)
        self.assertIsInstance(writer.option("property", "value"), DataFrameWriterV2)
        self.assertIsInstance(writer.options(property="value"), DataFrameWriterV2)
        self.assertIsInstance(writer.using("source"), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy("id"), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(col("id")), DataFrameWriterV2)
        self.assertIsInstance(writer.tableProperty("foo", "bar"), DataFrameWriterV2)

    def test_partitioning_functions(self):
        import datetime
        from pyspark.sql.functions import years, months, days, hours, bucket

        df = self.spark.createDataFrame(
            [(1, datetime.datetime(2000, 1, 1), "foo")],
            ("id", "ts", "value")
        )

        writer = df.writeTo("testcat.t")

        self.assertIsInstance(writer.partitionedBy(years("ts")), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(months("ts")), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(days("ts")), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(hours("ts")), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(bucket(11, "id")), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(bucket(11, col("id"))), DataFrameWriterV2)
        self.assertIsInstance(
            writer.partitionedBy(bucket(3, "id"), hours(col("ts"))), DataFrameWriterV2
        )


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_readwriter import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
