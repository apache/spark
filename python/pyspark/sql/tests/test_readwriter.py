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

from pyspark.errors import AnalysisException
from pyspark.sql.functions import col, lit
from pyspark.sql.readwriter import DataFrameWriterV2
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.testing.sqlutils import ReusedSQLTestCase


class ReadwriterTestsMixin:
    def test_save_and_load(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        try:
            df.write.json(tmpPath)
            actual = self.spark.read.json(tmpPath)
            self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

            schema = StructType([StructField("value", StringType(), True)])
            actual = self.spark.read.json(tmpPath, schema)
            self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))

            df.write.json(tmpPath, "overwrite")
            actual = self.spark.read.json(tmpPath)
            self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

            df.write.save(
                format="json",
                mode="overwrite",
                path=tmpPath,
                noUse="this options will not be used in save.",
            )
            actual = self.spark.read.load(
                format="json", path=tmpPath, noUse="this options will not be used in load."
            )
            self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

            with self.sql_conf({"spark.sql.sources.default": "org.apache.spark.sql.json"}):
                actual = self.spark.read.load(path=tmpPath)
                self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

            csvpath = os.path.join(tempfile.mkdtemp(), "data")
            df.write.option("quote", None).format("csv").save(csvpath)
        finally:
            shutil.rmtree(tmpPath)

    def test_save_and_load_builder(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        try:
            df.write.json(tmpPath)
            actual = self.spark.read.json(tmpPath)
            self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

            schema = StructType([StructField("value", StringType(), True)])
            actual = self.spark.read.json(tmpPath, schema)
            self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))

            df.write.mode("overwrite").json(tmpPath)
            actual = self.spark.read.json(tmpPath)
            self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

            df.write.mode("overwrite").options(
                noUse="this options will not be used in save."
            ).option("noUse", "this option will not be used in save.").format("json").save(
                path=tmpPath
            )
            actual = self.spark.read.format("json").load(
                path=tmpPath, noUse="this options will not be used in load."
            )
            self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

            with self.sql_conf({"spark.sql.sources.default": "org.apache.spark.sql.json"}):
                actual = self.spark.read.load(path=tmpPath)
                self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        finally:
            shutil.rmtree(tmpPath)

    def test_bucketed_write(self):
        data = [
            (1, "foo", 3.0),
            (2, "foo", 5.0),
            (3, "bar", -1.0),
            (4, "bar", 6.0),
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
            (
                df.write.bucketBy(2, "x")
                .sortBy(["y", "z"])
                .mode("overwrite")
                .saveAsTable("pyspark_bucket")
            )
            self.assertSetEqual(set(data), set(self.spark.table("pyspark_bucket").collect()))

            # Test write with bucket and sort with multiple columns
            (
                df.write.bucketBy(2, "x")
                .sortBy("y", "z")
                .mode("overwrite")
                .saveAsTable("pyspark_bucket")
            )
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

    def test_cached_table(self):
        with self.table("test_cached_table_1"):
            self.spark.range(10).withColumn(
                "value_1",
                lit(1),
            ).write.saveAsTable("test_cached_table_1")

            with self.table("test_cached_table_2"):
                self.spark.range(10).withColumnRenamed("id", "index").withColumn(
                    "value_2", lit(2)
                ).write.saveAsTable("test_cached_table_2")

                df1 = self.spark.read.table("test_cached_table_1")
                df2 = self.spark.read.table("test_cached_table_2")
                df3 = self.spark.read.table("test_cached_table_1")

                join1 = df1.join(df2, on=df1.id == df2.index).select(df2.index, df2.value_2)
                join2 = df3.join(join1, how="left", on=join1.index == df3.id)

                self.assertEqual(join2.columns, ["id", "value_1", "index", "value_2"])


class ReadwriterV2TestsMixin:
    def test_api(self):
        self.check_api(DataFrameWriterV2)

    def check_api(self, tpe):
        df = self.df
        writer = df.writeTo("testcat.t")
        self.assertIsInstance(writer, tpe)
        self.assertIsInstance(writer.option("property", "value"), tpe)
        self.assertIsInstance(writer.options(property="value"), tpe)
        self.assertIsInstance(writer.using("source"), tpe)
        self.assertIsInstance(writer.partitionedBy("id"), tpe)
        self.assertIsInstance(writer.partitionedBy(col("id")), tpe)
        self.assertIsInstance(writer.tableProperty("foo", "bar"), tpe)

    def test_partitioning_functions(self):
        self.check_partitioning_functions(DataFrameWriterV2)

    def check_partitioning_functions(self, tpe):
        import datetime
        from pyspark.sql.functions.partitioning import years, months, days, hours, bucket

        df = self.spark.createDataFrame(
            [(1, datetime.datetime(2000, 1, 1), "foo")], ("id", "ts", "value")
        )

        writer = df.writeTo("testcat.t")

        self.assertIsInstance(writer.partitionedBy(years("ts")), tpe)
        self.assertIsInstance(writer.partitionedBy(months("ts")), tpe)
        self.assertIsInstance(writer.partitionedBy(days("ts")), tpe)
        self.assertIsInstance(writer.partitionedBy(hours("ts")), tpe)
        self.assertIsInstance(writer.partitionedBy(bucket(11, "id")), tpe)
        self.assertIsInstance(writer.partitionedBy(bucket(11, col("id"))), tpe)
        self.assertIsInstance(writer.partitionedBy(bucket(3, "id"), hours(col("ts"))), tpe)

    def test_create(self):
        df = self.df
        with self.table("test_table"):
            df.writeTo("test_table").using("parquet").create()
            self.assertEqual(100, self.spark.sql("select * from test_table").count())

    def test_create_without_provider(self):
        df = self.df
        with self.table("test_table"):
            df.writeTo("test_table").create()
            self.assertEqual(100, self.spark.sql("select * from test_table").count())

    def test_table_overwrite(self):
        df = self.df
        with self.assertRaisesRegex(AnalysisException, "TABLE_OR_VIEW_NOT_FOUND"):
            df.writeTo("test_table").overwrite(lit(True))


class ReadwriterTests(ReadwriterTestsMixin, ReusedSQLTestCase):
    pass


class ReadwriterV2Tests(ReadwriterV2TestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_readwriter import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
