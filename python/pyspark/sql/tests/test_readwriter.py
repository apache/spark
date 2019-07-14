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

from pyspark.sql.types import *
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


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_readwriter import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
