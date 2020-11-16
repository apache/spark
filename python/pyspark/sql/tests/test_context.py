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
import sys
import tempfile
import unittest
try:
    from importlib import reload  # Python 3.4+ only.
except ImportError:
    # Otherwise, we will stick to Python 2's built-in reload.
    pass

import py4j

from pyspark import SparkContext, SQLContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.testing.utils import ReusedPySparkTestCase


class HiveContextSQLTests(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        cls.hive_available = True
        cls.spark = None
        try:
            cls.sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        except py4j.protocol.Py4JError:
            cls.tearDownClass()
            cls.hive_available = False
        except TypeError:
            cls.tearDownClass()
            cls.hive_available = False
        if cls.hive_available:
            cls.spark = SparkSession.builder.enableHiveSupport().getOrCreate()

        os.unlink(cls.tempdir.name)
        if cls.hive_available:
            cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
            cls.df = cls.sc.parallelize(cls.testData).toDF()

    def setUp(self):
        if not self.hive_available:
            self.skipTest("Hive is not available.")

    @classmethod
    def tearDownClass(cls):
        ReusedPySparkTestCase.tearDownClass()
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)
        if cls.spark is not None:
            cls.spark.stop()
            cls.spark = None

    def test_save_and_load_table(self):
        df = self.df
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        df.write.saveAsTable("savedJsonTable", "json", "append", path=tmpPath)
        actual = self.spark.catalog.createTable("externalJsonTable", tmpPath, "json")
        self.assertEqual(sorted(df.collect()),
                         sorted(self.spark.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertEqual(sorted(df.collect()),
                         sorted(self.spark.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.spark.sql("DROP TABLE externalJsonTable")

        df.write.saveAsTable("savedJsonTable", "json", "overwrite", path=tmpPath)
        schema = StructType([StructField("value", StringType(), True)])
        actual = self.spark.catalog.createTable("externalJsonTable", source="json",
                                                schema=schema, path=tmpPath,
                                                noUse="this options will not be used")
        self.assertEqual(sorted(df.collect()),
                         sorted(self.spark.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertEqual(sorted(df.select("value").collect()),
                         sorted(self.spark.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))
        self.spark.sql("DROP TABLE savedJsonTable")
        self.spark.sql("DROP TABLE externalJsonTable")

        defaultDataSourceName = self.spark.conf.get("spark.sql.sources.default",
                                                    "org.apache.spark.sql.parquet")
        self.spark.sql("SET spark.sql.sources.default=org.apache.spark.sql.json")
        df.write.saveAsTable("savedJsonTable", path=tmpPath, mode="overwrite")
        actual = self.spark.catalog.createTable("externalJsonTable", path=tmpPath)
        self.assertEqual(sorted(df.collect()),
                         sorted(self.spark.sql("SELECT * FROM savedJsonTable").collect()))
        self.assertEqual(sorted(df.collect()),
                         sorted(self.spark.sql("SELECT * FROM externalJsonTable").collect()))
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.spark.sql("DROP TABLE savedJsonTable")
        self.spark.sql("DROP TABLE externalJsonTable")
        self.spark.sql("SET spark.sql.sources.default=" + defaultDataSourceName)

        shutil.rmtree(tmpPath)

    def test_window_functions(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        w = Window.partitionBy("value").orderBy("key")
        from pyspark.sql import functions as F
        sel = df.select(df.value, df.key,
                        F.max("key").over(w.rowsBetween(0, 1)),
                        F.min("key").over(w.rowsBetween(0, 1)),
                        F.count("key").over(w.rowsBetween(float('-inf'), float('inf'))),
                        F.row_number().over(w),
                        F.rank().over(w),
                        F.dense_rank().over(w),
                        F.ntile(2).over(w))
        rs = sorted(sel.collect())
        expected = [
            ("1", 1, 1, 1, 1, 1, 1, 1, 1),
            ("2", 1, 1, 1, 3, 1, 1, 1, 1),
            ("2", 1, 2, 1, 3, 2, 1, 1, 1),
            ("2", 2, 2, 2, 3, 3, 3, 2, 2)
        ]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[:len(r)])

    def test_window_functions_without_partitionBy(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        w = Window.orderBy("key", df.value)
        from pyspark.sql import functions as F
        sel = df.select(df.value, df.key,
                        F.max("key").over(w.rowsBetween(0, 1)),
                        F.min("key").over(w.rowsBetween(0, 1)),
                        F.count("key").over(w.rowsBetween(float('-inf'), float('inf'))),
                        F.row_number().over(w),
                        F.rank().over(w),
                        F.dense_rank().over(w),
                        F.ntile(2).over(w))
        rs = sorted(sel.collect())
        expected = [
            ("1", 1, 1, 1, 4, 1, 1, 1, 1),
            ("2", 1, 1, 1, 4, 2, 2, 2, 1),
            ("2", 1, 2, 1, 4, 3, 2, 2, 2),
            ("2", 2, 2, 2, 4, 4, 4, 3, 2)
        ]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[:len(r)])

    def test_window_functions_cumulative_sum(self):
        df = self.spark.createDataFrame([("one", 1), ("two", 2)], ["key", "value"])
        from pyspark.sql import functions as F

        # Test cumulative sum
        sel = df.select(
            df.key,
            F.sum(df.value).over(Window.rowsBetween(Window.unboundedPreceding, 0)))
        rs = sorted(sel.collect())
        expected = [("one", 1), ("two", 3)]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[:len(r)])

        # Test boundary values less than JVM's Long.MinValue and make sure we don't overflow
        sel = df.select(
            df.key,
            F.sum(df.value).over(Window.rowsBetween(Window.unboundedPreceding - 1, 0)))
        rs = sorted(sel.collect())
        expected = [("one", 1), ("two", 3)]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[:len(r)])

        # Test boundary values greater than JVM's Long.MaxValue and make sure we don't overflow
        frame_end = Window.unboundedFollowing + 1
        sel = df.select(
            df.key,
            F.sum(df.value).over(Window.rowsBetween(Window.currentRow, frame_end)))
        rs = sorted(sel.collect())
        expected = [("one", 3), ("two", 2)]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[:len(r)])

    def test_collect_functions(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        from pyspark.sql import functions

        self.assertEqual(
            sorted(df.select(functions.collect_set(df.key).alias('r')).collect()[0].r),
            [1, 2])
        self.assertEqual(
            sorted(df.select(functions.collect_list(df.key).alias('r')).collect()[0].r),
            [1, 1, 1, 2])
        self.assertEqual(
            sorted(df.select(functions.collect_set(df.value).alias('r')).collect()[0].r),
            ["1", "2"])
        self.assertEqual(
            sorted(df.select(functions.collect_list(df.value).alias('r')).collect()[0].r),
            ["1", "2", "2", "2"])

    def test_limit_and_take(self):
        df = self.spark.range(1, 1000, numPartitions=10)

        def assert_runs_only_one_job_stage_and_task(job_group_name, f):
            tracker = self.sc.statusTracker()
            self.sc.setJobGroup(job_group_name, description="")
            f()
            jobs = tracker.getJobIdsForGroup(job_group_name)
            self.assertEqual(1, len(jobs))
            stages = tracker.getJobInfo(jobs[0]).stageIds
            self.assertEqual(1, len(stages))
            self.assertEqual(1, tracker.getStageInfo(stages[0]).numTasks)

        # Regression test for SPARK-10731: take should delegate to Scala implementation
        assert_runs_only_one_job_stage_and_task("take", lambda: df.take(1))
        # Regression test for SPARK-17514: limit(n).collect() should the perform same as take(n)
        assert_runs_only_one_job_stage_and_task("collect_limit", lambda: df.limit(1).collect())

    def test_datetime_functions(self):
        from pyspark.sql import functions
        from datetime import date
        df = self.spark.range(1).selectExpr("'2017-01-22' as dateCol")
        parse_result = df.select(functions.to_date(functions.col("dateCol"))).first()
        self.assertEquals(date(2017, 1, 22), parse_result['to_date(`dateCol`)'])

    def test_unbounded_frames(self):
        from pyspark.sql import functions as F
        from pyspark.sql import window

        df = self.spark.range(0, 3)

        def rows_frame_match():
            return "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in df.select(
                F.count("*").over(window.Window.rowsBetween(-sys.maxsize, sys.maxsize))
            ).columns[0]

        def range_frame_match():
            return "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in df.select(
                F.count("*").over(window.Window.rangeBetween(-sys.maxsize, sys.maxsize))
            ).columns[0]

        for new_maxsize in [2 ** 31 - 1, 2 ** 63 - 1, 2 ** 127 - 1]:
            old_maxsize = sys.maxsize
            sys.maxsize = new_maxsize
            try:
                # Manually reload window module to use monkey-patched sys.maxsize.
                reload(window)
                self.assertTrue(rows_frame_match())
                self.assertTrue(range_frame_match())
            finally:
                sys.maxsize = old_maxsize

        reload(window)


class SQLContextTests(unittest.TestCase):

    def test_get_or_create(self):
        sc = None
        sql_context = None
        try:
            sc = SparkContext('local[4]', "SQLContextTests")
            sql_context = SQLContext.getOrCreate(sc)
            assert(isinstance(sql_context, SQLContext))
        finally:
            SQLContext._instantiatedContext = None
            if sql_context is not None:
                sql_context.sparkSession.stop()
            if sc is not None:
                sc.stop()


if __name__ == "__main__":
    from pyspark.sql.tests.test_context import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
