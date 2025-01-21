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
from importlib import reload

import py4j

from pyspark import SparkContext, SQLContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.testing.sqlutils import ReusedSQLTestCase


class HiveContextSQLTests(ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()
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
        ReusedSQLTestCase.tearDownClass()
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
        self.assertEqual(
            sorted(df.collect()), sorted(self.spark.sql("SELECT * FROM savedJsonTable").collect())
        )
        self.assertEqual(
            sorted(df.collect()),
            sorted(self.spark.sql("SELECT * FROM externalJsonTable").collect()),
        )
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
        self.spark.sql("DROP TABLE externalJsonTable")

        df.write.saveAsTable("savedJsonTable", "json", "overwrite", path=tmpPath)
        schema = StructType([StructField("value", StringType(), True)])
        actual = self.spark.catalog.createTable(
            "externalJsonTable",
            source="json",
            schema=schema,
            path=tmpPath,
            noUse="this options will not be used",
        )
        self.assertEqual(
            sorted(df.collect()), sorted(self.spark.sql("SELECT * FROM savedJsonTable").collect())
        )
        self.assertEqual(
            sorted(df.select("value").collect()),
            sorted(self.spark.sql("SELECT * FROM externalJsonTable").collect()),
        )
        self.assertEqual(sorted(df.select("value").collect()), sorted(actual.collect()))
        self.spark.sql("DROP TABLE savedJsonTable")
        self.spark.sql("DROP TABLE externalJsonTable")

        with self.sql_conf({"spark.sql.sources.default": "org.apache.spark.sql.json"}):
            df.write.saveAsTable("savedJsonTable", path=tmpPath, mode="overwrite")
            actual = self.spark.catalog.createTable("externalJsonTable", path=tmpPath)
            self.assertEqual(
                sorted(df.collect()),
                sorted(self.spark.sql("SELECT * FROM savedJsonTable").collect()),
            )
            self.assertEqual(
                sorted(df.collect()),
                sorted(self.spark.sql("SELECT * FROM externalJsonTable").collect()),
            )
            self.assertEqual(sorted(df.collect()), sorted(actual.collect()))
            self.spark.sql("DROP TABLE savedJsonTable")
            self.spark.sql("DROP TABLE externalJsonTable")

        shutil.rmtree(tmpPath)

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

    def test_unbounded_frames(self):
        from pyspark.sql import functions as F
        from pyspark.sql import window

        df = self.spark.range(0, 3)

        def rows_frame_match():
            return (
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                in df.select(
                    F.count("*").over(window.Window.rowsBetween(-sys.maxsize, sys.maxsize))
                ).columns[0]
            )

        def range_frame_match():
            return (
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                in df.select(
                    F.count("*").over(window.Window.rangeBetween(-sys.maxsize, sys.maxsize))
                ).columns[0]
            )

        for new_maxsize in [2**31 - 1, 2**63 - 1, 2**127 - 1]:
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
            sc = SparkContext("local[4]", "SQLContextTests")
            sql_context = SQLContext.getOrCreate(sc)
            assert isinstance(sql_context, SQLContext)
        finally:
            if sql_context is not None:
                sql_context.sparkSession.stop()
            if sc is not None:
                sc.stop()


if __name__ == "__main__":
    from pyspark.sql.tests.test_context import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
