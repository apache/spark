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
import time

from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.errors import PySparkValueError


class StreamingTestsMixin:
    def test_streaming_query_functions_basic(self):
        df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
        query = (
            df.writeStream.format("memory")
            .queryName("test_streaming_query_functions_basic")
            .start()
        )
        try:
            self.assertEqual(query.name, "test_streaming_query_functions_basic")
            self.assertTrue(isinstance(query.id, str))
            self.assertTrue(isinstance(query.runId, str))
            self.assertTrue(query.isActive)
            self.assertEqual(query.exception(), None)
            self.assertFalse(query.awaitTermination(1))
            query.processAllAvailable()
            lastProgress = query.lastProgress
            recentProgress = query.recentProgress
            self.assertEqual(lastProgress["name"], query.name)
            self.assertEqual(lastProgress["id"], query.id)
            self.assertTrue(any(p == lastProgress for p in recentProgress))
            query.explain()

        except Exception as e:
            self.fail(
                "Streaming query functions sanity check shouldn't throw any error. "
                "Error message: " + str(e)
            )

        finally:
            query.stop()

    def test_streaming_progress(self):
        """
        Should be able to access fields using attributes in lastProgress / recentProgress
        e.g. q.lastProgress.id
        """
        df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
        query = df.writeStream.format("noop").start()
        try:
            query.processAllAvailable()
            lastProgress = query.lastProgress
            recentProgress = query.recentProgress
            self.assertEqual(lastProgress["name"], query.name)
            # Return str when accessed using dict get.
            self.assertEqual(lastProgress["id"], query.id)
            # SPARK-48567 Use attribute to access fields in q.lastProgress
            self.assertEqual(lastProgress.name, query.name)
            # Return uuid when accessed using attribute.
            self.assertEqual(str(lastProgress.id), query.id)
            self.assertTrue(any(p == lastProgress for p in recentProgress))
            self.assertTrue(lastProgress.numInputRows > 0)
            # Also access source / sink progress with attributes
            self.assertTrue(len(lastProgress.sources) > 0)
            self.assertTrue(lastProgress.sources[0].numInputRows > 0)
            self.assertTrue(lastProgress["sources"][0]["numInputRows"] > 0)
            self.assertTrue(lastProgress.sink.numOutputRows > 0)
            self.assertTrue(lastProgress["sink"]["numOutputRows"] > 0)
            # In Python, for historical reasons, changing field value
            # in StreamingQueryProgress is allowed.
            new_name = "myNewQuery"
            lastProgress["name"] = new_name
            self.assertEqual(lastProgress.name, new_name)

        except Exception as e:
            self.fail(
                "Streaming query functions sanity check shouldn't throw any error. "
                "Error message: " + str(e)
            )
        finally:
            query.stop()

    def test_streaming_query_name_edge_case(self):
        # Query name should be None when not specified
        q1 = self.spark.readStream.format("rate").load().writeStream.format("noop").start()
        self.assertEqual(q1.name, None)

        # Cannot set query name to be an empty string
        error_thrown = False
        try:
            (
                self.spark.readStream.format("rate")
                .load()
                .writeStream.format("noop")
                .queryName("")
                .start()
            )
        except PySparkValueError:
            error_thrown = True

        self.assertTrue(error_thrown)

    def test_stream_trigger(self):
        df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")

        # Should take at least one arg
        try:
            df.writeStream.trigger()
        except ValueError:
            pass

        # Should not take multiple args
        try:
            df.writeStream.trigger(once=True, processingTime="5 seconds")
        except ValueError:
            pass

        # Should not take multiple args
        try:
            df.writeStream.trigger(processingTime="5 seconds", continuous="1 second")
        except ValueError:
            pass

        # Should take only keyword args
        try:
            df.writeStream.trigger("5 seconds")
            self.fail("Should have thrown an exception")
        except TypeError:
            pass

    def test_stream_read_options(self):
        schema = StructType([StructField("data", StringType(), False)])
        df = (
            self.spark.readStream.format("text")
            .option("path", "python/test_support/sql/streaming")
            .schema(schema)
            .load()
        )
        self.assertTrue(df.isStreaming)
        self.assertEqual(df.schema.simpleString(), "struct<data:string>")

    def test_stream_read_options_overwrite(self):
        bad_schema = StructType([StructField("test", IntegerType(), False)])
        schema = StructType([StructField("data", StringType(), False)])
        # SPARK-32516 disables the overwrite behavior by default.
        with self.sql_conf({"spark.sql.legacy.pathOptionBehavior.enabled": True}):
            df = (
                self.spark.readStream.format("csv")
                .option("path", "python/test_support/sql/fake")
                .schema(bad_schema)
                .load(path="python/test_support/sql/streaming", schema=schema, format="text")
            )
            self.assertTrue(df.isStreaming)
            self.assertEqual(df.schema.simpleString(), "struct<data:string>")

    def test_stream_save_options(self):
        df = (
            self.spark.readStream.format("text")
            .load("python/test_support/sql/streaming")
            .withColumn("id", lit(1))
        )
        for q in self.spark.streams.active:
            q.stop()
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.assertTrue(df.isStreaming)
        out = os.path.join(tmpPath, "out")
        chk = os.path.join(tmpPath, "chk")
        q = (
            df.writeStream.option("checkpointLocation", chk)
            .queryName("this_query")
            .format("parquet")
            .partitionBy("id")
            .outputMode("append")
            .option("path", out)
            .start()
        )
        try:
            self.assertEqual(q.name, "this_query")
            self.assertTrue(q.isActive)
            q.processAllAvailable()
            output_files = []
            for _, _, files in os.walk(out):
                output_files.extend([f for f in files if not f.startswith(".")])
            self.assertTrue(len(output_files) > 0)
            self.assertTrue(len(os.listdir(chk)) > 0)
        finally:
            q.stop()
            shutil.rmtree(tmpPath)

    def test_stream_save_options_overwrite(self):
        df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
        for q in self.spark.streams.active:
            q.stop()
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.assertTrue(df.isStreaming)
        out = os.path.join(tmpPath, "out")
        chk = os.path.join(tmpPath, "chk")
        fake1 = os.path.join(tmpPath, "fake1")
        fake2 = os.path.join(tmpPath, "fake2")
        # SPARK-32516 disables the overwrite behavior by default.
        with self.sql_conf({"spark.sql.legacy.pathOptionBehavior.enabled": True}):
            q = (
                df.writeStream.option("checkpointLocation", fake1)
                .format("memory")
                .option("path", fake2)
                .queryName("fake_query")
                .outputMode("append")
                .start(path=out, format="parquet", queryName="this_query", checkpointLocation=chk)
            )

        try:
            self.assertEqual(q.name, "this_query")
            self.assertTrue(q.isActive)
            q.processAllAvailable()
            output_files = []
            for _, _, files in os.walk(out):
                output_files.extend([f for f in files if not f.startswith(".")])
            self.assertTrue(len(output_files) > 0)
            self.assertTrue(len(os.listdir(chk)) > 0)
            self.assertFalse(os.path.isdir(fake1))  # should not have been created
            self.assertFalse(os.path.isdir(fake2))  # should not have been created
        finally:
            q.stop()
            shutil.rmtree(tmpPath)

    def test_stream_status_and_progress(self):
        df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
        for q in self.spark.streams.active:
            q.stop()
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.assertTrue(df.isStreaming)
        out = os.path.join(tmpPath, "out")
        chk = os.path.join(tmpPath, "chk")

        def func(x):
            time.sleep(1)
            return x

        from pyspark.sql.functions import col, udf

        sleep_udf = udf(func)

        # Use "sleep_udf" to delay the progress update so that we can test `lastProgress` when there
        # were no updates.
        q = df.select(sleep_udf(col("value")).alias("value")).writeStream.start(
            path=out, format="parquet", queryName="this_query", checkpointLocation=chk
        )
        try:
            # "lastProgress" will return None in most cases. However, as it may be flaky when
            # Jenkins is very slow, we don't assert it. If there is something wrong, "lastProgress"
            # may throw error with a high chance and make this test flaky, so we should still be
            # able to detect broken codes.
            q.lastProgress

            q.processAllAvailable()
            lastProgress = q.lastProgress
            recentProgress = q.recentProgress
            status = q.status
            self.assertEqual(lastProgress["name"], q.name)
            self.assertEqual(lastProgress["id"], q.id)
            self.assertTrue(any(p == lastProgress for p in recentProgress))
            self.assertTrue(
                "message" in status and "isDataAvailable" in status and "isTriggerActive" in status
            )
        finally:
            q.stop()
            shutil.rmtree(tmpPath)

    def test_stream_await_termination(self):
        df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
        for q in self.spark.streams.active:
            q.stop()
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.assertTrue(df.isStreaming)
        out = os.path.join(tmpPath, "out")
        chk = os.path.join(tmpPath, "chk")
        q = df.writeStream.start(
            path=out, format="parquet", queryName="this_query", checkpointLocation=chk
        )
        try:
            self.assertTrue(q.isActive)
            try:
                q.awaitTermination("hello")
                self.fail("Expected a value exception")
            except ValueError:
                pass
            now = time.time()
            # test should take at least 2 seconds
            res = q.awaitTermination(2.6)
            duration = time.time() - now
            self.assertTrue(duration >= 2)
            self.assertFalse(res)

            q.processAllAvailable()
            q.stop()
            # Sanity check when no parameter is set
            q.awaitTermination()
            self.assertFalse(q.isActive)
        finally:
            q.stop()
            shutil.rmtree(tmpPath)

    def test_stream_exception(self):
        with self.sql_conf({"spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled": True}):
            sdf = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            sq = sdf.writeStream.format("memory").queryName("query_explain").start()
            try:
                sq.processAllAvailable()
                self.assertEqual(sq.exception(), None)
            finally:
                sq.stop()

            from pyspark.sql.functions import col, udf
            from pyspark.errors import StreamingQueryException

            bad_udf = udf(lambda x: 1 / 0)
            sq = (
                sdf.select(bad_udf(col("value")))
                .writeStream.format("memory")
                .queryName("this_query")
                .start()
            )
            try:
                # Process some data to fail the query
                sq.processAllAvailable()
                self.fail("bad udf should fail the query")
            except StreamingQueryException as e:
                # This is expected
                self._assert_exception_tree_contains_msg(e, "ZeroDivisionError")
            finally:
                exception = sq.exception()
                sq.stop()
            self.assertIsInstance(exception, StreamingQueryException)
            self._assert_exception_tree_contains_msg(exception, "ZeroDivisionError")

    def test_query_manager_no_recreation(self):
        # SPARK-46873: There should not be a new StreamingQueryManager created every time
        # spark.streams is called.
        for i in range(5):
            self.assertTrue(self.spark.streams == self.spark.streams)

    def test_query_manager_get(self):
        df = self.spark.readStream.format("rate").load()
        for q in self.spark.streams.active:
            q.stop()
        q = df.writeStream.format("noop").start()

        self.assertTrue(q.isActive)
        self.assertTrue(q.id == self.spark.streams.get(q.id).id)

        q.stop()

        self.assertIsNone(self.spark.streams.get(q.id))

    def test_query_manager_await_termination(self):
        df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
        for q in self.spark.streams.active:
            q.stop()
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.assertTrue(df.isStreaming)
        out = os.path.join(tmpPath, "out")
        chk = os.path.join(tmpPath, "chk")
        q = df.writeStream.start(
            path=out, format="parquet", queryName="this_query", checkpointLocation=chk
        )
        try:
            self.assertTrue(q.isActive)
            try:
                self.spark.streams.awaitAnyTermination("hello")
                self.fail("Expected a value exception")
            except ValueError:
                pass
            now = time.time()
            # test should take at least 2 seconds
            res = self.spark.streams.awaitAnyTermination(2.6)
            duration = time.time() - now
            self.assertTrue(duration >= 2)
            self.assertFalse(res)
        finally:
            q.processAllAvailable()
            q.stop()
            shutil.rmtree(tmpPath)

    def test_streaming_read_from_table(self):
        with self.table("input_table", "this_query"):
            self.spark.sql("CREATE TABLE input_table (value string) USING parquet")
            self.spark.sql("INSERT INTO input_table VALUES ('aaa'), ('bbb'), ('ccc')")
            df = self.spark.readStream.table("input_table")
            self.assertTrue(df.isStreaming)
            q = df.writeStream.format("memory").queryName("this_query").start()
            q.processAllAvailable()
            q.stop()
            result = self.spark.sql("SELECT * FROM this_query ORDER BY value").collect()
            self.assertEqual(
                set([Row(value="aaa"), Row(value="bbb"), Row(value="ccc")]), set(result)
            )

    def test_streaming_write_to_table(self):
        with self.table("output_table"), tempfile.TemporaryDirectory(prefix="to_table") as tmpdir:
            df = self.spark.readStream.format("rate").option("rowsPerSecond", 10).load()
            q = df.writeStream.toTable("output_table", format="parquet", checkpointLocation=tmpdir)
            self.assertTrue(q.isActive)
            time.sleep(10)
            q.stop()
            result = self.spark.sql("SELECT value FROM output_table").collect()
            self.assertTrue(len(result) > 0)

    def test_streaming_write_to_table_cluster_by(self):
        with self.table("output_table"), tempfile.TemporaryDirectory(prefix="to_table") as tmpdir:
            df = self.spark.readStream.format("rate").option("rowsPerSecond", 10).load()
            q = df.writeStream.clusterBy("value").toTable(
                "output_table", format="parquet", checkpointLocation=tmpdir
            )
            self.assertTrue(q.isActive)
            time.sleep(10)
            q.stop()
            result = self.spark.sql("DESCRIBE output_table").collect()
            self.assertEqual(
                set(
                    [
                        Row(col_name="timestamp", data_type="timestamp", comment=None),
                        Row(col_name="value", data_type="bigint", comment=None),
                        Row(col_name="# Clustering Information", data_type="", comment=""),
                        Row(col_name="# col_name", data_type="data_type", comment="comment"),
                        Row(col_name="value", data_type="bigint", comment=None),
                    ]
                ),
                set(result),
            )
            result = self.spark.sql("SELECT value FROM output_table").collect()
            self.assertTrue(len(result) > 0)

    def test_streaming_with_temporary_view(self):
        """
        This verifies createOrReplaceTempView() works with a streaming dataframe. An SQL
        SELECT query on such a table results in a streaming dataframe and the streaming query works
        as expected.
        """
        with self.table("input_table", "this_query"):
            self.spark.sql("CREATE TABLE input_table (value string) USING parquet")
            self.spark.sql("INSERT INTO input_table VALUES ('a'), ('b'), ('c')")
            df = self.spark.readStream.table("input_table")
            self.assertTrue(df.isStreaming)
            # Create a temp view
            df.createOrReplaceTempView("test_view")
            # Create a select query
            view_df = self.spark.sql("SELECT CONCAT('view_', value) as vv from test_view")
            self.assertTrue(view_df.isStreaming)
            q = view_df.writeStream.format("memory").queryName("this_query").start()
            q.processAllAvailable()
            q.stop()
            result = self.spark.sql("SELECT * FROM this_query ORDER BY vv").collect()
            self.assertEqual(
                set([Row(value="view_a"), Row(value="view_b"), Row(value="view_c")]), set(result)
            )

    def test_streaming_drop_duplicate_within_watermark(self):
        """
        This verifies dropDuplicatesWithinWatermark works with a streaming dataframe.
        """
        user_schema = StructType().add("time", TimestampType()).add("id", "integer")
        df = (
            self.spark.readStream.option("sep", ";")
            .schema(user_schema)
            .csv("python/test_support/sql/streaming/time")
        )
        q1 = (
            df.withWatermark("time", "2 seconds")
            .dropDuplicatesWithinWatermark(["id"])
            .writeStream.outputMode("update")
            .format("memory")
            .queryName("test_streaming_drop_duplicates_within_wm")
            .start()
        )
        self.assertTrue(q1.isActive)
        q1.processAllAvailable()
        q1.stop()
        result = self.spark.sql("SELECT * FROM test_streaming_drop_duplicates_within_wm").collect()
        self.assertTrue(len(result) >= 6 and len(result) <= 9)


class StreamingTests(StreamingTestsMixin, ReusedSQLTestCase):
    def _assert_exception_tree_contains_msg(self, exception, msg):
        e = exception
        contains = msg in e._desc
        while e._cause is not None and not contains:
            e = e._cause
            contains = msg in e._desc
        self.assertTrue(contains, "Exception tree doesn't contain the expected message: %s" % msg)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.streaming.test_streaming import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
