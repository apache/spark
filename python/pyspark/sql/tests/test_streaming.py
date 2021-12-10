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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.testing.sqlutils import ReusedSQLTestCase


class StreamingTests(ReusedSQLTestCase):
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
        for q in self.spark._wrapped.streams.active:
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
        for q in self.spark._wrapped.streams.active:
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
        for q in self.spark._wrapped.streams.active:
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
        for q in self.spark._wrapped.streams.active:
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
        finally:
            q.processAllAvailable()
            q.stop()
            shutil.rmtree(tmpPath)

    def test_stream_exception(self):
        sdf = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
        sq = sdf.writeStream.format("memory").queryName("query_explain").start()
        try:
            sq.processAllAvailable()
            self.assertEqual(sq.exception(), None)
        finally:
            sq.stop()

        from pyspark.sql.functions import col, udf
        from pyspark.sql.utils import StreamingQueryException

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
            sq.stop()
        self.assertTrue(type(sq.exception()) is StreamingQueryException)
        self._assert_exception_tree_contains_msg(sq.exception(), "ZeroDivisionError")

    def _assert_exception_tree_contains_msg(self, exception, msg):
        e = exception
        contains = msg in e.desc
        while e.cause is not None and not contains:
            e = e.cause
            contains = msg in e.desc
        self.assertTrue(contains, "Exception tree doesn't contain the expected message: %s" % msg)

    def test_query_manager_await_termination(self):
        df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
        for q in self.spark._wrapped.streams.active:
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
                self.spark._wrapped.streams.awaitAnyTermination("hello")
                self.fail("Expected a value exception")
            except ValueError:
                pass
            now = time.time()
            # test should take at least 2 seconds
            res = self.spark._wrapped.streams.awaitAnyTermination(2.6)
            duration = time.time() - now
            self.assertTrue(duration >= 2)
            self.assertFalse(res)
        finally:
            q.processAllAvailable()
            q.stop()
            shutil.rmtree(tmpPath)

    class ForeachWriterTester:
        def __init__(self, spark):
            self.spark = spark

        def write_open_event(self, partitionId, epochId):
            self._write_event(self.open_events_dir, {"partition": partitionId, "epoch": epochId})

        def write_process_event(self, row):
            self._write_event(self.process_events_dir, {"value": "text"})

        def write_close_event(self, error):
            self._write_event(self.close_events_dir, {"error": str(error)})

        def write_input_file(self):
            self._write_event(self.input_dir, "text")

        def open_events(self):
            return self._read_events(self.open_events_dir, "partition INT, epoch INT")

        def process_events(self):
            return self._read_events(self.process_events_dir, "value STRING")

        def close_events(self):
            return self._read_events(self.close_events_dir, "error STRING")

        def run_streaming_query_on_writer(self, writer, num_files):
            self._reset()
            try:
                sdf = self.spark.readStream.format("text").load(self.input_dir)
                sq = sdf.writeStream.foreach(writer).start()
                for i in range(num_files):
                    self.write_input_file()
                    sq.processAllAvailable()
            finally:
                self.stop_all()

        def assert_invalid_writer(self, writer, msg=None):
            self._reset()
            try:
                sdf = self.spark.readStream.format("text").load(self.input_dir)
                sq = sdf.writeStream.foreach(writer).start()
                self.write_input_file()
                sq.processAllAvailable()
                self.fail("invalid writer %s did not fail the query" % str(writer))  # not expected
            except Exception as e:
                if msg:
                    assert msg in str(e), "%s not in %s" % (msg, str(e))

            finally:
                self.stop_all()

        def stop_all(self):
            for q in self.spark._wrapped.streams.active:
                q.stop()

        def _reset(self):
            self.input_dir = tempfile.mkdtemp()
            self.open_events_dir = tempfile.mkdtemp()
            self.process_events_dir = tempfile.mkdtemp()
            self.close_events_dir = tempfile.mkdtemp()

        def _read_events(self, dir, json):
            rows = self.spark.read.schema(json).json(dir).collect()
            dicts = [row.asDict() for row in rows]
            return dicts

        def _write_event(self, dir, event):
            import uuid

            with open(os.path.join(dir, str(uuid.uuid4())), "w") as f:
                f.write("%s\n" % str(event))

        def __getstate__(self):
            return (self.open_events_dir, self.process_events_dir, self.close_events_dir)

        def __setstate__(self, state):
            self.open_events_dir, self.process_events_dir, self.close_events_dir = state

    # Those foreach tests are failed in Python 3.6 and macOS High Sierra by defined rules
    # at http://sealiesoftware.com/blog/archive/2017/6/5/Objective-C_and_fork_in_macOS_1013.html
    # To work around this, OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES.
    def test_streaming_foreach_with_simple_function(self):
        tester = self.ForeachWriterTester(self.spark)

        def foreach_func(row):
            tester.write_process_event(row)

        tester.run_streaming_query_on_writer(foreach_func, 2)
        self.assertEqual(len(tester.process_events()), 2)

    def test_streaming_foreach_with_basic_open_process_close(self):
        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def open(self, partitionId, epochId):
                tester.write_open_event(partitionId, epochId)
                return True

            def process(self, row):
                tester.write_process_event(row)

            def close(self, error):
                tester.write_close_event(error)

        tester.run_streaming_query_on_writer(ForeachWriter(), 2)

        open_events = tester.open_events()
        self.assertEqual(len(open_events), 2)
        self.assertSetEqual(set([e["epoch"] for e in open_events]), {0, 1})

        self.assertEqual(len(tester.process_events()), 2)

        close_events = tester.close_events()
        self.assertEqual(len(close_events), 2)
        self.assertSetEqual(set([e["error"] for e in close_events]), {"None"})

    def test_streaming_foreach_with_open_returning_false(self):
        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def open(self, partition_id, epoch_id):
                tester.write_open_event(partition_id, epoch_id)
                return False

            def process(self, row):
                tester.write_process_event(row)

            def close(self, error):
                tester.write_close_event(error)

        tester.run_streaming_query_on_writer(ForeachWriter(), 2)

        self.assertEqual(len(tester.open_events()), 2)

        self.assertEqual(len(tester.process_events()), 0)  # no row was processed

        close_events = tester.close_events()
        self.assertEqual(len(close_events), 2)
        self.assertSetEqual(set([e["error"] for e in close_events]), {"None"})

    def test_streaming_foreach_without_open_method(self):
        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def process(self, row):
                tester.write_process_event(row)

            def close(self, error):
                tester.write_close_event(error)

        tester.run_streaming_query_on_writer(ForeachWriter(), 2)
        self.assertEqual(len(tester.open_events()), 0)  # no open events
        self.assertEqual(len(tester.process_events()), 2)
        self.assertEqual(len(tester.close_events()), 2)

    def test_streaming_foreach_without_close_method(self):
        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def open(self, partition_id, epoch_id):
                tester.write_open_event(partition_id, epoch_id)
                return True

            def process(self, row):
                tester.write_process_event(row)

        tester.run_streaming_query_on_writer(ForeachWriter(), 2)
        self.assertEqual(len(tester.open_events()), 2)  # no open events
        self.assertEqual(len(tester.process_events()), 2)
        self.assertEqual(len(tester.close_events()), 0)

    def test_streaming_foreach_without_open_and_close_methods(self):
        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def process(self, row):
                tester.write_process_event(row)

        tester.run_streaming_query_on_writer(ForeachWriter(), 2)
        self.assertEqual(len(tester.open_events()), 0)  # no open events
        self.assertEqual(len(tester.process_events()), 2)
        self.assertEqual(len(tester.close_events()), 0)

    def test_streaming_foreach_with_process_throwing_error(self):
        from pyspark.sql.utils import StreamingQueryException

        tester = self.ForeachWriterTester(self.spark)

        class ForeachWriter:
            def process(self, row):
                raise RuntimeError("test error")

            def close(self, error):
                tester.write_close_event(error)

        try:
            tester.run_streaming_query_on_writer(ForeachWriter(), 1)
            self.fail("bad writer did not fail the query")  # this is not expected
        except StreamingQueryException as e:
            # TODO: Verify whether original error message is inside the exception
            pass

        self.assertEqual(len(tester.process_events()), 0)  # no row was processed
        close_events = tester.close_events()
        self.assertEqual(len(close_events), 1)
        # TODO: Verify whether original error message is inside the exception

    def test_streaming_foreach_with_invalid_writers(self):

        tester = self.ForeachWriterTester(self.spark)

        def func_with_iterator_input(iter):
            for x in iter:
                print(x)

        tester.assert_invalid_writer(func_with_iterator_input)

        class WriterWithoutProcess:
            def open(self, partition):
                pass

        tester.assert_invalid_writer(WriterWithoutProcess(), "does not have a 'process'")

        class WriterWithNonCallableProcess:
            process = True

        tester.assert_invalid_writer(
            WriterWithNonCallableProcess(), "'process' in provided object is not callable"
        )

        class WriterWithNoParamProcess:
            def process(self):
                pass

        tester.assert_invalid_writer(WriterWithNoParamProcess())

        # Abstract class for tests below
        class WithProcess:
            def process(self, row):
                pass

        class WriterWithNonCallableOpen(WithProcess):
            open = True

        tester.assert_invalid_writer(
            WriterWithNonCallableOpen(), "'open' in provided object is not callable"
        )

        class WriterWithNoParamOpen(WithProcess):
            def open(self):
                pass

        tester.assert_invalid_writer(WriterWithNoParamOpen())

        class WriterWithNonCallableClose(WithProcess):
            close = True

        tester.assert_invalid_writer(
            WriterWithNonCallableClose(), "'close' in provided object is not callable"
        )

    def test_streaming_foreachBatch(self):
        q = None
        collected = dict()

        def collectBatch(batch_df, batch_id):
            collected[batch_id] = batch_df.collect()

        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(collectBatch).start()
            q.processAllAvailable()
            self.assertTrue(0 in collected)
            self.assertTrue(len(collected[0]), 2)
        finally:
            if q:
                q.stop()

    def test_streaming_foreachBatch_propagates_python_errors(self):
        from pyspark.sql.utils import StreamingQueryException

        q = None

        def collectBatch(df, id):
            raise RuntimeError("this should fail the query")

        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(collectBatch).start()
            q.processAllAvailable()
            self.fail("Expected a failure")
        except StreamingQueryException as e:
            self.assertTrue("this should fail" in str(e))
        finally:
            if q:
                q.stop()

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
        with self.table("output_table"), tempfile.TemporaryDirectory() as tmpdir:
            df = self.spark.readStream.format("rate").option("rowsPerSecond", 10).load()
            q = df.writeStream.toTable("output_table", format="parquet", checkpointLocation=tmpdir)
            self.assertTrue(q.isActive)
            time.sleep(10)
            q.stop()
            result = self.spark.sql("SELECT value FROM output_table").collect()
            self.assertTrue(len(result) > 0)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_streaming import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
