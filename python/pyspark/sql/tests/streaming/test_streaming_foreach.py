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
import tempfile

from pyspark.testing.sqlutils import ReusedSQLTestCase


class StreamingTestsForeachMixin:
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
            for q in self.spark.streams.active:
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

    # Those foreach tests are failed in macOS High Sierra by defined rules
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
        from pyspark.errors import StreamingQueryException

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
            err_msg = str(e)
            self.assertTrue("test error" in err_msg)
            self.assertTrue("FOREACH_BATCH_USER_FUNCTION_ERROR" in err_msg)


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

        tester.assert_invalid_writer(WriterWithoutProcess(), "ATTRIBUTE_NOT_CALLABLE")

        class WriterWithNonCallableProcess:
            process = True

        tester.assert_invalid_writer(WriterWithNonCallableProcess(), "ATTRIBUTE_NOT_CALLABLE")

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

        tester.assert_invalid_writer(WriterWithNonCallableOpen(), "ATTRIBUTE_NOT_CALLABLE")

        class WriterWithNoParamOpen(WithProcess):
            def open(self):
                pass

        tester.assert_invalid_writer(WriterWithNoParamOpen())

        class WriterWithNonCallableClose(WithProcess):
            close = True

        tester.assert_invalid_writer(WriterWithNonCallableClose(), "ATTRIBUTE_NOT_CALLABLE")


class StreamingTestsForeach(StreamingTestsForeachMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.streaming.test_streaming_foreach import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
