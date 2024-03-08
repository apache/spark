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

import unittest
import time

import pyspark.cloudpickle
from pyspark.errors import PySparkPicklingError
from pyspark.sql.tests.streaming.test_streaming_listener import StreamingListenerTestsMixin
from pyspark.sql.streaming.listener import StreamingQueryListener
from pyspark.sql.functions import count, lit
from pyspark.testing.connectutils import ReusedConnectTestCase


# V1: Initial interface of StreamingQueryListener containing methods `onQueryStarted`,
# `onQueryProgress`, `onQueryTerminated`. It is prior to Spark 3.5.
class TestListenerV1(StreamingQueryListener):
    def onQueryStarted(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_start_events_v1")

    def onQueryProgress(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_progress_events_v1")

    def onQueryTerminated(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_terminated_events_v1")


# V2: The interface after the method `onQueryIdle` is added. It is Spark 3.5+.
class TestListenerV2(StreamingQueryListener):
    def onQueryStarted(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_start_events_v2")

    def onQueryProgress(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_progress_events_v2")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_terminated_events_v2")


class StreamingListenerParityTests(StreamingListenerTestsMixin, ReusedConnectTestCase):
    def test_listener_events(self):
        def verify(test_listener, table_postfix):
            try:
                self.spark.streams.addListener(test_listener)

                # This ensures the read socket on the server won't crash (i.e. because of timeout)
                # when there hasn't been a new event for a long time
                time.sleep(30)

                df = self.spark.readStream.format("rate").option("rowsPerSecond", 10).load()
                df_observe = df.observe("my_event", count(lit(1)).alias("rc"))
                df_stateful = df_observe.groupBy().count()  # make query stateful
                q = (
                    df_stateful.writeStream.format("noop")
                    .queryName("test")
                    .outputMode("complete")
                    .start()
                )

                self.assertTrue(q.isActive)
                # ensure at least one batch is ran
                while q.lastProgress is None or q.lastProgress["batchId"] == 0:
                    time.sleep(5)
                q.stop()
                self.assertFalse(q.isActive)

                # Sleep to make sure listener_terminated_events is written successfully
                time.sleep(60)

                start_table_name = "listener_start_events" + table_postfix
                progress_tbl_name = "listener_progress_events" + table_postfix
                terminated_tbl_name = "listener_terminated_events" + table_postfix

                start_event = pyspark.cloudpickle.loads(
                    self.spark.read.table(start_table_name).collect()[0][0]
                )

                progress_event = pyspark.cloudpickle.loads(
                    self.spark.read.table(progress_tbl_name).collect()[0][0]
                )

                terminated_event = pyspark.cloudpickle.loads(
                    self.spark.read.table(terminated_tbl_name).collect()[0][0]
                )

                self.check_start_event(start_event)
                self.check_progress_event(progress_event)
                self.check_terminated_event(terminated_event)

            finally:
                self.spark.streams.removeListener(test_listener)

                # Remove again to verify this won't throw any error
                self.spark.streams.removeListener(test_listener)

        verify(TestListenerV1(), "_v1")
        verify(TestListenerV2(), "_v2")

    def test_accessing_spark_session(self):
        spark = self.spark

        class TestListener(StreamingQueryListener):
            def onQueryStarted(self, event):
                spark.createDataFrame([("do", "not"), ("serialize", "spark")]).collect()

            def onQueryProgress(self, event):
                pass

            def onQueryIdle(self, event):
                pass

            def onQueryTerminated(self, event):
                pass

        error_thrown = False
        try:
            self.spark.streams.addListener(TestListener())
        except PySparkPicklingError as e:
            self.assertEqual(e.getErrorClass(), "STREAMING_CONNECT_SERIALIZATION_ERROR")
            error_thrown = True
        self.assertTrue(error_thrown)

    def test_accessing_spark_session_through_df(self):
        dataframe = self.spark.createDataFrame([("do", "not"), ("serialize", "dataframe")])

        class TestListener(StreamingQueryListener):
            def onQueryStarted(self, event):
                dataframe.collect()

            def onQueryProgress(self, event):
                pass

            def onQueryIdle(self, event):
                pass

            def onQueryTerminated(self, event):
                pass

        error_thrown = False
        try:
            self.spark.streams.addListener(TestListener())
        except PySparkPicklingError as e:
            self.assertEqual(e.getErrorClass(), "STREAMING_CONNECT_SERIALIZATION_ERROR")
            error_thrown = True
        self.assertTrue(error_thrown)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.streaming.test_parity_listener import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
