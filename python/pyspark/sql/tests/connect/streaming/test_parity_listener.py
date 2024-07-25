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
import time
import os
import unittest

import pyspark.cloudpickle
from pyspark.sql.tests.streaming.test_streaming_listener import StreamingListenerTestsMixin
from pyspark.sql.streaming.listener import StreamingQueryListener
from pyspark.sql.functions import count, lit
from pyspark.testing.connectutils import ReusedConnectTestCase


class TestListenerSpark(StreamingQueryListener):
    def onQueryStarted(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_start_events")

    def onQueryProgress(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_progress_events")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_terminated_events")


class StreamingListenerParityTests(StreamingListenerTestsMixin, ReusedConnectTestCase):
    @unittest.skipIf(
        "SPARK_SKIP_CONNECT_COMPAT_TESTS" in os.environ, "Failed with different Client <> Server"
    )
    def test_listener_events(self):
        test_listener = TestListenerSpark()

        try:
            with self.table(
                "listener_start_events",
                "listener_progress_events",
                "listener_terminated_events",
            ):
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
                time.sleep(10)
                self.assertTrue(q.lastProgress["batchId"] > 0)  # ensure at least one batch is ran
                q.stop()
                self.assertFalse(q.isActive)

                start_event = pyspark.cloudpickle.loads(
                    self.spark.read.table("listener_start_events").collect()[0][0]
                )

                progress_event = pyspark.cloudpickle.loads(
                    self.spark.read.table("listener_progress_events").collect()[0][0]
                )

                terminated_event = pyspark.cloudpickle.loads(
                    self.spark.read.table("listener_terminated_events").collect()[0][0]
                )

                self.check_start_event(start_event)
                self.check_progress_event(progress_event)
                self.check_terminated_event(terminated_event)

        finally:
            self.spark.streams.removeListener(test_listener)

            # Remove again to verify this won't throw any error
            self.spark.streams.removeListener(test_listener)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.streaming.test_parity_listener import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
