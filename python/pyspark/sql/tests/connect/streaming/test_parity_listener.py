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

from pyspark.sql.tests.streaming.test_streaming_listener import StreamingListenerTestsMixin
from pyspark.sql.streaming.listener import StreamingQueryListener, QueryStartedEvent
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.testing.connectutils import ReusedConnectTestCase


def get_start_event_schema():
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("runId", StringType(), True),
            StructField("name", StringType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )


class TestListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        df = self.spark.createDataFrame(
            data=[(str(event.id), str(event.runId), event.name, event.timestamp)],
            schema=get_start_event_schema(),
        )
        df.write.saveAsTable("listener_start_events")

    def onQueryProgress(self, event):
        pass

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


class StreamingListenerParityTests(StreamingListenerTestsMixin, ReusedConnectTestCase):
    def test_listener_events(self):
        test_listener = TestListener()

        try:
            self.spark.streams.addListener(test_listener)

            df = self.spark.readStream.format("rate").option("rowsPerSecond", 10).load()
            q = df.writeStream.format("noop").queryName("test").start()

            self.assertTrue(q.isActive)
            time.sleep(10)
            q.stop()

            start_event = QueryStartedEvent.fromJson(
                self.spark.read.table("listener_start_events").collect()[0].asDict()
            )

            self.check_start_event(start_event)

        finally:
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
