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
import json
import time
import uuid
from datetime import datetime

from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import (
    QueryStartedEvent,
    QueryProgressEvent,
    QueryTerminatedEvent,
    SinkProgress,
    SourceProgress,
    StateOperatorProgress,
    StreamingQueryProgress,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase


class StreamingListenerTests(ReusedSQLTestCase):
    def test_number_of_public_methods(self):
        msg = (
            "New field or method was detected in JVM side. If you added a new public "
            "field or method, implement that in the corresponding Python class too."
            "Otherwise, fix the number on the assert here."
        )

        def get_number_of_public_methods(clz):
            return len(
                self.spark.sparkContext._jvm.org.apache.spark.util.Utils.classForName(
                    clz, True, False
                ).getMethods()
            )

        self.assertEquals(
            get_number_of_public_methods(
                "org.apache.spark.sql.streaming.StreamingQueryListener$QueryStartedEvent"
            ),
            14,
            msg,
        )
        self.assertEquals(
            get_number_of_public_methods(
                "org.apache.spark.sql.streaming.StreamingQueryListener$QueryProgressEvent"
            ),
            11,
            msg,
        )
        self.assertEquals(
            get_number_of_public_methods(
                "org.apache.spark.sql.streaming.StreamingQueryListener$QueryTerminatedEvent"
            ),
            13,
            msg,
        )
        self.assertEquals(
            get_number_of_public_methods("org.apache.spark.sql.streaming.StreamingQueryProgress"),
            38,
            msg,
        )
        self.assertEquals(
            get_number_of_public_methods("org.apache.spark.sql.streaming.StateOperatorProgress"),
            27,
            msg,
        )
        self.assertEquals(
            get_number_of_public_methods("org.apache.spark.sql.streaming.SourceProgress"), 21, msg
        )
        self.assertEquals(
            get_number_of_public_methods("org.apache.spark.sql.streaming.SinkProgress"), 19, msg
        )

    def test_listener_events(self):
        start_event = None
        progress_event = None
        terminated_event = None

        class TestListener(StreamingQueryListener):
            def onQueryStarted(self, event):
                nonlocal start_event
                start_event = event

            def onQueryProgress(self, event):
                nonlocal progress_event
                progress_event = event

            def onQueryTerminated(self, event):
                nonlocal terminated_event
                terminated_event = event

        test_listener = TestListener()

        try:
            self.spark.streams.addListener(test_listener)

            df = self.spark.readStream.format("rate").option("rowsPerSecond", 10).load()
            q = df.writeStream.format("noop").queryName("test").start()
            self.assertTrue(q.isActive)
            time.sleep(10)
            q.stop()

            # Make sure all events are empty
            self.spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty()

            self.check_start_event(start_event)
            self.check_progress_event(progress_event)
            self.check_terminated_event(terminated_event)
        finally:
            self.spark.streams.removeListener(test_listener)

    def check_start_event(self, event):
        """Check QueryStartedEvent"""
        self.assertTrue(isinstance(event, QueryStartedEvent))
        self.assertTrue(isinstance(event.id, uuid.UUID))
        self.assertTrue(isinstance(event.runId, uuid.UUID))
        self.assertEquals(event.name, "test")
        try:
            datetime.strptime(event.timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            self.fail("'%s' is not in ISO 8601 format.")

    def check_progress_event(self, event):
        """Check QueryProgressEvent"""
        self.assertTrue(isinstance(event, QueryProgressEvent))
        self.check_streaming_query_progress(event.progress)

    def check_terminated_event(self, event):
        """Check QueryTerminatedEvent"""
        self.assertTrue(isinstance(event, QueryTerminatedEvent))
        self.assertTrue(isinstance(event.id, uuid.UUID))
        self.assertTrue(isinstance(event.runId, uuid.UUID))
        # TODO: Needs a test for exception.
        self.assertEquals(event.exception, None)

    def check_streaming_query_progress(self, progress):
        """Check StreamingQueryProgress"""
        self.assertTrue(isinstance(progress, StreamingQueryProgress))
        self.assertTrue(isinstance(progress.id, uuid.UUID))
        self.assertTrue(isinstance(progress.runId, uuid.UUID))
        self.assertEquals(progress.name, "test")
        try:
            json.loads(progress.json)
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        try:
            json.loads(progress.prettyJson)
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        try:
            json.loads(str(progress))
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        try:
            datetime.strptime(progress.timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        except Exception:
            self.fail("'%s' is not in ISO 8601 format.")
        self.assertTrue(isinstance(progress.batchId, int))
        self.assertTrue(isinstance(progress.batchDuration, int))
        self.assertTrue(isinstance(progress.durationMs, dict))
        self.assertTrue(
            set(progress.durationMs.keys()).issubset(
                {
                    "triggerExecution",
                    "queryPlanning",
                    "getBatch",
                    "commitOffsets",
                    "latestOffset",
                    "addBatch",
                    "walCommit",
                }
            )
        )
        self.assertTrue(all(map(lambda v: isinstance(v, int), progress.durationMs.values())))

        self.assertEquals(progress.eventTime, {})

        self.assertTrue(isinstance(progress.stateOperators, list))
        for so in progress.stateOperators:
            self.check_state_operator_progress(so)

        self.assertTrue(isinstance(progress.sources, list))
        for so in progress.sources:
            self.check_source_progress(so)

        self.assertTrue(isinstance(progress.sink, SinkProgress))
        self.check_sink_progress(progress.sink)
        self.assertTrue(isinstance(progress.observedMetrics, dict))

    def check_state_operator_progress(self, progress):
        """Check StateOperatorProgress"""
        self.assertTrue(isinstance(progress, StateOperatorProgress))
        try:
            json.loads(progress.json)
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        try:
            json.loads(progress.prettyJson)
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        try:
            json.loads(str(progress))
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        self.assertTrue(isinstance(progress.operatorName, str))
        self.assertTrue(isinstance(progress.numRowsTotal, int))
        self.assertTrue(isinstance(progress.numRowsUpdated, int))
        self.assertTrue(isinstance(progress.allUpdatesTimeMs, int))
        self.assertTrue(isinstance(progress.numRowsRemoved, int))
        self.assertTrue(isinstance(progress.allRemovalsTimeMs, int))
        self.assertTrue(isinstance(progress.commitTimeMs, int))
        self.assertTrue(isinstance(progress.memoryUsedBytes, int))
        self.assertTrue(isinstance(progress.numRowsDroppedByWatermark, int))
        self.assertTrue(isinstance(progress.numShufflePartitions, int))
        self.assertTrue(isinstance(progress.numStateStoreInstances, int))
        self.assertTrue(isinstance(progress.customMetrics, dict))

    def check_source_progress(self, progress):
        """Check SourceProgress"""
        self.assertTrue(isinstance(progress, SourceProgress))
        try:
            json.loads(progress.json)
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        try:
            json.loads(progress.prettyJson)
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        try:
            json.loads(str(progress))
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        self.assertTrue(isinstance(progress.description, str))
        self.assertTrue(isinstance(progress.startOffset, (str, type(None))))
        self.assertTrue(isinstance(progress.endOffset, (str, type(None))))
        self.assertTrue(isinstance(progress.latestOffset, (str, type(None))))
        self.assertTrue(isinstance(progress.numInputRows, int))
        self.assertTrue(isinstance(progress.inputRowsPerSecond, float))
        self.assertTrue(isinstance(progress.processedRowsPerSecond, float))
        self.assertTrue(isinstance(progress.metrics, dict))

    def check_sink_progress(self, progress):
        """Check SinkProgress"""
        self.assertTrue(isinstance(progress, SinkProgress))
        try:
            json.loads(progress.json)
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        try:
            json.loads(progress.prettyJson)
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        try:
            json.loads(str(progress))
        except Exception:
            self.fail("'%s' is not a valid JSON.")
        self.assertTrue(isinstance(progress.description, str))
        self.assertTrue(isinstance(progress.numOutputRows, int))
        self.assertTrue(isinstance(progress.metrics, dict))

    def test_remove_listener(self):
        # SPARK-38804: Test StreamingQueryManager.removeListener
        class TestListener(StreamingQueryListener):
            def onQueryStarted(self, event):
                pass

            def onQueryProgress(self, event):
                pass

            def onQueryTerminated(self, event):
                pass

        test_listener = TestListener()

        num_listeners = len(self.spark.streams._jsqm.listListeners())
        self.spark.streams.addListener(test_listener)
        self.assertEqual(num_listeners + 1, len(self.spark.streams._jsqm.listListeners()))
        self.spark.streams.removeListener(test_listener)
        self.assertEqual(num_listeners, len(self.spark.streams._jsqm.listListeners()))


if __name__ == "__main__":
    import unittest

    from pyspark.sql.tests.streaming.test_streaming_listener import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
