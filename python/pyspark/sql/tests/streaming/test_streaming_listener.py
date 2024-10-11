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

from pyspark import Row
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
from pyspark.sql.functions import count, col, lit
from pyspark.testing.sqlutils import ReusedSQLTestCase


class StreamingListenerTestsMixin:
    def check_start_event(self, event):
        """Check QueryStartedEvent"""
        self.assertTrue(isinstance(event, QueryStartedEvent))
        self.assertTrue(isinstance(event.id, uuid.UUID))
        self.assertTrue(isinstance(event.runId, uuid.UUID))
        self.assertTrue(event.name is None or event.name.startswith("test"))
        try:
            datetime.strptime(event.timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            self.fail("'%s' is not in ISO 8601 format.")

    def check_progress_event(self, event, is_stateful):
        """Check QueryProgressEvent"""
        self.assertTrue(isinstance(event, QueryProgressEvent))
        self.check_streaming_query_progress(event.progress, is_stateful)

    def check_terminated_event(self, event, exception=None, errorClass=None):
        """Check QueryTerminatedEvent"""
        self.assertTrue(isinstance(event, QueryTerminatedEvent))
        self.assertTrue(isinstance(event.id, uuid.UUID))
        self.assertTrue(isinstance(event.runId, uuid.UUID))
        if exception:
            self.assertTrue(exception in event.exception)
        else:
            self.assertEqual(event.exception, None)

        if errorClass:
            self.assertTrue(errorClass in event.errorClassOnException)
        else:
            self.assertEqual(event.errorClassOnException, None)

    def check_streaming_query_progress(self, progress, is_stateful):
        """Check StreamingQueryProgress"""
        self.assertTrue(isinstance(progress, StreamingQueryProgress))
        self.assertTrue(isinstance(progress.id, uuid.UUID))
        self.assertTrue(isinstance(progress.runId, uuid.UUID))
        self.assertTrue(progress.name.startswith("test"))
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

        self.assertTrue(all(map(lambda v: isinstance(v, str), progress.eventTime.values())))

        self.assertTrue(isinstance(progress.stateOperators, list))
        if is_stateful:
            self.assertTrue(len(progress.stateOperators) >= 1)
            for so in progress.stateOperators:
                self.check_state_operator_progress(so)

        self.assertTrue(isinstance(progress.sources, list))
        self.assertTrue(len(progress.sources) >= 1)
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

    # This is a generic test work for both classic Spark and Spark Connect
    def test_listener_observed_metrics(self):
        class MyErrorListener(StreamingQueryListener):
            def __init__(self):
                self.num_rows = -1
                self.num_error_rows = -1

            def onQueryStarted(self, event):
                pass

            def onQueryProgress(self, event):
                row = event.progress.observedMetrics.get("my_event")
                # Save observed metrics for later verification
                self.num_rows = row["rc"]
                self.num_error_rows = row["erc"]

            def onQueryIdle(self, event):
                pass

            def onQueryTerminated(self, event):
                pass

        q = None
        try:
            error_listener = MyErrorListener()
            self.spark.streams.addListener(error_listener)

            sdf = self.spark.readStream.format("rate").load().withColumn("error", col("value"))

            # Observe row count (rc) and error row count (erc) in the streaming Dataset
            observed_ds = sdf.observe(
                "my_event", count(lit(1)).alias("rc"), count(col("error")).alias("erc")
            )

            q = observed_ds.writeStream.format("noop").start()

            while q.lastProgress is None or q.lastProgress.batchId == 0:
                q.awaitTermination(0.5)

            time.sleep(5)

            self.assertTrue(error_listener.num_rows > 0)
            self.assertTrue(error_listener.num_error_rows > 0)

        finally:
            if q is not None:
                q.stop()
            self.spark.streams.removeListener(error_listener)

    def test_streaming_progress(self):
        q = None
        try:
            # Test a fancier query with stateful operation and observed metrics
            df = self.spark.readStream.format("rate").option("rowsPerSecond", 10).load()
            df_observe = df.observe("my_event", count(lit(1)).alias("rc"))
            df_stateful = df_observe.groupBy().count()  # make query stateful
            q = (
                df_stateful.writeStream.format("noop")
                .queryName("test")
                .outputMode("update")
                .trigger(processingTime="5 seconds")
                .start()
            )

            while q.lastProgress is None or q.lastProgress.batchId == 0:
                q.awaitTermination(0.5)

            q.stop()

            self.check_streaming_query_progress(q.lastProgress, True)
            for p in q.recentProgress:
                self.check_streaming_query_progress(p, True)

        finally:
            if q is not None:
                q.stop()


class StreamingListenerTests(StreamingListenerTestsMixin, ReusedSQLTestCase):
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

        self.assertEqual(
            get_number_of_public_methods(
                "org.apache.spark.sql.streaming.StreamingQueryListener$QueryStartedEvent"
            ),
            15,
            msg,
        )
        self.assertEqual(
            get_number_of_public_methods(
                "org.apache.spark.sql.streaming.StreamingQueryListener$QueryProgressEvent"
            ),
            12,
            msg,
        )
        self.assertEqual(
            get_number_of_public_methods(
                "org.apache.spark.sql.streaming.StreamingQueryListener$QueryTerminatedEvent"
            ),
            15,
            msg,
        )
        self.assertEqual(
            get_number_of_public_methods("org.apache.spark.sql.streaming.StreamingQueryProgress"),
            38,
            msg,
        )
        self.assertEqual(
            get_number_of_public_methods("org.apache.spark.sql.streaming.StateOperatorProgress"),
            27,
            msg,
        )
        self.assertEqual(
            get_number_of_public_methods("org.apache.spark.sql.streaming.SourceProgress"), 21, msg
        )
        self.assertEqual(
            get_number_of_public_methods("org.apache.spark.sql.streaming.SinkProgress"), 19, msg
        )

    def test_listener_events(self):
        start_event = None
        progress_event = None
        terminated_event = None

        # V1: Initial interface of StreamingQueryListener containing methods `onQueryStarted`,
        # `onQueryProgress`, `onQueryTerminated`. It is prior to Spark 3.5.
        class TestListenerV1(StreamingQueryListener):
            def onQueryStarted(self, event):
                nonlocal start_event
                start_event = event

            def onQueryProgress(self, event):
                nonlocal progress_event
                progress_event = event

            def onQueryTerminated(self, event):
                nonlocal terminated_event
                terminated_event = event

        # V2: The interface after the method `onQueryIdle` is added. It is Spark 3.5+.
        class TestListenerV2(StreamingQueryListener):
            def onQueryStarted(self, event):
                nonlocal start_event
                start_event = event

            def onQueryProgress(self, event):
                nonlocal progress_event
                progress_event = event

            def onQueryIdle(self, event):
                pass

            def onQueryTerminated(self, event):
                nonlocal terminated_event
                terminated_event = event

        def verify(test_listener):
            nonlocal start_event
            nonlocal progress_event
            nonlocal terminated_event

            start_event = None
            progress_event = None
            terminated_event = None

            try:
                self.spark.streams.addListener(test_listener)

                df = self.spark.readStream.format("rate").option("rowsPerSecond", 10).load()

                # check successful stateful query
                df_stateful = df.groupBy().count()  # make query stateful
                q = (
                    df_stateful.writeStream.format("noop")
                    .queryName("test")
                    .outputMode("complete")
                    .start()
                )
                self.assertTrue(q.isActive)
                while progress_event is None or progress_event.batchId == 0:
                    q.awaitTermination(0.5)
                q.stop()

                # Make sure all events are empty
                self.spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty()

                self.check_start_event(start_event)
                self.check_progress_event(progress_event, True)
                self.check_terminated_event(terminated_event)

                # Check query terminated with exception
                from pyspark.sql.functions import col, udf

                bad_udf = udf(lambda x: 1 / 0)
                q = df.select(bad_udf(col("value"))).writeStream.format("noop").start()
                time.sleep(5)
                q.stop()
                self.spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty()
                self.check_terminated_event(terminated_event, "ZeroDivisionError")

            finally:
                self.spark.streams.removeListener(test_listener)

        verify(TestListenerV1())
        verify(TestListenerV2())

    def test_remove_listener(self):
        # SPARK-38804: Test StreamingQueryManager.removeListener
        # V1: Initial interface of StreamingQueryListener containing methods `onQueryStarted`,
        # `onQueryProgress`, `onQueryTerminated`. It is prior to Spark 3.5.
        class TestListenerV1(StreamingQueryListener):
            def onQueryStarted(self, event):
                pass

            def onQueryProgress(self, event):
                pass

            def onQueryTerminated(self, event):
                pass

        # V2: The interface after the method `onQueryIdle` is added. It is Spark 3.5+.
        class TestListenerV2(StreamingQueryListener):
            def onQueryStarted(self, event):
                pass

            def onQueryProgress(self, event):
                pass

            def onQueryIdle(self, event):
                pass

            def onQueryTerminated(self, event):
                pass

        def verify(test_listener):
            num_listeners = len(self.spark.streams._jsqm.listListeners())
            self.spark.streams.addListener(test_listener)
            self.assertEqual(num_listeners + 1, len(self.spark.streams._jsqm.listListeners()))
            self.spark.streams.removeListener(test_listener)
            self.assertEqual(num_listeners, len(self.spark.streams._jsqm.listListeners()))

        verify(TestListenerV1())
        verify(TestListenerV2())

    def test_query_started_event_fromJson(self):
        start_event = """
            {
                "id" : "78923ec2-8f4d-4266-876e-1f50cf3c283b",
                "runId" : "55a95d45-e932-4e08-9caa-0a8ecd9391e8",
                "name" : null,
                "timestamp" : "2023-06-09T18:13:29.741Z"
            }
        """
        start_event = QueryStartedEvent.fromJson(json.loads(start_event))
        self.check_start_event(start_event)
        self.assertEqual(start_event.id, uuid.UUID("78923ec2-8f4d-4266-876e-1f50cf3c283b"))
        self.assertEqual(start_event.runId, uuid.UUID("55a95d45-e932-4e08-9caa-0a8ecd9391e8"))
        self.assertIsNone(start_event.name)
        self.assertEqual(start_event.timestamp, "2023-06-09T18:13:29.741Z")

    def test_query_terminated_event_fromJson(self):
        terminated_json = """
            {
                "id" : "78923ec2-8f4d-4266-876e-1f50cf3c283b",
                "runId" : "55a95d45-e932-4e08-9caa-0a8ecd9391e8",
                "exception" : "org.apache.spark.SparkException: Job aborted due to stage failure",
                "errorClassOnException" : null}
        """
        terminated_event = QueryTerminatedEvent.fromJson(json.loads(terminated_json))
        self.check_terminated_event(terminated_event, "SparkException")
        self.assertEqual(terminated_event.id, uuid.UUID("78923ec2-8f4d-4266-876e-1f50cf3c283b"))
        self.assertEqual(terminated_event.runId, uuid.UUID("55a95d45-e932-4e08-9caa-0a8ecd9391e8"))
        self.assertIn("SparkException", terminated_event.exception)
        self.assertIsNone(terminated_event.errorClassOnException)

    def test_streaming_query_progress_fromJson(self):
        progress_json = """
            {
              "id" : "00000000-0000-0001-0000-000000000001",
              "runId" : "00000000-0000-0001-0000-000000000002",
              "name" : "test",
              "timestamp" : "2016-12-05T20:54:20.827Z",
              "batchId" : 2,
              "numInputRows" : 678,
              "inputRowsPerSecond" : 10.0,
              "processedRowsPerSecond" : 5.4,
              "batchDuration": 5,
              "durationMs" : {
                "getBatch" : 0
              },
              "eventTime" : {
                "min" : "2016-12-05T20:54:20.827Z",
                "avg" : "2016-12-05T20:54:20.827Z",
                "watermark" : "2016-12-05T20:54:20.827Z",
                "max" : "2016-12-05T20:54:20.827Z"
              },
              "stateOperators" : [ {
                "operatorName" : "op1",
                "numRowsTotal" : 0,
                "numRowsUpdated" : 1,
                "allUpdatesTimeMs" : 1,
                "numRowsRemoved" : 2,
                "allRemovalsTimeMs" : 34,
                "commitTimeMs" : 23,
                "memoryUsedBytes" : 3,
                "numRowsDroppedByWatermark" : 0,
                "numShufflePartitions" : 2,
                "numStateStoreInstances" : 2,
                "customMetrics" : {
                  "loadedMapCacheHitCount" : 1,
                  "loadedMapCacheMissCount" : 0,
                  "stateOnCurrentVersionSizeBytes" : 2
                }
              } ],
              "sources" : [ {
                "description" : "source",
                "startOffset" : 123,
                "endOffset" : 456,
                "latestOffset" : 789,
                "numInputRows" : 678,
                "inputRowsPerSecond" : 10.0,
                "processedRowsPerSecond" : 5.4,
                "metrics": {}
              } ],
              "sink" : {
                "description" : "sink",
                "numOutputRows" : -1,
                "metrics": {}
              },
              "observedMetrics" : {
                "event1" : {
                  "c1" : 1,
                  "c2" : 3.0
                },
                "event2" : {
                  "rc" : 1,
                  "min_q" : "hello",
                  "max_q" : "world"
                }
              }
            }
        """
        progress = StreamingQueryProgress.fromJson(json.loads(progress_json))

        self.check_streaming_query_progress(progress, True)

        # checks for progress
        self.assertEqual(progress.id, uuid.UUID("00000000-0000-0001-0000-000000000001"))
        self.assertEqual(progress.runId, uuid.UUID("00000000-0000-0001-0000-000000000002"))
        self.assertEqual(progress.name, "test")
        self.assertEqual(progress.timestamp, "2016-12-05T20:54:20.827Z")
        self.assertEqual(progress.batchId, 2)
        self.assertEqual(progress.numInputRows, 678)
        self.assertEqual(progress.inputRowsPerSecond, 10.0)
        self.assertEqual(progress.batchDuration, 5)
        self.assertEqual(progress.durationMs, {"getBatch": 0})
        self.assertEqual(
            progress.eventTime,
            {
                "min": "2016-12-05T20:54:20.827Z",
                "avg": "2016-12-05T20:54:20.827Z",
                "watermark": "2016-12-05T20:54:20.827Z",
                "max": "2016-12-05T20:54:20.827Z",
            },
        )
        self.assertEqual(
            progress.observedMetrics,
            {
                "event1": Row("c1", "c2")(1, 3.0),
                "event2": Row("rc", "min_q", "max_q")(1, "hello", "world"),
            },
        )

        # Check stateOperators list
        self.assertEqual(len(progress.stateOperators), 1)
        state_operator = progress.stateOperators[0]
        self.assertTrue(isinstance(state_operator, StateOperatorProgress))
        self.assertEqual(state_operator.operatorName, "op1")
        self.assertEqual(state_operator.numRowsTotal, 0)
        self.assertEqual(state_operator.numRowsUpdated, 1)
        self.assertEqual(state_operator.allUpdatesTimeMs, 1)
        self.assertEqual(state_operator.numRowsRemoved, 2)
        self.assertEqual(state_operator.allRemovalsTimeMs, 34)
        self.assertEqual(state_operator.commitTimeMs, 23)
        self.assertEqual(state_operator.memoryUsedBytes, 3)
        self.assertEqual(state_operator.numRowsDroppedByWatermark, 0)
        self.assertEqual(state_operator.numShufflePartitions, 2)
        self.assertEqual(state_operator.numStateStoreInstances, 2)
        self.assertEqual(
            state_operator.customMetrics,
            {
                "loadedMapCacheHitCount": 1,
                "loadedMapCacheMissCount": 0,
                "stateOnCurrentVersionSizeBytes": 2,
            },
        )

        # Check sources list
        self.assertEqual(len(progress.sources), 1)
        source = progress.sources[0]
        self.assertTrue(isinstance(source, SourceProgress))
        self.assertEqual(source.description, "source")
        self.assertEqual(source.startOffset, "123")
        self.assertEqual(source.endOffset, "456")
        self.assertEqual(source.latestOffset, "789")
        self.assertEqual(source.numInputRows, 678)
        self.assertEqual(source.inputRowsPerSecond, 10.0)
        self.assertEqual(source.processedRowsPerSecond, 5.4)
        self.assertEqual(source.metrics, {})

        # Check sink
        sink = progress.sink
        self.assertTrue(isinstance(sink, SinkProgress))
        self.assertEqual(sink.description, "sink")
        self.assertEqual(sink.numOutputRows, -1)
        self.assertEqual(sink.metrics, {})

    def test_spark_property_in_listener(self):
        # SPARK-48560: Make StreamingQueryListener.spark settable
        class TestListener(StreamingQueryListener):
            def __init__(self, session):
                self.spark = session

            def onQueryStarted(self, event):
                pass

            def onQueryProgress(self, event):
                pass

            def onQueryTerminated(self, event):
                pass

        self.assertEqual(TestListener(self.spark).spark, self.spark)


if __name__ == "__main__":
    import unittest

    from pyspark.sql.tests.streaming.test_streaming_listener import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
