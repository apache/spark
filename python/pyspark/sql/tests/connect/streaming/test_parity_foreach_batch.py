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

from pyspark.errors import PySparkPicklingError
from pyspark.sql.tests.streaming.test_streaming_foreach_batch import StreamingTestsForeachBatchMixin
from pyspark.testing.connectutils import ReusedConnectTestCase, should_test_connect
from pyspark.testing.utils import eventually, timeout

if should_test_connect:
    from pyspark.errors.exceptions.connect import StreamingPythonRunnerInitializationException


class StreamingForeachBatchParityTests(StreamingTestsForeachBatchMixin, ReusedConnectTestCase):
    def test_streaming_foreach_batch_propagates_python_errors(self):
        super().test_streaming_foreach_batch_propagates_python_errors()

    @eventually(timeout=180, catch_timeout=True)
    @timeout(timeout=60)
    def test_streaming_foreach_batch_graceful_stop(self):
        # SPARK-39218: Make foreachBatch streaming query stop gracefully
        def func(batch_df, _):
            time.sleep(10)

        q = self.spark.readStream.format("rate").load().writeStream.foreachBatch(func).start()
        time.sleep(3)  # 'rowsPerSecond' defaults to 1. Waits 3 secs out for the input.
        q.stop()
        self.assertIsNone(q.exception(), "No exception has to be propagated.")

    def test_nested_dataframes(self):
        def curried_function(df):
            def inner(batch_df, batch_id):
                df.createOrReplaceTempView("updates")
                batch_df.createOrReplaceTempView("batch_updates")

            return inner

        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            other_df = self.spark.range(100)
            q = df.writeStream.foreachBatch(curried_function(other_df)).start()
            q.processAllAvailable()
            collected = self.spark.sql("select * from batch_updates").collect()
            self.assertTrue(len(collected), 2)
            self.assertEqual(100, self.spark.sql("select * from updates").count())
        finally:
            if q:
                q.stop()

    def test_batch_dataframe_in_sql_from_captured_session(self):
        """Expected: #55410 fails DATAFRAME_NOT_FOUND; final fix passes with batch rows."""
        root = self.spark

        def process(batch_df, _):
            expected = sorted(row.value for row in batch_df.select("value").collect())
            if root.range(1).count() != 1:
                raise AssertionError("Captured session cannot run an independent query")
            actual = sorted(
                row.value
                for row in root.sql("SELECT value FROM {batch}", batch=batch_df).collect()
            )
            if actual != expected:
                raise AssertionError(f"SQL rows {actual} != batch rows {expected}")

        q = None
        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(process).start()
            q.processAllAvailable()
            self.assertTrue(any(p.numInputRows > 0 for p in q.recentProgress))
            self.assertIsNone(q.exception())
        finally:
            if q:
                q.stop()

    def test_batch_uses_stream_session_for_stateful_aqe(self):
        """Expected: master fails the AQE check; #55410 and final fix pass."""
        root = self.spark

        def process(batch_df, _):
            batch_count = batch_df.count()
            root_aqe = root.conf.get("spark.sql.adaptive.enabled")
            batch_aqe = batch_df.sparkSession.conf.get("spark.sql.adaptive.enabled")
            same_session = root.session_id == batch_df.sparkSession.session_id
            if same_session or root_aqe != "true" or batch_aqe != "false":
                raise AssertionError(
                    f"batch_count={batch_count}, same_session={same_session}, "
                    f"AQE root={root_aqe}, batch={batch_aqe}; "
                    f"root session={root.session_id}, "
                    f"batch session={batch_df.sparkSession.session_id}"
                )

        q = None
        with self.sql_conf({"spark.sql.adaptive.enabled": "true"}):
            try:
                df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
                q = df.groupBy("value").count().writeStream.outputMode("complete").foreachBatch(
                    process
                ).start()
                q.processAllAvailable()
                self.assertTrue(any(p.numInputRows > 0 for p in q.recentProgress))
                self.assertIsNone(q.exception())
            finally:
                if q:
                    q.stop()

    def test_batch_dataframe_join_with_captured_dataframe(self):
        """Expected: #55410 fails SESSION_NOT_SAME; final fix passes with joined rows."""
        lookup = self.spark.range(2).selectExpr(
            "CASE id WHEN 0 THEN 'hello' ELSE 'this' END AS value"
        )

        def process(batch_df, _):
            expected = sorted(row.value for row in batch_df.select("value").collect())
            lookup_values = sorted(row.value for row in lookup.select("value").collect())
            if lookup_values != ["hello", "this"]:
                raise AssertionError(f"Captured lookup rows changed: {lookup_values}")
            actual = sorted(
                row.value for row in batch_df.join(lookup, "value").select("value").collect()
            )
            if actual != expected:
                raise AssertionError(f"Joined rows {actual} != batch rows {expected}")

        q = None
        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(process).start()
            q.processAllAvailable()
            self.assertTrue(any(p.numInputRows > 0 for p in q.recentProgress))
            self.assertIsNone(q.exception())
        finally:
            if q:
                q.stop()

    def test_pickling_error(self):
        class NoPickle:
            def __reduce__(self):
                raise ValueError("No pickle")

        no_pickle = NoPickle()

        def func(df, _):
            print(no_pickle)
            df.count()

        with self.assertRaises(PySparkPicklingError):
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(func).start()
            q.processAllAvailable()

    def test_worker_initialization_error(self):
        class SerializableButNotDeserializable:
            @staticmethod
            def _reduce_function():
                raise ValueError("Cannot unpickle this object")

            def __reduce__(self):
                # Return a static method that cannot be called during unpickling
                return self._reduce_function, ()

        # Create an instance of the class
        obj = SerializableButNotDeserializable()

        df = (
            self.spark.readStream.format("rate")
            .option("rowsPerSecond", "10")
            .option("numPartitions", "1")
            .load()
        )

        obj = SerializableButNotDeserializable()

        def fcn(df, _):
            print(obj)

        # Assert that an exception occurs during the initialization
        with self.assertRaises(StreamingPythonRunnerInitializationException) as error:
            df.select("value").writeStream.foreachBatch(fcn).start()

        # Assert that the error message contains the expected string
        self.assertIn(
            "Streaming Runner initialization failed",
            str(error.exception),
        )

    def test_accessing_spark_session(self):
        spark = self.spark

        def func(df, _):
            spark.createDataFrame([("you", "can"), ("serialize", "spark")]).createOrReplaceTempView(
                "test_accessing_spark_session"
            )

        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(func).start()
            q.processAllAvailable()
            self.assertEqual(2, spark.table("test_accessing_spark_session").count())
        finally:
            if q:
                q.stop()

    def test_accessing_spark_session_through_df(self):
        dataframe = self.spark.createDataFrame([("you", "can"), ("serialize", "dataframe")])

        def func(df, _):
            dataframe.createOrReplaceTempView("test_accessing_spark_session_through_df")

        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(func).start()
            q.processAllAvailable()
            self.assertEqual(2, self.spark.table("test_accessing_spark_session_through_df").count())
        finally:
            if q:
                q.stop()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
