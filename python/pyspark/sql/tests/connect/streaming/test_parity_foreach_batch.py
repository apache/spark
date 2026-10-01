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
import uuid

from pyspark.errors import PySparkPicklingError, StreamingQueryException
from pyspark.sql.tests.streaming.test_streaming_foreach_batch import StreamingTestsForeachBatchMixin
from pyspark.testing.connectutils import ReusedConnectTestCase, should_test_connect
from pyspark.testing.utils import eventually, timeout

if should_test_connect:
    from pyspark.errors.exceptions.connect import (
        AnalysisException,
        StreamingPythonRunnerInitializationException,
    )


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
        # Tests that closured DataFrames and batch DataFrames can both be used
        # inside foreachBatch. In Connect, batch_df runs in a separate cloned session,
        # so we use saveAsTable (visible cross-session) instead of temp views.
        def curried_function(df):
            def inner(batch_df, batch_id):
                df.write.format("parquet").mode("overwrite").saveAsTable("nested_df_updates")
                batch_df.write.format("parquet").mode("overwrite").saveAsTable(
                    "nested_df_batch_updates"
                )

            return inner

        q = None
        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            other_df = self.spark.range(100)
            q = df.writeStream.foreachBatch(curried_function(other_df)).start()
            q.processAllAvailable()
            collected = self.spark.sql("select * from nested_df_batch_updates").collect()
            self.assertTrue(len(collected) > 0)
            self.assertEqual(100, self.spark.sql("select * from nested_df_updates").count())
        finally:
            if q:
                q.stop()
            self.spark.sql("DROP TABLE IF EXISTS nested_df_updates")
            self.spark.sql("DROP TABLE IF EXISTS nested_df_batch_updates")

    def test_temp_view_is_isolated_from_root_session(self):
        # A temp view created inside foreachBatch lives in the cloned session and must
        # not be visible from the root session.
        suffix = uuid.uuid4().hex
        view_name = f"cloned_only_view_{suffix}"
        sentinel_table = f"cloned_session_ran_{suffix}"

        def collect_batch(batch_df, _):
            from pyspark.sql import SparkSession

            # The batch runs on the cloned session, so that is what the standard entry point
            # must hand back inside the function.
            assert SparkSession.active() is batch_df.sparkSession, (
                f"active session {SparkSession.active().session_id} is not the batch session "
                f"{batch_df.sparkSession.session_id}"
            )
            batch_df.createOrReplaceTempView(view_name)
            # Read through the cloned session; a resolution failure fails the query.
            batch_df.sparkSession.sql(f"SELECT * FROM {view_name}").collect()
            # Persist a sentinel so the test can prove collect_batch actually ran,
            # otherwise the negative assertion below would trivially pass.
            batch_df.sparkSession.createDataFrame([(1,)], ["v"]).write.mode(
                "overwrite"
            ).saveAsTable(sentinel_table)

        q = None
        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(collect_batch).start()
            q.processAllAvailable()
            self.assertEqual(1, self.spark.table(sentinel_table).count())
            with self.assertRaisesRegex(AnalysisException, "TABLE_OR_VIEW_NOT_FOUND"):
                self.spark.sql(f"SELECT * FROM {view_name}").collect()
        finally:
            if q:
                q.stop()
            self.spark.sql(f"DROP TABLE IF EXISTS {sentinel_table}")
            # Best-effort cleanup in case a regression ever leaks the view.
            try:
                self.spark.catalog.dropTempView(view_name)
            except Exception:
                pass

    def test_batch_uses_stream_session_for_stateful_aqe(self):
        # SPARK-44462: the batch runs on the stream's cloned session, which pins the stateful
        # configs (AQE off) independently of the root session. Proves the isolation this PR adds.
        root = self.spark

        def process(batch_df, _):
            batch_count = batch_df.count()
            root_aqe = root.conf.get("spark.sql.adaptive.enabled")
            batch_aqe = batch_df.sparkSession.conf.get("spark.sql.adaptive.enabled")
            same_session = root.session_id == batch_df.sparkSession.session_id
            if same_session or root_aqe != "true" or batch_aqe != "false":
                raise AssertionError(
                    f"batch_count={batch_count}, same_session={same_session}, "
                    f"AQE root={root_aqe}, batch={batch_aqe}"
                )

        q = None
        with self.sql_conf({"spark.sql.adaptive.enabled": "true"}):
            try:
                df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
                q = (
                    df.groupBy("value")
                    .count()
                    .writeStream.outputMode("complete")
                    .foreachBatch(process)
                    .start()
                )
                q.processAllAvailable()
                self.assertIsNone(q.exception())
            finally:
                if q:
                    q.stop()

    def test_batch_dataframe_in_root_session_sql_is_unsupported(self):
        # SPARK-44462 contract: the batch DataFrame is bound to the stream's cloned session, so its
        # cached relation cannot be resolved through a captured root session. Documented behavior.
        root = self.spark

        def process(batch_df, _):
            root.sql("SELECT value FROM {batch}", batch=batch_df).collect()

        q = None
        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(process).start()
            self.assertRaises(StreamingQueryException, q.processAllAvailable)
            self.assertIn("DATAFRAME_NOT_FOUND", str(q.exception()))
        finally:
            if q:
                q.stop()

    def test_batch_dataframe_join_with_root_session_dataframe_is_unsupported(self):
        # SPARK-44462 contract: the batch DataFrame (cloned session) cannot be combined with a
        # DataFrame captured from the root session; the client rejects the cross-session join.
        lookup = self.spark.range(2).selectExpr(
            "CASE id WHEN 0 THEN 'hello' ELSE 'this' END AS value"
        )

        def process(batch_df, _):
            batch_df.join(lookup, "value").collect()

        q = None
        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(process).start()
            self.assertRaises(StreamingQueryException, q.processAllAvailable)
            self.assertIn("SESSION_NOT_SAME", str(q.exception()))
        finally:
            if q:
                q.stop()

    def test_batch_dataframe_in_batch_session_sql_is_supported(self):
        # SPARK-44462 migration path: resolve the batch DataFrame through its own (cloned) session
        # instead of a captured root session.
        def process(batch_df, _):
            expected = sorted(row.value for row in batch_df.select("value").collect())
            actual = sorted(
                row.value
                for row in batch_df.sparkSession.sql(
                    "SELECT value FROM {batch}", batch=batch_df
                ).collect()
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

    def test_batch_dataframe_join_in_batch_session_is_supported(self):
        # SPARK-44462 migration path: rebuild the lookup on the batch's own (cloned) session so the
        # join stays within a single session.
        def process(batch_df, _):
            spark = batch_df.sparkSession
            lookup = spark.range(2).selectExpr(
                "CASE id WHEN 0 THEN 'hello' ELSE 'this' END AS value"
            )
            expected = sorted(row.value for row in batch_df.select("value").collect())
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

    def test_disabling_fix_restores_root_session_behavior(self):
        # With the fix disabled, the batch DataFrame is bound to the root session again, so it can
        # be combined with a DataFrame captured from the root session.
        with self.sql_conf(
            {"spark.sql.connect.streaming.foreachBatch.useClonedSession": "false"}
        ):
            lookup = self.spark.range(2).selectExpr(
                "CASE id WHEN 0 THEN 'hello' ELSE 'this' END AS value"
            )

            def process(batch_df, _):
                expected = sorted(row.value for row in batch_df.select("value").collect())
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
