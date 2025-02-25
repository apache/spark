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

from pyspark.sql.tests.streaming.test_streaming_foreach_batch import StreamingTestsForeachBatchMixin
from pyspark.testing.connectutils import ReusedConnectTestCase, should_test_connect
from pyspark.errors import PySparkPicklingError

if should_test_connect:
    from pyspark.errors.exceptions.connect import StreamingPythonRunnerInitializationException


class StreamingForeachBatchParityTests(StreamingTestsForeachBatchMixin, ReusedConnectTestCase):
    def test_streaming_foreach_batch_propagates_python_errors(self):
        super().test_streaming_foreach_batch_propagates_python_errors()

    @unittest.skip("This seems specific to py4j and pinned threads. The intention is unclear")
    def test_streaming_foreach_batch_graceful_stop(self):
        super().test_streaming_foreach_batch_graceful_stop()

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
    import unittest
    from pyspark.sql.tests.connect.streaming.test_parity_foreach_batch import *  # noqa: F401,E501

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
