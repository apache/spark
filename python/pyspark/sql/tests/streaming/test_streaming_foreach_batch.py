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
from pyspark.sql.dataframe import DataFrame
from pyspark.testing.sqlutils import ReusedSQLTestCase


def my_test_function_1():
    return 1


class StreamingTestsForeachBatchMixin:
    def test_streaming_foreach_batch(self):
        q = None

        def collectBatch(batch_df, batch_id):
            batch_df.createOrReplaceGlobalTempView("test_view")

        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(collectBatch).start()
            q.processAllAvailable()
            collected = self.spark.sql("select * from global_temp.test_view").collect()
            self.assertTrue(len(collected), 2)
        finally:
            if q:
                q.stop()

    def test_streaming_foreach_batch_tempview(self):
        q = None

        def collectBatch(batch_df, batch_id):
            batch_df.createOrReplaceTempView("updates")
            # it should use the spark session within given DataFrame, as microbatch execution will
            # clone the session which is no longer same with the session used to start the
            # streaming query
            assert len(batch_df.sparkSession.sql("SELECT * FROM updates").collect()) == 2
            # Write to a global view verify on the repl/client side.
            batch_df.createOrReplaceGlobalTempView("temp_view")

        try:
            df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
            q = df.writeStream.foreachBatch(collectBatch).start()
            q.processAllAvailable()
            collected = self.spark.sql("SELECT * FROM global_temp.temp_view").collect()
            self.assertTrue(len(collected[0]), 2)
        finally:
            if q:
                q.stop()

    def test_streaming_foreach_batch_propagates_python_errors(self):
        from pyspark.errors import StreamingQueryException

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

    def test_streaming_foreach_batch_graceful_stop(self):
        # SPARK-39218: Make foreachBatch streaming query stop gracefully
        def func(batch_df, _):
            batch_df.sparkSession._jvm.java.lang.Thread.sleep(10000)

        q = self.spark.readStream.format("rate").load().writeStream.foreachBatch(func).start()
        time.sleep(3)  # 'rowsPerSecond' defaults to 1. Waits 3 secs out for the input.
        q.stop()
        self.assertIsNone(q.exception(), "No exception has to be propagated.")

    def test_streaming_foreach_batch_spark_session(self):
        table_name = "testTable_foreach_batch"

        def func(df: DataFrame, batch_id: int):
            if batch_id > 0:  # only process once
                return
            spark = df.sparkSession
            df1 = spark.createDataFrame([("structured",), ("streaming",)])
            df1.union(df).write.mode("append").saveAsTable(table_name)

        df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
        q = df.writeStream.foreachBatch(func).start()
        q.processAllAvailable()
        q.stop()

        actual = self.spark.read.table(table_name)
        df = (
            self.spark.read.format("text")
            .load(path="python/test_support/sql/streaming/")
            .union(self.spark.createDataFrame([("structured",), ("streaming",)]))
        )
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

    def test_streaming_foreach_batch_path_access(self):
        table_name = "testTable_foreach_batch_path"

        def func(df: DataFrame, batch_id: int):
            if batch_id > 0:  # only process once
                return
            spark = df.sparkSession
            df1 = spark.read.format("text").load("python/test_support/sql/streaming")
            df1.union(df).write.mode("append").saveAsTable(table_name)

        df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")
        q = df.writeStream.foreachBatch(func).start()
        q.processAllAvailable()
        q.stop()

        actual = self.spark.read.table(table_name)
        df = self.spark.read.format("text").load(path="python/test_support/sql/streaming/")
        df = df.union(df)
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

    @staticmethod
    def my_test_function_2():
        return 2

    def test_streaming_foreach_batch_function_calling(self):
        def my_test_function_3():
            return 3

        table_name = "testTable_foreach_batch_function"

        def func(df: DataFrame, batch_id: int):
            if batch_id > 0:  # only process once
                return
            spark = df.sparkSession
            df1 = spark.createDataFrame(
                [
                    (my_test_function_1(),),
                    (StreamingTestsForeachBatchMixin.my_test_function_2(),),
                    (my_test_function_3(),),
                ]
            )
            df1.write.mode("append").saveAsTable(table_name)

        df = self.spark.readStream.format("rate").load()
        q = df.writeStream.foreachBatch(func).start()
        q.processAllAvailable()
        q.stop()

        actual = self.spark.read.table(table_name)
        df = self.spark.createDataFrame(
            [
                (my_test_function_1(),),
                (StreamingTestsForeachBatchMixin.my_test_function_2(),),
                (my_test_function_3(),),
            ]
        )
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))

    def test_streaming_foreach_batch_import(self):
        import time  # not imported in foreach_batch_worker

        table_name = "testTable_foreach_batch_import"

        def func(df: DataFrame, batch_id: int):
            if batch_id > 0:  # only process once
                return
            time.sleep(1)
            spark = df.sparkSession
            df1 = spark.read.format("text").load("python/test_support/sql/streaming")
            df1.write.mode("append").saveAsTable(table_name)

        df = self.spark.readStream.format("rate").load()
        q = df.writeStream.foreachBatch(func).start()
        q.processAllAvailable()
        q.stop()

        actual = self.spark.read.table(table_name)
        df = self.spark.read.format("text").load("python/test_support/sql/streaming")
        self.assertEqual(sorted(df.collect()), sorted(actual.collect()))


class StreamingTestsForeachBatch(StreamingTestsForeachBatchMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.streaming.test_streaming_foreach_batch import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
