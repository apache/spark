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

from pyspark.testing.sqlutils import ReusedSQLTestCase


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
