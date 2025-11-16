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
import os
import time
import unittest

from pyspark.sql.datasource import DataSource, DataSourceStreamReader
from pyspark.sql.functions import F
from pyspark.sql.streaming import StreamTest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class RateLimitStreamReader(DataSourceStreamReader):
    def __init__(self, start, max_rows_per_batch):
        self._start = start
        self._max_rows_per_batch = max_rows_per_batch
        self._next_offset = start

    def initialOffset(self):
        return str(self._start)

    def latestOffset(self, start, read_limit):
        max_rows = read_limit.get("maxRows", self._max_rows_per_batch)
        self._next_offset += max_rows
        return str(self._next_offset)

    def partitions(self, start, end):
        return [str(i).encode("utf-8") for i in range(int(start), int(end))]


class RateLimitDataSource(DataSource):
    def __init__(self, options):
        self._max_rows_per_batch = int(options.get("maxRowsPerBatch", "100"))

    def streamReader(self, schema):
        return RateLimitStreamReader(0, self._max_rows_per_batch)


class BackwardCompatibilityStreamReader(DataSourceStreamReader):
    def __init__(self, start):
        self._start = start
        self._next_offset = start

    def initialOffset(self):
        return str(self._start)

    def latestOffset(self):
        self._next_offset += 1
        return str(self._next_offset)

    def partitions(self, start, end):
        return [str(i).encode("utf-8") for i in range(int(start), int(end))]


class BackwardCompatibilityDataSource(DataSource):
    def streamReader(self, schema):
        return BackwardCompatibilityStreamReader(0)


class StreamingDataSourceAdmissionControlTests(StreamTest):
    def test_backward_compatibility(self):
        df = (
            self.spark.readStream.format(
                "org.apache.spark.sql.streaming.test.BackwardCompatibilityDataSource"
            )
            .option("includeTimestamp", "true")
            .load()
        )
        self.assertTrue(df.isStreaming)

        q = df.writeStream.queryName("test").format("memory").start()
        try:
            time.sleep(5)
            self.assertTrue(self.spark.table("test").count() > 0)
        finally:
            q.stop()

    def test_rate_limit(self):
        df = (
            self.spark.readStream.format("org.apache.spark.sql.streaming.test.RateLimitDataSource")
            .option("maxRowsPerBatch", "5")
            .load()
        )
        self.assertTrue(df.isStreaming)

        q = df.writeStream.queryName("test_rate_limit").format("memory").start()
        try:
            time.sleep(5)
            # The exact count can vary, but it should be a multiple of 5.
            count = self.spark.table("test_rate_limit").count()
            self.assertTrue(count > 0)
            self.assertEqual(count % 5, 0)
        finally:
            q.stop()


if __name__ == "__main__":
    from pyspark.sql.tests.streaming.test_streaming_datasource_admission_control import *

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
