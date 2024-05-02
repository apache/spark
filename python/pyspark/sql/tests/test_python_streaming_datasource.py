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
import tempfile
import time
import unittest

from pyspark.sql.datasource import (
    DataSource,
    DataSourceStreamReader,
    InputPartition,
    DataSourceStreamWriter,
    SimpleDataSourceStreamReader,
    WriterCommitMessage,
)
from pyspark.sql.types import Row
from pyspark.testing.sqlutils import (
    have_pyarrow,
    pyarrow_requirement_message,
)
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import ReusedSQLTestCase


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class BasePythonStreamingDataSourceTestsMixin:
    def test_basic_streaming_data_source_class(self):
        class MyDataSource(DataSource):
            ...

        options = dict(a=1, b=2)
        ds = MyDataSource(options=options)
        self.assertEqual(ds.options, options)
        self.assertEqual(ds.name(), "MyDataSource")
        with self.assertRaises(NotImplementedError):
            ds.schema()
        with self.assertRaises(NotImplementedError):
            ds.streamReader(None)
        with self.assertRaises(NotImplementedError):
            ds.streamWriter(None, None)

    def test_basic_data_source_stream_reader_class(self):
        class MyDataSourceStreamReader(DataSourceStreamReader):
            def read(self, partition):
                yield (1, "abc")

        stream_reader = MyDataSourceStreamReader()
        self.assertEqual(list(stream_reader.read(None)), [(1, "abc")])

    def _get_test_data_source(self):
        class RangePartition(InputPartition):
            def __init__(self, start, end):
                self.start = start
                self.end = end

        class TestStreamReader(DataSourceStreamReader):
            current = 0

            def initialOffset(self):
                return {"offset": 0}

            def latestOffset(self):
                self.current += 2
                return {"offset": self.current}

            def partitions(self, start, end):
                return [RangePartition(start["offset"], end["offset"])]

            def commit(self, end):
                pass

            def read(self, partition):
                start, end = partition.start, partition.end
                for i in range(start, end):
                    yield (i,)

        import json
        import os
        from dataclasses import dataclass

        @dataclass
        class SimpleCommitMessage(WriterCommitMessage):
            partition_id: int
            count: int

        class TestStreamWriter(DataSourceStreamWriter):
            def __init__(self, options):
                self.options = options
                self.path = self.options.get("path")
                assert self.path is not None

            def write(self, iterator):
                from pyspark import TaskContext

                context = TaskContext.get()
                partition_id = context.partitionId()
                cnt = 0
                for row in iterator:
                    if row.id > 50:
                        raise Exception("invalid value")
                    cnt += 1
                return SimpleCommitMessage(partition_id=partition_id, count=cnt)

            def commit(self, messages, batchId) -> None:
                status = dict(num_partitions=len(messages), rows=sum(m.count for m in messages))

                with open(os.path.join(self.path, f"{batchId}.json"), "a") as file:
                    file.write(json.dumps(status) + "\\n")

            def abort(self, messages, batchId) -> None:
                with open(os.path.join(self.path, f"{batchId}.txt"), "w") as file:
                    file.write(f"failed in batch {batchId}")

        class TestDataSource(DataSource):
            def schema(self):
                return "id INT"

            def streamReader(self, schema):
                return TestStreamReader()

            def streamWriter(self, schema, overwrite):
                return TestStreamWriter(self.options)

        return TestDataSource

    def test_stream_reader(self):
        self.spark.dataSource.register(self._get_test_data_source())
        df = self.spark.readStream.format("TestDataSource").load()

        def check_batch(df, batch_id):
            assertDataFrameEqual(df, [Row(batch_id * 2), Row(batch_id * 2 + 1)])

        q = df.writeStream.foreachBatch(check_batch).start()
        while len(q.recentProgress) < 10:
            time.sleep(0.2)
        q.stop()
        q.awaitTermination
        self.assertIsNone(q.exception(), "No exception has to be propagated.")

    def test_simple_stream_reader(self):
        class SimpleStreamReader(SimpleDataSourceStreamReader):
            def initialOffset(self):
                return {"offset": 0}

            def read(self, start: dict):
                start_idx = start["offset"]
                it = iter([(i,) for i in range(start_idx, start_idx + 2)])
                return (it, {"offset": start_idx + 2})

            def commit(self, end):
                pass

            def readBetweenOffsets(self, start: dict, end: dict):
                start_idx = start["offset"]
                end_idx = end["offset"]
                return iter([(i,) for i in range(start_idx, end_idx)])

        class SimpleDataSource(DataSource):
            def schema(self):
                return "id INT"

            def simpleStreamReader(self, schema):
                return SimpleStreamReader()

        self.spark.dataSource.register(SimpleDataSource)
        df = self.spark.readStream.format("SimpleDataSource").load()

        def check_batch(df, batch_id):
            assertDataFrameEqual(df, [Row(batch_id * 2), Row(batch_id * 2 + 1)])

        q = df.writeStream.foreachBatch(check_batch).start()
        while len(q.recentProgress) < 10:
            time.sleep(0.2)
        q.stop()
        q.awaitTermination()
        self.assertIsNone(q.exception(), "No exception has to be propagated.")

    def test_stream_writer(self):
        input_dir = tempfile.TemporaryDirectory(prefix="test_data_stream_write_input")
        output_dir = tempfile.TemporaryDirectory(prefix="test_data_stream_write_output")
        checkpoint_dir = tempfile.TemporaryDirectory(prefix="test_data_stream_write_checkpoint")

        try:
            self.spark.range(0, 30).repartition(2).write.format("json").mode("append").save(
                input_dir.name
            )
            self.spark.dataSource.register(self._get_test_data_source())
            df = self.spark.readStream.schema("id int").json(input_dir.name)
            q = (
                df.writeStream.format("TestDataSource")
                .option("checkpointLocation", checkpoint_dir.name)
                .start(output_dir.name)
            )
            while not q.recentProgress:
                time.sleep(0.2)

            # Test stream writer write and commit.
            # The first microbatch contain 30 rows and 2 partitions.
            # Number of rows and partitions is writen by StreamWriter.commit().
            assertDataFrameEqual(self.spark.read.json(output_dir.name), [Row(2, 30)])

            self.spark.range(50, 80).repartition(2).write.format("json").mode("append").save(
                input_dir.name
            )

            # Test StreamWriter write and abort.
            # When row id > 50, write tasks throw exception and fail.
            # 1.txt is written by StreamWriter.abort() to record the failure.
            while q.exception() is None:
                time.sleep(0.2)
            assertDataFrameEqual(
                self.spark.read.text(os.path.join(output_dir.name, "1.txt")),
                [Row("failed in batch 1")],
            )
            q.awaitTermination
        finally:
            input_dir.cleanup()
            output_dir.cleanup()
            checkpoint_dir.cleanup()


class PythonStreamingDataSourceTests(BasePythonStreamingDataSourceTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_python_streaming_datasource import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
