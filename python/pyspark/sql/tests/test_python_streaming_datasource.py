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
    DataSourceStreamArrowWriter,
    SimpleDataSourceStreamReader,
    WriterCommitMessage,
)
from pyspark.sql.streaming import StreamingQueryException
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
        q.awaitTermination()
        self.assertIsNone(q.exception(), "No exception has to be propagated.")

    def test_stream_reader_pyarrow(self):
        import pyarrow as pa

        class TestStreamReader(DataSourceStreamReader):
            def initialOffset(self):
                return {"offset": 0}

            def latestOffset(self):
                return {"offset": 2}

            def partitions(self, start, end):
                # hardcoded number of partitions
                num_part = 1
                return [InputPartition(i) for i in range(num_part)]

            def read(self, partition):
                keys = pa.array([1, 2, 3, 4, 5], type=pa.int32())
                values = pa.array(["one", "two", "three", "four", "five"], type=pa.string())
                schema = pa.schema([("key", pa.int32()), ("value", pa.string())])
                record_batch = pa.RecordBatch.from_arrays([keys, values], schema=schema)
                yield record_batch

        class TestDataSourcePyarrow(DataSource):
            @classmethod
            def name(cls):
                return "testdatasourcepyarrow"

            def schema(self):
                return "key int, value string"

            def streamReader(self, schema):
                return TestStreamReader()

        self.spark.dataSource.register(TestDataSourcePyarrow)
        df = self.spark.readStream.format("testdatasourcepyarrow").load()

        output_dir = tempfile.TemporaryDirectory(prefix="test_data_stream_write_output")
        checkpoint_dir = tempfile.TemporaryDirectory(prefix="test_data_stream_write_checkpoint")

        q = (
            df.writeStream.format("json")
            .option("checkpointLocation", checkpoint_dir.name)
            .start(output_dir.name)
        )
        while not q.recentProgress:
            time.sleep(0.2)
        q.stop()
        q.awaitTermination()

        expected_data = [
            Row(key=1, value="one"),
            Row(key=2, value="two"),
            Row(key=3, value="three"),
            Row(key=4, value="four"),
            Row(key=5, value="five"),
        ]
        df = self.spark.read.json(output_dir.name)

        assertDataFrameEqual(df, expected_data)

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
            q.awaitTermination()
        except StreamingQueryException as e:
            self.assertIn("invalid value", str(e))
        finally:
            input_dir.cleanup()
            output_dir.cleanup()
            checkpoint_dir.cleanup()

    def test_stream_arrow_writer(self):
        """Test DataSourceStreamArrowWriter with Arrow RecordBatch format."""
        import tempfile
        import shutil
        import json
        import os
        import pyarrow as pa
        from dataclasses import dataclass

        @dataclass
        class ArrowCommitMessage(WriterCommitMessage):
            partition_id: int
            batch_count: int
            total_rows: int

        class TestStreamArrowWriter(DataSourceStreamArrowWriter):
            def __init__(self, options):
                self.options = options
                self.path = self.options.get("path")
                assert self.path is not None

            def write(self, iterator):
                from pyspark import TaskContext

                context = TaskContext.get()
                partition_id = context.partitionId()
                batch_count = 0
                total_rows = 0

                for batch in iterator:
                    assert isinstance(batch, pa.RecordBatch)
                    batch_count += 1
                    total_rows += batch.num_rows

                    # Convert to pandas and write to temp JSON file
                    df = batch.to_pandas()

                    filename = f"partition_{partition_id}_batch_{batch_count}.json"
                    filepath = os.path.join(self.path, filename)

                    # Actually write the JSON file
                    df.to_json(filepath, orient="records")

                commit_msg = ArrowCommitMessage(
                    partition_id=partition_id, batch_count=batch_count, total_rows=total_rows
                )
                return commit_msg

            def commit(self, messages, batchId):
                """Write commit metadata for successful batch."""
                total_batches = sum(m.batch_count for m in messages if m)
                total_rows = sum(m.total_rows for m in messages if m)

                status = {
                    "batch_id": batchId,
                    "num_partitions": len([m for m in messages if m]),
                    "total_batches": total_batches,
                    "total_rows": total_rows,
                }

                with open(os.path.join(self.path, f"commit_{batchId}.json"), "w") as f:
                    json.dump(status, f)

            def abort(self, messages, batchId):
                """Handle batch failure."""
                with open(os.path.join(self.path, f"abort_{batchId}.txt"), "w") as f:
                    f.write(f"Batch {batchId} aborted")

        class TestDataSource(DataSource):
            @classmethod
            def name(cls):
                return "TestArrowStreamWriter"

            def schema(self):
                return "id INT, name STRING, value DOUBLE"

            def streamWriter(self, schema, overwrite):
                return TestStreamArrowWriter(self.options)

        # Create temporary directory for test
        temp_dir = tempfile.mkdtemp()
        try:
            # Register the data source
            self.spark.dataSource.register(TestDataSource)

            # Create test data
            df = (
                self.spark.readStream.format("rate")
                .option("rowsPerSecond", 10)
                .option("numPartitions", 3)
                .load()
                .selectExpr("value as id", "concat('name_', value) as name", "value * 2.5 as value")
            )

            # Write using streaming with Arrow writer
            query = (
                df.writeStream.format("TestArrowStreamWriter")
                .option("path", temp_dir)
                .option("checkpointLocation", os.path.join(temp_dir, "checkpoint"))
                .trigger(processingTime="1 seconds")
                .start()
            )

            # Wait a bit for data to be processed, then stop
            time.sleep(6)  # Allow a few batches to run
            query.stop()
            query.awaitTermination()

            # Since we're writing actual JSON files, verify commit metadata and written files
            commit_files = [f for f in os.listdir(temp_dir) if f.startswith("commit_")]
            self.assertTrue(len(commit_files) > 0, "No commit files were created")

            # Read and verify commit metadata - check all commits for any with data
            total_committed_rows = 0
            total_committed_batches = 0

            for commit_file in commit_files:
                with open(os.path.join(temp_dir, commit_file), "r") as f:
                    commit_data = json.load(f)
                    total_committed_rows += commit_data.get("total_rows", 0)
                    total_committed_batches += commit_data.get("total_batches", 0)

            # We should have both committed data AND JSON files written
            json_files = [
                f
                for f in os.listdir(temp_dir)
                if f.startswith("partition_") and f.endswith(".json")
            ]

            # Verify that we have both committed data AND JSON files
            has_committed_data = total_committed_rows > 0
            has_json_files = len(json_files) > 0

            self.assertTrue(
                has_committed_data, f"Expected committed data but got {total_committed_rows} rows"
            )
            self.assertTrue(
                has_json_files, f"Expected JSON files but found {len(json_files)} files"
            )

            # Verify JSON files contain valid data
            for json_file in json_files:
                with open(os.path.join(temp_dir, json_file), "r") as f:
                    data = json.load(f)
                    self.assertTrue(len(data) > 0, f"JSON file {json_file} is empty")

        finally:
            # Clean up
            shutil.rmtree(temp_dir, ignore_errors=True)


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
