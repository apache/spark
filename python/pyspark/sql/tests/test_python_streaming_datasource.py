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
import os
import tempfile
import time
import unittest

from pyspark.errors import PySparkException
from pyspark.sql.datasource import (
    DataSource,
    DataSourceStreamArrowWriter,
    DataSourceStreamReader,
    DataSourceStreamWriter,
    InputPartition,
    SimpleDataSourceStreamReader,
    WriterCommitMessage,
)
from pyspark.sql.streaming import StreamingQueryException
from pyspark.sql.streaming.datasource import (
    ReadAllAvailable,
    ReadLimit,
    ReadMaxRows,
    SupportsTriggerAvailableNow,
)
from pyspark.sql.types import Row
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import eventually, have_pyarrow, pyarrow_requirement_message


def wait_for_condition(query, condition_fn, timeout_sec=30):
    """
    Wait for a condition on a streaming query to be met, with timeout and error context.

    :param query: StreamingQuery object
    :param condition_fn: Function that takes query and returns True when condition is met
    :param timeout_sec: Timeout in seconds (default 30)
    :raises TimeoutError: If condition is not met within timeout, with query context
    """
    start_time = time.time()
    sleep_interval = 0.2

    while not condition_fn(query):
        elapsed = time.time() - start_time
        if elapsed >= timeout_sec:
            # Collect context for debugging
            exception_info = query.exception()
            recent_progresses = query.recentProgress

            error_msg = (
                f"Timeout after {timeout_sec} seconds waiting for condition. "
                f"Query exception: {exception_info}. "
                f"Recent progress count: {len(recent_progresses)}. "
            )

            if recent_progresses:
                error_msg += f"Last progress: {recent_progresses[-1]}. "
                error_msg += f"All recent progresses: {recent_progresses}"
            else:
                error_msg += "No progress recorded."

            raise TimeoutError(error_msg)

        time.sleep(sleep_interval)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class BasePythonStreamingDataSourceTestsMixin:
    def test_basic_streaming_data_source_class(self):
        class MyDataSource(DataSource): ...

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

            def latestOffset(self, start, limit):
                return {"offset": start["offset"] + 2}

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

    def _get_test_data_source_old_latest_offset_signature(self):
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

        class TestDataSource(DataSource):
            def schema(self):
                return "id INT"

            def streamReader(self, schema):
                return TestStreamReader()

        return TestDataSource

    def _get_test_data_source_for_admission_control(self):
        class TestDataStreamReader(DataSourceStreamReader):
            def initialOffset(self):
                return {"partition-1": 0}

            def getDefaultReadLimit(self):
                return ReadMaxRows(2)

            def latestOffset(self, start: dict, limit: ReadLimit):
                start_idx = start["partition-1"]
                if isinstance(limit, ReadAllAvailable):
                    end_offset = start_idx + 10
                else:
                    assert isinstance(limit, ReadMaxRows), (
                        "Expected ReadMaxRows read limit but got " + str(type(limit))
                    )
                    end_offset = start_idx + limit.max_rows
                return {"partition-1": end_offset}

            def reportLatestOffset(self):
                return {"partition-1": 1000000}

            def partitions(self, start: dict, end: dict):
                start_index = start["partition-1"]
                end_index = end["partition-1"]
                return [InputPartition(i) for i in range(start_index, end_index)]

            def read(self, partition):
                yield (partition.value,)

        class TestDataSource(DataSource):
            def schema(self) -> str:
                return "id INT"

            def streamReader(self, schema):
                return TestDataStreamReader()

        return TestDataSource

    def _get_test_data_source_for_trigger_available_now(self):
        class TestDataStreamReader(DataSourceStreamReader, SupportsTriggerAvailableNow):
            def initialOffset(self):
                return {"partition-1": 0}

            def getDefaultReadLimit(self):
                return ReadMaxRows(2)

            def latestOffset(self, start: dict, limit: ReadLimit):
                start_idx = start["partition-1"]
                if isinstance(limit, ReadAllAvailable):
                    end_offset = start_idx + 10
                else:
                    assert isinstance(limit, ReadMaxRows), (
                        "Expected ReadMaxRows read limit but got " + str(type(limit))
                    )
                    end_offset = min(
                        start_idx + limit.max_rows, self.desired_end_offset["partition-1"]
                    )
                return {"partition-1": end_offset}

            def reportLatestOffset(self):
                return {"partition-1": 1000000}

            def prepareForTriggerAvailableNow(self) -> None:
                self.desired_end_offset = {"partition-1": 10}

            def partitions(self, start: dict, end: dict):
                start_index = start["partition-1"]
                end_index = end["partition-1"]
                return [InputPartition(i) for i in range(start_index, end_index)]

            def read(self, partition):
                yield (partition.value,)

        class TestDataSource(DataSource):
            def schema(self) -> str:
                return "id INT"

            def streamReader(self, schema):
                return TestDataStreamReader()

        return TestDataSource

    def _test_stream_reader(self, test_data_source):
        self.spark.dataSource.register(test_data_source)
        df = self.spark.readStream.format("TestDataSource").load()

        def check_batch(df, batch_id):
            assertDataFrameEqual(df, [Row(batch_id * 2), Row(batch_id * 2 + 1)])

        q = df.writeStream.foreachBatch(check_batch).start()
        wait_for_condition(q, lambda query: len(query.recentProgress) >= 5)
        q.stop()
        q.awaitTermination()
        self.assertIsNone(q.exception(), "No exception has to be propagated.")

    def test_stream_reader(self):
        self._test_stream_reader(self._get_test_data_source())

    def test_stream_reader_old_latest_offset_signature(self):
        self._test_stream_reader(self._get_test_data_source_old_latest_offset_signature())

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
        wait_for_condition(q, lambda query: len(query.recentProgress) > 0)
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

    def test_stream_reader_admission_control_trigger_once(self):
        self.spark.dataSource.register(self._get_test_data_source_for_admission_control())
        df = self.spark.readStream.format("TestDataSource").load()

        def check_batch(df, batch_id):
            assertDataFrameEqual(df, [Row(x) for x in range(10)])

        q = df.writeStream.trigger(once=True).foreachBatch(check_batch).start()
        q.awaitTermination()
        self.assertIsNone(q.exception(), "No exception has to be propagated.")
        self.assertEqual(len(q.recentProgress), 1)
        self.assertEqual(q.lastProgress.numInputRows, 10)
        self.assertEqual(q.lastProgress.sources[0].numInputRows, 10)
        self.assertEqual(
            json.loads(q.lastProgress.sources[0].latestOffset), {"partition-1": 1000000}
        )

    def test_stream_reader_admission_control_processing_time_trigger(self):
        self.spark.dataSource.register(self._get_test_data_source_for_admission_control())
        df = self.spark.readStream.format("TestDataSource").load()

        def check_batch(df, batch_id):
            assertDataFrameEqual(df, [Row(batch_id * 2), Row(batch_id * 2 + 1)])

        q = df.writeStream.foreachBatch(check_batch).start()
        wait_for_condition(q, lambda query: len(query.recentProgress) >= 5)
        q.stop()
        q.awaitTermination()
        self.assertIsNone(q.exception(), "No exception has to be propagated.")

    def test_stream_reader_trigger_available_now(self):
        self.spark.dataSource.register(self._get_test_data_source_for_trigger_available_now())
        df = self.spark.readStream.format("TestDataSource").load()

        def check_batch(df, batch_id):
            assertDataFrameEqual(df, [Row(batch_id * 2), Row(batch_id * 2 + 1)])

        q = df.writeStream.foreachBatch(check_batch).trigger(availableNow=True).start()
        q.awaitTermination(timeout=30)
        self.assertIsNone(q.exception(), "No exception has to be propagated.")
        # 2 rows * 5 batches = 10 rows
        self.assertEqual(len(q.recentProgress), 5)
        for progress in q.recentProgress:
            self.assertEqual(progress.numInputRows, 2)
            self.assertEqual(q.lastProgress.sources[0].numInputRows, 2)
            self.assertEqual(
                json.loads(q.lastProgress.sources[0].latestOffset), {"partition-1": 1000000}
            )

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
        wait_for_condition(q, lambda query: len(query.recentProgress) >= 5)
        q.stop()
        q.awaitTermination()
        self.assertIsNone(q.exception(), "No exception has to be propagated.")

    def test_simple_stream_reader_trigger_available_now(self):
        class SimpleStreamReader(SimpleDataSourceStreamReader, SupportsTriggerAvailableNow):
            def initialOffset(self):
                return {"offset": 0}

            def read(self, start: dict):
                start_idx = start["offset"]
                end_offset = min(start_idx + 2, self.desired_end_offset["offset"])
                it = iter([(i,) for i in range(start_idx, end_offset)])
                return (it, {"offset": end_offset})

            def commit(self, end):
                pass

            def readBetweenOffsets(self, start: dict, end: dict):
                start_idx = start["offset"]
                end_idx = end["offset"]
                return iter([(i,) for i in range(start_idx, end_idx)])

            def prepareForTriggerAvailableNow(self) -> None:
                self.desired_end_offset = {"offset": 10}

        class SimpleDataSource(DataSource):
            def schema(self):
                return "id INT"

            def simpleStreamReader(self, schema):
                return SimpleStreamReader()

        self.spark.dataSource.register(SimpleDataSource)
        df = self.spark.readStream.format("SimpleDataSource").load()

        def check_batch(df, batch_id):
            # the last offset for the data is 9 since the desired end offset is 10
            # the batch isn't triggered with no data, so either we have one data or two data in each batch
            if batch_id * 2 + 1 > 9:
                assertDataFrameEqual(df, [Row(batch_id * 2)])
            else:
                assertDataFrameEqual(df, [Row(batch_id * 2), Row(batch_id * 2 + 1)])

        q = df.writeStream.foreachBatch(check_batch).trigger(availableNow=True).start()
        q.awaitTermination(timeout=30)
        self.assertIsNone(q.exception(), "No exception has to be propagated.")

    def test_simple_stream_reader_offset_did_not_advance_raises(self):
        """Validate that returning end == start with non-empty data raises SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE."""
        from pyspark.sql.datasource_internal import _SimpleStreamReaderWrapper

        class BuggySimpleStreamReader(SimpleDataSourceStreamReader):
            def initialOffset(self):
                return {"offset": 0}

            def read(self, start: dict):
                # Bug: return same offset as end despite returning data
                start_idx = start["offset"]
                it = iter([(i,) for i in range(start_idx, start_idx + 3)])
                return (it, start)

            def readBetweenOffsets(self, start: dict, end: dict):
                return iter([])

            def commit(self, end: dict):
                pass

        reader = BuggySimpleStreamReader()
        wrapper = _SimpleStreamReaderWrapper(reader)
        with self.assertRaises(PySparkException) as cm:
            wrapper.latestOffset({"offset": 0}, ReadAllAvailable())
        self.assertEqual(
            cm.exception.getCondition(),
            "SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE",
        )

    def test_simple_stream_reader_empty_iterator_start_equals_end_allowed(self):
        """When read() returns end == start with an empty iterator, no exception and no cache entry."""
        from pyspark.sql.datasource_internal import _SimpleStreamReaderWrapper

        class EmptyBatchReader(SimpleDataSourceStreamReader):
            def initialOffset(self):
                return {"offset": 0}

            def read(self, start: dict):
                # Valid: same offset as end but empty iterator (no data)
                return (iter([]), start)

            def readBetweenOffsets(self, start: dict, end: dict):
                return iter([])

            def commit(self, end: dict):
                pass

        reader = EmptyBatchReader()
        wrapper = _SimpleStreamReaderWrapper(reader)
        start = {"offset": 0}
        end = wrapper.latestOffset(start, ReadAllAvailable())
        self.assertEqual(end, start)
        self.assertEqual(len(wrapper.cache), 0)

    def test_simple_stream_reader_returns_list(self):
        """read() returning a list instead of an iterator is handled like an iterator."""
        from pyspark.sql.datasource_internal import _SimpleStreamReaderWrapper

        class ListReader(SimpleDataSourceStreamReader):
            def __init__(self, rows, advance):
                self.rows = rows
                self.advance = advance

            def initialOffset(self):
                return {"offset": 0}

            def read(self, start: dict):
                end = {"offset": start["offset"] + len(self.rows)} if self.advance else start
                return (list(self.rows), end)

            def readBetweenOffsets(self, start: dict, end: dict):
                return iter([])

            def commit(self, end: dict):
                pass

        start = {"offset": 0}

        # Non-empty list with an advanced end offset is cached and readable.
        wrapper = _SimpleStreamReaderWrapper(ListReader([(0,), (1,)], advance=True))
        end = wrapper.latestOffset(start, ReadAllAvailable())
        self.assertEqual(end, {"offset": 2})
        self.assertEqual(list(wrapper.getCache(start, end)), [(0,), (1,)])

        # Empty list with end == start is allowed and not cached.
        wrapper = _SimpleStreamReaderWrapper(ListReader([], advance=False))
        end = wrapper.latestOffset(start, ReadAllAvailable())
        self.assertEqual(end, start)
        self.assertEqual(len(wrapper.cache), 0)

        # Non-empty list with end == start raises the offset validation error.
        wrapper = _SimpleStreamReaderWrapper(ListReader([(0,)], advance=False))
        with self.assertRaises(PySparkException) as cm:
            wrapper.latestOffset(start, ReadAllAvailable())
        self.assertEqual(
            cm.exception.getCondition(),
            "SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE",
        )

    def test_simple_stream_reader_returns_non_iterator_iterables(self):
        """read() returning a non-iterator iterable (e.g. tuple, range) is handled like a list."""
        from collections import deque

        from pyspark.sql.datasource_internal import _SimpleStreamReaderWrapper

        class IterableOnly:
            """An iterable that implements __iter__ but not __next__."""

            def __init__(self, rows):
                self._rows = rows

            def __iter__(self):
                return iter(self._rows)

        class NextOnly:
            """Implements only __next__ without __iter__, which next() accepts."""

            def __init__(self, rows):
                self._rows = iter(rows)

            def __next__(self):
                return next(self._rows)

        class GetitemOnly:
            """An iterable that implements only __getitem__, without __iter__ or __next__."""

            def __init__(self, rows):
                self._rows = rows

            def __getitem__(self, index):
                return self._rows[index]

        class NonIteratorIterableReader(SimpleDataSourceStreamReader):
            def __init__(self, rows, to_result, advance):
                self.rows = rows
                self.to_result = to_result
                self.advance = advance

            def initialOffset(self):
                return {"offset": 0}

            def read(self, start: dict):
                end = {"offset": start["offset"] + len(self.rows)} if self.advance else start
                return (self.to_result(self.rows), end)

            def readBetweenOffsets(self, start: dict, end: dict):
                return iter([])

            def commit(self, end: dict):
                pass

        start = {"offset": 0}
        # Examples of non-iterator iterables that read() may return instead of an iterator.
        # GetitemOnly is included because it is the one kind that a guard narrowing the
        # conversion to isinstance(it, Iterable) would mishandle.
        non_iterator_results = [
            tuple,
            lambda rows: range(len(rows)),
            lambda rows: deque(rows),
            IterableOnly,
            GetitemOnly,
        ]

        for to_result in non_iterator_results:
            # Non-empty iterable with an advanced end offset is cached and readable.
            wrapper = _SimpleStreamReaderWrapper(
                NonIteratorIterableReader([0, 1], to_result, advance=True)
            )
            end = wrapper.latestOffset(start, ReadAllAvailable())
            self.assertEqual(end, {"offset": 2})
            self.assertEqual(list(wrapper.getCache(start, end)), [0, 1])

            # Empty iterable with end == start is allowed and not cached.
            wrapper = _SimpleStreamReaderWrapper(
                NonIteratorIterableReader([], to_result, advance=False)
            )
            end = wrapper.latestOffset(start, ReadAllAvailable())
            self.assertEqual(end, start)
            self.assertEqual(len(wrapper.cache), 0)

            # Non-empty iterable with end == start raises the offset validation error.
            wrapper = _SimpleStreamReaderWrapper(
                NonIteratorIterableReader([0], to_result, advance=False)
            )
            with self.assertRaises(PySparkException) as cm:
                wrapper.latestOffset(start, ReadAllAvailable())
            self.assertEqual(
                cm.exception.getCondition(),
                "SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE",
            )

        # Objects that implement only __next__ keep being accepted by next() directly.
        wrapper = _SimpleStreamReaderWrapper(NonIteratorIterableReader([], NextOnly, advance=False))
        end = wrapper.latestOffset(start, ReadAllAvailable())
        self.assertEqual(end, start)
        self.assertEqual(len(wrapper.cache), 0)

        wrapper = _SimpleStreamReaderWrapper(
            NonIteratorIterableReader([0], NextOnly, advance=False)
        )
        with self.assertRaises(PySparkException) as cm:
            wrapper.latestOffset(start, ReadAllAvailable())
        self.assertEqual(
            cm.exception.getCondition(),
            "SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE",
        )

    def test_simple_stream_reader_next_lookup_uses_type(self):
        """The __next__ check uses the type, so __getattr__ hooks and instance attributes
        on the result cannot misroute the empty/non-empty validation."""
        from pyspark.sql.datasource_internal import _SimpleStreamReaderWrapper

        class GetattrReturnsList(list):
            def __getattr__(self, name):
                return None

        class GetattrRaisesList(list):
            def __getattr__(self, name):
                raise KeyError(name)

        class IteratorProxy:
            def __init__(self, rows):
                self._rows = iter(rows)

            def __getattr__(self, name):
                return getattr(self._rows, name)

            def __iter__(self):
                return self._rows

        def with_instance_next(rows):
            # An instance __next__ attribute must not be consulted by the check.
            result = GetattrReturnsList(rows)
            result.__next__ = lambda: (_ for _ in ()).throw(AssertionError("unused"))
            return result

        class HookedResultReader(SimpleDataSourceStreamReader):
            def __init__(self, rows, to_result, advance):
                self.rows = rows
                self.to_result = to_result
                self.advance = advance

            def initialOffset(self):
                return {"offset": 0}

            def read(self, start: dict):
                end = {"offset": start["offset"] + len(self.rows)} if self.advance else start
                return (self.to_result(self.rows), end)

            def readBetweenOffsets(self, start: dict, end: dict):
                return iter([])

            def commit(self, end: dict):
                pass

        start = {"offset": 0}
        # Instance lookups answer __next__ (or raise for it), but the type does not
        # define it, so the result must be treated as the plain iterable it is.
        for to_result in [GetattrReturnsList, GetattrRaisesList, with_instance_next]:
            # Empty result with end == start is allowed and not cached.
            wrapper = _SimpleStreamReaderWrapper(HookedResultReader([], to_result, advance=False))
            end = wrapper.latestOffset(start, ReadAllAvailable())
            self.assertEqual(end, start)
            self.assertEqual(len(wrapper.cache), 0)

            # Non-empty result with end == start raises the offset validation error,
            # not a TypeError and not an exception escaping from a hook.
            wrapper = _SimpleStreamReaderWrapper(HookedResultReader([0], to_result, advance=False))
            with self.assertRaises(PySparkException) as cm:
                wrapper.latestOffset(start, ReadAllAvailable())
            self.assertEqual(
                cm.exception.getCondition(),
                "SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE",
            )

        # A proxy forwarding __getattr__ and __iter__ to an iterator is converted with
        # iter() and validated like any other iterable. It is only checked on paths
        # where the guard runs: the cache readback copies entries with copy.copy,
        # which a proxy rebuilt without __init__ cannot survive.
        wrapper = _SimpleStreamReaderWrapper(HookedResultReader([0], IteratorProxy, advance=False))
        with self.assertRaises(PySparkException) as cm:
            wrapper.latestOffset(start, ReadAllAvailable())
        self.assertEqual(
            cm.exception.getCondition(),
            "SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE",
        )

        wrapper = _SimpleStreamReaderWrapper(HookedResultReader([], IteratorProxy, advance=False))
        end = wrapper.latestOffset(start, ReadAllAvailable())
        self.assertEqual(end, start)
        self.assertEqual(len(wrapper.cache), 0)

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
            wait_for_condition(q, lambda query: len(query.recentProgress) > 0)

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
            wait_for_condition(q, lambda query: query.exception() is not None)
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
        import json
        import os
        import shutil
        import tempfile
        from dataclasses import dataclass

        import pyarrow as pa

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

            @eventually(
                timeout=20,
                interval=2.0,
                catch_assertions=True,
                expected_exceptions=(json.JSONDecodeError,),
            )
            def check():
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

                self.assertTrue(
                    total_committed_rows > 0,
                    f"Expected committed data but got {total_committed_rows} rows",
                )

            check()

            query.stop()
            query.awaitTermination()

            json_files = [
                f
                for f in os.listdir(temp_dir)
                if f.startswith("partition_") and f.endswith(".json")
            ]

            self.assertTrue(
                len(json_files) > 0, f"Expected JSON files but found {len(json_files)} files"
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
    from pyspark.testing import main

    main()
