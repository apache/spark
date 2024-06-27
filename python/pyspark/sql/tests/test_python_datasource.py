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
import unittest
from typing import Callable, Union

from pyspark.errors import PythonException, AnalysisException
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    InputPartition,
    DataSourceWriter,
    WriterCommitMessage,
    CaseInsensitiveDict,
)
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import Row, StructType
from pyspark.testing.sqlutils import (
    have_pyarrow,
    pyarrow_requirement_message,
)
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import SPARK_HOME


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class BasePythonDataSourceTestsMixin:
    def test_basic_data_source_class(self):
        class MyDataSource(DataSource):
            ...

        options = dict(a=1, b=2)
        ds = MyDataSource(options=options)
        self.assertEqual(ds.options, options)
        self.assertEqual(ds.name(), "MyDataSource")
        with self.assertRaises(NotImplementedError):
            ds.schema()
        with self.assertRaises(NotImplementedError):
            ds.reader(None)
        with self.assertRaises(NotImplementedError):
            ds.writer(None, None)

    def test_basic_data_source_reader_class(self):
        class MyDataSourceReader(DataSourceReader):
            def read(self, partition):
                yield None,

        reader = MyDataSourceReader()
        self.assertEqual(list(reader.read(None)), [(None,)])

    def test_data_source_register(self):
        class TestReader(DataSourceReader):
            def read(self, partition):
                yield (0, 1)

        class TestDataSource(DataSource):
            def schema(self):
                return "a INT, b INT"

            def reader(self, schema):
                return TestReader()

        self.spark.dataSource.register(TestDataSource)
        df = self.spark.read.format("TestDataSource").load()
        assertDataFrameEqual(df, [Row(a=0, b=1)])

        class MyDataSource(TestDataSource):
            @classmethod
            def name(cls):
                return "TestDataSource"

            def schema(self):
                return "c INT, d INT"

        # Should be able to register the data source with the same name.
        self.spark.dataSource.register(MyDataSource)

        df = self.spark.read.format("TestDataSource").load()
        assertDataFrameEqual(df, [Row(c=0, d=1)])

    def register_data_source(
        self,
        read_func: Callable,
        partition_func: Callable = None,
        output: Union[str, StructType] = "i int, j int",
        name: str = "test",
    ):
        class TestDataSourceReader(DataSourceReader):
            def __init__(self, schema):
                self.schema = schema

            def partitions(self):
                if partition_func is not None:
                    return partition_func()
                else:
                    raise NotImplementedError

            def read(self, partition):
                return read_func(self.schema, partition)

        class TestDataSource(DataSource):
            @classmethod
            def name(cls):
                return name

            def schema(self):
                return output

            def reader(self, schema) -> "DataSourceReader":
                return TestDataSourceReader(schema)

        self.spark.dataSource.register(TestDataSource)

    def test_data_source_read_output_tuple(self):
        self.register_data_source(read_func=lambda schema, partition: iter([(0, 1)]))
        df = self.spark.read.format("test").load()
        assertDataFrameEqual(df, [Row(0, 1)])

    def test_data_source_read_output_list(self):
        self.register_data_source(read_func=lambda schema, partition: iter([[0, 1]]))
        df = self.spark.read.format("test").load()
        assertDataFrameEqual(df, [Row(0, 1)])

    def test_data_source_read_output_row(self):
        self.register_data_source(read_func=lambda schema, partition: iter([Row(0, 1)]))
        df = self.spark.read.format("test").load()
        assertDataFrameEqual(df, [Row(0, 1)])

    def test_data_source_read_output_named_row(self):
        self.register_data_source(
            read_func=lambda schema, partition: iter([Row(j=1, i=0), Row(i=1, j=2)])
        )
        df = self.spark.read.format("test").load()
        assertDataFrameEqual(df, [Row(0, 1), Row(1, 2)])

    def test_data_source_read_output_named_row_with_wrong_schema(self):
        self.register_data_source(
            read_func=lambda schema, partition: iter([Row(i=1, j=2), Row(j=3, k=4)])
        )
        with self.assertRaisesRegex(
            PythonException,
            r"\[DATA_SOURCE_RETURN_SCHEMA_MISMATCH\] Return schema mismatch in the result",
        ):
            self.spark.read.format("test").load().show()

    def test_data_source_read_output_none(self):
        self.register_data_source(read_func=lambda schema, partition: None)
        df = self.spark.read.format("test").load()
        with self.assertRaisesRegex(PythonException, "DATA_SOURCE_INVALID_RETURN_TYPE"):
            assertDataFrameEqual(df, [])

    def test_data_source_read_output_empty_iter(self):
        self.register_data_source(read_func=lambda schema, partition: iter([]))
        df = self.spark.read.format("test").load()
        assertDataFrameEqual(df, [])

    def test_data_source_read_cast_output_schema(self):
        self.register_data_source(
            read_func=lambda schema, partition: iter([(0, 1)]), output="i long, j string"
        )
        df = self.spark.read.format("test").load()
        assertDataFrameEqual(df, [Row(i=0, j="1")])

    def test_data_source_read_output_with_partition(self):
        def partition_func():
            return [InputPartition(0), InputPartition(1)]

        def read_func(schema, partition):
            if partition.value == 0:
                return iter([])
            elif partition.value == 1:
                yield (0, 1)

        self.register_data_source(read_func=read_func, partition_func=partition_func)
        df = self.spark.read.format("test").load()
        assertDataFrameEqual(df, [Row(0, 1)])

    def test_data_source_read_output_with_schema_mismatch(self):
        self.register_data_source(read_func=lambda schema, partition: iter([(0, 1)]))
        df = self.spark.read.format("test").schema("i int").load()
        with self.assertRaisesRegex(PythonException, "DATA_SOURCE_RETURN_SCHEMA_MISMATCH"):
            df.collect()
        self.register_data_source(
            read_func=lambda schema, partition: iter([(0, 1)]), output="i int, j int, k int"
        )
        with self.assertRaisesRegex(PythonException, "DATA_SOURCE_RETURN_SCHEMA_MISMATCH"):
            df.collect()

    def test_read_with_invalid_return_row_type(self):
        self.register_data_source(read_func=lambda schema, partition: iter([1]))
        df = self.spark.read.format("test").load()
        with self.assertRaisesRegex(PythonException, "DATA_SOURCE_INVALID_RETURN_TYPE"):
            df.collect()

    def test_in_memory_data_source(self):
        class InMemDataSourceReader(DataSourceReader):
            DEFAULT_NUM_PARTITIONS: int = 3

            def __init__(self, options):
                self.options = options

            def partitions(self):
                if "num_partitions" in self.options:
                    num_partitions = int(self.options["num_partitions"])
                else:
                    num_partitions = self.DEFAULT_NUM_PARTITIONS
                return [InputPartition(i) for i in range(num_partitions)]

            def read(self, partition):
                yield partition.value, str(partition.value)

        class InMemoryDataSource(DataSource):
            @classmethod
            def name(cls):
                return "memory"

            def schema(self):
                return "x INT, y STRING"

            def reader(self, schema) -> "DataSourceReader":
                return InMemDataSourceReader(self.options)

        self.spark.dataSource.register(InMemoryDataSource)
        df = self.spark.read.format("memory").load()
        self.assertEqual(df.select(spark_partition_id()).distinct().count(), 3)
        assertDataFrameEqual(df, [Row(x=0, y="0"), Row(x=1, y="1"), Row(x=2, y="2")])

        df = self.spark.read.format("memory").option("num_partitions", 2).load()
        assertDataFrameEqual(df, [Row(x=0, y="0"), Row(x=1, y="1")])
        self.assertEqual(df.select(spark_partition_id()).distinct().count(), 2)

    def _get_test_json_data_source(self):
        import json
        import os
        from dataclasses import dataclass

        class TestJsonReader(DataSourceReader):
            def __init__(self, options):
                self.options = options

            def read(self, partition):
                path = self.options.get("path")
                if path is None:
                    raise Exception("path is not specified")
                with open(path, "r") as file:
                    for line in file.readlines():
                        if line.strip():
                            data = json.loads(line)
                            yield data.get("name"), data.get("age")

        @dataclass
        class TestCommitMessage(WriterCommitMessage):
            count: int

        class TestJsonWriter(DataSourceWriter):
            def __init__(self, options):
                self.options = options
                self.path = self.options.get("path")

            def write(self, iterator):
                from pyspark import TaskContext

                context = TaskContext.get()
                output_path = os.path.join(self.path, f"{context.partitionId}.json")
                count = 0
                with open(output_path, "w") as file:
                    for row in iterator:
                        count += 1
                        if "id" in row and row.id > 5:
                            raise Exception("id > 5")
                        file.write(json.dumps(row.asDict()) + "\n")
                return TestCommitMessage(count=count)

            def commit(self, messages):
                total_count = sum(message.count for message in messages)
                with open(os.path.join(self.path, "_success.txt"), "a") as file:
                    file.write(f"count: {total_count}\n")

            def abort(self, messages):
                with open(os.path.join(self.path, "_failed.txt"), "a") as file:
                    file.write("failed")

        class TestJsonDataSource(DataSource):
            @classmethod
            def name(cls):
                return "my-json"

            def schema(self):
                return "name STRING, age INT"

            def reader(self, schema) -> "DataSourceReader":
                return TestJsonReader(self.options)

            def writer(self, schema, overwrite):
                return TestJsonWriter(self.options)

        return TestJsonDataSource

    def test_custom_json_data_source_read(self):
        data_source = self._get_test_json_data_source()
        self.spark.dataSource.register(data_source)
        path1 = os.path.join(SPARK_HOME, "python/test_support/sql/people.json")
        path2 = os.path.join(SPARK_HOME, "python/test_support/sql/people1.json")
        assertDataFrameEqual(
            self.spark.read.format("my-json").load(path1),
            [Row(name="Michael", age=None), Row(name="Andy", age=30), Row(name="Justin", age=19)],
        )
        assertDataFrameEqual(
            self.spark.read.format("my-json").load(path2),
            [Row(name="Jonathan", age=None)],
        )

    def test_custom_json_data_source_write(self):
        data_source = self._get_test_json_data_source()
        self.spark.dataSource.register(data_source)
        input_path = os.path.join(SPARK_HOME, "python/test_support/sql/people.json")
        df = self.spark.read.json(input_path)
        with tempfile.TemporaryDirectory(prefix="test_custom_json_data_source_write") as d:
            df.write.format("my-json").mode("append").save(d)
            assertDataFrameEqual(self.spark.read.json(d), self.spark.read.json(input_path))

    def test_custom_json_data_source_commit(self):
        data_source = self._get_test_json_data_source()
        self.spark.dataSource.register(data_source)
        with tempfile.TemporaryDirectory(prefix="test_custom_json_data_source_commit") as d:
            self.spark.range(0, 5, 1, 3).write.format("my-json").mode("append").save(d)
            with open(os.path.join(d, "_success.txt"), "r") as file:
                text = file.read()
            assert text == "count: 5\n"

    def test_custom_json_data_source_abort(self):
        data_source = self._get_test_json_data_source()
        self.spark.dataSource.register(data_source)
        with tempfile.TemporaryDirectory(prefix="test_custom_json_data_source_abort") as d:
            with self.assertRaises(PythonException):
                self.spark.range(0, 8, 1, 3).write.format("my-json").mode("append").save(d)
            with open(os.path.join(d, "_failed.txt"), "r") as file:
                text = file.read()
            assert text == "failed"

    def test_case_insensitive_dict(self):
        d = CaseInsensitiveDict({"foo": 1, "Bar": 2})
        self.assertEqual(d["foo"], d["FOO"])
        self.assertEqual(d["bar"], d["BAR"])
        self.assertTrue("baR" in d)
        d["BAR"] = 3
        self.assertEqual(d["BAR"], 3)
        # Test update
        d.update({"BaZ": 3})
        self.assertEqual(d["BAZ"], 3)
        d.update({"FOO": 4})
        self.assertEqual(d["foo"], 4)
        # Test delete
        del d["FoO"]
        self.assertFalse("FOO" in d)
        # Test copy
        d2 = d.copy()
        self.assertEqual(d2["BaR"], 3)
        self.assertEqual(d2["baz"], 3)

    def test_arrow_batch_data_source(self):
        import pyarrow as pa

        class ArrowBatchDataSource(DataSource):
            """
            A data source for testing Arrow Batch Serialization
            """

            @classmethod
            def name(cls):
                return "arrowbatch"

            def schema(self):
                return "key int, value string"

            def reader(self, schema: str):
                return ArrowBatchDataSourceReader(schema, self.options)

        class ArrowBatchDataSourceReader(DataSourceReader):
            def __init__(self, schema, options):
                self.schema: str = schema
                self.options = options

            def read(self, partition):
                # Create Arrow Record Batch
                keys = pa.array([1, 2, 3, 4, 5], type=pa.int32())
                values = pa.array(["one", "two", "three", "four", "five"], type=pa.string())
                schema = pa.schema([("key", pa.int32()), ("value", pa.string())])

            def __init__(self, schema, options):
                self.schema: str = schema
                self.options = options

            def read(self, partition):
                # Create Arrow Record Batch
                keys = pa.array([1, 2, 3, 4, 5], type=pa.int32())
                values = pa.array(["one", "two", "three", "four", "five"], type=pa.string())
                schema = pa.schema([("key", pa.int32()), ("value", pa.string())])
                record_batch = pa.RecordBatch.from_arrays([keys, values], schema=schema)
                yield record_batch

            def partitions(self):
                # hardcoded number of partitions
                num_part = 1
                return [InputPartition(i) for i in range(num_part)]

        self.spark.dataSource.register(ArrowBatchDataSource)
        df = self.spark.read.format("arrowbatch").load()
        expected_data = [
            Row(key=1, value="one"),
            Row(key=2, value="two"),
            Row(key=3, value="three"),
            Row(key=4, value="four"),
            Row(key=5, value="five"),
        ]
        assertDataFrameEqual(df, expected_data)

        with self.assertRaisesRegex(
            PythonException,
            "PySparkRuntimeError: \\[DATA_SOURCE_RETURN_SCHEMA_MISMATCH\\] Return schema mismatch in the "
            "result from 'read' method\. Expected: 1 columns, Found: 2 columns",
        ):
            self.spark.read.format("arrowbatch").schema("dummy int").load().show()

        with self.assertRaisesRegex(
            PythonException,
            "PySparkRuntimeError: \\[DATA_SOURCE_RETURN_SCHEMA_MISMATCH\\] Return schema mismatch in the "
            "result from 'read' method\. Expected: \['key', 'dummy'\] columns, Found: \['key', 'value'\] columns",
        ):
            self.spark.read.format("arrowbatch").schema("key int, dummy string").load().show()

    def test_data_source_type_mismatch(self):
        class TestDataSource(DataSource):
            @classmethod
            def name(cls):
                return "test"

            def schema(self):
                return "id int"

            def reader(self, schema):
                return TestReader()

            def writer(self, schema, overwrite):
                return TestWriter()

        class TestReader:
            def partitions(self):
                return []

            def read(self, partition):
                yield (0,)

        class TestWriter:
            def write(self, iterator):
                return WriterCommitMessage()

        self.spark.dataSource.register(TestDataSource)

        with self.assertRaisesRegex(
            AnalysisException,
            r"\[DATA_SOURCE_TYPE_MISMATCH\] Expected an instance of DataSourceReader",
        ):
            self.spark.read.format("test").load().show()

        df = self.spark.range(10)
        with self.assertRaisesRegex(
            AnalysisException,
            r"\[DATA_SOURCE_TYPE_MISMATCH\] Expected an instance of DataSourceWriter",
        ):
            df.write.format("test").mode("append").saveAsTable("test_table")


class PythonDataSourceTests(BasePythonDataSourceTestsMixin, ReusedSQLTestCase):
    ...


if __name__ == "__main__":
    from pyspark.sql.tests.test_python_datasource import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
