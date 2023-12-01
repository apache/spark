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
import unittest

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import Row
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import SPARK_HOME


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
        self.assertEqual(df.rdd.getNumPartitions(), 3)
        assertDataFrameEqual(df, [Row(x=0, y="0"), Row(x=1, y="1"), Row(x=2, y="2")])

        df = self.spark.read.format("memory").option("num_partitions", 2).load()
        assertDataFrameEqual(df, [Row(x=0, y="0"), Row(x=1, y="1")])
        self.assertEqual(df.rdd.getNumPartitions(), 2)

    def test_custom_json_data_source(self):
        import json

        class JsonDataSourceReader(DataSourceReader):
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

        class JsonDataSource(DataSource):
            @classmethod
            def name(cls):
                return "my-json"

            def schema(self):
                return "name STRING, age INT"

            def reader(self, schema) -> "DataSourceReader":
                return JsonDataSourceReader(self.options)

        self.spark.dataSource.register(JsonDataSource)
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
