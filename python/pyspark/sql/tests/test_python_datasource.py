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

from pyspark.sql.types import (
    IntegerType,
    Row,
    StructField,
    StructType,
)
from pyspark.testing import assertDataFrameEqual
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.testing.sqlutils import ReusedSQLTestCase


class TestDataSourceReader(DataSourceReader):
    def read(self, partition):
        yield (partition, 0)
        yield (partition, 1)


class DataSourcePartitionReader(TestDataSourceReader):
    def partitions(self):
        return [1, 2]


class TestDataSource(DataSource):
    def schema(self):
        return StructType([StructField("id", IntegerType()), StructField("value", IntegerType())])

    def reader(self, schema):
        return TestDataSourceReader()


class TestPartitionedDataSource(TestDataSource):
    def reader(self, schema):
        return DataSourcePartitionReader()


class BasePythonDataSourceTestsMixin:
    def test_data_source_read(self):
        df = self.spark.read.format(TestDataSource).load()
        assertDataFrameEqual(df, [Row(id=None, value=0), Row(id=None, value=1)])

    # TODO(SPARK-45559): support read with schema
    # def test_data_source_read_with_schema(self):
    #     df = self.spark.read.format(TestDataSource).schema("value INT").load()
    #     assertDataFrameEqual(df, [Row(value=0), Row(value=1)])

    def test_data_source_read_with_partitions(self):
        df = self.spark.read.format(TestPartitionedDataSource).load()
        assertDataFrameEqual(
            df, [Row(id=1, value=0), Row(id=1, value=1), Row(id=2, value=0), Row(id=2, value=1)]
        )
        self.assertEqual(df.rdd.getNumPartitions(), 2)


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
