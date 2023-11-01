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

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.testing.sqlutils import ReusedSQLTestCase


class BasePythonDataSourceTestsMixin:
    def test_basic_data_source_class(self):
        class MyDataSource(DataSource):
            ...

        options = dict(a=1, b=2)
        ds = MyDataSource(paths=[], userSpecifiedSchema=None, options=options)
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
        self.assertEqual(list(reader.partitions()), [None])
        self.assertEqual(list(reader.read(None)), [(None,)])

    def test_register_data_source(self):
        class MyDataSource(DataSource):
            ...

        self.spark.dataSource.register(MyDataSource)

        self.assertTrue(
            self.spark._jsparkSession.sharedState()
            .dataSourceRegistry()
            .dataSourceExists("MyDataSource")
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
