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
import sys
import time
import unittest

if sys.version >= '3':
    unicode = str

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pandas, have_pyarrow, \
    pandas_requirement_message, pyarrow_requirement_message

if have_pandas:
    import pandas as pd


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
class MapInPandasTests(ReusedSQLTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()

        # Synchronize default timezone between Python and Java
        cls.tz_prev = os.environ.get("TZ", None)  # save current tz if set
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()

        cls.sc.environment["TZ"] = tz
        cls.spark.conf.set("spark.sql.session.timeZone", tz)

    @classmethod
    def tearDownClass(cls):
        del os.environ["TZ"]
        if cls.tz_prev is not None:
            os.environ["TZ"] = cls.tz_prev
        time.tzset()
        ReusedSQLTestCase.tearDownClass()

    def test_map_partitions_in_pandas(self):
        def func(iterator):
            for pdf in iterator:
                assert isinstance(pdf, pd.DataFrame)
                assert pdf.columns == ['id']
                yield pdf

        df = self.spark.range(10)
        actual = df.mapInPandas(func, 'id long').collect()
        expected = df.collect()
        self.assertEquals(actual, expected)

    def test_multiple_columns(self):
        data = [(1, "foo"), (2, None), (3, "bar"), (4, "bar")]
        df = self.spark.createDataFrame(data, "a int, b string")

        def func(iterator):
            for pdf in iterator:
                assert isinstance(pdf, pd.DataFrame)
                assert [d.name for d in list(pdf.dtypes)] == ['int32', 'object']
                yield pdf

        actual = df.mapInPandas(func, df.schema).collect()
        expected = df.collect()
        self.assertEquals(actual, expected)

    def test_different_output_length(self):
        def func(iterator):
            for _ in iterator:
                yield pd.DataFrame({'a': list(range(100))})

        df = self.spark.range(10)
        actual = df.repartition(1).mapInPandas(func, 'a long').collect()
        self.assertEquals(set((r.a for r in actual)), set(range(100)))

    def test_empty_iterator(self):
        def empty_iter(_):
            return iter([])

        self.assertEqual(
            self.spark.range(10).mapInPandas(empty_iter, 'a int, b string').count(), 0)

    def test_empty_rows(self):
        def empty_rows(_):
            return iter([pd.DataFrame({'a': []})])

        self.assertEqual(
            self.spark.range(10).mapInPandas(empty_rows, 'a int').count(), 0)

    def test_chain_map_partitions_in_pandas(self):
        def func(iterator):
            for pdf in iterator:
                assert isinstance(pdf, pd.DataFrame)
                assert pdf.columns == ['id']
                yield pdf

        df = self.spark.range(10)
        actual = df.mapInPandas(func, 'id long').mapInPandas(func, 'id long').collect()
        expected = df.collect()
        self.assertEquals(actual, expected)


if __name__ == "__main__":
    from pyspark.sql.tests.test_pandas_map import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
