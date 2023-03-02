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
import shutil
import tempfile
import time
import unittest
from typing import cast

from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.errors import PythonException
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing.utils import QuietTest

if have_pandas:
    import pandas as pd


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class MapInPandasTestsMixin:
    def test_map_in_pandas(self):
        def func(iterator):
            for pdf in iterator:
                assert isinstance(pdf, pd.DataFrame)
                assert pdf.columns == ["id"]
                yield pdf

        df = self.spark.range(10, numPartitions=3)
        actual = df.mapInPandas(func, "id long").collect()
        expected = df.collect()
        self.assertEqual(actual, expected)

    def test_multiple_columns(self):
        data = [(1, "foo"), (2, None), (3, "bar"), (4, "bar")]
        df = self.spark.createDataFrame(data, "a int, b string")

        def func(iterator):
            for pdf in iterator:
                assert isinstance(pdf, pd.DataFrame)
                assert [d.name for d in list(pdf.dtypes)] == ["int32", "object"]
                yield pdf

        actual = df.mapInPandas(func, df.schema).collect()
        expected = df.collect()
        self.assertEqual(actual, expected)

    def test_different_output_length(self):
        def func(iterator):
            for _ in iterator:
                yield pd.DataFrame({"a": list(range(100))})

        df = self.spark.range(10)
        actual = df.repartition(1).mapInPandas(func, "a long").collect()
        self.assertEqual(set((r.a for r in actual)), set(range(100)))

    def test_other_than_dataframe(self):
        with QuietTest(self.sc):
            self.check_other_than_dataframe()

    def check_other_than_dataframe(self):
        def bad_iter(_):
            return iter([1])

        with self.assertRaisesRegex(
            PythonException,
            "Return type of the user-defined function should be Pandas.DataFrame, "
            "but is <class 'int'>",
        ):
            self.spark.range(10, numPartitions=3).mapInPandas(bad_iter, "a int, b string").count()

    def test_empty_iterator(self):
        def empty_iter(_):
            return iter([])

        mapped = self.spark.range(10, numPartitions=3).mapInPandas(empty_iter, "a int, b string")
        self.assertEqual(mapped.count(), 0)

    def test_empty_dataframes(self):
        def empty_dataframes(_):
            return iter([pd.DataFrame({"a": []})])

        mapped = self.spark.range(10, numPartitions=3).mapInPandas(empty_dataframes, "a int")
        self.assertEqual(mapped.count(), 0)

    def test_empty_dataframes_without_columns(self):
        def empty_dataframes_wo_columns(iterator):
            for pdf in iterator:
                yield pdf
            # after yielding all elements of the iterator, also yield one dataframe without columns
            yield pd.DataFrame([])

        mapped = (
            self.spark.range(10, numPartitions=3)
            .toDF("id")
            .mapInPandas(empty_dataframes_wo_columns, "id int")
        )
        self.assertEqual(mapped.count(), 10)

    def test_empty_dataframes_with_less_columns(self):
        with QuietTest(self.sc):
            self.check_empty_dataframes_with_less_columns()

    def check_empty_dataframes_with_less_columns(self):
        def empty_dataframes_with_less_columns(iterator):
            for pdf in iterator:
                yield pdf
            # after yielding all elements of the iterator, also yield a dataframe with less columns
            yield pd.DataFrame([(1,)], columns=["id"])

        with self.assertRaisesRegex(PythonException, "KeyError: 'value'"):
            self.spark.range(10, numPartitions=3).withColumn("value", lit(0)).toDF(
                "id", "value"
            ).mapInPandas(empty_dataframes_with_less_columns, "id int, value int").collect()

    def test_chain_map_partitions_in_pandas(self):
        def func(iterator):
            for pdf in iterator:
                assert isinstance(pdf, pd.DataFrame)
                assert pdf.columns == ["id"]
                yield pdf

        df = self.spark.range(10, numPartitions=3)
        actual = df.mapInPandas(func, "id long").mapInPandas(func, "id long").collect()
        expected = df.collect()
        self.assertEqual(actual, expected)

    def test_self_join(self):
        # SPARK-34319: self-join with MapInPandas
        df1 = self.spark.range(10, numPartitions=3)
        df2 = df1.mapInPandas(lambda iter: iter, "id long")
        actual = df2.join(df2).collect()
        expected = df1.join(df1).collect()
        self.assertEqual(sorted(actual), sorted(expected))

    # SPARK-33277
    def test_map_in_pandas_with_column_vector(self):
        path = tempfile.mkdtemp()
        shutil.rmtree(path)

        try:
            self.spark.range(0, 200000, 1, 1).write.parquet(path)

            def func(iterator):
                for pdf in iterator:
                    yield pd.DataFrame({"id": [0] * len(pdf)})

            for offheap in ["true", "false"]:
                with self.sql_conf({"spark.sql.columnVector.offheap.enabled": offheap}):
                    self.assertEquals(
                        self.spark.read.parquet(path).mapInPandas(func, "id long").head(), Row(0)
                    )
        finally:
            shutil.rmtree(path)


class MapInPandasTests(ReusedSQLTestCase, MapInPandasTestsMixin):
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


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_map import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
