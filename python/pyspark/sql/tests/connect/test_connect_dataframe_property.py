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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.utils import is_remote

from pyspark.sql.tests.connect.test_connect_basic import SparkConnectSQLTestCase
from pyspark.testing.sqlutils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

if have_pyarrow:
    import pyarrow as pa

if have_pandas:
    import pandas as pd


class SparkConnectDataFramePropertyTests(SparkConnectSQLTestCase):
    def test_cached_schema_to(self):
        cdf = self.connect.read.table(self.tbl_name)
        sdf = self.spark.read.table(self.tbl_name)

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        cdf1 = cdf.to(schema)
        self.assertEqual(cdf1._cached_schema, schema)

        sdf1 = sdf.to(schema)

        self.assertEqual(cdf1.schema, sdf1.schema)
        self.assertEqual(cdf1.collect(), sdf1.collect())

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_cached_schema_map_in_pandas(self):
        data = [(1, "foo"), (2, None), (3, "bar"), (4, "bar")]
        cdf = self.connect.createDataFrame(data, "a int, b string")
        sdf = self.spark.createDataFrame(data, "a int, b string")

        def func(iterator):
            for pdf in iterator:
                assert isinstance(pdf, pd.DataFrame)
                assert [d.name for d in list(pdf.dtypes)] == ["int32", "object"]
                yield pdf

        schema = StructType(
            [
                StructField("a", IntegerType(), True),
                StructField("b", StringType(), True),
            ]
        )

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": "1"}):
            self.assertTrue(is_remote())
            cdf1 = cdf.mapInPandas(func, schema)
            self.assertEqual(cdf1._cached_schema, schema)

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": None}):
            # 'mapInPandas' depends on the method 'pandas_udf', which is dispatched
            # based on 'is_remote'. However, in SparkConnectSQLTestCase, the remote
            # mode is always on, so 'sdf.mapInPandas' fails with incorrect dispatch.
            # Using this temp env to properly invoke mapInPandas in PySpark Classic.
            self.assertFalse(is_remote())
            sdf1 = sdf.mapInPandas(func, schema)

        self.assertEqual(cdf1.schema, sdf1.schema)
        self.assertEqual(cdf1.collect(), sdf1.collect())

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_cached_schema_map_in_arrow(self):
        data = [(1, "foo"), (2, None), (3, "bar"), (4, "bar")]
        cdf = self.connect.createDataFrame(data, "a int, b string")
        sdf = self.spark.createDataFrame(data, "a int, b string")

        def func(iterator):
            for batch in iterator:
                assert isinstance(batch, pa.RecordBatch)
                assert batch.schema.types == [pa.int32(), pa.string()]
                yield batch

        schema = StructType(
            [
                StructField("a", IntegerType(), True),
                StructField("b", StringType(), True),
            ]
        )

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": "1"}):
            self.assertTrue(is_remote())
            cdf1 = cdf.mapInArrow(func, schema)
            self.assertEqual(cdf1._cached_schema, schema)

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": None}):
            self.assertFalse(is_remote())
            sdf1 = sdf.mapInArrow(func, schema)

        self.assertEqual(cdf1.schema, sdf1.schema)
        self.assertEqual(cdf1.collect(), sdf1.collect())


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_dataframe_property import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
