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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.utils import is_remote
from pyspark.sql import functions as SF
from pyspark.sql.tests.connect.test_connect_basic import SparkConnectSQLTestCase
from pyspark.testing.connectutils import should_test_connect
from pyspark.testing.sqlutils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

if have_pyarrow:
    import pyarrow as pa
    import pyarrow.compute as pc

if have_pandas:
    import pandas as pd

if should_test_connect:
    from pyspark.sql.connect import functions as CF


class SparkConnectDataFramePropertyTests(SparkConnectSQLTestCase):
    def test_cached_property_is_copied(self):
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )
        # Create some dummy data
        data = [
            (1, "Alice", 30, "New York"),
            (2, "Bob", 25, "San Francisco"),
            (3, "Cathy", 29, "Los Angeles"),
            (4, "David", 35, "Chicago"),
        ]
        df = self.spark.createDataFrame(data, schema)
        df_columns = df.columns
        assert len(df.columns) == 4
        for col in ["id", "name"]:
            df_columns.remove(col)
        assert len(df.columns) == 4

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

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": "1"}):
            self.assertTrue(is_remote())
            cdf1 = cdf.mapInPandas(func, "a int, b string")
            # Properly cache the parsed schema
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

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_cached_schema_group_apply_in_pandas(self):
        data = [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)]
        cdf = self.connect.createDataFrame(data, ("id", "v"))
        sdf = self.spark.createDataFrame(data, ("id", "v"))

        def normalize(pdf):
            v = pdf.v
            return pdf.assign(v=(v - v.mean()) / v.std())

        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("v", DoubleType(), True),
            ]
        )

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": "1"}):
            self.assertTrue(is_remote())
            cdf1 = cdf.groupby("id").applyInPandas(normalize, schema)
            self.assertEqual(cdf1._cached_schema, schema)

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": "1"}):
            self.assertTrue(is_remote())
            cdf1 = cdf.groupby("id").applyInPandas(normalize, "id long, v double")
            # Properly cache the parsed schema
            self.assertEqual(cdf1._cached_schema, schema)

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": None}):
            self.assertFalse(is_remote())
            sdf1 = sdf.groupby("id").applyInPandas(normalize, schema)

        self.assertEqual(cdf1.schema, sdf1.schema)
        self.assertEqual(cdf1.collect(), sdf1.collect())

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_cached_schema_group_apply_in_arrow(self):
        data = [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)]
        cdf = self.connect.createDataFrame(data, ("id", "v"))
        sdf = self.spark.createDataFrame(data, ("id", "v"))

        def normalize(table):
            v = table.column("v")
            norm = pc.divide(pc.subtract(v, pc.mean(v)), pc.stddev(v, ddof=1))
            return table.set_column(1, "v", norm)

        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("v", DoubleType(), True),
            ]
        )

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": "1"}):
            self.assertTrue(is_remote())
            cdf1 = cdf.groupby("id").applyInArrow(normalize, schema)
            self.assertEqual(cdf1._cached_schema, schema)

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": None}):
            self.assertFalse(is_remote())
            sdf1 = sdf.groupby("id").applyInArrow(normalize, schema)

        self.assertEqual(cdf1.schema, sdf1.schema)
        self.assertEqual(cdf1.collect(), sdf1.collect())

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_cached_schema_cogroup_apply_in_pandas(self):
        data1 = [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)]
        data2 = [(20000101, 1, "x"), (20000101, 2, "y")]

        cdf1 = self.connect.createDataFrame(data1, ("time", "id", "v1"))
        sdf1 = self.spark.createDataFrame(data1, ("time", "id", "v1"))
        cdf2 = self.connect.createDataFrame(data2, ("time", "id", "v2"))
        sdf2 = self.spark.createDataFrame(data2, ("time", "id", "v2"))

        def asof_join(left, right):
            return pd.merge_asof(left, right, on="time", by="id")

        schema = StructType(
            [
                StructField("time", IntegerType(), True),
                StructField("id", IntegerType(), True),
                StructField("v1", DoubleType(), True),
                StructField("v2", StringType(), True),
            ]
        )

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": "1"}):
            self.assertTrue(is_remote())
            cdf3 = cdf1.groupby("id").cogroup(cdf2.groupby("id")).applyInPandas(asof_join, schema)
            self.assertEqual(cdf3._cached_schema, schema)

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": None}):
            self.assertFalse(is_remote())
            sdf3 = sdf1.groupby("id").cogroup(sdf2.groupby("id")).applyInPandas(asof_join, schema)

        self.assertEqual(cdf3.schema, sdf3.schema)
        self.assertEqual(cdf3.collect(), sdf3.collect())

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_cached_schema_cogroup_apply_in_arrow(self):
        data1 = [(1, 1.0), (2, 2.0), (1, 3.0), (2, 4.0)]
        data2 = [(1, "x"), (2, "y")]

        cdf1 = self.connect.createDataFrame(data1, ("id", "v1"))
        sdf1 = self.spark.createDataFrame(data1, ("id", "v1"))
        cdf2 = self.connect.createDataFrame(data2, ("id", "v2"))
        sdf2 = self.spark.createDataFrame(data2, ("id", "v2"))

        def summarize(left, right):
            return pa.Table.from_pydict(
                {
                    "left": [left.num_rows],
                    "right": [right.num_rows],
                }
            )

        schema = StructType(
            [
                StructField("left", LongType(), True),
                StructField("right", LongType(), True),
            ]
        )

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": "1"}):
            self.assertTrue(is_remote())
            cdf3 = cdf1.groupby("id").cogroup(cdf2.groupby("id")).applyInArrow(summarize, schema)
            self.assertEqual(cdf3._cached_schema, schema)

        with self.temp_env({"SPARK_CONNECT_MODE_ENABLED": None}):
            self.assertFalse(is_remote())
            sdf3 = sdf1.groupby("id").cogroup(sdf2.groupby("id")).applyInArrow(summarize, schema)

        self.assertEqual(cdf3.schema, sdf3.schema)
        self.assertEqual(cdf3.collect(), sdf3.collect())

    def test_cached_schema_set_op(self):
        data1 = [(1, 2, 3)]
        data2 = [(6, 2, 5)]
        data3 = [(6, 2, 5.0)]

        cdf1 = self.connect.createDataFrame(data1, ["a", "b", "c"])
        sdf1 = self.spark.createDataFrame(data1, ["a", "b", "c"])
        cdf2 = self.connect.createDataFrame(data2, ["a", "b", "c"])
        sdf2 = self.spark.createDataFrame(data2, ["a", "b", "c"])
        cdf3 = self.connect.createDataFrame(data3, ["a", "b", "c"])
        sdf3 = self.spark.createDataFrame(data3, ["a", "b", "c"])

        # schema not yet cached
        self.assertTrue(cdf1._cached_schema is None)
        self.assertTrue(cdf2._cached_schema is None)
        self.assertTrue(cdf3._cached_schema is None)

        # no cached schema in result dataframe
        self.assertTrue(cdf1.union(cdf1)._cached_schema is None)
        self.assertTrue(cdf1.union(cdf2)._cached_schema is None)
        self.assertTrue(cdf1.union(cdf3)._cached_schema is None)

        self.assertTrue(cdf1.unionAll(cdf1)._cached_schema is None)
        self.assertTrue(cdf1.unionAll(cdf2)._cached_schema is None)
        self.assertTrue(cdf1.unionAll(cdf3)._cached_schema is None)

        self.assertTrue(cdf1.unionByName(cdf1)._cached_schema is None)
        self.assertTrue(cdf1.unionByName(cdf2)._cached_schema is None)
        self.assertTrue(cdf1.unionByName(cdf3)._cached_schema is None)

        self.assertTrue(cdf1.subtract(cdf1)._cached_schema is None)
        self.assertTrue(cdf1.subtract(cdf2)._cached_schema is None)
        self.assertTrue(cdf1.subtract(cdf3)._cached_schema is None)

        self.assertTrue(cdf1.exceptAll(cdf1)._cached_schema is None)
        self.assertTrue(cdf1.exceptAll(cdf2)._cached_schema is None)
        self.assertTrue(cdf1.exceptAll(cdf3)._cached_schema is None)

        self.assertTrue(cdf1.intersect(cdf1)._cached_schema is None)
        self.assertTrue(cdf1.intersect(cdf2)._cached_schema is None)
        self.assertTrue(cdf1.intersect(cdf3)._cached_schema is None)

        self.assertTrue(cdf1.intersectAll(cdf1)._cached_schema is None)
        self.assertTrue(cdf1.intersectAll(cdf2)._cached_schema is None)
        self.assertTrue(cdf1.intersectAll(cdf3)._cached_schema is None)

        # trigger analysis of cdf1.schema
        self.assertEqual(cdf1.schema, sdf1.schema)
        self.assertTrue(cdf1._cached_schema is not None)

        self.assertEqual(cdf1.union(cdf1)._cached_schema, cdf1._cached_schema)
        # cannot infer when cdf2 doesn't cache schema
        self.assertTrue(cdf1.union(cdf2)._cached_schema is None)
        # cannot infer when cdf3 doesn't cache schema
        self.assertTrue(cdf1.union(cdf3)._cached_schema is None)

        # trigger analysis of cdf2.schema, cdf3.schema
        self.assertEqual(cdf2.schema, sdf2.schema)
        self.assertEqual(cdf3.schema, sdf3.schema)

        # now all the schemas are cached
        self.assertTrue(cdf1._cached_schema is not None)
        self.assertTrue(cdf2._cached_schema is not None)
        self.assertTrue(cdf3._cached_schema is not None)

        self.assertEqual(cdf1.union(cdf1)._cached_schema, cdf1._cached_schema)
        self.assertEqual(cdf1.union(cdf2)._cached_schema, cdf1._cached_schema)
        # cannot infer when schemas mismatch
        self.assertTrue(cdf1.union(cdf3)._cached_schema is None)

        self.assertEqual(cdf1.unionAll(cdf1)._cached_schema, cdf1._cached_schema)
        self.assertEqual(cdf1.unionAll(cdf2)._cached_schema, cdf1._cached_schema)
        # cannot infer when schemas mismatch
        self.assertTrue(cdf1.unionAll(cdf3)._cached_schema is None)

        self.assertEqual(cdf1.unionByName(cdf1)._cached_schema, cdf1._cached_schema)
        self.assertEqual(cdf1.unionByName(cdf2)._cached_schema, cdf1._cached_schema)
        # cannot infer when schemas mismatch
        self.assertTrue(cdf1.unionByName(cdf3)._cached_schema is None)

        self.assertEqual(cdf1.subtract(cdf1)._cached_schema, cdf1._cached_schema)
        self.assertEqual(cdf1.subtract(cdf2)._cached_schema, cdf1._cached_schema)
        # cannot infer when schemas mismatch
        self.assertTrue(cdf1.subtract(cdf3)._cached_schema is None)

        self.assertEqual(cdf1.exceptAll(cdf1)._cached_schema, cdf1._cached_schema)
        self.assertEqual(cdf1.exceptAll(cdf2)._cached_schema, cdf1._cached_schema)
        # cannot infer when schemas mismatch
        self.assertTrue(cdf1.exceptAll(cdf3)._cached_schema is None)

        self.assertEqual(cdf1.intersect(cdf1)._cached_schema, cdf1._cached_schema)
        self.assertEqual(cdf1.intersect(cdf2)._cached_schema, cdf1._cached_schema)
        # cannot infer when schemas mismatch
        self.assertTrue(cdf1.intersect(cdf3)._cached_schema is None)

        self.assertEqual(cdf1.intersectAll(cdf1)._cached_schema, cdf1._cached_schema)
        self.assertEqual(cdf1.intersectAll(cdf2)._cached_schema, cdf1._cached_schema)
        # cannot infer when schemas mismatch
        self.assertTrue(cdf1.intersectAll(cdf3)._cached_schema is None)

    def test_cached_schema_in_chain_op(self):
        data = [(1, 1.0), (2, 2.0), (1, 3.0), (2, 4.0)]

        cdf = self.connect.createDataFrame(data, ("id", "v1"))
        sdf = self.spark.createDataFrame(data, ("id", "v1"))

        cdf1 = cdf.withColumn("v2", CF.lit(1))
        sdf1 = sdf.withColumn("v2", SF.lit(1))

        self.assertTrue(cdf1._cached_schema is None)
        # trigger analysis of cdf1.schema
        self.assertEqual(cdf1.schema, sdf1.schema)
        self.assertTrue(cdf1._cached_schema is not None)

        cdf2 = cdf1.where(cdf1.v2 > 0)
        sdf2 = sdf1.where(sdf1.v2 > 0)
        self.assertEqual(cdf1._cached_schema, cdf2._cached_schema)

        cdf3 = cdf2.repartition(10)
        sdf3 = sdf2.repartition(10)
        self.assertEqual(cdf1._cached_schema, cdf3._cached_schema)

        cdf4 = cdf3.distinct()
        sdf4 = sdf3.distinct()
        self.assertEqual(cdf1._cached_schema, cdf4._cached_schema)

        cdf5 = cdf4.sample(fraction=0.5)
        sdf5 = sdf4.sample(fraction=0.5)
        self.assertEqual(cdf1._cached_schema, cdf5._cached_schema)

        self.assertEqual(cdf5.schema, sdf5.schema)


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_dataframe_property import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
