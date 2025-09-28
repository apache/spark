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

from pyspark.sql import Row
from pyspark.sql.functions import udf, col
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    MapType,
    StringType,
    StructType,
    StructField,
)
from pyspark.testing.sqlutils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
    ReusedSQLTestCase,
)
from pyspark.util import PythonEvalType


@unittest.skipIf(
    not have_pandas or not have_pyarrow, pandas_requirement_message or pyarrow_requirement_message
)
class BinaryAsBytesUDFTests(ReusedSQLTestCase):
    def test_arrow_batched_udf_binary_type(self):
        def get_binary_type(x):
            return type(x).__name__

        binary_udf = udf(get_binary_type, returnType="string", useArrow=True)

        df = self.spark.createDataFrame([
            Row(b=b"hello"),
            Row(b=b"world")
        ], schema=StructType([StructField("b", BinaryType())]))

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.binaryAsBytes": "true"}):
            result = df.select(binary_udf(col("b")).alias("type_name")).collect()
            self.assertEqual(result[0]["type_name"], "bytes")
            self.assertEqual(result[1]["type_name"], "bytes")

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.binaryAsBytes": "false"}):
            result = df.select(binary_udf(col("b")).alias("type_name")).collect()
            self.assertEqual(result[0]["type_name"], "bytearray")
            self.assertEqual(result[1]["type_name"], "bytearray")

    def test_arrow_batched_udf_array_binary_type(self):
        """Test SQL_ARROW_BATCHED_UDF with array of binary"""
        def check_array_binary_types(arr):
            return [type(x).__name__ for x in arr]

        array_binary_udf = udf(check_array_binary_types, returnType="array<string>", useArrow=True)

        df = self.spark.createDataFrame([
            Row(arr_b=[b"a", b"b"]),
            Row(arr_b=[b"c", b"d"])
        ], schema=StructType([StructField("arr_b", ArrayType(BinaryType()))]))

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.binaryAsBytes": "true"}):
            result = df.select(array_binary_udf(col("arr_b")).alias("types")).collect()
            self.assertEqual(result[0]["types"], ["bytes", "bytes"])
            self.assertEqual(result[1]["types"], ["bytes", "bytes"])

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.binaryAsBytes": "false"}):
            result = df.select(array_binary_udf(col("arr_b")).alias("types")).collect()
            self.assertEqual(result[0]["types"], ["bytearray", "bytearray"])
            self.assertEqual(result[1]["types"], ["bytearray", "bytearray"])

    def test_arrow_batched_udf_map_binary_type(self):
        def check_map_binary_types(m):
            return [type(v).__name__ for v in m.values()]

        map_binary_udf = udf(check_map_binary_types, returnType="array<string>", useArrow=True)

        df = self.spark.createDataFrame([
            Row(map_b={"k1": b"v1", "k2": b"v2"}),
            Row(map_b={"k3": b"v3"})
        ], schema=StructType([StructField("map_b", MapType(StringType(), BinaryType()))]))

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.binaryAsBytes": "true"}):
            result = df.select(map_binary_udf(col("map_b")).alias("types")).collect()
            self.assertEqual(set(result[0]["types"]), {"bytes"})
            self.assertEqual(result[1]["types"], ["bytes"])

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.binaryAsBytes": "false"}):
            result = df.select(map_binary_udf(col("map_b")).alias("types")).collect()
            self.assertEqual(set(result[0]["types"]), {"bytearray"})
            self.assertEqual(result[1]["types"], ["bytearray"])

    def test_arrow_batched_udf_struct_binary_type(self):
        def check_struct_binary_type(s):
            return type(s.b).__name__

        struct_binary_udf = udf(check_struct_binary_type, returnType="string", useArrow=True)

        struct_schema = StructType([
            StructField("i", IntegerType()),
            StructField("b", BinaryType())
        ])

        df = self.spark.createDataFrame([
            Row(struct_b=Row(i=1, b=b"data1")),
            Row(struct_b=Row(i=2, b=b"data2"))
        ], schema=StructType([StructField("struct_b", struct_schema)]))

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.binaryAsBytes": "true"}):
            result = df.select(struct_binary_udf(col("struct_b")).alias("type_name")).collect()
            self.assertEqual(result[0]["type_name"], "bytes")
            self.assertEqual(result[1]["type_name"], "bytes")

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.binaryAsBytes": "false"}):
            result = df.select(struct_binary_udf(col("struct_b")).alias("type_name")).collect()
            self.assertEqual(result[0]["type_name"], "bytearray")
            self.assertEqual(result[1]["type_name"], "bytearray")


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_binary_as_bytes_udf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
 