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

from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    StructType,
    StructField,
)
from pyspark.errors import PySparkTypeError
from pyspark.testing.sqlutils import ReusedSQLTestCase


class DataFrameRepartitionTestsMixin:
    def test_repartition(self):
        df = self.spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        with self.assertRaises(PySparkTypeError) as pe:
            df.repartition([10], "name", "age").rdd.getNumPartitions()

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "numPartitions", "arg_type": "list"},
        )

    def test_repartition_by_range(self):
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("height", DoubleType(), True),
            ]
        )

        df1 = self.spark.createDataFrame(
            [("Bob", 27, 66.0), ("Alice", 10, 10.0), ("Bob", 10, 66.0)], schema
        )
        df2 = self.spark.createDataFrame(
            [("Alice", 10, 10.0), ("Bob", 10, 66.0), ("Bob", 27, 66.0)], schema
        )

        # test repartitionByRange(numPartitions, *cols)
        df3 = df1.repartitionByRange(2, "name", "age")

        self.assertEqual(df3.select(spark_partition_id()).distinct().count(), 2)
        self.assertEqual(df3.first(), df2.first())
        self.assertEqual(df3.take(3), df2.take(3))

        # test repartitionByRange(numPartitions, *cols)
        df4 = df1.repartitionByRange(3, "name", "age")
        self.assertEqual(df4.select(spark_partition_id()).distinct().count(), 3)
        self.assertEqual(df4.first(), df2.first())
        self.assertEqual(df4.take(3), df2.take(3))

        # test repartitionByRange(*cols)
        df5 = df1.repartitionByRange(5, "name", "age")
        self.assertEqual(df5.first(), df2.first())
        self.assertEqual(df5.take(3), df2.take(3))

        with self.assertRaises(PySparkTypeError) as pe:
            df1.repartitionByRange([10], "name", "age")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_INT_OR_STR",
            messageParameters={"arg_name": "numPartitions", "arg_type": "list"},
        )


class DataFrameRepartitionTests(
    DataFrameRepartitionTestsMixin,
    ReusedSQLTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_repartition import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
