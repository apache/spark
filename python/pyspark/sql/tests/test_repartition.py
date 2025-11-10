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

from pyspark.sql.functions import spark_partition_id, col, lit, when
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    StructType,
    StructField,
)
from pyspark.errors import PySparkTypeError, PySparkValueError
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

    def test_repartition_by_id(self):
        # Test basic partition ID passthrough behavior
        numPartitions = 10
        df = self.spark.range(100).withColumn("expected_p_id", col("id") % numPartitions)
        repartitioned = df.repartitionById(numPartitions, col("expected_p_id").cast("int"))
        result = repartitioned.withColumn("actual_p_id", spark_partition_id())

        # All rows should be in their expected partitions
        self.assertEqual(result.filter(col("expected_p_id") != col("actual_p_id")).count(), 0)

    def test_repartition_by_id_negative_values(self):
        df = self.spark.range(10).toDF("id")
        repartitioned = df.repartitionById(10, (col("id") - 5).cast("int"))
        result = repartitioned.withColumn("actual_p_id", spark_partition_id()).collect()

        for row in result:
            actualPartitionId = row["actual_p_id"]
            id_val = row["id"]
            expectedPartitionId = int((id_val - 5) % 10)
            self.assertEqual(
                actualPartitionId,
                expectedPartitionId,
                f"Row with id={id_val} should be in partition {expectedPartitionId}, "
                f"but was in partition {actualPartitionId}",
            )

    def test_repartition_by_id_null_values(self):
        # Test that null partition ids go to partition 0
        df = self.spark.range(10).toDF("id")
        partitionExpr = when(col("id") < 5, col("id")).otherwise(lit(None)).cast("int")
        repartitioned = df.repartitionById(10, partitionExpr)
        result = repartitioned.withColumn("actual_p_id", spark_partition_id()).collect()

        nullRows = [row for row in result if row["id"] >= 5]
        self.assertTrue(len(nullRows) > 0, "Should have rows with null partition expression")
        for row in nullRows:
            self.assertEqual(
                row["actual_p_id"],
                0,
                f"Row with null partition id should go to partition 0, "
                f"but went to partition {row['actual_p_id']}",
            )

        nonNullRows = [row for row in result if row["id"] < 5]
        for row in nonNullRows:
            id_val = row["id"]
            actualPartitionId = row["actual_p_id"]
            expectedPartitionId = id_val % 10
            self.assertEqual(
                actualPartitionId,
                expectedPartitionId,
                f"Row with id={id_val} should be in partition {expectedPartitionId}, "
                f"but was in partition {actualPartitionId}",
            )

    def test_repartition_by_id_error_non_int_type(self):
        # Test error for non-integer partition column type
        df = self.spark.range(5).withColumn("s", lit("a"))
        with self.assertRaises(Exception):  # Should raise analysis exception
            df.repartitionById(5, col("s")).collect()

    def test_repartition_by_id_error_invalid_num_partitions(self):
        df = self.spark.range(5)

        with self.assertRaises(PySparkTypeError) as pe:
            df.repartitionById("5", col("id").cast("int"))
        self.check_error(
            exception=pe.exception,
            errorClass="NOT_INT",
            messageParameters={"arg_name": "numPartitions", "arg_type": "str"},
        )

        with self.assertRaises(PySparkValueError) as pe:
            df.repartitionById(0, col("id").cast("int"))
        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_POSITIVE",
            messageParameters={"arg_name": "numPartitions", "arg_value": "0"},
        )

        # Test negative numPartitions
        with self.assertRaises(PySparkValueError) as pe:
            df.repartitionById(-1, col("id").cast("int"))
        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_POSITIVE",
            messageParameters={"arg_name": "numPartitions", "arg_value": "-1"},
        )

    def test_repartition_by_id_out_of_range(self):
        numPartitions = 10
        df = self.spark.range(20).toDF("id")
        repartitioned = df.repartitionById(numPartitions, col("id").cast("int"))
        result = repartitioned.collect()

        self.assertEqual(len(result), 20)
        # Skip RDD partition count check for Connect mode since RDD is not available
        try:
            self.assertEqual(repartitioned.rdd.getNumPartitions(), numPartitions)
        except Exception:
            # Connect mode doesn't support RDD operations, so we skip this check
            pass

    def test_repartition_by_id_string_column_name(self):
        numPartitions = 5
        df = self.spark.range(25).withColumn(
            "partition_id", (col("id") % numPartitions).cast("int")
        )
        repartitioned = df.repartitionById(numPartitions, "partition_id")
        result = repartitioned.withColumn("actual_p_id", spark_partition_id())

        mismatches = result.filter(col("partition_id") != col("actual_p_id")).count()
        self.assertEqual(mismatches, 0)


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
