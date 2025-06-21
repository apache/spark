/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DataFrameCacheSuite extends QueryTest with SharedSparkSession {

  test("SPARK-52401: DataFrame.collect() should reflect table updates after saveAsTable append") {
    val schema = StructType(Seq(
      StructField("col1", IntegerType(), true),
      StructField("col2", StringType(), true)
    ))

    val tableName = "test_table_spark_52401"
    
    withTable(tableName) {
      // Create empty table
      spark.createDataFrame(Seq.empty[Row], schema)
        .write.saveAsTable(tableName)

      // Get DataFrame reference to the table
      val df = spark.table(tableName)

      // Verify initial state
      assert(df.count() === 0)
      assert(df.collect().isEmpty)

      // Append data to the table
      spark.createDataFrame(Seq(Row(1, "foo")), schema)
        .write.mode("append").saveAsTable(tableName)

      // Both count() and collect() should reflect the update
      assert(df.count() === 1)
      assert(df.collect().length === 1)
      assert(df.collect()(0) === Row(1, "foo"))
    }
  }

  test("SPARK-52401: DataFrame.collect() should reflect table updates after multiple saveAsTable append operations") {
    val schema = StructType(Seq(
      StructField("col1", IntegerType(), true),
      StructField("col2", StringType(), true)
    ))

    val tableName = "test_table_spark_52401_multiple"
    
    withTable(tableName) {
      // Create empty table
      spark.createDataFrame(Seq.empty[Row], schema)
        .write.saveAsTable(tableName)

      // Get DataFrame reference to the table
      val df = spark.table(tableName)

      // Verify initial state
      assert(df.count() === 0)
      assert(df.collect().isEmpty)

      // First append
      spark.createDataFrame(Seq(Row(1, "foo")), schema)
        .write.mode("append").saveAsTable(tableName)

      // Verify first append
      assert(df.count() === 1)
      assert(df.collect().length === 1)
      assert(df.collect()(0) === Row(1, "foo"))

      // Second append
      spark.createDataFrame(Seq(Row(2, "bar")), schema)
        .write.mode("append").saveAsTable(tableName)

      // Verify both appends
      assert(df.count() === 2)
      val collected = df.collect()
      assert(collected.length === 2)
      assert(collected.contains(Row(1, "foo")))
      assert(collected.contains(Row(2, "bar")))
    }
  }

  test("SPARK-52401: DataFrame operations should work correctly after table updates") {
    val schema = StructType(Seq(
      StructField("col1", IntegerType(), true),
      StructField("col2", StringType(), true)
    ))

    val tableName = "test_table_spark_52401_operations"
    
    withTable(tableName) {
      // Create empty table
      spark.createDataFrame(Seq.empty[Row], schema)
        .write.saveAsTable(tableName)

      // Get DataFrame reference to the table
      val df = spark.table(tableName)

      // Append data
      spark.createDataFrame(Seq(Row(1, "foo"), Row(2, "bar")), schema)
        .write.mode("append").saveAsTable(tableName)

      // Test various DataFrame operations
      assert(df.filter($"col1" === 1).count() === 1)
      assert(df.filter($"col1" === 1).collect().length === 1)
      assert(df.filter($"col1" === 1).collect()(0) === Row(1, "foo"))

      assert(df.select("col2").collect().length === 2)
      assert(df.groupBy("col2").count().collect().length === 2)
    }
  }
} 