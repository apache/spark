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
package org.apache.spark.sql.execution.datasources.v2.state

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeRow}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider}
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.unsafe.Platform

/**
 * Test suite to verify StatePartitionReaderAllColumnFamilies functionality.
 */
@SlowSQLTest
class StatePartitionReaderAllColumnFamiliesSuite extends StateDataSourceTestBase {

  import testImplicits._

  /**
   * Returns a set of (partitionId, key, value) tuples from a normal state read.
   */
  private def getNormalReadData(checkpointDir: String): DataFrame = {
    spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointDir)
      .load()
      .selectExpr("partition_id", "key", "value")
  }

  /**
   * Returns a DataFrame with raw bytes mode (INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES = true).
   */
  private def getBytesReadDf(checkpointDir: String): DataFrame = {
    spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointDir)
      .option(StateSourceOptions.INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES, "true")
      .load()
  }

  /**
   * Validates the schema and column families of the bytes read DataFrame.
   */
  private def validateBytesReadSchema(df: DataFrame): Unit = {
    // Verify schema
    val schema = df.schema
    assert(schema.fieldNames === Array(
      "partition_key", "key_bytes", "value_bytes", "column_family_name", "value", "key"))
    assert(schema("partition_key").dataType.typeName === "struct")
    assert(schema("key_bytes").dataType.typeName === "binary")
    assert(schema("value_bytes").dataType.typeName === "binary")
    assert(schema("column_family_name").dataType.typeName === "string")
  }

  private def parseBytesReadData(df: Array[Row], keyLength: Int, valueLength: Int)
    : Set[(GenericRowWithSchema, UnsafeRow, UnsafeRow, String)] = {
    df.map { row =>
        val partitionKey = row.getAs[GenericRowWithSchema](0)
        val keyBytes = row.getAs[Array[Byte]](1)
        val valueBytes = row.getAs[Array[Byte]](2)
        val columnFamily = row.getString(3)

        // Deserialize key bytes to UnsafeRow
        val keyRow = new UnsafeRow(keyLength)
        keyRow.pointTo(
          keyBytes,
          Platform.BYTE_ARRAY_OFFSET,
          keyBytes.length)

        // Deserialize value bytes to UnsafeRow
        val valueRow = new UnsafeRow(valueLength)
        valueRow.pointTo(
          valueBytes,
          Platform.BYTE_ARRAY_OFFSET,
          valueBytes.length)
        (partitionKey, keyRow.copy(), valueRow.copy(), columnFamily)
      }
      .toSet
  }

  /**
   * Compares normal read data with bytes read data for a specific column family.
   */
  private def compareNormalAndBytesData(
      normalReadDf: DataFrame,
      bytesReadDf: DataFrame,
      columnFamily: String,
      keySchema: StructType,
      valueSchema: StructType): Unit = {
    // Verify data
    val bytesDf = bytesReadDf
      .selectExpr("partition_key", "key_bytes", "value_bytes", "column_family_name")
      .collect()
    assert(bytesDf.length == 10,
      s"Expected 10 rows but got: ${bytesDf.length}")

    // Filter bytes data for the specified column family
    val bytesData = parseBytesReadData(bytesDf, keySchema.length, valueSchema.length)
    val filteredBytesData = bytesData.filter(_._4 == columnFamily)

    // Apply the projection
    // Convert to comparable format (extract field values)
    val normalSet = normalReadDf.collect().map { row =>
      val key = row.getStruct(1)
      val value = row.getStruct(2)
      val keyFields = (0 until key.length).map(i => key.get(i))
      val valueFields = (0 until value.length).map(i => value.get(i))
      (keyFields, valueFields)
    }.toSet

    val bytesSet = filteredBytesData.map { case (_, keyRow, valueRow, _) =>
      val keyFields = (0 until keySchema.length).map(i =>
        keyRow.get(i, keySchema(i).dataType))
      val valueFields = (0 until valueSchema.length).map(i =>
        valueRow.get(i, valueSchema(i).dataType))
      (keyFields, valueFields)
    }
    // Verify same number of rows
    assert(filteredBytesData.size == normalSet.size,
      s"Row count mismatch for column family '$columnFamily': " +
        s"normal read has ${filteredBytesData.size} rows, bytes read has ${normalSet.size} rows")

    assert(normalSet == bytesSet)
  }

    test(s"read all column families with simple operator") {
      withTempDir { tempDir =>
        withSQLConf(
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.SHUFFLE_PARTITIONS.key -> "2") {

          val inputData = MemoryStream[Int]
          val aggregated = inputData.toDF()
            .selectExpr("value", "value % 10 AS groupKey")
            .groupBy($"groupKey")
            .agg(
              count("*").as("cnt"),
              sum("value").as("sum")
            )
            .as[(Int, Long, Long)]

          testStream(aggregated, OutputMode.Update)(
            StartStream(checkpointLocation = tempDir.getAbsolutePath),
            // batch 0
            AddData(inputData, 0 until 20: _*),
            CheckLastBatch(
              (0, 2, 10), // 0, 10
              (1, 2, 12), // 1, 11
              (2, 2, 14), // 2, 12
              (3, 2, 16), // 3, 13
              (4, 2, 18), // 4, 14
              (5, 2, 20), // 5, 15
              (6, 2, 22), // 6, 16
              (7, 2, 24), // 7, 17
              (8, 2, 26), // 8, 18
              (9, 2, 28) // 9, 19
            ),
            StopStream
          )

          // Read state data once with INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES = true
          val bytesReadDf = getBytesReadDf(tempDir.getAbsolutePath)

          // Verify schema and column families
          validateBytesReadSchema(bytesReadDf)
          // Compare normal and bytes data for default column family
          val keySchema: StructType = StructType(Array(
            StructField("key", IntegerType, nullable = false)
          ))

          // Value schema for the aggregation: count and sum columns
          val valueSchema: StructType = StructType(Array(
            StructField("count", LongType, nullable = false),
            StructField("sum", LongType, nullable = false)
          ))

          // Get normal read data for comparison
          val normalData = getNormalReadData(tempDir.getAbsolutePath)
          compareNormalAndBytesData(
            normalData, bytesReadDf, "default", keySchema, valueSchema)
      }
    }
  }

  test("internalOnlyReadAllColumnFamilies should fail with HDFS-backed state store") {
    withTempDir { tempDir =>
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[HDFSBackedStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {

        val inputData = MemoryStream[Int]
        val aggregated = inputData.toDF()
          .selectExpr("value", "value % 10 AS groupKey")
          .groupBy($"groupKey")
          .agg(
            count("*").as("cnt"),
            sum("value").as("sum")
          )
          .as[(Int, Long, Long)]

        testStream(aggregated, OutputMode.Update)(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          // batch 0
          AddData(inputData, 0 until 1: _*),
          CheckLastBatch(
            (0, 1, 0)
          ),
          StopStream
        )

        // Attempt to read with internalOnlyReadAllColumnFamilies=true should fail
        val e = intercept[StateDataSourceException] {
          spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
            .option(StateSourceOptions.INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES, "true")
            .load()
            .collect()
        }
        assert(e.getMessage.contains("internalOnlyReadAllColumnFamilies=true is only " +
          s"supported with RocksDBStateStoreProvider"))
      }
    }
  }
}
