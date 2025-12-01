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

import java.nio.ByteOrder
import java.util.Arrays

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider, StateRepartitionUnsupportedProviderError, StateStore}
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, NullType, StructField, StructType, TimestampType}

/**
 * Note: This extends StateDataSourceTestBase to access
 * helper methods like runDropDuplicatesQuery without inheriting all predefined tests.
 */
class StatePartitionAllColumnFamiliesReaderSuite extends StateDataSourceTestBase {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      classOf[RocksDBStateStoreProvider].getName)
  }

  private def getNormalReadDf(
      checkpointDir: String,
      storeName: Option[String] = Option.empty[String]): DataFrame = {
    spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointDir)
      .option(StateSourceOptions.STORE_NAME, storeName.orNull)
      .load()
      .selectExpr("partition_id", "key", "value")
  }

  private def getBytesReadDf(
       checkpointDir: String,
       storeName: Option[String] = Option.empty[String]): DataFrame = {
    spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointDir)
      .option(StateSourceOptions.INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES, "true")
      .option(StateSourceOptions.STORE_NAME, storeName.orNull)
      .load()
  }

  /**
   * Validates the schema and column families of the bytes read DataFrame.
   */
  private def validateBytesReadDfSchema(df: DataFrame): Unit = {
    // Verify schema
    val schema = df.schema
    assert(schema.fieldNames === Array(
      "partition_key", "key_bytes", "value_bytes", "column_family_name"))
    assert(schema("partition_key").dataType.typeName === "struct")
    assert(schema("key_bytes").dataType.typeName === "binary")
    assert(schema("value_bytes").dataType.typeName === "binary")
    assert(schema("column_family_name").dataType.typeName === "string")
  }

  /**
   * Compares normal read data with bytes read data for a specific column family.
   * Converts normal rows to bytes then compares with bytes read.
   */
  private def compareNormalAndBytesData(
      normalDf: Array[Row],
      bytesDf: Array[Row],
      columnFamily: String,
      keySchema: StructType,
      valueSchema: StructType): Unit = {

    // Filter bytes data for the specified column family and extract raw bytes directly
    val filteredBytesData = bytesDf.filter { row =>
      row.getString(3) == columnFamily
    }

    // Verify same number of rows
    assert(filteredBytesData.length == normalDf.length,
      s"Row count mismatch for column family '$columnFamily': " +
        s"normal read has ${normalDf.length} rows, " +
        s"bytes read has ${filteredBytesData.length} rows")

    // Create projections to convert Row to UnsafeRow bytes
    val keyProjection = UnsafeProjection.create(keySchema)
    val valueProjection = UnsafeProjection.create(valueSchema)

    // Create converters to convert external Row types to internal Catalyst types
    val keyConverter = CatalystTypeConverters.createToCatalystConverter(keySchema)
    val valueConverter = CatalystTypeConverters.createToCatalystConverter(valueSchema)

    // Convert normal data to bytes
    val normalAsBytes = normalDf.toSeq.map { row =>
      val key = row.getStruct(1)
      val value = if (row.isNullAt(2)) null else row.getStruct(2)

      // Convert key to InternalRow, then to UnsafeRow, then get bytes
      val keyInternalRow = keyConverter(key).asInstanceOf[InternalRow]
      val keyUnsafeRow = keyProjection(keyInternalRow)
      // IMPORTANT: Must clone the bytes array since getBytes() returns a reference
      // that may be overwritten by subsequent UnsafeRow operations
      val keyBytes = keyUnsafeRow.getBytes.clone()

      // Convert value to bytes
      val valueBytes = if (value == null) {
        Array.empty[Byte]
      } else {
        val valueInternalRow = valueConverter(value).asInstanceOf[InternalRow]
        val valueUnsafeRow = valueProjection(valueInternalRow)
        // IMPORTANT: Must clone the bytes array since getBytes() returns a reference
        // that may be overwritten by subsequent UnsafeRow operations
        valueUnsafeRow.getBytes.clone()
      }

      (keyBytes, valueBytes)
    }

    // Extract raw bytes from bytes read data (no deserialization/reserialization)
    val bytesAsBytes = filteredBytesData.map { row =>
      val keyBytes = row.getAs[Array[Byte]](1)
      val valueBytes = row.getAs[Array[Byte]](2)
      (keyBytes, valueBytes)
    }

    // Sort both for comparison (since Set equality doesn't work well with byte arrays)
    val normalSorted = normalAsBytes.sortBy(x => (x._1.mkString(","), x._2.mkString(",")))
    val bytesSorted = bytesAsBytes.sortBy(x => (x._1.mkString(","), x._2.mkString(",")))

    assert(normalSorted.length == bytesSorted.length,
      s"Size mismatch: normal has ${normalSorted.length}, bytes has ${bytesSorted.length}")

    // Compare each pair
    normalSorted.zip(bytesSorted).zipWithIndex.foreach {
      case (((normalKey, normalValue), (bytesKey, bytesValue)), idx) =>
        assert(Arrays.equals(normalKey, bytesKey),
          s"Key mismatch at index $idx:\n" +
            s"  Normal: ${normalKey.mkString("[", ",", "]")}\n" +
            s"  Bytes:  ${bytesKey.mkString("[", ",", "]")}")
        assert(Arrays.equals(normalValue, bytesValue),
          s"Value mismatch at index $idx:\n" +
            s"  Normal: ${normalValue.mkString("[", ",", "]")}\n" +
            s"  Bytes:  ${bytesValue.mkString("[", ",", "]")}")
    }
  }

  // Run all tests with both changelog checkpointing enabled and disabled
  Seq(true, false).foreach { changelogCheckpointingEnabled =>
    val testSuffix = if (changelogCheckpointingEnabled) {
      "with changelog checkpointing"
    } else {
      "without changelog checkpointing"
    }

    def testWithChangelogConfig(testName: String)(testFun: => Unit): Unit = {
      test(s"$testName ($testSuffix)") {
        withSQLConf(
          "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled" ->
            changelogCheckpointingEnabled.toString) {
          testFun
        }
      }
    }

    testWithChangelogConfig("SPARK-54388: simple aggregation state ver 1") {
      withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "1") {
      withTempDir { tempDir =>
        runLargeDataStreamingAggregationQuery(tempDir.getAbsolutePath)

        val keySchema = StructType(Array(StructField("groupKey", IntegerType, nullable = false)))
        // State version 1 includes key columns in the value
        val valueSchema = StructType(Array(
          StructField("groupKey", IntegerType, nullable = false),
          StructField("count", LongType, nullable = false),
          StructField("sum", LongType, nullable = false),
          StructField("max", IntegerType, nullable = false),
          StructField("min", IntegerType, nullable = false)
        ))

        val normalData = getNormalReadDf(tempDir.getAbsolutePath).collect()
        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)

        validateBytesReadDfSchema(bytesDf)
        compareNormalAndBytesData(normalData, bytesDf.collect(), "default", keySchema, valueSchema)
      }
      }
    }

    testWithChangelogConfig("SPARK-54388: simple aggregation state ver 2") {
      withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2") {
      withTempDir { tempDir =>
        runLargeDataStreamingAggregationQuery(tempDir.getAbsolutePath)

        val keySchema = StructType(Array(StructField("groupKey", IntegerType, nullable = false)))
        val valueSchema = StructType(Array(
          StructField("count", LongType, nullable = false),
          StructField("sum", LongType, nullable = false),
          StructField("max", IntegerType, nullable = false),
          StructField("min", IntegerType, nullable = false)
        ))

        val normalData = getNormalReadDf(tempDir.getAbsolutePath).collect()
        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)

        validateBytesReadDfSchema(bytesDf)
        compareNormalAndBytesData(normalData, bytesDf.collect(), "default", keySchema, valueSchema)
      }
      }
    }

    testWithChangelogConfig("SPARK-54388: composite key aggregation state ver 1") {
      withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "1") {
      withTempDir { tempDir =>
        runCompositeKeyStreamingAggregationQuery(tempDir.getAbsolutePath)

        val keySchema = StructType(Array(
          StructField("groupKey", IntegerType, nullable = false),
          StructField("fruit", org.apache.spark.sql.types.StringType, nullable = true)
        ))
        // State version 1 includes key columns in the value
        val valueSchema = StructType(Array(
          StructField("groupKey", IntegerType, nullable = false),
          StructField("fruit", org.apache.spark.sql.types.StringType, nullable = true),
          StructField("count", LongType, nullable = false),
          StructField("sum", LongType, nullable = false),
          StructField("max", IntegerType, nullable = false),
          StructField("min", IntegerType, nullable = false)
        ))

        val normalData = getNormalReadDf(tempDir.getAbsolutePath).collect()
        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)

        validateBytesReadDfSchema(bytesDf)
        compareNormalAndBytesData(normalData, bytesDf.collect(), "default", keySchema, valueSchema)
      }
      }
    }

    testWithChangelogConfig("SPARK-54388: composite key aggregation state ver 2") {
      withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2") {
      withTempDir { tempDir =>
        runCompositeKeyStreamingAggregationQuery(tempDir.getAbsolutePath)

        val keySchema = StructType(Array(
          StructField("groupKey", IntegerType, nullable = false),
          StructField("fruit", org.apache.spark.sql.types.StringType, nullable = true)
        ))
        val valueSchema = StructType(Array(
          StructField("count", LongType, nullable = false),
          StructField("sum", LongType, nullable = false),
          StructField("max", IntegerType, nullable = false),
          StructField("min", IntegerType, nullable = false)
        ))

        val normalData = getNormalReadDf(tempDir.getAbsolutePath).collect()
        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)

        validateBytesReadDfSchema(bytesDf)
        compareNormalAndBytesData(normalData, bytesDf.collect(), "default", keySchema, valueSchema)
      }
      }
    }

    testWithChangelogConfig("SPARK-54388: dropDuplicates validation") {
      withTempDir { tempDir =>
        runDropDuplicatesQuery(tempDir.getAbsolutePath)

        val keySchema = StructType(Array(
          StructField("value", IntegerType, nullable = false),
          StructField("eventTime", org.apache.spark.sql.types.TimestampType)
        ))
        val valueSchema = StructType(Array(
          StructField("__dummy__", NullType, nullable = true)
        ))

        val normalData = getNormalReadDf(tempDir.getAbsolutePath).collect()
        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)

        validateBytesReadDfSchema(bytesDf)
        compareNormalAndBytesData(normalData, bytesDf.collect(), "default", keySchema, valueSchema)
      }
    }

    testWithChangelogConfig("SPARK-54388: dropDuplicates with column specified") {
      withTempDir { tempDir =>
        runDropDuplicatesQueryWithColumnSpecified(tempDir.getAbsolutePath)

        val keySchema = StructType(Array(
          StructField("col1", org.apache.spark.sql.types.StringType, nullable = true)
        ))
        val valueSchema = StructType(Array(
          StructField("__dummy__", NullType, nullable = true)
        ))

        val normalData = getNormalReadDf(tempDir.getAbsolutePath).collect()
        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)

        validateBytesReadDfSchema(bytesDf)
        compareNormalAndBytesData(normalData, bytesDf.collect(), "default", keySchema, valueSchema)
      }
    }

    testWithChangelogConfig("SPARK-54388: dropDuplicatesWithinWatermark") {
      withTempDir { tempDir =>
        runDropDuplicatesWithinWatermarkQuery(tempDir.getAbsolutePath)

        val keySchema = StructType(Array(
          StructField("_1", org.apache.spark.sql.types.StringType, nullable = true)
        ))
        val valueSchema = StructType(Array(
          StructField("expiresAtMicros", LongType, nullable = false)
        ))

        val normalData = getNormalReadDf(tempDir.getAbsolutePath).collect()
        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)

        validateBytesReadDfSchema(bytesDf)
        compareNormalAndBytesData(normalData, bytesDf.collect(), "default", keySchema, valueSchema)
      }
    }

    testWithChangelogConfig("SPARK-54388: session window aggregation") {
      withTempDir { tempDir =>
        runSessionWindowAggregationQuery(tempDir.getAbsolutePath)

        val keySchema = StructType(Array(
          StructField("sessionId", org.apache.spark.sql.types.StringType, nullable = false),
          StructField("sessionStartTime",
            org.apache.spark.sql.types.TimestampType, nullable = false)
        ))
        val valueSchema = StructType(Array(
          StructField("session_window", org.apache.spark.sql.types.StructType(Array(
            StructField("start", org.apache.spark.sql.types.TimestampType),
            StructField("end", org.apache.spark.sql.types.TimestampType)
          )), nullable = false),
          StructField("sessionId", org.apache.spark.sql.types.StringType, nullable = false),
          StructField("count", LongType, nullable = false)
        ))

        val normalData = getNormalReadDf(tempDir.getAbsolutePath).collect()
        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)

        validateBytesReadDfSchema(bytesDf)
        compareNormalAndBytesData(normalData, bytesDf.collect(), "default", keySchema, valueSchema)
      }
    }

    testWithChangelogConfig("SPARK-54388: flatMapGroupsWithState, state ver 1") {
      // Skip this test on big endian platforms
      assume(java.nio.ByteOrder.nativeOrder().equals(java.nio.ByteOrder.LITTLE_ENDIAN))
      withSQLConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> "1") {
        withTempDir { tempDir =>
          assume(ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN))
          runFlatMapGroupsWithStateQuery(tempDir.getAbsolutePath)

          val keySchema = StructType(Array(
            StructField("value", org.apache.spark.sql.types.StringType, nullable = true)
          ))
          val valueSchema = StructType(Array(
            StructField("numEvents", IntegerType, nullable = false),
            StructField("startTimestampMs", LongType, nullable = false),
            StructField("endTimestampMs", LongType, nullable = false),
            StructField("timeoutTimestamp", IntegerType, nullable = false)
          ))

          val normalData = getNormalReadDf(tempDir.getAbsolutePath).collect()
          val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)

          validateBytesReadDfSchema(bytesDf)
          compareNormalAndBytesData(
            normalData, bytesDf.collect(), "default", keySchema, valueSchema)
        }
      }
    }

    testWithChangelogConfig("SPARK-54388: flatMapGroupsWithState, state ver 2") {
      withSQLConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> "2") {
        withTempDir { tempDir =>
          runFlatMapGroupsWithStateQuery(tempDir.getAbsolutePath)

          val keySchema = StructType(Array(
            StructField("value", org.apache.spark.sql.types.StringType, nullable = true)
          ))
          val valueSchema = StructType(Array(
            StructField("groupState", org.apache.spark.sql.types.StructType(Array(
              StructField("numEvents", IntegerType, nullable = false),
              StructField("startTimestampMs", LongType, nullable = false),
              StructField("endTimestampMs", LongType, nullable = false)
            )), nullable = false),
            StructField("timeoutTimestamp", LongType, nullable = false)
          ))

          val normalData = getNormalReadDf(tempDir.getAbsolutePath).collect()
          val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)

          validateBytesReadDfSchema(bytesDf)
          compareNormalAndBytesData(
            normalData, bytesDf.collect(), "default", keySchema, valueSchema)
        }
      }
    }

    def testStreamStreamJoin(stateVersion: Int): Unit = {
      withSQLConf(SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> stateVersion.toString) {
        withTempDir { tempDir =>
          runStreamStreamJoinQuery(tempDir.getAbsolutePath)

          Seq("right-keyToNumValues", "left-keyToNumValues").foreach(storeName => {
            val stateReaderForRight = getNormalReadDf(
              tempDir.getAbsolutePath, Option(storeName))
            val stateBytesDfForRight = getBytesReadDf(
              tempDir.getAbsolutePath, Option(storeName))

            val keyToNumValuesKeySchema = StructType(Array(
              StructField("key", IntegerType)
            ))
            val keyToNumValueValueSchema = StructType(Array(
              StructField("value", LongType)
            ))

            validateBytesReadDfSchema(stateBytesDfForRight)
            compareNormalAndBytesData(
              stateReaderForRight.collect(),
              stateBytesDfForRight.collect(),
              StateStore.DEFAULT_COL_FAMILY_NAME,
              keyToNumValuesKeySchema,
              keyToNumValueValueSchema)
          })

          Seq("right-keyWithIndexToValue", "left-keyWithIndexToValue").foreach(storeName => {
            val stateReaderForRight = getNormalReadDf(
              tempDir.getAbsolutePath, Option(storeName))
            val stateBytesDfForRight = getBytesReadDf(
              tempDir.getAbsolutePath, Option(storeName))

            val keyToNumValuesKeySchema = StructType(Array(
              StructField("key", IntegerType, nullable = false),
              StructField("index", LongType)
            ))
            val keyToNumValueValueSchema = if (stateVersion == 2) {
              StructType(Array(
                StructField("value", IntegerType, nullable = false),
                StructField("time", TimestampType, nullable = false),
                StructField("matched", BooleanType)
              ))
            } else {
              StructType(Array(
                StructField("value", IntegerType, nullable = false),
                StructField("time", TimestampType, nullable = false)
              ))
            }

            validateBytesReadDfSchema(stateBytesDfForRight)
            compareNormalAndBytesData(
              stateReaderForRight.collect(),
              stateBytesDfForRight.collect(),
              StateStore.DEFAULT_COL_FAMILY_NAME,
              keyToNumValuesKeySchema,
              keyToNumValueValueSchema)
          })
        }
      }
    }

    testWithChangelogConfig("stream-stream join, state ver 1") {
      testStreamStreamJoin(1)
    }

    testWithChangelogConfig("stream-stream join, state ver 2") {
      testStreamStreamJoin(2)
    }
  } // End of foreach loop for changelog checkpointing dimension

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
          AddData(inputData, 0 until 1: _*),
          CheckLastBatch((0, 1, 0)),
          StopStream
        )

        checkError(
          exception = intercept[StateRepartitionUnsupportedProviderError] {
            spark.read
              .format("statestore")
              .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
              .option(StateSourceOptions.INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES, "true")
              .load()
              .collect()
          },
          condition = "STATE_REPARTITION_INVALID_CHECKPOINT.UNSUPPORTED_PROVIDER",
          parameters = Map(
            "checkpointLocation" -> s".*${tempDir.getAbsolutePath}",
            "provider" -> classOf[HDFSBackedStateStoreProvider].getName
          ),
          matchPVals = true
        )
      }
    }
  }
}
