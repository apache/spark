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
import java.sql.Timestamp
import java.time.Duration
import java.util.Arrays

import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider, StateRepartitionUnsupportedProviderError, StateStore}
import org.apache.spark.sql.functions.{col, count, sum, timestamp_seconds}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{ExpiredTimerInfo, ListState, MapState, OutputMode, SimpleMapValue, StatefulProcessor, TimeMode, TimerValues, TransformWithStateSuiteUtils, Trigger, TTLConfig, ValueState}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, LongType, NullType, StringType, StructField, StructType, TimestampType}

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
        UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null)).getBytes
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

  /**
   * Reads normal data for a state variable and validates it against bytes data.
   * This helper reduces boilerplate when testing multiple state variables.
   */
  private def readAndValidateStateVar(
      checkpointDir: String,
      allBytesData: Array[Row],
      stateVarName: String,
      keySchema: StructType,
      valueSchema: StructType,
      extraOptions: Map[String, String] = Map.empty,
      selectExprs: Seq[String] = Seq("partition_id", "key", "value")): Unit = {
    var reader = spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointDir)
      .option(StateSourceOptions.STATE_VAR_NAME, stateVarName)

    extraOptions.foreach { case (k, v) => reader = reader.option(k, v) }

    val normalDf = reader.load().selectExpr(selectExprs: _*)

    compareNormalAndBytesData(
      normalDf.collect(),
      allBytesData,
      stateVarName,
      keySchema,
      valueSchema)
  }

  /**
   * Validates timer column families for both event time and processing time timers.
   * @param checkpointDir The checkpoint directory path
   * @param timerPrefix The timer prefix: "event" for event time, "proc" for processing time
   */
  private def validateTimerColumnFamilies(
      checkpointDir: String,
      timerPrefix: String): Unit = {
    val bytesDf = getBytesReadDf(checkpointDir)

    validateBytesReadDfSchema(bytesDf)
    // Collect all bytes data
    val allBytesData = bytesDf.collect()

    // Get distinct column family names
    val columnFamilies = allBytesData.map(_.getString(3)).distinct.sorted

    // Verify countState column family exists
    assert(columnFamilies.toSet ==
      Set(s"$$${timerPrefix}Timers_keyToTimestamp",
        s"$$${timerPrefix}Timers_timestampToKey", "countState"))

    val groupByKeySchema = StructType(Array(
      StructField("key", StringType, nullable = true)
    ))
    val stateValueSchema = StructType(Array(
      StructField("value", LongType, nullable = true)
    ))
    val stateNormalDf = spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointDir)
      .option(StateSourceOptions.STATE_VAR_NAME, "countState")
      .load()
      .selectExpr("partition_id", "key", "value")
    // Validate countState data using compareNormalAndBytesData
    compareNormalAndBytesData(
      stateNormalDf.collect(),
      allBytesData,
      "countState",
      groupByKeySchema,
      stateValueSchema)

    val keyToTimestampSchema = StructType(Array(
      StructField("key", groupByKeySchema),
      StructField("expiryTimestampMs", LongType, nullable = false)
    ))
    val dummySchema = StructType(Array(StructField("__dummy__", NullType)))

    // Read timer DataFrame ONCE and reuse for both comparisons
    val timerBaseDf = spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointDir)
      .option(StateSourceOptions.READ_REGISTERED_TIMERS, true)
      .load()

    val keyToTimestampNormalDf = timerBaseDf.selectExpr(
      "partition_id",
      "STRUCT(key AS groupingKey, expiration_timestamp_ms AS key)",
      "NULL AS value")
    compareNormalAndBytesData(
      keyToTimestampNormalDf.collect(),
      allBytesData,
      s"$$${timerPrefix}Timers_keyToTimestamp",
      keyToTimestampSchema,
      dummySchema)

    val timestampToKeySchema = StructType(Array(
      StructField("expiryTimestampMs", LongType, nullable = false),
      StructField("key", groupByKeySchema)
    ))
    val timestampToKeyNormalDf = timerBaseDf.selectExpr(
      "partition_id",
      "STRUCT(expiration_timestamp_ms AS key, key AS groupingKey)",
      "NULL AS value")
    compareNormalAndBytesData(
      timestampToKeyNormalDf.collect(),
      allBytesData,
      s"$$${timerPrefix}Timers_timestampToKey",
      timestampToKeySchema,
      dummySchema)
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
          StructField("fruit", StringType, nullable = true)
        ))
        // State version 1 includes key columns in the value
        val valueSchema = StructType(Array(
          StructField("groupKey", IntegerType, nullable = false),
          StructField("fruit", StringType, nullable = true),
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
          StructField("fruit", StringType, nullable = true)
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
          StructField("col1", StringType, nullable = true)
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
          StructField("_1", StringType, nullable = true)
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
          StructField("sessionId", StringType, nullable = false),
          StructField("sessionStartTime",
            org.apache.spark.sql.types.TimestampType, nullable = false)
        ))
        val valueSchema = StructType(Array(
          StructField("session_window", org.apache.spark.sql.types.StructType(Array(
            StructField("start", org.apache.spark.sql.types.TimestampType),
            StructField("end", org.apache.spark.sql.types.TimestampType)
          )), nullable = false),
          StructField("sessionId", StringType, nullable = false),
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
            StructField("value", StringType, nullable = true)
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
            StructField("value", StringType, nullable = true)
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

    def testStreamStreamJoinV2(stateVersion: Int): Unit = {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> stateVersion.toString) {
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
      testStreamStreamJoinV2(1)
    }

    testWithChangelogConfig("stream-stream join, state ver 2") {
      testStreamStreamJoinV2(2)
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
            getBytesReadDf(tempDir.getAbsolutePath).collect()
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

  test("SPARK-54419: transformWithState with multiple column families") {
    withTempDir { tempDir =>
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key ->
          TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new MultiStateVarProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, "a", "b", "a"),
          CheckNewAnswer(("a", "2"), ("b", "1")),
          AddData(inputData, "b", "c"),
          CheckNewAnswer(("b", "2"), ("c", "1")),
          StopStream
        )

        // Read all column families using internalOnlyReadAllColumnFamilies
        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)
        validateBytesReadDfSchema(bytesDf)
        val allBytesData = bytesDf.collect()

        val columnFamilies = allBytesData.map(_.getString(3)).distinct.sorted

        // Verify countState column family exists
        assert(columnFamilies.toSet ==
          Set("countState", "itemsList", "$rowCounter_itemsList", "itemsMap"))

        // Define schemas for each column family based on provided schema info
        val groupByKeySchema = StructType(Array(
          StructField("value", StringType, nullable = true)
        ))
        val countStateValueSchema = StructType(Array(
          StructField("value", LongType, nullable = false)
        ))
        val itemsListValueSchema = StructType(Array(
          StructField("value", StringType, nullable = true)
        ))
        val rowCounterValueSchema = StructType(Array(
          StructField("count", LongType, nullable = true)
        ))
        val itemsMapKeySchema = StructType(Array(
          StructField("key", groupByKeySchema),
          StructField("user_map_key", groupByKeySchema, nullable = true)
        ))
        val itemsMapValueSchema = StructType(Array(
          StructField("user_map_value", IntegerType, nullable = true)
        ))

        // Validate countState
        readAndValidateStateVar(
          tempDir.getAbsolutePath, allBytesData,
          stateVarName = "countState", groupByKeySchema, countStateValueSchema)

        // Validate itemsList
        readAndValidateStateVar(
          tempDir.getAbsolutePath, allBytesData,
          stateVarName = "itemsList", groupByKeySchema, itemsListValueSchema,
          extraOptions = Map(StateSourceOptions.FLATTEN_COLLECTION_TYPES -> "true"),
          selectExprs = Seq("partition_id", "key", "list_element"))

        // Validate $rowCounter_itemsList - intentionally reuses countState's data
        val countStateNormalDf = getNormalReadDf(tempDir.getAbsolutePath, Option("countState"))
        compareNormalAndBytesData(
          countStateNormalDf.collect(),
          allBytesData,
          "$rowCounter_itemsList",
          groupByKeySchema,
          rowCounterValueSchema)

        // Validate itemsMap
        readAndValidateStateVar(
          tempDir.getAbsolutePath, allBytesData,
          stateVarName = "itemsMap", itemsMapKeySchema, itemsMapValueSchema,
          selectExprs = Seq("partition_id", "STRUCT(key, user_map_key) AS KEY",
            "user_map_value AS value"))
      }
    }
  }

  test("SPARK-54419: read all column families with event time timers") {
    withTempDir { tempDir =>
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val inputData = MemoryStream[(String, Long)]
        val result = inputData.toDS()
          .select(col("_1").as("key"), timestamp_seconds(col("_2")).as("eventTime"))
          .withWatermark("eventTime", "10 seconds")
          .as[(String, Timestamp)]
          .groupByKey(_._1)
          .transformWithState(
            new EventTimeTimerProcessor(),
            TimeMode.EventTime(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, ("a", 1L), ("b", 2L), ("c", 3L)),
          CheckLastBatch(("a", "1"), ("b", "1"), ("c", "1")),
          StopStream
        )

        validateTimerColumnFamilies(tempDir.getAbsolutePath, "event")
      }
    }
  }

  test("SPARK-54419: read all column families with processing time timers") {
    withTempDir { tempDir =>
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val clock = new StreamManualClock
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorWithProcTimeTimer(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock),
          AddData(inputData, "a"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", "1")),
          StopStream
        )

        validateTimerColumnFamilies(tempDir.getAbsolutePath, "proc")
      }
    }
  }

  test("SPARK-54419: transformWithState with list state and TTL") {
    withTempDir { tempDir =>
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key ->
          TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
        val clock = new StreamManualClock
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new ListStateTTLProcessor(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock),
          AddData(inputData, "a", "b", "a"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", "2"), ("b", "1")),
          StopStream
        )

        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)
        validateBytesReadDfSchema(bytesDf)

        val allBytesData = bytesDf.collect()
        val columnFamilies = allBytesData.map(_.getString(3)).distinct.sorted

        assert(columnFamilies.toSet ==
          Set("listState", "$ttl_listState", "$min_listState", "$count_listState"))

        // Define schemas for list state with TTL column families
        val groupByKeySchema = StructType(Array(
          StructField("value", StringType, nullable = true)
        ))
        val listStateValueSchema = StructType(Array(
          StructField("value", StructType(Array(
            StructField("value", StringType, nullable = true)
          )), nullable = false),
          StructField("ttlExpirationMs", LongType, nullable = false)
        ))

        val listStateNormalDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "listState")
          .option(StateSourceOptions.FLATTEN_COLLECTION_TYPES, "true")
          .load()
          .selectExpr("partition_id", "key", "list_element")

        compareNormalAndBytesData(
          listStateNormalDf.collect(),
          allBytesData,
          "listState",
          groupByKeySchema,
          listStateValueSchema)

        // Validate that TTL-related column families have the expected number of entries
        val ttlIndexRows = allBytesData.filter(_.getString(3) == "$ttl_listState")
        val minExpiryRows = allBytesData.filter(_.getString(3) == "$min_listState")
        val countIndexRows = allBytesData.filter(_.getString(3) == "$count_listState")

        // We have 2 grouping keys (a, b), so each secondary index should have entries
        // TTL index has one entry per unique (expirationMs, groupingKey) pair
        // Min expiry and count indexes have one entry per grouping key
        assert(minExpiryRows.length == 2,
          s"Expected 2 min expiry entries (one per key), got ${minExpiryRows.length}")
        assert(countIndexRows.length == 2,
          s"Expected 2 count index entries (one per key), got ${countIndexRows.length}")
        // TTL index entries depend on batching - we processed 2 batches with different timestamps
        assert(ttlIndexRows.length >= 2,
          s"Expected at least 2 TTL index entries, got ${ttlIndexRows.length}")
      }
    }
  }

  def testStreamStreamJoinV3(): Unit = {
    withSQLConf(
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "3"
    ) {
      withTempDir { tempDir =>
        val inputData = MemoryStream[(Int, Long)]
        val query = getStreamStreamJoinQuery(inputData)
        testStream(query)(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
          ProcessAllAvailable(),
          StopStream
        )
        val stateBytesDf = getBytesReadDf(tempDir.getAbsolutePath)
        validateBytesReadDfSchema(stateBytesDf)

        Seq("right-keyToNumValues", "left-keyToNumValues").foreach(colFamilyName => {
          val normalDf = getNormalReadDf(tempDir.getAbsolutePath, Option(colFamilyName))

          val keyToNumValuesKeySchema = StructType(Array(
            StructField("key", IntegerType)
          ))
          val keyToNumValueValueSchema = StructType(Array(
            StructField("value", LongType)
          ))

          compareNormalAndBytesData(
            normalDf.collect(),
            stateBytesDf.collect(),
            colFamilyName,
            keyToNumValuesKeySchema,
            keyToNumValueValueSchema)
        })

        Seq("right-keyWithIndexToValue", "left-keyWithIndexToValue").foreach(colFamilyName => {
          val normalDf = getNormalReadDf(tempDir.getAbsolutePath, Option(colFamilyName))
          val keyToNumValuesKeySchema = StructType(Array(
            StructField("key", IntegerType, nullable = false),
            StructField("index", LongType)
          ))
          val keyToNumValueValueSchema = StructType(Array(
              StructField("value", IntegerType, nullable = false),
              StructField("time", TimestampType, nullable = false),
              StructField("matched", BooleanType)
          ))

          compareNormalAndBytesData(
            normalDf.collect(),
            stateBytesDf.collect(),
            colFamilyName,
            keyToNumValuesKeySchema,
            keyToNumValueValueSchema)
        })
      }
    }
  }

  test("SPARK-54419: stream-stream joinV3") {
    testStreamStreamJoinV3()
  }
}

/**
 * Stateful processor with multiple state variables (ValueState + ListState)
 * for testing multi-column family reading.
 */
class MultiStateVarProcessor extends StatefulProcessor[String, String, (String, String)] {
  @transient private var _countState: ValueState[Long] = _
  @transient private var _itemsList: ListState[String] = _
  @transient private var _itemsMap: MapState[String, SimpleMapValue] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong, TTLConfig.NONE)
    _itemsList = getHandle.getListState[String]("itemsList", Encoders.STRING, TTLConfig.NONE)
    _itemsMap = getHandle.getMapState[String, SimpleMapValue](
      "itemsMap", Encoders.STRING, Encoders.product[SimpleMapValue], TTLConfig.NONE)
  }

  override def handleInputRows(
          key: String,
          inputRows: Iterator[String],
          timerValues: TimerValues): Iterator[(String, String)] = {
    val currentCount = Option(_countState.get()).getOrElse(0L)
    var newCount = currentCount
    inputRows.foreach { item =>
      newCount += 1
      _itemsList.appendValue(item)
      _itemsMap.updateValue(item, SimpleMapValue(newCount.toInt))
    }
    _countState.update(newCount)
    Iterator((key, newCount.toString))
  }
}

class RunningCountStatefulProcessorWithProcTimeTimer
  extends StatefulProcessor[String, String, (String, String)] {
  import implicits._
  @transient protected var _countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState", TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val currCount = Option(_countState.get()).getOrElse(0L)

    getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 5000)

    val count = currCount + 1
    _countState.update(count)
    Iterator((key, count.toString))
  }

  override def handleExpiredTimer(
     key: String,
     timerValues: TimerValues,
     expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    _countState.clear()
    Iterator((key, "-1"))
  }
}

class EventTimeTimerProcessor
  extends StatefulProcessor[String, (String, Timestamp), (String, String)] {
  @transient var _valueState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _valueState = getHandle.getValueState("countState", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[(String, Timestamp)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    var maxTimestamp = 0L
    var rowCount = 0
    rows.foreach { case (_, timestamp) =>
      maxTimestamp = Math.max(maxTimestamp, timestamp.getTime)
      rowCount += 1
    }

    val count = Option(_valueState.get()).getOrElse(0L) + rowCount
    _valueState.update(count)

    // Register an event time timer
    if (maxTimestamp > 0) {
      getHandle.registerTimer(maxTimestamp + 5000)
    }

    Iterator((key, count.toString))
  }
}

/**
 * Stateful processor with ListState and TTL for testing
 * StatePartitionAllColumnFamiliesReader with TTL-enabled list state.
 */
class ListStateTTLProcessor extends StatefulProcessor[String, String, (String, String)] {
  @transient private var _listState: ListState[String] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _listState = getHandle.getListState("listState", Encoders.STRING,
      TTLConfig(Duration.ofMinutes(1)))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    inputRows.foreach { item =>
      _listState.appendValue(item)
    }
    Iterator((key, _listState.get().size.toString))
  }
}
