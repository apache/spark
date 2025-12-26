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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider, StateRepartitionUnsupportedProviderError, StateStore}
import org.apache.spark.sql.functions.{col, count, sum, timestamp_seconds}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{InputEvent, ListStateTTLProcessor, MapInputEvent, MapStateTTLProcessor, OutputMode, RunningCountStatefulProcessorWithProcTimeTimer, TimeMode, Trigger, TTLConfig, ValueStateTTLProcessor}
import org.apache.spark.sql.streaming.util.{StreamManualClock, TTLProcessorUtils}
import org.apache.spark.sql.streaming.util.{EventTimeTimerProcessor, MultiStateVarProcessor, MultiStateVarProcessorTestUtils, TimerTestUtils}
import org.apache.spark.sql.types.{DataType, NullType, StructField, StructType}

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
    spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "2")
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

    val (groupByKeySchema, stateValueSchema) = TimerTestUtils.getCountStateSchemas()
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

    val dummySchema = StructType(Array(StructField("__dummy__", NullType)))
    val (keyToTimestampSchema, timestampToKeySchema) =
      TimerTestUtils.getTimerKeySchemas(groupByKeySchema)

    // Read timer DataFrame ONCE and reuse for both comparisons
    val timerBaseDf = spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointDir)
      .option(StateSourceOptions.READ_REGISTERED_TIMERS, true)
      .load()

    val keyToTimestampNormalDf = timerBaseDf.selectExpr(
      TimerTestUtils.getTimerSelectExpressions(s"$$${timerPrefix}Timers_keyToTimestamp"): _*)
    compareNormalAndBytesData(
      keyToTimestampNormalDf.collect(),
      allBytesData,
      s"$$${timerPrefix}Timers_keyToTimestamp",
      keyToTimestampSchema,
      dummySchema)

    val timestampToKeyNormalDf = timerBaseDf.selectExpr(
      TimerTestUtils.getTimerSelectExpressions(s"$$${timerPrefix}Timers_timestampToKey"): _*)
    compareNormalAndBytesData(
      timestampToKeyNormalDf.collect(),
      allBytesData,
      s"$$${timerPrefix}Timers_timestampToKey",
      timestampToKeySchema,
      dummySchema)
  }

  /**
   * Validates a state store by reading both normal and bytes DataFrames separately.
   * Used for V1/V2 multi-store architecture where each store must be read independently,
   * or for single-store tests reading the default column family.
   *
   * @param tempDir The checkpoint directory
   * @param keySchema The key schema for the state store
   * @param valueSchema The value schema for the state store
   * @param storeName Optional name of the state store to validate. If None, reads default store.
   */
  private def validateStateStore(
      tempDir: String,
      keySchema: StructType,
      valueSchema: StructType,
      storeName: Option[String] = None): Unit = {
    val normalDf = getNormalReadDf(tempDir, storeName)
    val bytesDf = getBytesReadDf(tempDir, storeName)

    validateBytesReadDfSchema(bytesDf)
    compareNormalAndBytesData(
      normalDf.collect(),
      bytesDf.collect(),
      StateStore.DEFAULT_COL_FAMILY_NAME,
      keySchema,
      valueSchema)
  }

  /**
   * Validates a column family using a shared bytes DataFrame.
   * Used for V3 multi-column-family architecture where all column families
   * are read together in a single bytes DataFrame.
   *
   * @param tempDir The checkpoint directory
   * @param colFamilyName The name of the column family to validate
   * @param sharedBytesDf The shared bytes DataFrame containing all column families
   * @param keySchema The key schema for the column family
   * @param valueSchema The value schema for the column family
   */
  private def validateColumnFamily(
      tempDir: String,
      colFamilyName: String,
      sharedBytesDf: DataFrame,
      keySchema: StructType,
      valueSchema: StructType): Unit = {
    val normalDf = getNormalReadDf(tempDir, Option(colFamilyName))

    compareNormalAndBytesData(
      normalDf.collect(),
      sharedBytesDf.collect(),
      colFamilyName,
      keySchema,
      valueSchema)
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

    Seq(1, 2).foreach(version =>
      testWithChangelogConfig(s"SPARK-54388: simple aggregation state ver $version") {
        withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> s"$version") {
          withTempDir { tempDir =>
            runLargeDataStreamingAggregationQuery(tempDir.getAbsolutePath)

            val (keySchema, valueSchema) = SimpleAggregationTestUtils.getSchemas(version)

            validateStateStore(tempDir.getAbsolutePath, keySchema, valueSchema)
          }
        }
    })

    Seq(1, 2).foreach(version =>
      testWithChangelogConfig(s"SPARK-54388: composite key aggregation state ver $version") {
        withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> s"$version") {
          withTempDir { tempDir =>
            runCompositeKeyStreamingAggregationQuery(tempDir.getAbsolutePath)

            val (keySchema, valueSchema) = CompositeKeyAggregationTestUtils.getSchemas(version)

            validateStateStore(tempDir.getAbsolutePath, keySchema, valueSchema)
          }
        }
    })

    testWithChangelogConfig("SPARK-54388: dropDuplicates validation") {
      withTempDir { tempDir =>
        runDropDuplicatesQuery(tempDir.getAbsolutePath)

        val (keySchema, valueSchema) = DropDuplicatesTestUtils.getDropDuplicatesSchemas()

        validateStateStore(tempDir.getAbsolutePath, keySchema, valueSchema)
      }
    }

    testWithChangelogConfig("SPARK-54388: dropDuplicates with column specified") {
      withTempDir { tempDir =>
        runDropDuplicatesQueryWithColumnSpecified(tempDir.getAbsolutePath)

        val (keySchema, valueSchema) = DropDuplicatesTestUtils.getDropDuplicatesWithColumnSchemas()

        validateStateStore(tempDir.getAbsolutePath, keySchema, valueSchema)
      }
    }

    testWithChangelogConfig("SPARK-54388: dropDuplicatesWithinWatermark") {
      withTempDir { tempDir =>
        runDropDuplicatesWithinWatermarkQuery(tempDir.getAbsolutePath)

        val (keySchema, valueSchema) =
          DropDuplicatesTestUtils.getDropDuplicatesWithinWatermarkSchemas()

        validateStateStore(tempDir.getAbsolutePath, keySchema, valueSchema)
      }
    }

    testWithChangelogConfig("SPARK-54388: session window aggregation") {
      withTempDir { tempDir =>
        runSessionWindowAggregationQuery(tempDir.getAbsolutePath)

        val (keySchema, valueSchema) = SessionWindowTestUtils.getSchemas()

        validateStateStore(tempDir.getAbsolutePath, keySchema, valueSchema)
      }
    }

    Seq(1, 2).foreach(version =>
      testWithChangelogConfig(s"SPARK-54388: flatMapGroupsWithState, state ver $version") {
        // Skip this test on big endian platforms and is V1
        assume(version == 2 || ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN))
        withSQLConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> s"$version") {
          withTempDir { tempDir =>
            runFlatMapGroupsWithStateQuery(tempDir.getAbsolutePath)
            val (keySchema, valueSchema) = FlatMapGroupsWithStateTestUtils.getSchemas(version)
            val normalData = getNormalReadDf(tempDir.getAbsolutePath).collect()
            val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)

            validateBytesReadDfSchema(bytesDf)
            compareNormalAndBytesData(
              normalData, bytesDf.collect(), "default", keySchema, valueSchema)
          }
        }
      }
    )

    Seq(1, 2).foreach(version =>
      testWithChangelogConfig(s"stream-stream join, state ver $version") {
        withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString) {
          withTempDir { tempDir =>
            runStreamStreamJoinQuery(tempDir.getAbsolutePath)

            // Validate keyToNumValues stores
            val (keyToNumValuesKeySchema, keyToNumValueValueSchema) =
              StreamStreamJoinTestUtils.getKeyToNumValuesSchemas()
            StreamStreamJoinTestUtils.KEY_TO_NUM_VALUES_ALL.foreach { storeName =>
              validateStateStore(
                tempDir.getAbsolutePath,
                keyToNumValuesKeySchema,
                keyToNumValueValueSchema,
                Some(storeName))
            }

            // Validate keyWithIndexToValue stores
            val (keyWithIndexKeySchema, keyWithIndexValueSchema) =
              StreamStreamJoinTestUtils.getKeyWithIndexToValueSchemas(version)
            StreamStreamJoinTestUtils.KEY_WITH_INDEX_ALL.foreach { storeName =>
              validateStateStore(
                tempDir.getAbsolutePath,
                keyWithIndexKeySchema,
                keyWithIndexValueSchema,
                Some(storeName))
            }
          }
        }
    })

    testWithChangelogConfig("SPARK-54419: transformWithState with multiple column families") {
      withTempDir { tempDir =>
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
        assert(columnFamilies.toSet == MultiStateVarProcessorTestUtils.ALL_COLUMN_FAMILIES)

        // Define schemas for each column family.
        // count_state, items_list and row_counter all share the same key schema
        val schemas = MultiStateVarProcessorTestUtils.getSchemas()
        val (keySchema, countStateValueSchema, _) =
          schemas(MultiStateVarProcessorTestUtils.COUNT_STATE)
        val (_, itemsListValueSchema, _) = schemas(MultiStateVarProcessorTestUtils.ITEMS_LIST)
        val (_, rowCounterValueSchema, _) = schemas(MultiStateVarProcessorTestUtils.ROW_COUNTER)
        val (mapKeySchema, mapValueSchema, _) = schemas(MultiStateVarProcessorTestUtils.ITEMS_MAP)

        // Validate countState
        readAndValidateStateVar(
          tempDir.getAbsolutePath, allBytesData,
          MultiStateVarProcessorTestUtils.COUNT_STATE, keySchema, countStateValueSchema)

        // Validate itemsList
        readAndValidateStateVar(
          tempDir.getAbsolutePath, allBytesData,
          MultiStateVarProcessorTestUtils.ITEMS_LIST, keySchema, itemsListValueSchema,
          extraOptions = Map(StateSourceOptions.FLATTEN_COLLECTION_TYPES -> "true"),
          selectExprs = MultiStateVarProcessorTestUtils.getSelectExpressions(
            MultiStateVarProcessorTestUtils.ITEMS_LIST))

        // Validate $rowCounter_itemsList
        readAndValidateStateVar(
          tempDir.getAbsolutePath, allBytesData,
          MultiStateVarProcessorTestUtils.ROW_COUNTER, keySchema, rowCounterValueSchema)

        // Validate itemsMap
        readAndValidateStateVar(
          tempDir.getAbsolutePath, allBytesData,
          MultiStateVarProcessorTestUtils.ITEMS_MAP, mapKeySchema, mapValueSchema,
          selectExprs = MultiStateVarProcessorTestUtils.getSelectExpressions(
            MultiStateVarProcessorTestUtils.ITEMS_MAP))
      }
    }

    testWithChangelogConfig("SPARK-54419: read all column families with event time timers") {
      withTempDir { tempDir =>
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

    testWithChangelogConfig("SPARK-54419: read all column families with processing time timers") {
      withTempDir { tempDir =>
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

    testWithChangelogConfig("SPARK-54419: transformWithState with list state and TTL") {
      withTempDir { tempDir =>
        val clock = new StreamManualClock
        val inputData = MemoryStream[InputEvent]
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        val result = inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new ListStateTTLProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock),
          AddData(inputData, InputEvent("k1", "put", 1)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          StopStream
        )

        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)
        validateBytesReadDfSchema(bytesDf)

        val allBytesData = bytesDf.collect()
        val columnFamilies = allBytesData.map(_.getString(3)).distinct.sorted

        assert(columnFamilies.toSet == TTLProcessorUtils.LIST_STATE_ALL)

        // Define schemas for list state with TTL column families
        val schemas = TTLProcessorUtils.getListStateTTLSchemas()
        val (groupByKeySchema, listStateValueSchema) = schemas(TTLProcessorUtils.LIST_STATE)

        val listStateNormalDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "listState")
          .option(StateSourceOptions.FLATTEN_COLLECTION_TYPES, "true")
          .load()
          .selectExpr(TTLProcessorUtils.getTTLSelectExpressions(TTLProcessorUtils.LIST_STATE): _*)

        compareNormalAndBytesData(
          listStateNormalDf.collect(),
          allBytesData,
          TTLProcessorUtils.LIST_STATE,
          groupByKeySchema,
          listStateValueSchema)
        val (ttlIndexKeySchema, ttlValueSchema) = schemas(TTLProcessorUtils.LIST_STATE_TTL_INDEX)
        val (_, minExpiryValueSchema) = schemas(TTLProcessorUtils.LIST_STATE_MIN)
        val (_, countValueSchema) = schemas(TTLProcessorUtils.LIST_STATE_COUNT)
        val columnFamilyAndKeyValueSchema = Seq(
          (TTLProcessorUtils.LIST_STATE_TTL_INDEX, ttlIndexKeySchema, ttlValueSchema),
          (TTLProcessorUtils.LIST_STATE_MIN, groupByKeySchema, minExpiryValueSchema),
          (TTLProcessorUtils.LIST_STATE_COUNT, groupByKeySchema, countValueSchema)
        )
        columnFamilyAndKeyValueSchema.foreach(pair => {
          validateColumnFamily(tempDir.getAbsolutePath, pair._1, bytesDf, pair._2, pair._3)
        })
      }
    }

    testWithChangelogConfig("SPARK-54419: transformWithState with map state and TTL") {
      withTempDir { tempDir =>
        val clock = new StreamManualClock
        val inputData = MemoryStream[MapInputEvent]
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        val result = inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new MapStateTTLProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result)(
          StartStream(checkpointLocation = tempDir.getAbsolutePath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock),
          AddData(inputData, MapInputEvent("a", "key1", "put", 1)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          StopStream
        )

        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)
        validateBytesReadDfSchema(bytesDf)

        val allBytesData = bytesDf.collect()
        val columnFamilies = allBytesData.map(_.getString(3)).distinct.sorted

        // Map state with TTL should have: mapState (main) and $ttl_mapState (TTL index)
        assert(columnFamilies.toSet == TTLProcessorUtils.MAP_STATE_ALL)

        // Define schemas for map state with TTL column families
        val schemas = TTLProcessorUtils.getMapStateTTLSchemas()
        val (compositeKeySchema, mapStateValueSchema) = schemas(TTLProcessorUtils.MAP_STATE)
        val (ttlIndexKeySchema, dummyValueSchema) = schemas(TTLProcessorUtils.MAP_STATE_TTL_INDEX)

        readAndValidateStateVar(
          tempDir.getAbsolutePath, allBytesData,
          stateVarName = TTLProcessorUtils.MAP_STATE, compositeKeySchema, mapStateValueSchema,
          selectExprs = TTLProcessorUtils.getTTLSelectExpressions(TTLProcessorUtils.MAP_STATE))

        // Validate $ttl_mapState column family
        readAndValidateStateVar(
          tempDir.getAbsolutePath, allBytesData,
          stateVarName = TTLProcessorUtils.MAP_STATE_TTL_INDEX, ttlIndexKeySchema, dummyValueSchema)
      }
    }

    testWithChangelogConfig("SPARK-54419: transformWithState with value state and TTL") {
      withTempDir { tempDir =>
        val clock = new StreamManualClock
        val inputData = MemoryStream[InputEvent]
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        val result = inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new ValueStateTTLProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result)(
          StartStream(checkpointLocation = tempDir.getAbsolutePath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock),
          AddData(inputData, InputEvent("k1", "put", 1)),
          AddData(inputData, InputEvent("k2", "put", 2)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          StopStream
        )

        val bytesDf = getBytesReadDf(tempDir.getAbsolutePath)
        validateBytesReadDfSchema(bytesDf)

        val allBytesData = bytesDf.collect()
        val columnFamilies = allBytesData.map(_.getString(3)).distinct.sorted

        // Value state with TTL should have: valueState (main) and $ttl_valueState (TTL index)
        assert(columnFamilies.toSet == TTLProcessorUtils.VALUE_STATE_ALL)

        // Define schemas for value state with TTL column families
        val schemas = TTLProcessorUtils.getValueStateTTLSchemas()
        val (groupByKeySchema, valueStateValueSchema) = schemas(TTLProcessorUtils.VALUE_STATE)
        val (ttlIndexKeySchema, dummyValueSchema) = schemas(TTLProcessorUtils.VALUE_STATE_TTL_INDEX)

        val valueStateNormalDf = getNormalReadDf(tempDir.getAbsolutePath,
          Option(TTLProcessorUtils.VALUE_STATE))

        compareNormalAndBytesData(
          valueStateNormalDf.collect(),
          allBytesData,
          TTLProcessorUtils.VALUE_STATE,
          groupByKeySchema,
          valueStateValueSchema)

        // Validate $ttl_valueState column family
        val ttlValueStateNormalDf = getNormalReadDf(
          tempDir.getAbsolutePath, Option(TTLProcessorUtils.VALUE_STATE_TTL_INDEX))
        compareNormalAndBytesData(
          ttlValueStateNormalDf.collect(),
          allBytesData,
          TTLProcessorUtils.VALUE_STATE_TTL_INDEX,
          ttlIndexKeySchema,
          dummyValueSchema)
      }
    }

    testWithChangelogConfig("SPARK-54419: stream-stream joinV3") {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "3"
      ) {
        withTempDir { tempDir =>
          runStreamStreamJoinQuery(tempDir.getAbsolutePath)
          val stateBytesDf = getBytesReadDf(tempDir.getAbsolutePath)
          validateBytesReadDfSchema(stateBytesDf)

          // Validate keyToNumValues column families
          val (keyToNumValuesKeySchema, keyToNumValueValueSchema) =
            StreamStreamJoinTestUtils.getKeyToNumValuesSchemas()
          StreamStreamJoinTestUtils.KEY_TO_NUM_VALUES_ALL.foreach { colFamilyName =>
            validateColumnFamily(
              tempDir.getAbsolutePath,
              colFamilyName,
              stateBytesDf,
              keyToNumValuesKeySchema,
              keyToNumValueValueSchema)
          }

          // Validate keyWithIndexToValue column families (V3 always has matched field)
          val (keyWithIndexKeySchema, keyWithIndexValueSchema) =
            StreamStreamJoinTestUtils.getKeyWithIndexToValueSchemas(stateVersion = 3)
          StreamStreamJoinTestUtils.KEY_WITH_INDEX_ALL.foreach { colFamilyName =>
            validateColumnFamily(
              tempDir.getAbsolutePath,
              colFamilyName,
              stateBytesDf,
              keyWithIndexKeySchema,
              keyWithIndexValueSchema)
          }
        }
      }
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
}
