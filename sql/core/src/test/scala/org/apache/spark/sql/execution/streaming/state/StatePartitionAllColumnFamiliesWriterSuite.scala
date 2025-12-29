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
package org.apache.spark.sql.execution.streaming.state

import java.io.File
import java.sql.Timestamp
import java.time.Duration

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.state.{CompositeKeyAggregationTestUtils, DropDuplicatesTestUtils, FlatMapGroupsWithStateTestUtils, SessionWindowTestUtils, SimpleAggregationTestUtils, StateDataSourceTestBase, StateSourceOptions, StreamStreamJoinTestUtils}
import org.apache.spark.sql.execution.streaming.operators.stateful.StatefulOperatorsUtils
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.timers.TimerStateUtils
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryCheckpointMetadata}
import org.apache.spark.sql.execution.streaming.utils.StreamingUtils
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{InputEvent, ListStateTTLProcessor, MapInputEvent, MapStateTTLProcessor, OutputMode, RunningCountStatefulProcessorWithProcTimeTimer, TimeMode, Trigger, TTLConfig, ValueStateTTLProcessor}
import org.apache.spark.sql.streaming.util.{StreamManualClock, TTLProcessorUtils}
import org.apache.spark.sql.streaming.util.{EventTimeTimerProcessor, MultiStateVarProcessor, MultiStateVarProcessorTestUtils, TimerTestUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Test suite for StatePartitionAllColumnFamiliesWriter.
 * Tests the writer's ability to correctly write raw bytes read from
 * StatePartitionAllColumnFamiliesReader to a state store without loading previous versions.
 */
class StatePartitionAllColumnFamiliesWriterSuite extends StateDataSourceTestBase {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      classOf[RocksDBStateStoreProvider].getName)
    spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "2")
  }

  /**
   * Helper method to create a StateSchemaProvider from column family schema map.
   */
  private def createStateSchemaProvider(
      columnFamilyToSchemaMap: Map[String, StatePartitionWriterColumnFamilyInfo]
  ): StateSchemaProvider = {
    val testSchemaProvider = new TestStateSchemaProvider()
    columnFamilyToSchemaMap.foreach { case (cfName, cfInfo) =>
      testSchemaProvider.captureSchema(
        colFamilyName = cfName,
        keySchema = cfInfo.schema.keySchema,
        valueSchema = cfInfo.schema.valueSchema,
        keySchemaId = cfInfo.schema.keySchemaId,
        valueSchemaId = cfInfo.schema.valueSchemaId
      )
    }
    testSchemaProvider
  }

  /**
   * Common helper method to perform round-trip test: read state bytes from source,
   * write to target, and verify target matches source.
   *
   * @param sourceDir Source checkpoint directory
   * @param targetDir Target checkpoint directory
   * @param columnFamilyToSchemaMap Map of column family names to their schemas
   * @param storeName Optional store name (for stream-stream join which has multiple stores)
   * @param columnFamilyToSelectExprs Map of column family names to custom selectExprs
   * @param columnFamilyToStateSourceOptions Map of column family names to state source options
   */
  private def performRoundTripTest(
      sourceDir: String,
      targetDir: String,
      columnFamilyToSchemaMap: Map[String, StatePartitionWriterColumnFamilyInfo],
      storeName: Option[String] = None,
      columnFamilyToSelectExprs: Map[String, Seq[String]] = Map.empty,
      columnFamilyToStateSourceOptions: Map[String, Map[String, String]] = Map.empty,
      operatorName: String): Unit = {

    val columnFamiliesToValidate: Seq[String] = if (columnFamilyToSchemaMap.size > 1) {
      columnFamilyToSchemaMap.keys.toSeq
    } else {
      Seq(StateStore.DEFAULT_COL_FAMILY_NAME)
    }

    // Step 1: Read from source using AllColumnFamiliesReader (raw bytes)
    val sourceBytesReader = spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, sourceDir)
      .option(StateSourceOptions.INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES, "true")
    val sourceBytesData = (storeName match {
      case Some(name) => sourceBytesReader.option(StateSourceOptions.STORE_NAME, name)
      case None => sourceBytesReader
    }).load()

    // Verify schema of raw bytes
    val schema = sourceBytesData.schema
    assert(schema.fieldNames === Array(
      "partition_key", "key_bytes", "value_bytes", "column_family_name"))

    // Step 2: Write raw bytes to target checkpoint location
    val hadoopConf = spark.sessionState.newHadoopConf()
    val targetCpLocation = StreamingUtils.resolvedCheckpointLocation(
      hadoopConf, targetDir)
    val targetCheckpointMetadata = new StreamingQueryCheckpointMetadata(
      spark, targetCpLocation)
    // increase offsetCheckpoint
    val lastBatch = targetCheckpointMetadata.commitLog.getLatestBatchId().get
    val targetOffsetSeq = targetCheckpointMetadata.offsetLog.get(lastBatch).get
    val currentBatchId = lastBatch + 1
    targetCheckpointMetadata.offsetLog.add(currentBatchId, targetOffsetSeq)

    val storeConf: StateStoreConf = StateStoreConf(spark.sessionState.conf)
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)

    // Create StateSchemaProvider if needed (for Avro encoding)
    val stateSchemaProvider = if (storeConf.stateStoreEncodingFormat == "avro") {
      Some(createStateSchemaProvider(columnFamilyToSchemaMap))
    } else {
      None
    }
    val baseConfs: Map[String, String] = spark.sessionState.conf.getAllConfs
    val putPartitionFunc: Iterator[InternalRow] => Unit = partition => {
      val newConf = new SQLConf
      baseConfs.foreach { case (k, v) =>
        newConf.setConfString(k, v)
      }
      val allCFWriter = new StatePartitionAllColumnFamiliesWriter(
        storeConf,
        serializableHadoopConf.value,
        TaskContext.getPartitionId(),
        targetCpLocation,
        0,
        storeName.getOrElse(StateStoreId.DEFAULT_STORE_NAME),
        currentBatchId,
        columnFamilyToSchemaMap,
        operatorName,
        stateSchemaProvider,
        newConf
      )
      allCFWriter.write(partition)
    }
    sourceBytesData.queryExecution.toRdd.foreachPartition(putPartitionFunc)

    // Commit to commitLog
    val latestCommit = targetCheckpointMetadata.commitLog.get(lastBatch).get
    targetCheckpointMetadata.commitLog.add(currentBatchId, latestCommit)
    val versionToCheck = currentBatchId + 1
    val storeNamePath = s"state/0/0${storeName.fold("")("/" + _)}"
    assert(!checkpointFileExists(new File(targetDir, storeNamePath), versionToCheck, ".changelog"))
    assert(checkpointFileExists(new File(targetDir, storeNamePath), versionToCheck, ".zip"))

    // Step 3: Validate by reading from both source and target using normal reader"
    // Default selectExprs for most column families
    val defaultSelectExprs = Seq("key", "value", "partition_id")

    columnFamiliesToValidate
      // filtering out "default" for TWS operator because it doesn't contain any data
      .filter(cfName => !(cfName == StateStore.DEFAULT_COL_FAMILY_NAME &&
        StatefulOperatorsUtils.TRANSFORM_WITH_STATE_OP_NAMES.contains(operatorName)
      ))
      .foreach { cfName =>
        val selectExprs = columnFamilyToSelectExprs.getOrElse(cfName, defaultSelectExprs)
        val readerOptions = columnFamilyToStateSourceOptions.getOrElse(cfName, Map.empty)

        def readNormalData(dir: String): Array[Row] = {
          var reader = spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, dir)
            .option(StateSourceOptions.STORE_NAME, storeName.orNull)
          readerOptions.foreach { case (k, v) => reader = reader.option(k, v) }
          reader.load()
            .selectExpr(selectExprs: _*)
            .collect()
        }

        val sourceNormalData = readNormalData(sourceDir)
        val targetNormalData = readNormalData(targetDir)

        validateDataMatches(sourceNormalData, targetNormalData)
      }
  }

  /**
   * Helper method to validate that source and target data match.
   */
  private def validateDataMatches(
      sourceNormalData: Array[Row],
      targetNormalData: Array[Row]): Unit = {
    assert(sourceNormalData.length == targetNormalData.length,
      s"Row count mismatch: source=${sourceNormalData.length}, " +
        s"target=${targetNormalData.length}")

    // Sort and compare row by row
    val sourceSorted = sourceNormalData.sortBy(_.toString)
    val targetSorted = targetNormalData.sortBy(_.toString)

    sourceSorted.zip(targetSorted).zipWithIndex.foreach {
      case ((sourceRow, targetRow), idx) =>
        assert(sourceRow == targetRow,
          s"Row mismatch at index $idx:\n" +
            s"  Source: $sourceRow\n" +
            s"  Target: $targetRow")
    }
  }

    /**
     * Checks if a changelog file for the specified version exists in the given directory.
     * A changelog file has the suffix ".changelog".
     *
     * @param dir Directory to search for changelog files
     * @param version The version to check for existence
     * @param suffix Either 'zip' or 'changelog'
     * @return true if a changelog file with the given version exists, false otherwise
     */
    private def checkpointFileExists(dir: File, version: Long, suffix: String): Boolean = {
      Option(dir.listFiles)
        .getOrElse(Array.empty)
        .map { file =>
          file
        }
        .filter { file =>
          file.getName.endsWith(suffix) && !file.getName.startsWith(".")
        }
        .exists { file =>
          val nameWithoutSuffix = file.getName.stripSuffix(suffix)
          val parts = nameWithoutSuffix.split("_")
          parts.headOption match {
            case Some(ver) if ver.forall(_.isDigit) => ver.toLong == version
            case _ => false
          }
        }
    }

  private def createColFamilyInfo(
       keySchema: StructType,
       valueSchema: StructType,
       keyStateEncoderSpec: KeyStateEncoderSpec,
       colFamilyName: String,
       useMultipleValuePerKey: Boolean = false): StatePartitionWriterColumnFamilyInfo = {
    StatePartitionWriterColumnFamilyInfo(
      schema = StateStoreColFamilySchema(
        colFamilyName,
        keySchemaId = 0,
        keySchema,
        valueSchemaId = 0,
        valueSchema,
        keyStateEncoderSpec = Some(keyStateEncoderSpec)
      ),
      useMultipleValuePerKey)
  }

  /**
   * Helper method to create a single-entry column family schema map.
   * This simplifies the common case where only the default column family is used.
   */
  private def createSingleColumnFamilySchemaMap(
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME
  ): Map[String, StatePartitionWriterColumnFamilyInfo] = {
    Map(colFamilyName -> createColFamilyInfo(keySchema, valueSchema,
      keyStateEncoderSpec, colFamilyName))
  }

  /**
   * Helper method to test SPARK-54420 read and write with different state format versions
   * for simple aggregation (single grouping key).
   * @param stateVersion The state format version (1 or 2)
   */
  private def testRoundTripForAggrStateVersion(stateVersion: Int): Unit = {
    withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> stateVersion.toString) {
      withTempDir { sourceDir =>
        withTempDir { targetDir =>
          // Step 1: Create state by running a streaming aggregation
          runLargeDataStreamingAggregationQuery(sourceDir.getAbsolutePath)
          val inputData: MemoryStream[Int] = MemoryStream[Int]
          val aggregated = getLargeDataStreamingAggregationQuery(inputData)

          // add dummy data to target source to test writer won't load previous store
          testStream(aggregated, OutputMode.Update)(
            StartStream(checkpointLocation = targetDir.getAbsolutePath),
            // batch 0
            AddData(inputData, 0 until 2: _*),
            CheckLastBatch(
              (0, 1, 0, 0, 0), // 0
              (1, 1, 1, 1, 1) // 1
            ),
            // batch 1
            AddData(inputData, 0 until 2: _*),
            CheckLastBatch(
              (0, 2, 0, 0, 0), // 0
              (1, 2, 2, 1, 1) // 1
            )
          )

          // Step 2: Define schemas based on state version
          val metadata = SimpleAggregationTestUtils.getSchemasWithMetadata(stateVersion)

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            createSingleColumnFamilySchemaMap(
              metadata.keySchema, metadata.valueSchema, metadata.encoderSpec),
            operatorName = StatefulOperatorsUtils.STATE_STORE_SAVE_EXEC_OP_NAME
          )
        }
      }
    }
  }

  /**
   * Helper method to test SPARK-54420 read and write with different state format versions
   * for composite key aggregation (multiple grouping keys).
   * @param stateVersion The state format version (1 or 2)
   */
  private def testCompositeKeyRoundTripForStateVersion(stateVersion: Int): Unit = {
    withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> stateVersion.toString) {
      withTempDir { sourceDir =>
        withTempDir { targetDir =>
          // Step 1: Create state by running a composite key streaming aggregation
          runCompositeKeyStreamingAggregationQuery(sourceDir.getAbsolutePath)
          val inputData: MemoryStream[Int] = MemoryStream[Int]
          val aggregated = getCompositeKeyStreamingAggregationQuery(inputData)

          // add dummy data to target source to test writer won't load previous store
          testStream(aggregated, OutputMode.Update)(
            StartStream(checkpointLocation = targetDir.getAbsolutePath),
            // batch 0
            AddData(inputData, 0, 1),
            CheckLastBatch(
              (0, "Apple", 1, 0, 0, 0),
              (1, "Banana", 1, 1, 1, 1)
            )
          )

          // Step 2: Define schemas based on state version for composite key
          val metadata = CompositeKeyAggregationTestUtils.getSchemasWithMetadata(stateVersion)

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            createSingleColumnFamilySchemaMap(
              metadata.keySchema, metadata.valueSchema, metadata.encoderSpec),
            operatorName = StatefulOperatorsUtils.STATE_STORE_SAVE_EXEC_OP_NAME
          )
        }
      }
    }
  }

  private def getJoinV3ColumnSchemaMap(): Map[String, StatePartitionWriterColumnFamilyInfo] = {
    val schemas = StreamStreamJoinTestUtils.getJoinV3ColumnSchemaMapWithMetadata()
    schemas.map { case (cfName, metadata) =>
      cfName -> createColFamilyInfo(
        metadata.keySchema,
        metadata.valueSchema,
        metadata.encoderSpec,
        cfName,
        metadata.useMultipleValuePerKey)
    }
  }
  /**
   * Helper method to test round-trip for stream-stream join with different versions.
   */
  private def testStreamStreamJoinRoundTrip(stateVersion: Int): Unit = {
    withSQLConf(SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> stateVersion.toString) {
      withTempDir { sourceDir =>
        withTempDir { targetDir =>
          // Step 1: Create state by running stream-stream join
          runStreamStreamJoinQuery(sourceDir.getAbsolutePath)

          // Create dummy data in target
          val inputData: MemoryStream[(Int, Long)] = MemoryStream[(Int, Long)]
          val query = getStreamStreamJoinQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = targetDir.getAbsolutePath),
            AddData(inputData, (1, 1L)),
            CheckNewAnswer()
          )

          // Step 2: Test all 4 state stores created by stream-stream join
          // Test keyToNumValues stores (both left and right)
          StreamStreamJoinTestUtils.KEY_TO_NUM_VALUES_ALL.foreach { storeName =>
            val metadata = StreamStreamJoinTestUtils.getKeyToNumValuesSchemasWithMetadata()

            // Perform round-trip test using common helper
            performRoundTripTest(
              sourceDir.getAbsolutePath,
              targetDir.getAbsolutePath,
              createSingleColumnFamilySchemaMap(
                metadata.keySchema, metadata.valueSchema, metadata.encoderSpec),
              storeName = Some(storeName),
              operatorName = StatefulOperatorsUtils.SYMMETRIC_HASH_JOIN_EXEC_OP_NAME
            )
          }

          // Test keyWithIndexToValue stores (both left and right)
          StreamStreamJoinTestUtils.KEY_WITH_INDEX_ALL.foreach { storeName =>
            val metadata =
              StreamStreamJoinTestUtils.getKeyWithIndexToValueSchemasWithMetadata(stateVersion)

            // Perform round-trip test using common helper
            performRoundTripTest(
              sourceDir.getAbsolutePath,
              targetDir.getAbsolutePath,
              createSingleColumnFamilySchemaMap(
                metadata.keySchema, metadata.valueSchema, metadata.encoderSpec),
              storeName = Some(storeName),
              operatorName = StatefulOperatorsUtils.SYMMETRIC_HASH_JOIN_EXEC_OP_NAME
            )
          }
        }
      }
    }
  }

  /**
   * Helper method to test round-trip for flatMapGroupsWithState with different versions.
   */
  private def testFlatMapGroupsWithStateRoundTrip(stateVersion: Int): Unit = {
    // Skip this test on big endian platforms (version 1 only)
    if (stateVersion == 1) {
      assume(java.nio.ByteOrder.nativeOrder().equals(java.nio.ByteOrder.LITTLE_ENDIAN))
    }

    withSQLConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> stateVersion.toString) {
      withTempDir { sourceDir =>
        withTempDir { targetDir =>
          // Step 1: Create state by running flatMapGroupsWithState
          runFlatMapGroupsWithStateQuery(sourceDir.getAbsolutePath)

          // Create dummy data in target
          val clock = new StreamManualClock
          val inputData: MemoryStream[(String, Long)] = MemoryStream[(String, Long)]
          val query = getFlatMapGroupsWithStateQuery(inputData)
          testStream(query, OutputMode.Update)(
            StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
              checkpointLocation = targetDir.getAbsolutePath),
            AddData(inputData, ("a", 1L)),
            AdvanceManualClock(1 * 1000),
            CheckLastBatch(("a", 1, 0, false))
          )

          // Step 2: Define schemas for flatMapGroupsWithState
          val metadata = FlatMapGroupsWithStateTestUtils.getSchemasWithMetadata(stateVersion)

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            createSingleColumnFamilySchemaMap(
              metadata.keySchema, metadata.valueSchema, metadata.encoderSpec),
            operatorName = StatefulOperatorsUtils.FLAT_MAP_GROUPS_WITH_STATE_EXEC_OP_NAME
          )
        }
      }
    }
  }


  /**
   * Helper method to build timer column family schemas and options for
   * RunningCountStatefulProcessorWithProcTimeTimer and EventTimeTimerProcessor
   *
   * @param timeMode Either TimeMode.EventTime() or TimeMode.ProcessingTime()
   * @return A tuple of three elements:
   *         - columnFamilyToSchemaMap: Maps column family names to their schema info
   *         - columnFamilyToSelectExprs: Maps column family names to custom select expressions
   *         - columnFamilyToStateSourceOptions: Maps column family names to state source options
   */
  private def getTimerStateConfigsForCountState(timeMode: TimeMode): (
      Map[String, StatePartitionWriterColumnFamilyInfo],
      Map[String, Seq[String]],
      Map[String, Map[String, String]]) = {

    val schemas = TimerTestUtils.getTimerConfigsForCountState(timeMode)
    val columnFamilyToSchemaMap = schemas.map { case (cfName, metadata) =>
      cfName -> createColFamilyInfo(
        metadata.keySchema,
        metadata.valueSchema,
        metadata.encoderSpec,
        cfName,
        metadata.useMultipleValuePerKey)
    }

    val (keyToTimestampCF, timestampToKeyCF) =
      TimerStateUtils.getTimerStateVarNames(timeMode.toString)

    val columnFamilyToSelectExprs = TimerTestUtils.getTimerColumnFamilyToSelectExprs(timeMode)

    val columnFamilyToStateSourceOptions = schemas.keys.map { cfName =>
        if (cfName == keyToTimestampCF || cfName == timestampToKeyCF) {
          cfName -> Map(StateSourceOptions.READ_REGISTERED_TIMERS -> "true")
        } else {
          cfName -> Map(StateSourceOptions.STATE_VAR_NAME -> cfName)
        }
    }.toMap

    (columnFamilyToSchemaMap, columnFamilyToSelectExprs, columnFamilyToStateSourceOptions)
  }


  // Run all tests with both changelog checkpointing enabled and disabled
  Seq(true, false).foreach { changelogCheckpointingEnabled =>
    val changelogCpTestSuffix = if (changelogCheckpointingEnabled) {
      "with changelog checkpointing"
    } else {
      "without changelog checkpointing"
    }

    def testWithChangelogConfig(testName: String)(testFun: => Unit): Unit = {
      test(s"$testName ($changelogCpTestSuffix)") {
        withSQLConf(
          "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled" ->
            changelogCheckpointingEnabled.toString) {
          testFun
        }
      }
    }

    testWithChangelogConfig("SPARK-54420: aggregation state ver 1") {
      testRoundTripForAggrStateVersion(1)
    }

    testWithChangelogConfig("SPARK-54420: aggregation state ver 2") {
      testRoundTripForAggrStateVersion(2)
    }

    Seq(1, 2).foreach { version =>
      testWithChangelogConfig(s"SPARK-54420: composite key aggregation state ver $version") {
        testCompositeKeyRoundTripForStateVersion(version)
      }
    }

    testWithChangelogConfig("SPARK-54420: dropDuplicatesWithinWatermark") {
      withTempDir { sourceDir =>
        withTempDir { targetDir =>
          // Step 1: Create state by running dropDuplicatesWithinWatermark
          runDropDuplicatesWithinWatermarkQuery(sourceDir.getAbsolutePath)

          // Create dummy data in target
          val inputData: MemoryStream[(String, Int)] = MemoryStream[(String, Int)]
          val deduped = getDropDuplicatesWithinWatermarkQuery(inputData)
          testStream(deduped, OutputMode.Append)(
            StartStream(checkpointLocation = targetDir.getAbsolutePath),
            AddData(inputData, ("a", 1)),
            CheckAnswer(("a", 1))
          )

          // Step 2: Define schemas for dropDuplicatesWithinWatermark
          val metadata =
            DropDuplicatesTestUtils.getDropDuplicatesWithinWatermarkSchemasWithMetadata()

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            createSingleColumnFamilySchemaMap(
              metadata.keySchema, metadata.valueSchema, metadata.encoderSpec),
            operatorName = StatefulOperatorsUtils.DEDUPLICATE_WITHIN_WATERMARK_EXEC_OP_NAME
          )
        }
      }
    }

    testWithChangelogConfig("SPARK-54420: dropDuplicates with column specified") {
      withTempDir { sourceDir =>
        withTempDir { targetDir =>
          // Step 1: Create state by running dropDuplicates with column
          runDropDuplicatesQueryWithColumnSpecified(sourceDir.getAbsolutePath)

          // Create dummy data in target
          val inputData: MemoryStream[(String, Int)] = MemoryStream[(String, Int)]
          val deduped = getDropDuplicatesQueryWithColumnSpecified(inputData)
          testStream(deduped, OutputMode.Append)(
            StartStream(checkpointLocation = targetDir.getAbsolutePath),
            AddData(inputData, ("a", 1)),
            CheckAnswer(("a", 1))
          )

          // Step 2: Define schemas for dropDuplicates with column specified
          val metadata =
            DropDuplicatesTestUtils.getDropDuplicatesWithColumnSchemasWithMetadata()

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            createSingleColumnFamilySchemaMap(
              metadata.keySchema, metadata.valueSchema, metadata.encoderSpec),
            operatorName = StatefulOperatorsUtils.DEDUPLICATE_EXEC_OP_NAME
          )
        }
      }
    }

    testWithChangelogConfig("SPARK-54420: session window aggregation") {
      withTempDir { sourceDir =>
        withTempDir { targetDir =>
          // Step 1: Create state by running session window aggregation
          runSessionWindowAggregationQuery(sourceDir.getAbsolutePath)

          // Create dummy data in target
          val inputData: MemoryStream[(String, Long)] = MemoryStream[(String, Long)]
          val aggregated = getSessionWindowAggregationQuery(inputData)
          testStream(aggregated, OutputMode.Complete())(
            StartStream(checkpointLocation = targetDir.getAbsolutePath),
            AddData(inputData, ("a", 40L)),
            CheckNewAnswer(
              ("a", 40, 50, 10, 1)
            ),
            StopStream
          )

          // Step 2: Define schemas for session window aggregation
          val (keySchema, valueSchema) = SessionWindowTestUtils.getSchemas()
          // Session window aggregation uses prefix key scanning where sessionId is the prefix
          val keyStateEncoderSpec = PrefixKeyScanStateEncoderSpec(keySchema, 1)

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            createSingleColumnFamilySchemaMap(keySchema, valueSchema, keyStateEncoderSpec),
            operatorName = StatefulOperatorsUtils.SESSION_WINDOW_STATE_STORE_SAVE_EXEC_OP_NAME
          )
        }
      }
    }

    testWithChangelogConfig("SPARK-54420: dropDuplicates") {
      withTempDir { sourceDir =>
        withTempDir { targetDir =>

          // Step 1: Create state by running a streaming aggregation
          runDropDuplicatesQuery(sourceDir.getAbsolutePath)
          val inputData: MemoryStream[Int] = MemoryStream[Int]
          val stream = getDropDuplicatesQuery(inputData)
          testStream(stream, OutputMode.Append)(
            StartStream(checkpointLocation = targetDir.getAbsolutePath),
            AddData(inputData, (1 to 5).flatMap(_ => (10 to 15)): _*),
            CheckAnswer(10 to 15: _*),
            assertNumStateRows(total = 6, updated = 6)
          )

          // Step 2: Define schemas for dropDuplicates (state version 2)
          val metadata = DropDuplicatesTestUtils.getDropDuplicatesSchemasWithMetadata()

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            createSingleColumnFamilySchemaMap(
              metadata.keySchema, metadata.valueSchema, metadata.encoderSpec),
            operatorName = StatefulOperatorsUtils.DEDUPLICATE_EXEC_OP_NAME
          )
        }
      }
    }

    Seq(1, 2).foreach { version =>
      testWithChangelogConfig(s"SPARK-54420: flatMapGroupsWithState state ver $version") {
        testFlatMapGroupsWithStateRoundTrip(version)
      }
    }

    Seq(1, 2).foreach { version =>
      testWithChangelogConfig(s"SPARK-54420: stream-stream join state ver $version") {
        testStreamStreamJoinRoundTrip(version)
      }
    }

    testWithChangelogConfig("SPARK-54411: stream-stream join state ver 3") {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "3"
      ) {
        withTempDir { sourceDir =>
          withTempDir { targetDir =>
            val inputData = MemoryStream[(Int, Long)]
            val query = getStreamStreamJoinQuery(inputData)

            def runQuery(checkpointLocation: String, roundsOfData: Int): Unit = {
              val dataActions = (1 to roundsOfData).flatMap { _ =>
                Seq(
                  AddData(inputData, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
                  ProcessAllAvailable()
                )
              }
              testStream(query)(
                (Seq(StartStream(checkpointLocation = checkpointLocation)) ++
                  dataActions ++
                  Seq(StopStream)): _*
              )
            }

            runQuery(sourceDir.getAbsolutePath, roundsOfData = 2)
            runQuery(targetDir.getAbsolutePath, roundsOfData = 1)

            val allColFamilyNames = StreamStreamJoinTestUtils.KEY_TO_NUM_VALUES_ALL ++
              StreamStreamJoinTestUtils.KEY_WITH_INDEX_ALL
            performRoundTripTest(
              sourceDir.getAbsolutePath,
              targetDir.getAbsolutePath,
              getJoinV3ColumnSchemaMap(),
              columnFamilyToStateSourceOptions = allColFamilyNames.map {
                colName => colName -> Map(StateSourceOptions.STORE_NAME -> colName)
              }.toMap,
              operatorName = StatefulOperatorsUtils.SYMMETRIC_HASH_JOIN_EXEC_OP_NAME
            )
          }
        }
      }
    }

    // Run transformWithState tests with different encoding formats
    Seq("unsaferow", "avro").foreach { encodingFormat =>
      def testWithChangelogAndEncodingConfig(testName: String)(testFun: => Unit): Unit = {
        test(s"$testName ($changelogCpTestSuffix, encoding = $encodingFormat)") {
          withSQLConf(
            "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled" ->
              changelogCheckpointingEnabled.toString,
            SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> encodingFormat) {
            testFun
          }
        }
      }

      testWithChangelogAndEncodingConfig(
          "SPARK-54411: transformWithState with multiple column families") {
        withTempDir { sourceDir =>
          withTempDir { targetDir =>
            val inputData = MemoryStream[String]
            val query = inputData.toDS()
              .groupByKey(x => x)
              .transformWithState(new MultiStateVarProcessor(),
                TimeMode.None(),
                OutputMode.Update())
            def runQuery(checkpointLocation: String, roundsOfData: Int): Unit = {
              val dataActions = (1 to roundsOfData).flatMap { _ =>
                Seq(
                  AddData(inputData, "a", "b", "a"),
                  ProcessAllAvailable()
                )
              }
              testStream(query)(
                Seq(StartStream(checkpointLocation = checkpointLocation)) ++
                  dataActions ++
                  Seq(StopStream): _*
              )
            }

            runQuery(sourceDir.getAbsolutePath, 2)
            runQuery(targetDir.getAbsolutePath, 1)

            val schemas = MultiStateVarProcessorTestUtils.getSchemasWithMetadata()
            val columnFamilyToSchemaMap = schemas.map { case (cfName, metadata) =>
              cfName -> createColFamilyInfo(
                metadata.keySchema,
                metadata.valueSchema,
                metadata.encoderSpec,
                cfName,
                metadata.useMultipleValuePerKey)
            }
            val columnFamilyToSelectExprs = MultiStateVarProcessorTestUtils
              .getColumnFamilyToSelectExprs()

            val columnFamilyToStateSourceOptions = schemas.keys.map { cfName =>
              val base = Map(
                StateSourceOptions.STATE_VAR_NAME -> cfName
              )

              val withFlatten =
                if (cfName == MultiStateVarProcessorTestUtils.ITEMS_LIST) {
                  base + (StateSourceOptions.FLATTEN_COLLECTION_TYPES -> "true")
                } else {
                  base
                }

              cfName -> withFlatten
            }.toMap

            performRoundTripTest(
              sourceDir.getAbsolutePath,
              targetDir.getAbsolutePath,
              columnFamilyToSchemaMap,
              columnFamilyToSelectExprs = columnFamilyToSelectExprs,
              columnFamilyToStateSourceOptions = columnFamilyToStateSourceOptions,
              operatorName = StatefulOperatorsUtils.TRANSFORM_WITH_STATE_EXEC_OP_NAME
            )
          }
        }
      }

      testWithChangelogAndEncodingConfig("SPARK-54411: transformWithState with eventTime timers") {
        withTempDir { sourceDir =>
          withTempDir { targetDir =>
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
              StartStream(checkpointLocation = sourceDir.getAbsolutePath),
              AddData(inputData, ("a", 1L), ("b", 2L), ("c", 3L)),
              ProcessAllAvailable(),
              StopStream
            )

            testStream(result, OutputMode.Update())(
              StartStream(checkpointLocation = targetDir.getAbsolutePath),
              AddData(inputData, ("x", 1L)),
              ProcessAllAvailable(),
              StopStream
            )

            val (schemaMap, selectExprs, stateSourceOptions) =
              getTimerStateConfigsForCountState(TimeMode.EventTime())

            performRoundTripTest(
              sourceDir.getAbsolutePath,
              targetDir.getAbsolutePath,
              schemaMap,
              columnFamilyToSelectExprs = selectExprs,
              columnFamilyToStateSourceOptions = stateSourceOptions,
              operatorName = StatefulOperatorsUtils.TRANSFORM_WITH_STATE_EXEC_OP_NAME
            )
          }
        }
      }

      testWithChangelogAndEncodingConfig(
        "SPARK-54411: transformWithState with processing time timers") {
        withTempDir { sourceDir =>
          withTempDir { targetDir =>
            val clock = new StreamManualClock
            val inputData = MemoryStream[String]
            val result = inputData.toDS()
              .groupByKey(x => x)
              .transformWithState(new RunningCountStatefulProcessorWithProcTimeTimer(),
                TimeMode.ProcessingTime(),
                OutputMode.Update())

            testStream(result, OutputMode.Update())(
              StartStream(checkpointLocation = sourceDir.getAbsolutePath,
                trigger = Trigger.ProcessingTime("1 second"),
                triggerClock = clock),
              AddData(inputData, "a"),
              AdvanceManualClock(1 * 1000),
              CheckNewAnswer(("a", "1")),
              StopStream
            )

            val clock2 = new StreamManualClock
            testStream(result, OutputMode.Update())(
              StartStream(checkpointLocation = targetDir.getAbsolutePath,
                trigger = Trigger.ProcessingTime("1 second"),
                triggerClock = clock2),
              AddData(inputData, "x"),
              AdvanceManualClock(1 * 1000),
              CheckNewAnswer(("a", "1"), ("x", "1")),
              StopStream
            )

            val (schemaMap, selectExprs, sourceOptions) =
              getTimerStateConfigsForCountState(TimeMode.ProcessingTime())

            performRoundTripTest(
              sourceDir.getAbsolutePath,
              targetDir.getAbsolutePath,
              schemaMap,
              columnFamilyToSelectExprs = selectExprs,
              columnFamilyToStateSourceOptions = sourceOptions,
              operatorName = StatefulOperatorsUtils.TRANSFORM_WITH_STATE_EXEC_OP_NAME
            )
          }
        }
      }

      testWithChangelogAndEncodingConfig("SPARK-54411: transformWithState with list and TTL") {
        withTempDir { sourceDir =>
          withTempDir { targetDir =>
            val clock = new StreamManualClock
            val inputData = MemoryStream[InputEvent]
            val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
            val result = inputData.toDS()
              .groupByKey(x => x.key)
              .transformWithState(new ListStateTTLProcessor(ttlConfig),
                TimeMode.ProcessingTime(),
                OutputMode.Update())

            testStream(result, OutputMode.Update())(
              StartStream(checkpointLocation = sourceDir.getAbsolutePath,
                trigger = Trigger.ProcessingTime("1 second"),
                triggerClock = clock),
              AddData(inputData, InputEvent("k1", "put", 1)),
              AdvanceManualClock(1 * 1000),
              CheckNewAnswer(),
              StopStream
            )

            val clock2 = new StreamManualClock
            testStream(result, OutputMode.Update())(
              StartStream(checkpointLocation = targetDir.getAbsolutePath,
                trigger = Trigger.ProcessingTime("1 second"),
                triggerClock = clock2),
              AddData(inputData, InputEvent("k1", "append", 1)),
              AdvanceManualClock(1 * 1000),
              CheckNewAnswer(),
              StopStream
            )

            val schemas = TTLProcessorUtils.getListStateTTLSchemasWithMetadata()
            val columnFamilyToSchemaMap = schemas.map { case (cfName, metadata) =>
              cfName -> createColFamilyInfo(
                metadata.keySchema,
                metadata.valueSchema,
                metadata.encoderSpec,
                cfName,
                metadata.useMultipleValuePerKey)
            }

            val columnFamilyToSelectExprs = Map(
              TTLProcessorUtils.LIST_STATE -> TTLProcessorUtils.getTTLSelectExpressions(
                TTLProcessorUtils.LIST_STATE
            ))

            val columnFamilyToStateSourceOptions = schemas.keys.map { cfName =>
              val base = Map(StateSourceOptions.STATE_VAR_NAME -> cfName)

              val withFlatten =
                if (cfName == TTLProcessorUtils.LIST_STATE) {
                  base + (StateSourceOptions.FLATTEN_COLLECTION_TYPES -> "true")
                } else {
                  base
                }

              cfName -> withFlatten
            }.toMap

            performRoundTripTest(
              sourceDir.getAbsolutePath,
              targetDir.getAbsolutePath,
              columnFamilyToSchemaMap,
              columnFamilyToSelectExprs = columnFamilyToSelectExprs,
              columnFamilyToStateSourceOptions = columnFamilyToStateSourceOptions,
              operatorName = StatefulOperatorsUtils.TRANSFORM_WITH_STATE_EXEC_OP_NAME
            )
          }
        }
      }

      testWithChangelogAndEncodingConfig("SPARK-54411: transformWithState with map and TTL") {
        withTempDir { sourceDir =>
          withTempDir { targetDir =>
            val clock = new StreamManualClock
            val inputData = MemoryStream[MapInputEvent]
            val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
            val result = inputData.toDS()
              .groupByKey(x => x.key)
              .transformWithState(new MapStateTTLProcessor(ttlConfig),
                TimeMode.ProcessingTime(),
                OutputMode.Update())

            testStream(result)(
              StartStream(checkpointLocation = sourceDir.getAbsolutePath,
                trigger = Trigger.ProcessingTime("1 second"),
                triggerClock = clock),
              AddData(inputData, MapInputEvent("a", "key1", "put", 1)),
              AdvanceManualClock(1 * 1000),
              CheckNewAnswer(),
              StopStream
            )

            val clock2 = new StreamManualClock
            testStream(result)(
              StartStream(checkpointLocation = targetDir.getAbsolutePath,
                trigger = Trigger.ProcessingTime("1 second"),
                triggerClock = clock2),
              AddData(inputData, MapInputEvent("x", "key1", "put", 1)),
              AdvanceManualClock(1 * 1000),
              CheckNewAnswer(),
              StopStream
            )

            val schemas = TTLProcessorUtils.getMapStateTTLSchemasWithMetadata()
            val columnFamilyToSchemaMap = schemas.map { case (cfName, metadata) =>
              cfName -> createColFamilyInfo(
                metadata.keySchema,
                metadata.valueSchema,
                metadata.encoderSpec,
                cfName,
                metadata.useMultipleValuePerKey)
            }

            val columnFamilyToSelectExprs = Map(
              TTLProcessorUtils.MAP_STATE -> TTLProcessorUtils.getTTLSelectExpressions(
                TTLProcessorUtils.MAP_STATE
            ))

            val columnFamilyToStateSourceOptions = schemas.keys.map { cfName =>
              cfName -> Map(StateSourceOptions.STATE_VAR_NAME -> cfName)
            }.toMap

            performRoundTripTest(
              sourceDir.getAbsolutePath,
              targetDir.getAbsolutePath,
              columnFamilyToSchemaMap,
              columnFamilyToSelectExprs = columnFamilyToSelectExprs,
              columnFamilyToStateSourceOptions = columnFamilyToStateSourceOptions,
              operatorName = StatefulOperatorsUtils.TRANSFORM_WITH_STATE_EXEC_OP_NAME
            )
          }
        }
      }

      testWithChangelogAndEncodingConfig("SPARK-54411: transformWithState with value and TTL") {
        withTempDir { sourceDir =>
          withTempDir { targetDir =>
            val clock = new StreamManualClock
            val inputData = MemoryStream[InputEvent]
            val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
            val result = inputData.toDS()
              .groupByKey(x => x.key)
              .transformWithState(new ValueStateTTLProcessor(ttlConfig),
                TimeMode.ProcessingTime(),
                OutputMode.Update())

            testStream(result)(
              StartStream(checkpointLocation = sourceDir.getAbsolutePath,
                trigger = Trigger.ProcessingTime("1 second"),
                triggerClock = clock),
              AddData(inputData, InputEvent("k1", "put", 1)),
              AddData(inputData, InputEvent("k2", "put", 2)),
              AdvanceManualClock(1 * 1000),
              CheckNewAnswer(),
              StopStream
            )

            val clock2 = new StreamManualClock
            testStream(result)(
              StartStream(checkpointLocation = targetDir.getAbsolutePath,
                trigger = Trigger.ProcessingTime("1 second"),
                triggerClock = clock2),
              AddData(inputData, InputEvent("x", "put", 1)),
              AdvanceManualClock(1 * 1000),
              CheckNewAnswer(),
              StopStream
            )

            val schemas = TTLProcessorUtils.getValueStateTTLSchemasWithMetadata()
            val columnFamilyToSchemaMap = schemas.map { case (cfName, metadata) =>
              cfName -> createColFamilyInfo(
                metadata.keySchema,
                metadata.valueSchema,
                metadata.encoderSpec,
                cfName,
                metadata.useMultipleValuePerKey)
            }

            val columnFamilyToStateSourceOptions = schemas.keys.map { cfName =>
              cfName -> Map(StateSourceOptions.STATE_VAR_NAME -> cfName)
            }.toMap

            performRoundTripTest(
              sourceDir.getAbsolutePath,
              targetDir.getAbsolutePath,
              columnFamilyToSchemaMap,
              columnFamilyToStateSourceOptions = columnFamilyToStateSourceOptions,
              operatorName = StatefulOperatorsUtils.TRANSFORM_WITH_STATE_EXEC_OP_NAME
            )
          }
        }
      }
    } // End of foreach loop for encoding format (transformWithState tests only)
  } // End of foreach loop for changelog checkpointing dimension

  test("SPARK-54411: Non-JoinV3 operator requires default column family in schema") {
    withTempDir { targetDir =>
      // Create a checkpoint with proper structure
      val inputData = MemoryStream[String]
      val query = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new MultiStateVarProcessor(),
          TimeMode.None(),
          OutputMode.Update())
      testStream(query)(
        StartStream(checkpointLocation = targetDir.getAbsolutePath),
        AddData(inputData, "a"),
        ProcessAllAvailable(),
        StopStream
      )

      val hadoopConf = spark.sessionState.newHadoopConf()
      val targetCpLocation = StreamingUtils.resolvedCheckpointLocation(
        hadoopConf, targetDir.getAbsolutePath)
      val targetCheckpointMetadata = new StreamingQueryCheckpointMetadata(
        spark, targetCpLocation)
      val lastBatch = targetCheckpointMetadata.commitLog.getLatestBatchId().get
      val currentBatchId = lastBatch + 1

      val storeConf = StateStoreConf(spark.sessionState.conf)

      // Create column family map WITHOUT default column family
      val schemas = MultiStateVarProcessorTestUtils.getSchemasWithMetadata()
      val columnFamilyToSchemaMap = schemas
        .view
        .filterKeys(_ != StateStore.DEFAULT_COL_FAMILY_NAME)
        .map { case (cfName, metadata) =>
          cfName -> createColFamilyInfo(
            metadata.keySchema,
            metadata.valueSchema,
            metadata.encoderSpec,
            cfName,
            metadata.useMultipleValuePerKey)
        }.toMap

      // This SHOULD throw - non-JoinV3 requires default column family
      val e = intercept[AssertionError] {
        new StatePartitionAllColumnFamiliesWriter(
          storeConf,
          hadoopConf,
          0, // partitionId
          targetCpLocation,
          0, // operatorId
          StateStoreId.DEFAULT_STORE_NAME,
          currentBatchId,
          columnFamilyToSchemaMap,
          StatefulOperatorsUtils.TRANSFORM_WITH_STATE_EXEC_OP_NAME,
          None,
          new SQLConf
        )
      }

      // Verify error message
      assert(e.getMessage.contains(
        "Please provide the schema of 'default' column family in StateStoreColFamilySchema"))
      assert(e.getMessage.contains("transformWithState"))
    }
  }
}
