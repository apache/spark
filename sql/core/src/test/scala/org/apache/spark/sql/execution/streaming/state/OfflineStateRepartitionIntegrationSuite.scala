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

import java.sql.Timestamp
import java.time.Duration

import org.apache.spark.sql.{Dataset, Encoder, Row}
import org.apache.spark.sql.execution.datasources.v2.state.{StateDataSourceTestBase, StateSourceOptions}
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingAggregationStateManager
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.timers.TimerStateUtils
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryCheckpointMetadata}
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{InputEvent, ListStateTTLProcessor, MapInputEvent, MapOutputEvent, MapStateTTLProcessor, OutputEvent, OutputMode, RunningCountStatefulProcessorWithProcTimeTimer, TimeMode, Trigger, TTLConfig, ValueStateTTLProcessor}
import org.apache.spark.sql.streaming.util.{EventTimeTimerProcessor, MultiStateVarProcessor, MultiStateVarProcessorTestUtils, StreamManualClock, TimerTestUtils, TTLProcessorUtils}

/**
 * Integration test suite helper class for OfflineStateRepartitionRunner with stateful operators.
 * Tests state repartitioning (increase and decrease partitions) and validates:
 * 1. State data remains identical after repartitioning
 * 2. Offset and commit logs are updated correctly
 * 3. Query can resume successfully and compute correctly with repartitioned state
 */
abstract class OfflineStateRepartitionIntegrationSuiteBase extends StateDataSourceTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      classOf[RocksDBStateStoreProvider].getName)
    spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
  }

  /**
   * Reads state data for a specific store and column family
   */
  private def readStateData(
      checkpointDir: String,
      batchId: Long,
      storeName: String,
      additionalOptions: Map[String, String],
      selectExprs: Seq[String]): Dataset[Row] = {
    var reader = spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointDir)
      .option(StateSourceOptions.BATCH_ID, batchId)
      .option(StateSourceOptions.STORE_NAME, storeName)

    // Apply additional options dynamically
    additionalOptions.foreach { case (k, v) =>
      reader = reader.option(k, v)
    }

    // Not querying partition id because a key will be in different partitions
    // before and after repartitioning
    val selectExprsWithoutPartitionId = selectExprs.filterNot(_ == "partition_id")
    reader.load()
      .selectExpr(selectExprsWithoutPartitionId: _*)
      .orderBy("key")
  }

  /**
   * Read states for all specified stores and column families.
   * @param storeToColumnFamilyToStateSourceOptions: Map[store -> Map [colFamily -> sourceOptions]]
   * Returns Map[store -> Map[columnFamily -> state rows]]
   */
  private def readStateDataByStoreName(
      checkpointDir: String,
      batchId: Long,
      storeToColumnFamilyToStateSourceOptions: Map[String, Map[String, Map[String, String]]],
      storeToColumnFamilyToSelectExprs: Map[String, Map[String, Seq[String]]] = Map.empty
  ): Map[String, Map[String, Array[Row]]] = {
    storeToColumnFamilyToStateSourceOptions.map { case (storeName, columnFamilyToOptions) =>
      val columnFamilyData = columnFamilyToOptions.map { case (cfName, options) =>
        val selectExprs = storeToColumnFamilyToSelectExprs
          .getOrElse(storeName, Map.empty)
          .getOrElse(cfName, Seq("key", "value"))
        val stateData = readStateData(
          checkpointDir, batchId, storeName, options, selectExprs).collect()
        cfName -> stateData
      }
      storeName -> columnFamilyData
    }
  }

  /**
   * Unified helper to build state source options for transformWithState tests.
   * Handles basic state variables, timer and TTL column families.
   *
   * @param columnFamilyNames List of column family names to configure
   * @param timeMode Optional TimeMode for timer-based tests (adds READ_REGISTERED_TIMERS)
   * @param listStateName Optional list state name for TTL tests (adds FLATTEN_COLLECTION_TYPES)
   * @return Map of column family names to their state source options
   */
  def buildStateSourceOptionsForTWS(
      columnFamilyNames: Seq[String],
      timeMode: Option[TimeMode] = None,
      listStateName: Option[String] = None): Map[String, Map[String, String]] = {
    // Get timer column family names if timeMode is provided
    val (keyToTimestampCF, timestampToKeyCF) = timeMode match {
      case Some(tm) => TimerStateUtils.getTimerStateVarNames(tm.toString)
      case None => (null, null)
    }

    columnFamilyNames.map { cfName =>
      // Determine base options based on column family type
      val options = if (cfName == keyToTimestampCF || cfName == timestampToKeyCF) {
        // Timer column families
        Map(StateSourceOptions.READ_REGISTERED_TIMERS -> "true")
      } else if (cfName == StateStore.DEFAULT_COL_FAMILY_NAME) {
        throw new IllegalArgumentException("TWS operator shouldn't contain DEFAULT column family")
      } else {
        // Regular state variable column families
        val baseOptions = Map(StateSourceOptions.STATE_VAR_NAME -> cfName)
        if (listStateName.contains(cfName)) {
          baseOptions + (StateSourceOptions.FLATTEN_COLLECTION_TYPES -> "true")
        } else {
          baseOptions
        }
      }

      cfName -> options
    }.toMap
  }

  /**
   * Core helper function that encapsulates the complete repartition test workflow:
   * 1. Run query to create initial state
   * 2. Read state before repartition
   * 3. Run repartition operation
   * 4. Verify repartition batch and state data
   * 5. Resume query with new data and verify results
   *
   * @param newPartitions The target number of partitions after repartition
   * @param setupInitialState Callback to set up initial state
   *                          (receives inputStream, checkpointDir, clock)
   * @param verifyResumedQuery Callback to verify resumed query
   *                           (receives inputStream, checkpointDir, clock)
   * @param useManualClock Whether this test requires a manual clock (for processing time)
   * @param storeToColumnFamilyToStateSourceOptions Map of store name -> column family
   *                                                 name -> options
   * @param storeToColumnFamilyToSelectExprs Map of store name -> column family
   *                                          name -> select expressions
   * @tparam T The type of data in the input stream (requires implicit Encoder)
   */
  def testRepartitionWorkflow[T : Encoder](
      newPartitions: Int,
      setupInitialState: (MemoryStream[T], String, Option[StreamManualClock]) => Unit,
      verifyResumedQuery: (MemoryStream[T], String, Option[StreamManualClock]) => Unit,
      useManualClock: Boolean = false,
      storeToColumnFamilyToStateSourceOptions: Map[String, Map[String, Map[String, String]]] =
        Map(StateStoreId.DEFAULT_STORE_NAME ->
          Map(StateStore.DEFAULT_COL_FAMILY_NAME -> Map.empty)),
      storeToColumnFamilyToSelectExprs: Map[String, Map[String, Seq[String]]] = Map.empty): Unit = {
    withTempDir { checkpointDir =>
      val clock = if (useManualClock) Some(new StreamManualClock) else None
      val inputData = MemoryStream[T]

      // Step 1: Run initial query to create state
      setupInitialState(inputData, checkpointDir.getAbsolutePath, clock)

      // Step 2: Read state data before repartition
      val checkpointMetadata = new StreamingQueryCheckpointMetadata(
        spark, checkpointDir.getAbsolutePath)
      val lastBatchId = checkpointMetadata.commitLog.getLatestBatchId().get

      val stateBeforeRepartition = readStateDataByStoreName(
        checkpointDir.getAbsolutePath, lastBatchId, storeToColumnFamilyToStateSourceOptions,
        storeToColumnFamilyToSelectExprs)
      // Verify all stores and column families have data before repartition
      storeToColumnFamilyToStateSourceOptions.foreach { case (storeName, columnFamilies) =>
        columnFamilies.keys.foreach { cfName =>
          val data = stateBeforeRepartition(storeName)(cfName)
          assert(data.length > 0,
            s"Store '$storeName', CF '$cfName' has no state data before repartition." +
            s"Please make sure it has data so that we can test data correctness post-repartition"
          )
        }
      }

      // Step 3: Run repartition
      spark.streamingCheckpointManager.repartition(
        checkpointDir.getAbsolutePath, newPartitions)

      val repartitionBatchId = lastBatchId + 1
      val hadoopConf = spark.sessionState.newHadoopConf()

      // Step 4: Verify offset and commit logs
      OfflineStateRepartitionTestUtils.verifyRepartitionBatch(
        repartitionBatchId, checkpointMetadata, hadoopConf,
        checkpointDir.getAbsolutePath, newPartitions, spark)

      // Step 5: Validate state for each store and column family after repartition
      val stateAfterRepartition = readStateDataByStoreName(
        checkpointDir.getAbsolutePath, repartitionBatchId, storeToColumnFamilyToStateSourceOptions,
        storeToColumnFamilyToSelectExprs)

      storeToColumnFamilyToStateSourceOptions.foreach { case (storeName, columnFamilies) =>
        columnFamilies.keys.foreach { cfName =>
          val beforeState = stateBeforeRepartition(storeName)(cfName)
          val afterState = stateAfterRepartition(storeName)(cfName)

          // Validate row count
          assert(beforeState.length == afterState.length,
            s"Store '$storeName', CF '$cfName': State row count mismatch: " +
              s"before=${beforeState.length}, after=${afterState.length}")

          val sourceSorted = beforeState.sortBy(_.toString)
          val targetSorted = afterState.sortBy(_.toString)

          sourceSorted.zip(targetSorted).zipWithIndex.foreach {
            case ((sourceRow, targetRow), idx) =>
              assert(sourceRow == targetRow,
                s"Row mismatch at index $idx:\n" +
                  s"  Source: $sourceRow\n" +
                  s"  Target: $targetRow")
          }
        }
      }

      // Step 6: Resume query with new input and verify
      verifyResumedQuery(inputData, checkpointDir.getAbsolutePath, clock)
    }
  }

  def testWithChangelogConfig(testName: String)(testFun: => Unit): Unit = {
    // TODO[SPARK-55301]: add test with changelog checkpointing disabled after SPARK increases
    // its test timeout because CI signal "sql - other tests" is timing out after adding the
    // integration tests
    Seq(true).foreach { changelogCheckpointingEnabled =>
      test(s"$testName - enableChangelogCheckpointing=$changelogCheckpointingEnabled") {
        withSQLConf(
          "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled" ->
            changelogCheckpointingEnabled.toString) {
          testFun
        }
      }
    }
  }

  /**
   * Helper function to test with both increase and decrease repartition operations.
   * This reduces code duplication across different operator tests.
   *
   * @param testNamePrefix The prefix for the test name (e.g., "aggregation state v2")
   * @param testFun The test function that takes newPartitions as parameter
   */
  def testWithAllRepartitionOperations(testNamePrefix: String)
      (testFun: Int => Unit): Unit = {
    Seq(("increase", 8), ("decrease", 3)).foreach { case (direction, newPartitions) =>
      testWithChangelogConfig(s"$testNamePrefix - $direction partitions") {
        testFun(newPartitions)
      }
    }
  }
}

/**
 * Integration test suite for OfflineStateRepartitionRunner with single-column family operators.
 */
class OfflineStateRepartitionCkptV1IntegrationSuite
  extends OfflineStateRepartitionIntegrationSuiteBase {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key, "1")
  }

  // Test aggregation operator repartitioning
  StreamingAggregationStateManager.supportedVersions.foreach { version =>
    testWithAllRepartitionOperations(s"aggregation state v$version") { newPartitions =>
      withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> version.toString) {
        testRepartitionWorkflow[Int](
          newPartitions = newPartitions,
          setupInitialState = (inputData, checkpointDir, _) => {
            val aggregated = getLargeDataStreamingAggregationQuery(inputData)
            testStream(aggregated, OutputMode.Update)(
              StartStream(checkpointLocation = checkpointDir),
              AddData(inputData, 0 until 20: _*),
              CheckLastBatch(
                (0, 2, 10, 10, 0), (1, 2, 12, 11, 1), (2, 2, 14, 12, 2), (3, 2, 16, 13, 3),
                (4, 2, 18, 14, 4), (5, 2, 20, 15, 5), (6, 2, 22, 16, 6), (7, 2, 24, 17, 7),
                (8, 2, 26, 18, 8), (9, 2, 28, 19, 9)
              ),
              AddData(inputData, 20 until 40: _*),
              CheckLastBatch(
                (0, 4, 60, 30, 0), (1, 4, 64, 31, 1), (2, 4, 68, 32, 2), (3, 4, 72, 33, 3),
                (4, 4, 76, 34, 4), (5, 4, 80, 35, 5), (6, 4, 84, 36, 6), (7, 4, 88, 37, 7),
                (8, 4, 92, 38, 8), (9, 4, 96, 39, 9)
              ),
              StopStream
            )
          },
          verifyResumedQuery = (inputData, checkpointDir, _) => {
            val aggregated = getLargeDataStreamingAggregationQuery(inputData)
            testStream(aggregated, OutputMode.Update)(
              StartStream(checkpointLocation = checkpointDir),
              AddData(inputData, 40 until 50: _*),
              CheckLastBatch(
                (0, 5, 100, 40, 0), (1, 5, 105, 41, 1), (2, 5, 110, 42, 2),
                (3, 5, 115, 43, 3), (4, 5, 120, 44, 4), (5, 5, 125, 45, 5),
                (6, 5, 130, 46, 6), (7, 5, 135, 47, 7), (8, 5, 140, 48, 8),
                (9, 5, 145, 49, 9)
              )
            )
          }
        )
      }
    }

    testWithAllRepartitionOperations(s"composite key aggregation state v$version") {
      newPartitions =>
        withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> version.toString) {
          testRepartitionWorkflow[Int](
            newPartitions = newPartitions,
            setupInitialState = (inputData, checkpointDir, _) => {
              val aggregated = getCompositeKeyStreamingAggregationQuery(inputData)
              testStream(aggregated, OutputMode.Update)(
                StartStream(checkpointLocation = checkpointDir),
                AddData(inputData, 0 until 10: _*),
                CheckLastBatch(
                  (0, "Apple", 2, 6, 6, 0), (0, "Banana", 1, 4, 4, 4),
                  (0, "Strawberry", 2, 10, 8, 2), (1, "Apple", 2, 12, 9, 3),
                  (1, "Banana", 2, 8, 7, 1), (1, "Strawberry", 1, 5, 5, 5)
                ),
                StopStream
              )
            },
            verifyResumedQuery = (inputData, checkpointDir, _) => {
              val aggregated = getCompositeKeyStreamingAggregationQuery(inputData)
              testStream(aggregated, OutputMode.Update)(
                StartStream(checkpointLocation = checkpointDir),
                AddData(inputData, 0 until 2: _*),
                CheckLastBatch(
                  (0, "Apple", 3, 6, 6, 0), (1, "Banana", 3, 9, 7, 1)
                )
              )
            }
          )
        }
    }
  }

  // Test dedup operator repartitioning
  testWithAllRepartitionOperations("dedup") { newPartitions =>
    testRepartitionWorkflow[Int](
      newPartitions = newPartitions,
      setupInitialState = (inputData, checkpointDir, _) => {
        val deduplicated = getDropDuplicatesQuery(inputData)
        testStream(deduplicated, OutputMode.Append)(
          StartStream(checkpointLocation = checkpointDir),
          AddData(inputData, (1 to 5).flatMap(_ => (10 to 15)): _*),
          CheckAnswer(10 to 15: _*),
          assertNumStateRows(total = 6, updated = 6),
          AddData(inputData, (1 to 3).flatMap(_ => (10 to 15)) ++ (16 to 19): _*),
          CheckNewAnswer(16, 17, 18, 19),
          assertNumStateRows(total = 10, updated = 4),
          StopStream
        )
      },
      verifyResumedQuery = (inputData, checkpointDir, _) => {
        val deduplicated = getDropDuplicatesQuery(inputData)
        testStream(deduplicated, OutputMode.Append)(
          StartStream(checkpointLocation = checkpointDir),
          AddData(inputData, (1 to 2).flatMap(_ => 10 to 15) ++ (20 to 22): _*),
          CheckNewAnswer(20, 21, 22),
          assertNumStateRows(total = 10, updated = 3)
        )
      }
    )
  }

  // Test dropDuplicates with column specified repartitioning
  testWithAllRepartitionOperations("dropDuplicates with column specified") { newPartitions =>
    testRepartitionWorkflow[(String, Int)](
      newPartitions = newPartitions,
      setupInitialState = (inputData, checkpointDir, _) => {
        val deduplicated = getDropDuplicatesQueryWithColumnSpecified(inputData)
        testStream(deduplicated, OutputMode.Append)(
          StartStream(checkpointLocation = checkpointDir),
          AddData(inputData, ("a", 1), ("b", 2), ("c", 3)),
          CheckAnswer(("a", 1), ("b", 2), ("c", 3)),
          assertNumStateRows(total = 3, updated = 3),
          AddData(inputData, ("a", 10), ("d", 4)),
          CheckNewAnswer(("d", 4)),
          assertNumStateRows(total = 4, updated = 1),
          StopStream
        )
      },
      verifyResumedQuery = (inputData, checkpointDir, _) => {
        val deduplicated = getDropDuplicatesQueryWithColumnSpecified(inputData)
        testStream(deduplicated, OutputMode.Append)(
          StartStream(checkpointLocation = checkpointDir),
          AddData(inputData, ("b", 20), ("e", 5)),
          CheckNewAnswer(("e", 5)),
          assertNumStateRows(total = 5, updated = 1)
        )
      }
    )
  }

  // Test dropDuplicatesWithinWatermark repartitioning
  testWithAllRepartitionOperations("dropDuplicatesWithinWatermark") { newPartitions =>
    testRepartitionWorkflow[(String, Int)](
      newPartitions = newPartitions,
      setupInitialState = (inputData, checkpointDir, _) => {
        val deduplicated = getDropDuplicatesWithinWatermarkQuery(inputData)
        testStream(deduplicated, OutputMode.Append)(
          StartStream(checkpointLocation = checkpointDir),
          AddData(inputData, ("a", 1), ("b", 2), ("c", 3)),
          CheckNewAnswer(("a", 1), ("b", 2), ("c", 3)),
          assertNumStateRows(total = 3, updated = 3),
          AddData(inputData, ("a", 4), ("b", 5), ("d", 6)),
          CheckNewAnswer(("d", 6)),
          assertNumStateRows(total = 2, updated = 1),
          StopStream
        )
      },
      verifyResumedQuery = (inputData, checkpointDir, _) => {
        val deduplicated = getDropDuplicatesWithinWatermarkQuery(inputData)
        testStream(deduplicated, OutputMode.Append)(
          StartStream(checkpointLocation = checkpointDir),
          AddData(inputData, ("a", 5), ("e", 8), ("d", 7)),
          CheckNewAnswer(("a", 5), ("e", 8)),
          assertNumStateRows(total = 3, updated = 2)
        )
      }
    )
  }

  // Test session window aggregation repartitioning
  testWithAllRepartitionOperations("session window aggregation") { newPartitions =>
    testRepartitionWorkflow[(String, Long)](
      newPartitions = newPartitions,
      setupInitialState = (inputData, checkpointDir, _) => {
        val aggregated = getSessionWindowAggregationQuery(inputData)
        testStream(aggregated, OutputMode.Complete())(
          StartStream(checkpointLocation = checkpointDir),
          AddData(inputData,
            ("hello world spark streaming", 40L),
            ("world hello structured streaming", 41L)
          ),
          CheckNewAnswer(
            ("hello", 40, 51, 11, 2), ("world", 40, 51, 11, 2), ("streaming", 40, 51, 11, 2),
            ("spark", 40, 50, 10, 1), ("structured", 41, 51, 10, 1)
          ),
          assertNumStateRows(total = 5, updated = 5),
          StopStream
        )
      },
      verifyResumedQuery = (inputData, checkpointDir, _) => {
        val resumed = getSessionWindowAggregationQuery(inputData)
        testStream(resumed, OutputMode.Complete())(
          StartStream(checkpointLocation = checkpointDir),
          AddData(inputData, ("hello spark", 42L), ("new session", 100L)),
          CheckNewAnswer(
            ("hello", 40, 52, 12, 3), ("world", 40, 51, 11, 2), ("streaming", 40, 51, 11, 2),
            ("spark", 40, 52, 12, 2), ("structured", 41, 51, 10, 1),
            ("new", 100, 110, 10, 1), ("session", 100, 110, 10, 1))
        )
      }
    )
  }

  /**
   * Helper function to test FlatMapGroupsWithState operator with different state versions
   * and both increase/decrease repartition operations.
   * @param testNamePrefix The prefix for the test name (e.g., "flatMapGroupsWithState")
   * @param testFun The test function that takes in the number of partitions after repartition
   */
  def testFMGWOnAllPartitionOperation(testNamePrefix: String)
      (testFun: Int => Unit): Unit = {
    Seq(1, 2).foreach { stateVersion =>
      if (stateVersion == 1 &&
        !java.nio.ByteOrder.nativeOrder().equals(java.nio.ByteOrder.LITTLE_ENDIAN)) {
        // Skip test
      } else {
        withSQLConf(
          SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> stateVersion.toString) {
          testWithAllRepartitionOperations(s"$testNamePrefix v$stateVersion") { newPartition =>
            testFun(newPartition)
          }
        }
      }
    }
  }

  // Test flatMapGroupsWithState repartitioning
  testFMGWOnAllPartitionOperation(s"flatMapGroupsWithState") { newPartitions =>
    testRepartitionWorkflow[(String, Long)](
      newPartitions = newPartitions,
      setupInitialState = (inputData, checkpointDir, clockOpt) => {
        val clock = clockOpt.get
        val remapped = getFlatMapGroupsWithStateQuery(inputData)
        testStream(remapped, OutputMode.Update)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = checkpointDir),
          AddData(inputData, ("hello world", 1L), ("hello scala", 2L)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            ("hello", 2, 1000, false), ("world", 1, 0, false), ("scala", 1, 0, false)
          ),
          AddData(inputData, ("hello spark", 3L)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("hello", 3, 2000, false), ("spark", 1, 0, false)),
          StopStream
        )
      },
      verifyResumedQuery = (inputData, checkpointDir, clockOpt) => {
        val clock = clockOpt.get
        val resumed = getFlatMapGroupsWithStateQuery(inputData)
        testStream(resumed, OutputMode.Update)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = checkpointDir),
          AddData(inputData, ("hello", 10L), ("new", 11L)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            ("hello", 4, 9000, false),
            ("new", 1, 0, false)
          )
        )
      },
      useManualClock = true
    )
  }

  // Test combined dedup + aggregation repartitioning
  testWithAllRepartitionOperations("multiple operators (dedup + aggregation)") { newPartitions =>
    testRepartitionWorkflow[(String, Int)](
      newPartitions = newPartitions,
      setupInitialState = (inputData, checkpointDir, _) => {
        val query = getDedupAndAggregationQuery(inputData)
        testStream(query, OutputMode.Update)(
          StartStream(checkpointLocation = checkpointDir),
          // Batch 1: ("a",1), ("b",2), ("a",3) -> all unique after dedup
          // Aggregation: "a" -> count=2, sum=4, max=3; "b" -> count=1, sum=2, max=2
          AddData(inputData, ("a", 1), ("b", 2), ("a", 3)),
          CheckNewAnswer(("a", 2, 4, 3), ("b", 1, 2, 2)),
          // Batch 2: ("a",1) and ("a",3) are duplicates (filtered by dedup),
          // ("b",4) and ("c",5) are new
          // Aggregation: "b" -> count=2, sum=6, max=4; "c" -> count=1, sum=5, max=5
          AddData(inputData, ("a", 1), ("a", 3), ("b", 4), ("c", 5)),
          CheckNewAnswer(("b", 2, 6, 4), ("c", 1, 5, 5)),
          StopStream
        )
      },
      verifyResumedQuery = (inputData, checkpointDir, _) => {
        val query = getDedupAndAggregationQuery(inputData)
        testStream(query, OutputMode.Update)(
          StartStream(checkpointLocation = checkpointDir),
          // Batch 3 after repartition: ("a",5) is new, ("b",2) is duplicate,
          // ("d",6) is new
          // Aggregation: "a" -> count=3, sum=9, max=5; "d" -> count=1, sum=6, max=6
          AddData(inputData, ("a", 5), ("b", 2), ("d", 6)),
          CheckNewAnswer(("a", 3, 9, 5), ("d", 1, 6, 6))
        )
      }
    )
  }

  def testWithDifferentEncodingType(testNamePrefix: String)
      (testFun: Int => Unit): Unit = {
    Seq("unsaferow", "avro").foreach { encodingFormat =>
      testWithAllRepartitionOperations(
        s"$testNamePrefix (encoding = $encodingFormat)") { newPartitions =>
        withSQLConf(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> encodingFormat) {
          testFun(newPartitions)
        }
      }
    }
  }

  testWithDifferentEncodingType(
    "transformWithState with multiple column families") {
    newPartitions =>
      val allColFamilyNames = MultiStateVarProcessorTestUtils.ALL_COLUMN_FAMILIES.toList
      val stateSourceOptions = buildStateSourceOptionsForTWS(
        allColFamilyNames,
        listStateName = Some(MultiStateVarProcessorTestUtils.ITEMS_LIST))
      val selectExprs = MultiStateVarProcessorTestUtils.getColumnFamilyToSelectExprs()

      def buildQuery(inputData: MemoryStream[String]): Dataset[(String, String)] = {
        inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new MultiStateVarProcessor(),
            TimeMode.None(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[String](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, _) => {
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir),
            // Batch 1: Creates state in all column families
            AddData(inputData, "a", "b", "c"),
            CheckNewAnswer(("a", "1"), ("b", "1"), ("c", "1")),
            // Batch 2: Adds more state
            AddData(inputData, "a", "b", "d"),
            CheckNewAnswer(("a", "2"), ("b", "2"), ("d", "1")),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, _) => {
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir),
            // Batch 3: Resume with new data after repartition
            AddData(inputData, "a", "c", "e"),
            CheckNewAnswer(("a", "3"), ("c", "2"), ("e", "1"))
          )
        },
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        ),
        storeToColumnFamilyToSelectExprs = Map(
          StateStoreId.DEFAULT_STORE_NAME -> selectExprs
        )
      )
  }

  testWithDifferentEncodingType("transformWithState with eventTime timers") {
    newPartitions =>
      val columnFamilies = TimerTestUtils
        .getTimerConfigsForCountState(TimeMode.EventTime()).keys.toSeq
        .filterNot(_ == StateStore.DEFAULT_COL_FAMILY_NAME)
      val stateSourceOptions = buildStateSourceOptionsForTWS(
        columnFamilies, timeMode = Some(TimeMode.EventTime()))
      val selectExprs = TimerTestUtils.getTimerColumnFamilyToSelectExprs(TimeMode.EventTime())

      def buildQuery(inputData: MemoryStream[(String, Long)]): Dataset[(String, String)] = {
        inputData.toDS()
          .select(col("_1").as("key"), timestamp_seconds(col("_2")).as("eventTime"))
          .withWatermark("eventTime", "10 seconds")
          .as[(String, Timestamp)]
          .groupByKey(_._1)
          .transformWithState(
            new EventTimeTimerProcessor(),
            TimeMode.EventTime(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[(String, Long)](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, _) => {
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir),
            // Batch 1: Creates state with event time timers
            AddData(inputData, ("a", 1L), ("b", 2L), ("c", 3L)),
            CheckNewAnswer(("a", "1"), ("b", "1"), ("c", "1")),
            // Batch 2: More data
            AddData(inputData, ("a", 5L), ("d", 6L)),
            CheckNewAnswer(("a", "2"), ("d", "1")),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, _) => {
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir),
            // Batch 3: Resume with new data after repartition
            AddData(inputData, ("a", 10L), ("e", 11L)),
            // Simply maintaining a count for each key
            CheckNewAnswer(("a", "3"), ("e", "1"))
          )
        },
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        ),
        storeToColumnFamilyToSelectExprs = Map(
          StateStoreId.DEFAULT_STORE_NAME -> selectExprs
        )
      )
  }

  testWithDifferentEncodingType("transformWithState with processing time timers") {
    newPartitions =>
      val schemas = TimerTestUtils.getTimerConfigsForCountState(TimeMode.ProcessingTime())
      val columnFamilies = schemas.keys.toSeq.filterNot(_ == StateStore.DEFAULT_COL_FAMILY_NAME)
      val stateSourceOptions = buildStateSourceOptionsForTWS(
        columnFamilies,
        timeMode = Some(TimeMode.ProcessingTime()))
      val selectExprs = TimerTestUtils.getTimerColumnFamilyToSelectExprs(TimeMode.ProcessingTime())

      def buildQuery(inputData: MemoryStream[String]): Dataset[(String, String)] = {
        inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorWithProcTimeTimer(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[String](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            AddData(inputData, "a", "b"),
            AdvanceManualClock(1000),
            CheckNewAnswer(("a", "1"), ("b", "1")),
            AddData(inputData, "a", "c"),
            AdvanceManualClock(1000),
            CheckNewAnswer(("a", "2"), ("c", "1")),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            AddData(inputData, "c", "d"),
            AdvanceManualClock(5 * 1000),
            // "a" and "c" are expired, and processor fires eventTime with "-1"
            CheckNewAnswer(("a", "-1"), ("c", "-1"), ("c", "2"), ("d", "1")),
            AddData(inputData, "c"),
            AdvanceManualClock(1000),
            // "c" is cleared after timer went off, so recount from 1
            CheckNewAnswer(("c", "1"))
          )
        },
        useManualClock = true,
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        ),
        storeToColumnFamilyToSelectExprs = Map(
          StateStoreId.DEFAULT_STORE_NAME -> selectExprs
        )
      )
  }

  testWithDifferentEncodingType("transformWithState with list and TTL") {
    newPartitions =>
      val schemas = TTLProcessorUtils.getListStateTTLSchemasWithMetadata()
      val columnFamilies = schemas.keys.toSeq.filterNot(_ == StateStore.DEFAULT_COL_FAMILY_NAME)
      val stateSourceOptions = buildStateSourceOptionsForTWS(
        columnFamilies,
        listStateName = Some(TTLProcessorUtils.LIST_STATE))
      val selectExprs = TTLProcessorUtils.getTTLSelectExpressions(columnFamilies)

      def buildQuery(inputData: MemoryStream[InputEvent]): Dataset[OutputEvent] = {
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new ListStateTTLProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[InputEvent](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // Batch 1: Clock advances to 1000ms, TTL = 1000 + 60000 = 61000ms
            AddData(inputData, InputEvent("k1", "put", 1),
              InputEvent("k1", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k1", 1, true, 61000)),
            // Batch 2: Clock advances to 2000ms, TTL = 2000 + 60000 = 62000ms
            AddData(inputData, InputEvent("k2", "put", 2),
              InputEvent("k2", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k2", 2, true, 62000)),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // Batch 3: Clock advances to 3000ms
            // Value 1 has TTL from batch 1 (61000ms), value 3 gets TTL = 3000 + 60000 = 63000ms
            AddData(inputData, InputEvent("k1", "append", 3),
              InputEvent("k1", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k1", 1, true, 61000), OutputEvent("k1", 3, true, 63000))
          )
        },
        useManualClock = true,
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        ),
        storeToColumnFamilyToSelectExprs = Map(
          StateStoreId.DEFAULT_STORE_NAME -> selectExprs
        )
      )
  }

  testWithDifferentEncodingType("transformWithState with map and TTL") {
    newPartitions =>
      val schemas = TTLProcessorUtils.getMapStateTTLSchemasWithMetadata()
      val columnFamilies = schemas.keys.toSeq.filterNot(_ == StateStore.DEFAULT_COL_FAMILY_NAME)
      val stateSourceOptions = buildStateSourceOptionsForTWS(columnFamilies)
      val selectExprs = TTLProcessorUtils.getTTLSelectExpressions(columnFamilies)

      def buildQuery(inputData: MemoryStream[MapInputEvent]): Dataset[MapOutputEvent] = {
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new MapStateTTLProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[MapInputEvent](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // Batch 1: Clock advances to 1000ms, TTL = 1000 + 60000 = 61000ms
            AddData(inputData, MapInputEvent("a", "key1", "put", 1),
              MapInputEvent("a", "key1", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(MapOutputEvent("a", "key1", 1, true, 61000)),
            // Batch 2: Clock advances to 2000ms, TTL = 2000 + 60000 = 62000ms
            AddData(inputData, MapInputEvent("b", "key2", "put", 2),
              MapInputEvent("b", "key2", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(MapOutputEvent("b", "key2", 2, true, 62000)),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // Batch 3: Clock advances to 3000ms
            // key1 has TTL from batch 1 (61000ms), key3 gets TTL = 3000 + 60000 = 63000ms
            AddData(inputData, MapInputEvent("a", "key3", "put", 3),
              MapInputEvent("a", "key1", "get_ttl_value_from_state", 0),
              MapInputEvent("a", "key3", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(MapOutputEvent("a", "key1", 1, true, 61000),
              MapOutputEvent("a", "key3", 3, true, 63000))
          )
        },
        useManualClock = true,
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        ),
        storeToColumnFamilyToSelectExprs = Map(
          StateStoreId.DEFAULT_STORE_NAME -> selectExprs
        )
      )
  }

  testWithDifferentEncodingType("transformWithState with value and TTL") {
    newPartitions =>
      val schemas = TTLProcessorUtils.getValueStateTTLSchemasWithMetadata()
      val stateSourceOptions = buildStateSourceOptionsForTWS(
        schemas.keys.toSeq.filterNot(_ == StateStore.DEFAULT_COL_FAMILY_NAME))

      def buildQuery(inputData: MemoryStream[InputEvent]): Dataset[OutputEvent] = {
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new ValueStateTTLProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[InputEvent](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // Batch 1: Clock advances to 1000ms, TTL = 1000 + 60000 = 61000ms
            AddData(inputData, InputEvent("k1", "put", 1),
              InputEvent("k1", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k1", 1, true, 61000)),
            // Batch 2: Clock is at 2000ms, TTL = 2000 + 60000 = 62000ms
            AddData(inputData, InputEvent("k2", "put", 2),
              InputEvent("k2", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k2", 2, true, 62000)),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // k2 is still in the state
            AddData(inputData, InputEvent("k2", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k2", 2, true, 62000))
          )
        },
        useManualClock = true,
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        )
      )
  }
}

class OfflineStateRepartitionCkptV2IntegrationSuite
  extends OfflineStateRepartitionCkptV1IntegrationSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key, "2")
  }
}
