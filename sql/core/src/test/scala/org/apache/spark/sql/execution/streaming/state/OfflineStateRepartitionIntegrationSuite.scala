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

import org.apache.spark.sql.{Dataset, Encoder, Row}
import org.apache.spark.sql.execution.datasources.v2.state.{StateDataSourceTestBase, StateSourceOptions}
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingAggregationStateManager
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryCheckpointMetadata}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock

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
      additionalOptions: Map[String, String]): Dataset[Row] = {
    var reader = spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointDir)
      .option(StateSourceOptions.BATCH_ID, batchId)
      .option(StateSourceOptions.STORE_NAME, storeName)

    // Apply additional options dynamically
    additionalOptions.foreach { case (k, v) =>
      reader = reader.option(k, v)
    }

    reader.load()
      .selectExpr("key", "value", "partition_id")
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
      storeToColumnFamilyToStateSourceOptions: Map[String, Map[String, Map[String, String]]]
  ): Map[String, Map[String, Array[Row]]] = {
    storeToColumnFamilyToStateSourceOptions.map { case (storeName, columnFamilyToOptions) =>
      val columnFamilyData = columnFamilyToOptions.map { case (cfName, options) =>
        val stateData = readStateData(
          checkpointDir, batchId, storeName, options).collect()
        cfName -> stateData
      }
      storeName -> columnFamilyData
    }
  }

  /**
   * Extracts (key, value) pairs from Row array and sorts by key.
   */
  private def extractKeyValuePairs(rows: Array[Row]): Array[(String, String)] = {
    rows
      .map(row => (row.get(0).toString, row.get(1).toString))
      .sortBy(_._1)
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
   * @tparam T The type of data in the input stream (requires implicit Encoder)
   */
  def testRepartitionWorkflow[T : Encoder](
      newPartitions: Int,
      setupInitialState: (MemoryStream[T], String, Option[StreamManualClock]) => Unit,
      verifyResumedQuery: (MemoryStream[T], String, Option[StreamManualClock]) => Unit,
      useManualClock: Boolean = false,
      storeToColumnFamilyToStateSourceOptions: Map[String, Map[String, Map[String, String]]] =
        Map(StateStoreId.DEFAULT_STORE_NAME ->
          Map(StateStore.DEFAULT_COL_FAMILY_NAME -> Map.empty))): Unit = {
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
        checkpointDir.getAbsolutePath, lastBatchId, storeToColumnFamilyToStateSourceOptions)

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
        checkpointDir.getAbsolutePath, repartitionBatchId, storeToColumnFamilyToStateSourceOptions)

      storeToColumnFamilyToStateSourceOptions.foreach { case (storeName, columnFamilies) =>
        columnFamilies.keys.foreach { cfName =>
          val beforeState = stateBeforeRepartition(storeName)(cfName)
          val afterState = stateAfterRepartition(storeName)(cfName)

          // Validate row count
          assert(beforeState.length == afterState.length,
            s"Store '$storeName', CF '$cfName': State row count mismatch: " +
              s"before=${beforeState.length}, after=${afterState.length}")

          // Extract (key, value) pairs and compare
          val beforeByKey = extractKeyValuePairs(beforeState)
          val afterByKey = extractKeyValuePairs(afterState)

          // Compare each (key, value) pair
          beforeByKey.zip(afterByKey).zipWithIndex.foreach {
            case (((keyBefore, valueBefore), (keyAfter, valueAfter)), idx) =>
              assert(keyBefore == keyAfter,
                s"Store '$storeName', CF '$cfName': Key mismatch at index $idx")
              assert(valueBefore == valueAfter,
                s"Store '$storeName', CF '$cfName': Value mismatch for key $keyBefore")
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
}

class OfflineStateRepartitionCkptV2IntegrationSuite
  extends OfflineStateRepartitionCkptV1IntegrationSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key, "2")
  }
}
