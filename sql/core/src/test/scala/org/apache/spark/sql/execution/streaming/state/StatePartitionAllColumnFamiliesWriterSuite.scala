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

import scala.collection.immutable.HashMap

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.datasources.v2.state.{StateDataSourceTestBase, StateSourceOptions}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryCheckpointMetadata}
import org.apache.spark.sql.execution.streaming.utils.StreamingUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, NullType, StructField, StructType, TimestampType}
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
   * Common helper method to perform round-trip test: read state bytes from source,
   * write to target, and verify target matches source.
   *
   * @param sourceDir Source checkpoint directory
   * @param targetDir Target checkpoint directory
   * @param keySchema Key schema for the state store
   * @param valueSchema Value schema for the state store
   * @param keyStateEncoderSpec Key state encoder spec
   * @param storeName Optional store name (for stream-stream join which has multiple stores)
   */
  private def performRoundTripTest(
      sourceDir: String,
      targetDir: String,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      storeName: Option[String] = None): Unit = {

    // Step 1: Read original state using normal reader (for comparison later)
    val sourceReader = spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, sourceDir)
    val sourceNormalData = (storeName match {
      case Some(name) => sourceReader.option(StateSourceOptions.STORE_NAME, name)
      case None => sourceReader
    }).load()
      .selectExpr("key", "value", "partition_id")
      .collect()

    // Step 2: Read from source using AllColumnFamiliesReader (raw bytes)
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

    // Step 3: Write raw bytes to target checkpoint location
    val hadoopConf = spark.sessionState.newHadoopConf()
    val targetCpLocation = StreamingUtils.resolvedCheckpointLocation(
      hadoopConf, targetDir)
    val targetCheckpointMetadata = new StreamingQueryCheckpointMetadata(
      spark, targetCpLocation)
    val lastBatch = targetCheckpointMetadata.commitLog.getLatestBatchId().get
    val targetOffsetSeq = targetCheckpointMetadata.offsetLog.get(lastBatch).get
    val currentBatchId = lastBatch + 1
    targetCheckpointMetadata.offsetLog.add(currentBatchId, targetOffsetSeq)

    // Create column family to schema map
    val columnFamilyToSchemaMap = HashMap(
      StateStore.DEFAULT_COL_FAMILY_NAME -> StateStoreColFamilySchema(
        StateStore.DEFAULT_COL_FAMILY_NAME,
        keySchemaId = 0,
        keySchema,
        valueSchemaId = 0,
        valueSchema,
        keyStateEncoderSpec = Some(keyStateEncoderSpec)
      )
    )

    val storeConf: StateStoreConf = StateStoreConf(SQLConf.get)
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)

    // Define the partition processing function
    val putPartitionFunc: Iterator[Row] => Unit = partition => {
      val allCFWriter = new StatePartitionAllColumnFamiliesWriter(
        storeConf,
        serializableHadoopConf.value,
        TaskContext.getPartitionId(),
        targetCpLocation,
        0,
        storeName.getOrElse(StateStoreId.DEFAULT_STORE_NAME),
        currentBatchId,
        columnFamilyToSchemaMap
      )
      val rowConverter = CatalystTypeConverters.createToCatalystConverter(schema)

      allCFWriter.write(partition.map(rowConverter(_).asInstanceOf[InternalRow]))
    }

    // Write raw bytes to target using foreachPartition
    sourceBytesData.foreachPartition(putPartitionFunc)

    // Commit to commitLog
    val latestCommit = targetCheckpointMetadata.commitLog.get(lastBatch).get
    targetCheckpointMetadata.commitLog.add(currentBatchId, latestCommit)
    val versionToCheck = currentBatchId + 1
    val storeNamePath = s"state/0/0${storeName.fold("")("/" + _)}"
    assert(!checkpointFileExists(new File(targetDir, storeNamePath), versionToCheck, ".changelog"))
    assert(checkpointFileExists(new File(targetDir, storeNamePath), versionToCheck, ".zip"))

    // Step 4: Read from target using normal reader
    val targetReader = spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, targetDir)
    val targetNormalData = (storeName match {
      case Some(name) => targetReader.option(StateSourceOptions.STORE_NAME, name)
      case None => targetReader
    }).load()
      .selectExpr("key", "value", "partition_id")
      .collect()

    // Step 5: Verify data matches
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
          val keySchema = StructType(Array(
            StructField("groupKey", IntegerType, nullable = false)))
          val valueSchema = if (stateVersion == 1) {
            // State version 1 includes key columns in the value
            StructType(Array(
              StructField("groupKey", IntegerType, nullable = false),
              StructField("count", LongType, nullable = false),
              StructField("sum", LongType, nullable = false),
              StructField("max", IntegerType, nullable = false),
              StructField("min", IntegerType, nullable = false)
            ))
          } else {
            // State version 2 excludes key columns from the value
            StructType(Array(
              StructField("count", LongType, nullable = false),
              StructField("sum", LongType, nullable = false),
              StructField("max", IntegerType, nullable = false),
              StructField("min", IntegerType, nullable = false)
            ))
          }

          // Create key state encoder spec (no prefix key for simple aggregation)
          val keyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema)

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            keySchema,
            valueSchema,
            keyStateEncoderSpec
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
          val keySchema = StructType(Array(
            StructField("groupKey", IntegerType, nullable = false),
            StructField("fruit", org.apache.spark.sql.types.StringType, nullable = true)
          ))
          val valueSchema = if (stateVersion == 1) {
            // State version 1 includes key columns in the value
            StructType(Array(
              StructField("groupKey", IntegerType, nullable = false),
              StructField("fruit", org.apache.spark.sql.types.StringType, nullable = true),
              StructField("count", LongType, nullable = false),
              StructField("sum", LongType, nullable = false),
              StructField("max", IntegerType, nullable = false),
              StructField("min", IntegerType, nullable = false)
            ))
          } else {
            // State version 2 excludes key columns from the value
            StructType(Array(
              StructField("count", LongType, nullable = false),
              StructField("sum", LongType, nullable = false),
              StructField("max", IntegerType, nullable = false),
              StructField("min", IntegerType, nullable = false)
            ))
          }

          // Create key state encoder spec (no prefix key for composite key aggregation)
          val keyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema)

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            keySchema,
            valueSchema,
            keyStateEncoderSpec
          )
        }
      }
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
          Seq("left-keyToNumValues", "right-keyToNumValues").foreach { storeName =>
            val keySchema = StructType(Array(
              StructField("key", IntegerType)
            ))
            val valueSchema = StructType(Array(
              StructField("value", LongType)
            ))
            val keyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema)

            // Perform round-trip test using common helper
            performRoundTripTest(
              sourceDir.getAbsolutePath,
              targetDir.getAbsolutePath,
              keySchema,
              valueSchema,
              keyStateEncoderSpec,
              storeName = Some(storeName)
            )
          }

          // Test keyWithIndexToValue stores (both left and right)
          Seq("left-keyWithIndexToValue", "right-keyWithIndexToValue").foreach { storeName =>
            val keySchema = StructType(Array(
              StructField("key", IntegerType, nullable = false),
              StructField("index", LongType)
            ))
            val valueSchema = if (stateVersion == 2) {
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
            val keyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema)

            // Perform round-trip test using common helper
            performRoundTripTest(
              sourceDir.getAbsolutePath,
              targetDir.getAbsolutePath,
              keySchema,
              valueSchema,
              keyStateEncoderSpec,
              storeName = Some(storeName)
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
          val keySchema = StructType(Array(
            StructField("value", org.apache.spark.sql.types.StringType, nullable = true)
          ))
          val valueSchema = if (stateVersion == 1) {
            StructType(Array(
              StructField("numEvents", IntegerType, nullable = false),
              StructField("startTimestampMs", LongType, nullable = false),
              StructField("endTimestampMs", LongType, nullable = false),
              StructField("timeoutTimestamp", IntegerType, nullable = false)
            ))
          } else {
            StructType(Array(
              StructField("groupState", org.apache.spark.sql.types.StructType(Array(
                StructField("numEvents", IntegerType, nullable = false),
                StructField("startTimestampMs", LongType, nullable = false),
                StructField("endTimestampMs", LongType, nullable = false)
              )), nullable = false),
              StructField("timeoutTimestamp", LongType, nullable = false)
            ))
          }
          val keyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema)

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            keySchema,
            valueSchema,
            keyStateEncoderSpec
          )
        }
      }
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
          val keySchema = StructType(Array(
            StructField("_1", org.apache.spark.sql.types.StringType, nullable = true)
          ))
          val valueSchema = StructType(Array(
            StructField("expiresAtMicros", LongType, nullable = false)
          ))
          val keyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema)

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            keySchema,
            valueSchema,
            keyStateEncoderSpec
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
          val keySchema = StructType(Array(
            StructField("col1", org.apache.spark.sql.types.StringType, nullable = true)
          ))
          val valueSchema = StructType(Array(
            StructField("__dummy__", NullType, nullable = true)
          ))
          val keyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema)

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            keySchema,
            valueSchema,
            keyStateEncoderSpec
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
          // Session window aggregation uses prefix key scanning where sessionId is the prefix
          val keyStateEncoderSpec = PrefixKeyScanStateEncoderSpec(keySchema, 1)

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            keySchema,
            valueSchema,
            keyStateEncoderSpec
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
          val keySchema = StructType(Array(
            StructField("value", IntegerType, nullable = false),
            StructField("eventTime", org.apache.spark.sql.types.TimestampType)
          ))
          val valueSchema = StructType(Array(
            StructField("__dummy__", NullType)
          ))
          val keyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema)

          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            keySchema,
            valueSchema,
            keyStateEncoderSpec
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
  } // End of foreach loop for changelog checkpointing dimension
}
