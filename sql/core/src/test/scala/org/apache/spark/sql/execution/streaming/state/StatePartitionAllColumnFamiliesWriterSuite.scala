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

import scala.collection.immutable.HashMap

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.datasources.v2.state.{StateDataSourceTestBase, StateSourceOptions}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryCheckpointMetadata}
import org.apache.spark.sql.execution.streaming.utils.StreamingUtils
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, TimeMode, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, NullType, StringType, StructField, StructType, TimestampType}
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
    spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "1")
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
   * @param columnFamilyToReaderOptions Map of column family names to reader options
   */
  private def performRoundTripTest(
      sourceDir: String,
      targetDir: String,
      columnFamilyToSchemaMap: HashMap[String, StatePartitionWriterColumnFamilyInfo],
      storeName: Option[String] = None,
      columnFamilyToSelectExprs: Map[String, Seq[String]] = Map.empty,
      columnFamilyToReaderOptions: Map[String, Map[String, String]] = Map.empty): Unit = {

    // Determine column families to validate based on storeName and map size
    val columnFamiliesToValidate: Seq[String] = storeName match {
      case Some(name) => Seq(name)
      case None if columnFamilyToSchemaMap.size > 1 => columnFamilyToSchemaMap.keys.toSeq
      case None => Seq.empty // Will use default reader without store name filter
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

    // Step 2: Write raw bytes to target checkpoint location"
    val hadoopConf = spark.sessionState.newHadoopConf()
    val targetCpLocation = StreamingUtils.resolvedCheckpointLocation(
      hadoopConf, targetDir)
    val targetCheckpointMetadata = new StreamingQueryCheckpointMetadata(
      spark, targetCpLocation)
    val lastBatch = targetCheckpointMetadata.commitLog.getLatestBatchId().get
    val targetOffsetSeq = targetCheckpointMetadata.offsetLog.get(lastBatch).get
    val currentBatchId = lastBatch + 1
    targetCheckpointMetadata.offsetLog.add(currentBatchId, targetOffsetSeq)

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

      // Use per-column-family converters when there are multiple column families
      if (columnFamilyToSchemaMap.size > 1) {
        // can remove this once we have extractKeySchema landed
        val colNameToRowConverter = columnFamilyToSchemaMap.view.mapValues { colInfo =>
          val cfSchema = SchemaUtil.getScanAllColumnFamiliesSchema(colInfo.schema.keySchema)
          CatalystTypeConverters.createToCatalystConverter(cfSchema)
        }
        allCFWriter.write(partition.map { row =>
          val rowConverter = colNameToRowConverter(row.getString(3))
          rowConverter(row).asInstanceOf[InternalRow]
        })
      } else {
        val rowConverter = CatalystTypeConverters.createToCatalystConverter(schema)
        allCFWriter.write(partition.map(rowConverter(_).asInstanceOf[InternalRow]))
      }
    }

    // Write raw bytes to target using foreachPartition"
    sourceBytesData.foreachPartition(putPartitionFunc)

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

    def shouldCheckColumnFamilyName: String => Boolean = name => {
      (!name.startsWith("$")
        || (columnFamilyToReaderOptions.contains(name) &&
        columnFamilyToReaderOptions(name).contains(StateSourceOptions.READ_REGISTERED_TIMERS)))
    }
    if (columnFamiliesToValidate.nonEmpty) {
      // Validate each column family separately (skip internal column families starting with $)
      columnFamiliesToValidate
        .filter(shouldCheckColumnFamilyName)
        .foreach { cfName =>
          val selectExprs = columnFamilyToSelectExprs.getOrElse(cfName, defaultSelectExprs)
          val readerOptions = columnFamilyToReaderOptions.getOrElse(cfName, Map.empty)
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
    } else {
      // Default validation without store name filter
      val sourceNormalData = spark.read
        .format("statestore")
        .option(StateSourceOptions.PATH, sourceDir)
        .load()
        .selectExpr("key", "value", "partition_id")
        .collect()

      val targetNormalData = spark.read
        .format("statestore")
        .option(StateSourceOptions.PATH, targetDir)
        .load()
        .selectExpr("key", "value", "partition_id")
        .collect()

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

    // Sort and compare row by row"
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
  ): HashMap[String, StatePartitionWriterColumnFamilyInfo] = {
    HashMap(colFamilyName -> createColFamilyInfo(keySchema, valueSchema,
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
            createSingleColumnFamilySchemaMap(keySchema, valueSchema, keyStateEncoderSpec)
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
            createSingleColumnFamilySchemaMap(keySchema, valueSchema, keyStateEncoderSpec)
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
          Seq("left-keyToNumValues", "right-keyToNumValues").foreach { sName =>
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
              createSingleColumnFamilySchemaMap(keySchema, valueSchema, keyStateEncoderSpec),
              storeName = Some(sName)
            )
          }

          // Test keyWithIndexToValue stores (both left and right)
          Seq("left-keyWithIndexToValue", "right-keyWithIndexToValue").foreach { sName =>
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
              createSingleColumnFamilySchemaMap(keySchema, valueSchema, keyStateEncoderSpec),
              storeName = Some(sName)
            )
          }
        }
      }
    }
  }

  private def testStreamStreamJoinV3RoundTrip(): Unit = {
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

          runQuery(sourceDir.getAbsolutePath, 2)
          runQuery(targetDir.getAbsolutePath, 1)

          // Define schemas for V3 join state stores
          val keyToNumValuesKeySchema = StructType(Array(StructField("key", IntegerType)))
          val keyToNumValuesValueSchema = StructType(Array(StructField("value", LongType)))
          val keyToNumValuesEncoderSpec = NoPrefixKeyStateEncoderSpec(keyToNumValuesKeySchema)

          val keyWithIndexKeySchema = StructType(Array(
            StructField("key", IntegerType, nullable = false),
            StructField("index", LongType)
          ))
          val keyWithIndexValueSchema = StructType(Array(
            StructField("value", IntegerType, nullable = false),
            StructField("time", TimestampType, nullable = false),
            StructField("matched", BooleanType)
          ))
          val keyWithIndexEncoderSpec = NoPrefixKeyStateEncoderSpec(keyWithIndexKeySchema)

          val keyToNumValuesColFamilyNames = Seq("left-keyToNumValues", "right-keyToNumValues")
          val keyWithIndexToValueColFamilyNames = Seq(
            "left-keyWithIndexToValue", "right-keyWithIndexToValue")
          // Build column family to schema map for all 4 join stores
          val columnFamilyToSchemaMap =
            keyToNumValuesColFamilyNames.map { name =>
              createSingleColumnFamilySchemaMap(
                keyToNumValuesKeySchema, keyToNumValuesValueSchema, keyToNumValuesEncoderSpec, name)
            }.reduce(_ ++ _) ++ keyWithIndexToValueColFamilyNames.map { name =>
              createSingleColumnFamilySchemaMap(
                keyWithIndexKeySchema, keyWithIndexValueSchema, keyWithIndexEncoderSpec, name)
            }.reduce(_ ++ _)
          val columnFamilyToReaderOptions =
            (keyToNumValuesColFamilyNames ++ keyWithIndexToValueColFamilyNames).map {
              colName =>
                colName -> Map(StateSourceOptions.STORE_NAME -> colName)
            }.toMap
          // Perform round-trip test using common helper
          performRoundTripTest(
            sourceDir.getAbsolutePath,
            targetDir.getAbsolutePath,
            columnFamilyToSchemaMap,
            columnFamilyToReaderOptions = columnFamilyToReaderOptions
          )
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
            createSingleColumnFamilySchemaMap(keySchema, valueSchema, keyStateEncoderSpec)
          )
        }
      }
    }
  }

  /**
   * Helper method to test round-trip for transformWithState with multiple column families.
   * Uses MultiStateVarProcessor which creates ValueState, ListState, and MapState.
   */
  private def testTransformWithStateMultiColumnFamilies(): Unit = {
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

        // Step 1: Add data to source
        runQuery(sourceDir.getAbsolutePath, 2)
        // Step 2: Add data to target
        runQuery(targetDir.getAbsolutePath, 1)

        // Step 3: Define schemas for all column families
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
          StructField("key", StringType),
          StructField("user_map_key", groupByKeySchema, nullable = true)
        ))
        val itemsMapValueSchema = StructType(Array(
          StructField("user_map_value", IntegerType, nullable = true)
        ))

        // Build column family to schema map for all 4 state variables
        val countStateEncoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
        val itemsMapEncoderSpec = PrefixKeyScanStateEncoderSpec(itemsMapKeySchema, 1)

        val columnFamilyToSchemaMap = HashMap(
          "countState" -> createColFamilyInfo(
            groupByKeySchema, countStateValueSchema, countStateEncoderSpec, "countState"),
          "itemsList" -> createColFamilyInfo(
            groupByKeySchema, itemsListValueSchema, countStateEncoderSpec, "itemsList", true),
          "$rowCounter_itemsList" -> createColFamilyInfo(
            groupByKeySchema, rowCounterValueSchema,
            countStateEncoderSpec, "$rowCounter_itemsList"),
          "itemsMap" -> createColFamilyInfo(
            itemsMapKeySchema, itemsMapValueSchema, itemsMapEncoderSpec, "itemsMap")
        )

        // Define custom selectExprs for column families with non-standard schemas
        val columnFamilyToSelectExprs = Map(
          "itemsList" -> Seq("key", "list_element AS value", "partition_id"),
          "itemsMap" -> Seq("STRUCT(key, user_map_key) AS key", "user_map_value AS value",
            "partition_id")
        )

        // Define reader options for column families that need them
        val columnFamilyToReaderOptions = Map(
          "itemsList" -> Map(StateSourceOptions.FLATTEN_COLLECTION_TYPES -> "true",
            StateSourceOptions.STATE_VAR_NAME -> "itemsList"),
          "itemsMap" -> Map(StateSourceOptions.STATE_VAR_NAME -> "itemsMap"),
          "countState" -> Map(StateSourceOptions.STATE_VAR_NAME -> "countState")
        )

        // Perform round-trip test using common helper
        performRoundTripTest(
          sourceDir.getAbsolutePath,
          targetDir.getAbsolutePath,
          columnFamilyToSchemaMap,
          columnFamilyToSelectExprs = columnFamilyToSelectExprs,
          columnFamilyToReaderOptions = columnFamilyToReaderOptions
        )
      }
    }
  }

  /**
   * Helper method to test round-trip for transformWithState with event time timers.
   */
  private def testEventTimeTimersRoundTrip(): Unit = {
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

        // Step 1: Create source checkpoint
        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = sourceDir.getAbsolutePath),
          AddData(inputData, ("a", 1L), ("b", 2L), ("c", 3L)),
          ProcessAllAvailable(),
          StopStream
        )

        // Step 2: Create target checkpoint with dummy data
        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = targetDir.getAbsolutePath),
          AddData(inputData, ("x", 1L)),
          ProcessAllAvailable(),
          StopStream
        )

        // Step 3: Define schemas for timer column families
        val groupByKeySchema = StructType(Array(
          StructField("key", StringType, nullable = true)
        ))
        val stateValueSchema = StructType(Array(
          StructField("value", LongType, nullable = true)
        ))
        val keyToTimestampKeySchema = StructType(Array(
          StructField("key", StringType),
          StructField("expiryTimestampMs", LongType, nullable = false)
        ))
        val timestampToKeyKeySchema = StructType(Array(
          StructField("expiryTimestampMs", LongType, nullable = false),
          StructField("key", StringType)
        ))
        val dummyValueSchema = StructType(Array(StructField("__dummy__", NullType)))

        val encoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
        val keyToTimestampEncoderSpec = PrefixKeyScanStateEncoderSpec(
          keyToTimestampKeySchema, 1)
        val timestampToKeyEncoderSpec = RangeKeyScanStateEncoderSpec(
          timestampToKeyKeySchema, Seq(0))

        val columnFamilyToSchemaMap = HashMap(
          "countState" -> createColFamilyInfo(
            groupByKeySchema, stateValueSchema, encoderSpec, "countState"),
          "$eventTimers_keyToTimestamp" -> createColFamilyInfo(
            keyToTimestampKeySchema, dummyValueSchema,
            keyToTimestampEncoderSpec, "$eventTimers_keyToTimestamp"),
          "$eventTimers_timestampToKey" -> createColFamilyInfo(
            timestampToKeyKeySchema, dummyValueSchema,
            timestampToKeyEncoderSpec, "$eventTimers_timestampToKey")
        )

        // Timer column families need special selectExprs
        val columnFamilyToSelectExprs = Map(
          "$eventTimers_keyToTimestamp" -> Seq(
            "STRUCT(key AS groupingKey, expiration_timestamp_ms AS key) AS key",
            "NULL AS value", "partition_id"),
          "$eventTimers_timestampToKey" -> Seq(
            "STRUCT(expiration_timestamp_ms AS key, key AS groupingKey) AS key",
            "NULL AS value", "partition_id")
        )

        // Timer column families need READ_REGISTERED_TIMERS option
        val columnFamilyToReaderOptions = Map(
          "countState" -> Map(StateSourceOptions.STATE_VAR_NAME -> "countState"),
          "$eventTimers_keyToTimestamp" -> Map(
            StateSourceOptions.READ_REGISTERED_TIMERS -> "true"),
          "$eventTimers_timestampToKey" -> Map(
            StateSourceOptions.READ_REGISTERED_TIMERS -> "true")
        )

        performRoundTripTest(
          sourceDir.getAbsolutePath,
          targetDir.getAbsolutePath,
          columnFamilyToSchemaMap,
          columnFamilyToSelectExprs = columnFamilyToSelectExprs,
          columnFamilyToReaderOptions = columnFamilyToReaderOptions
        )
      }
    }
  }

  /**
   * Helper method to test round-trip for transformWithState with processing time timers.
   */
  private def testProcessingTimeTimersRoundTrip(): Unit = {
    withTempDir { sourceDir =>
      withTempDir { targetDir =>
        val clock = new StreamManualClock
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorWithProcTimeTimer(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        // Step 1: Create source checkpoint
        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = sourceDir.getAbsolutePath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock),
          AddData(inputData, "a"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", "1")),
          StopStream
        )

        // Step 2: Create target checkpoint with dummy data
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

        // Step 3: Define schemas for timer column families
        val groupByKeySchema = StructType(Array(
          StructField("key", StringType, nullable = true)
        ))
        val stateValueSchema = StructType(Array(
          StructField("value", LongType, nullable = true)
        ))
        val keyToTimestampKeySchema = StructType(Array(
          StructField("key", StringType),
          StructField("expiryTimestampMs", LongType, nullable = false)
        ))
        val timestampToKeyKeySchema = StructType(Array(
          StructField("expiryTimestampMs", LongType, nullable = false),
          StructField("key", StringType)
        ))
        val dummyValueSchema = StructType(Array(StructField("__dummy__", NullType)))

        val encoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
        val keyToTimestampEncoderSpec = PrefixKeyScanStateEncoderSpec(keyToTimestampKeySchema, 1)
        val timestampToKeyEncoderSpec = RangeKeyScanStateEncoderSpec(
          timestampToKeyKeySchema, Seq(0))

        val columnFamilyToSchemaMap = HashMap(
          "countState" -> createColFamilyInfo(
            groupByKeySchema, stateValueSchema, encoderSpec, "countState"),
          "$procTimers_keyToTimestamp" -> createColFamilyInfo(
            keyToTimestampKeySchema, dummyValueSchema,
            keyToTimestampEncoderSpec, "$procTimers_keyToTimestamp"),
          "$procTimers_timestampToKey" -> createColFamilyInfo(
            timestampToKeyKeySchema, dummyValueSchema,
            timestampToKeyEncoderSpec, "$procTimers_timestampToKey")
        )

        // Timer column families need special selectExprs
        val columnFamilyToSelectExprs = Map(
          "$procTimers_keyToTimestamp" -> Seq(
            "STRUCT(key AS groupingKey, expiration_timestamp_ms AS key) AS key",
            "NULL AS value", "partition_id"),
          "$procTimers_timestampToKey" -> Seq(
            "STRUCT(expiration_timestamp_ms AS key, key AS groupingKey) AS key",
            "NULL AS value", "partition_id")
        )

        // Timer column families need READ_REGISTERED_TIMERS option
        val columnFamilyToReaderOptions = Map(
          "countState" -> Map(StateSourceOptions.STATE_VAR_NAME -> "countState"),
          "$procTimers_keyToTimestamp" -> Map(
            StateSourceOptions.READ_REGISTERED_TIMERS -> "true"),
          "$procTimers_timestampToKey" -> Map(
            StateSourceOptions.READ_REGISTERED_TIMERS -> "true")
        )

        performRoundTripTest(
          sourceDir.getAbsolutePath,
          targetDir.getAbsolutePath,
          columnFamilyToSchemaMap,
          columnFamilyToSelectExprs = columnFamilyToSelectExprs,
          columnFamilyToReaderOptions = columnFamilyToReaderOptions
        )
      }
    }
  }

  /**
   * Helper method to test round-trip for transformWithState with list state and TTL.
   */
  private def testListStateTTLRoundTrip(): Unit = {
    withTempDir { sourceDir =>
      withTempDir { targetDir =>
        val clock = new StreamManualClock
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new ListStateTTLProcessor(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        // Step 1: Create source checkpoint
        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = sourceDir.getAbsolutePath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock),
          AddData(inputData, "a", "b", "a"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", "2"), ("b", "1")),
          StopStream
        )

        // Step 2: Create target checkpoint with dummy data
        val clock2 = new StreamManualClock
        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = targetDir.getAbsolutePath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock2),
          AddData(inputData, "x"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", "2"), ("b", "1"), ("x", "1")),
          StopStream
        )

        // Step 3: Define schemas for list state with TTL column families
        val groupByKeySchema = StructType(Array(
          StructField("value", StringType)
        ))
        val listStateValueSchema = StructType(Array(
          StructField("value", StructType(Array(
            StructField("value", StringType)
          ))),
          StructField("ttlExpirationMs", LongType)
        ))
        // TTL index key schema: (expirationMs, groupingKey)
        val ttlIndexKeySchema = StructType(Array(
          StructField("expirationMs", LongType, nullable = false),
          StructField("elementKey", groupByKeySchema)
        ))
        // Min expiry key schema is same as groupByKeySchema
        val minExpiryValueSchema = StructType(Array(
          StructField("minExpiry", LongType)
        ))
        // Count index value schema
        val countValueSchema = StructType(Array(
          StructField("count", LongType)
        ))
        val dummyValueSchema = StructType(Array(StructField("__dummy__", NullType)))

        val encoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
        val ttlIndexEncoderSpec = RangeKeyScanStateEncoderSpec(ttlIndexKeySchema, Seq(0))

        val columnFamilyToSchemaMap = HashMap(
          "listState" -> createColFamilyInfo(
            groupByKeySchema, listStateValueSchema, encoderSpec, "listState", true),
          "$ttl_listState" -> createColFamilyInfo(
            ttlIndexKeySchema, dummyValueSchema, ttlIndexEncoderSpec, "$ttl_listState"),
          "$min_listState" -> createColFamilyInfo(
            groupByKeySchema, minExpiryValueSchema, encoderSpec, "$min_listState"),
          "$count_listState" -> createColFamilyInfo(
            groupByKeySchema, countValueSchema, encoderSpec, "$count_listState")
        )

        // listState needs FLATTEN_COLLECTION_TYPES and uses list_element column
        val columnFamilyToSelectExprs = Map(
          "listState" -> Seq("key", "list_element AS value", "partition_id")
        )

        val columnFamilyToReaderOptions = Map(
          "listState" -> Map(
            StateSourceOptions.STATE_VAR_NAME -> "listState",
            StateSourceOptions.FLATTEN_COLLECTION_TYPES -> "true")
        )

        performRoundTripTest(
          sourceDir.getAbsolutePath,
          targetDir.getAbsolutePath,
          columnFamilyToSchemaMap,
          columnFamilyToSelectExprs = columnFamilyToSelectExprs,
          columnFamilyToReaderOptions = columnFamilyToReaderOptions
        )
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
            createSingleColumnFamilySchemaMap(keySchema, valueSchema, keyStateEncoderSpec)
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
            createSingleColumnFamilySchemaMap(keySchema, valueSchema, keyStateEncoderSpec)
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
            createSingleColumnFamilySchemaMap(keySchema, valueSchema, keyStateEncoderSpec)
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
            createSingleColumnFamilySchemaMap(keySchema, valueSchema, keyStateEncoderSpec)
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
      testStreamStreamJoinV3RoundTrip()
    }

    testWithChangelogConfig("SPARK-54411: transformWithState with multiple column families") {
      testTransformWithStateMultiColumnFamilies()
    }

    testWithChangelogConfig("SPARK-54411: transformWithState with event time timers") {
      testEventTimeTimersRoundTrip()
    }

    testWithChangelogConfig("SPARK-54411: transformWithState with processing time timers") {
      testProcessingTimeTimersRoundTrip()
    }

    testWithChangelogConfig("SPARK-54411: transformWithState with list state and TTL") {
      testListStateTTLRoundTrip()
    }
  } // End of foreach loop for changelog checkpointing dimension
}
