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

import java.util.UUID

import scala.collection.immutable.HashMap

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryCheckpointMetadata}
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, RocksDBStateStoreProvider, StateStore, StateStoreConf, StateStoreId}
import org.apache.spark.sql.execution.streaming.utils.StreamingUtils
import org.apache.spark.sql.functions.{count, max, min, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, LongType, NullType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

/**
 * Test suite for StatePartitionAllColumnFamiliesWriter.
 * Tests the writer's ability to correctly write raw bytes read from
 * StatePartitionAllColumnFamiliesReader to a new state store location.
 */
class StatePartitionAllColumnFamiliesWriterSuite extends StateDataSourceTestBase {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      classOf[RocksDBStateStoreProvider].getName)
  }

  test("write works with dropDuplicates operator") {
    withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      withTempDir { sourceDir =>
        withTempDir { targetDir =>

          // Step 1: Create state by running a streaming aggregation
          runDropDuplicatesQuery(sourceDir.getAbsolutePath)
          val inputData: MemoryStream[Int] = MemoryStream[Int]
          val stream = getDropDuplicatesQuery(inputData)
          println("Step 1:  Create state by running a streaming aggregation ====")
          testStream(stream, OutputMode.Append)(
            StartStream(checkpointLocation = targetDir.getAbsolutePath),
            AddData(inputData, (1 to 5).flatMap(_ => (10 to 15)): _*),
            CheckAnswer(10 to 15: _*),
            assertNumStateRows(total = 6, updated = 6)
          )

          // Step 2: Read original state using normal reader (for comparison later)"
          println("STEP 2: ReAD OG data")
          val sourceNormalData = spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, sourceDir.getAbsolutePath)
            .load()
            .selectExpr("key", "value", "partition_id")
            .collect()

          // Step 3: Read from source using AllColumnFamiliesReader (raw bytes)
          println("STEP 3: READ BYTES DATA")
          val sourceBytesData = spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, sourceDir.getAbsolutePath)
            .option(StateSourceOptions.INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES, "true")
            .load()

          // Verify schema of raw bytes
          val schema = sourceBytesData.schema
          assert(schema.fieldNames === Array(
            "partition_key", "key_bytes", "value_bytes", "column_family_name"))

          // Step 4: Write raw bytes to target checkpoint location
          println("STEP 4: WRITE BYTES DATA")
          val hadoopConf = spark.sessionState.newHadoopConf()
          val resolvedCpLocation = StreamingUtils.resolvedCheckpointLocation(
            hadoopConf, targetDir.getAbsolutePath)
          val checkpointMetadata = new StreamingQueryCheckpointMetadata(
            spark, resolvedCpLocation)
          val lastBatch = checkpointMetadata.commitLog.getLatestBatchId().get

          val targetOffsetSeq = checkpointMetadata.offsetLog.get(lastBatch).get
          checkpointMetadata.offsetLog.add(lastBatch + 1, targetOffsetSeq)

          // Define schemas for state version 2 (aggregation state without key columns in value)
          val keySchema = StructType(Array(
            StructField("value", IntegerType, nullable = false),
            StructField("eventTime", org.apache.spark.sql.types.TimestampType)
          ))
          val valueSchema = StructType(Array(
            StructField("__dummy__", NullType, nullable = true)
          ))

          // Create column family to schema map
          // Maps column family name to (keySchema, valueSchema) tuple
          val columnFamilyToSchemaMap = HashMap(
            StateStore.DEFAULT_COL_FAMILY_NAME -> (keySchema, valueSchema)
          )

          // Create key state encoder spec (no prefix key for simple aggregation)
          val keyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema)

          // Create StateSourceOptions for the target checkpoint
          // These options tell the writer where and how to write the state
          val targetStateSourceOptions = StateSourceOptions(
            resolvedCpLocation = resolvedCpLocation,
            batchId = lastBatch,
            operatorId = 0,
            storeName = StateStoreId.DEFAULT_STORE_NAME,
            joinSide = StateSourceOptions.JoinSideValues.none,
            readChangeFeed = false,
            fromSnapshotOptions = None,
            readChangeFeedOptions = None,
            stateVarName = None,
            readRegisteredTimers = false,
            flattenCollectionTypes = true,
            internalOnlyReadAllColumnFamilies = true
          )

          val storeConf: StateStoreConf = StateStoreConf(SQLConf.get)
          // Wrap hadoopConf in SerializableConfiguration for task serialization
          val serializableHadoopConf = new SerializableConfiguration(hadoopConf)

          // Generate a dummy query ID
          val queryId = UUID.randomUUID()
          // Define the partition processing function
          val func: Iterator[Row] => Unit = partition => {
            // partition_id is first field in partition_key struct
            val partitionInfo = new StateStoreInputPartition(
              TaskContext.getPartitionId(), queryId, targetStateSourceOptions
            )
            val allCFWriter = new StatePartitionAllColumnFamiliesWriter(
              storeConf, serializableHadoopConf.value, partitionInfo,
              columnFamilyToSchemaMap, keyStateEncoderSpec, None, None
            )
            allCFWriter.write(partition)
          }
          // Write raw bytes to target using foreachPartition (for side effects)
          sourceBytesData.foreachPartition(func)
          // commit to commitLog
          val latestCommit = checkpointMetadata.commitLog.get(lastBatch).get
          checkpointMetadata.commitLog.add(lastBatch + 1, latestCommit)
          println("STEP 5: READ UPDATED BYTES DATA")
          // Step 5: Read from target using normal reader
          val targetNormalData = spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, targetDir.getAbsolutePath)
            .load()
            .selectExpr("key", "value", "partition_id")
            .collect()

          // Step 6: Verify data matches
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
      }
    }
  }


  test("round-trip: read raw bytes and write to new location") {
    withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      withTempDir { sourceDir =>
        withTempDir { targetDir =>
          // Step 1: Create state by running a streaming aggregation
          runLargeDataStreamingAggregationQuery(sourceDir.getAbsolutePath)
          val inputData: MemoryStream[Int] = MemoryStream[Int]
          val aggregated = inputData.toDF()
            .selectExpr("value", "value % 10 AS groupKey")
            .groupBy("groupKey")
            .agg(
              count("*").as("cnt"),
              sum("value").as("sum"),
              max("value").as("max"),
              min("value").as("min")
            )
            .as[(Int, Long, Long, Int, Int)]

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

          // Step 2: Read original state using normal reader (for comparison later)"
          val sourceNormalData = spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, sourceDir.getAbsolutePath)
            .load()
            .selectExpr("key", "value", "partition_id")
            .collect()

          // Step 3: Read from source using AllColumnFamiliesReader (raw bytes)
          val sourceBytesData = spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, sourceDir.getAbsolutePath)
            .option(StateSourceOptions.INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES, "true")
            .load()

          // Verify schema of raw bytes
          val schema = sourceBytesData.schema
          assert(schema.fieldNames === Array(
            "partition_key", "key_bytes", "value_bytes", "column_family_name"))

          // Step 4: Write raw bytes to target checkpoint location
          val hadoopConf = spark.sessionState.newHadoopConf()
          val resolvedCpLocation = StreamingUtils.resolvedCheckpointLocation(
            hadoopConf, targetDir.getAbsolutePath)
          val checkpointMetadata = new StreamingQueryCheckpointMetadata(
            spark, resolvedCpLocation)
          val lastBatch = checkpointMetadata.commitLog.getLatestBatchId().get
          val targetOffsetSeq = checkpointMetadata.offsetLog.get(lastBatch).get
          checkpointMetadata.offsetLog.add(lastBatch + 1, targetOffsetSeq)

          // Define schemas for state version 2 (aggregation state without key columns in value)
          val keySchema = StructType(Array(StructField("groupKey", IntegerType, nullable = false)))
          val valueSchema = StructType(Array(
            StructField("count", LongType, nullable = false),
            StructField("sum", LongType, nullable = false),
            StructField("max", IntegerType, nullable = false),
            StructField("min", IntegerType, nullable = false)
          ))

          // Create column family to schema map
          // Maps column family name to (keySchema, valueSchema) tuple
          val columnFamilyToSchemaMap = HashMap(
            StateStore.DEFAULT_COL_FAMILY_NAME -> (keySchema, valueSchema)
          )

          // Create key state encoder spec (no prefix key for simple aggregation)
          val keyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(keySchema)

          // Create StateSourceOptions for the target checkpoint
          // These options tell the writer where and how to write the state
          val targetStateSourceOptions = StateSourceOptions(
            resolvedCpLocation = resolvedCpLocation,
            batchId = lastBatch,
            operatorId = 0,
            storeName = StateStoreId.DEFAULT_STORE_NAME,
            joinSide = StateSourceOptions.JoinSideValues.none,
            readChangeFeed = false,
            fromSnapshotOptions = None,
            readChangeFeedOptions = None,
            stateVarName = None,
            readRegisteredTimers = false,
            flattenCollectionTypes = true,
            internalOnlyReadAllColumnFamilies = true
          )

          val storeConf: StateStoreConf = StateStoreConf(SQLConf.get)
          // Wrap hadoopConf in SerializableConfiguration for task serialization
          val serializableHadoopConf = new SerializableConfiguration(hadoopConf)

          // Generate a dummy query ID
          val queryId = UUID.randomUUID()
          // Define the partition processing function
          val func: Iterator[Row] => Unit = partition => {
            // partition_id is first field in partition_key struct
            val partitionInfo = new StateStoreInputPartition(
              TaskContext.getPartitionId(), queryId, targetStateSourceOptions
            )
            val allCFWriter = new StatePartitionAllColumnFamiliesWriter(
              storeConf, serializableHadoopConf.value, partitionInfo,
              columnFamilyToSchemaMap, keyStateEncoderSpec, None, None
            )
            allCFWriter.write(partition)
          }
          // Write raw bytes to target using foreachPartition (for side effects)
          sourceBytesData.foreachPartition(func)
          // commit to commitLog
          val latestCommit = checkpointMetadata.commitLog.get(lastBatch).get
          checkpointMetadata.commitLog.add(lastBatch + 1, latestCommit)
          // Step 5: Read from target using normal reader
          val targetNormalData = spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, targetDir.getAbsolutePath)
            .load()
            .selectExpr("key", "value", "partition_id")
            .collect()

          // Step 6: Verify data matches
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
      }
    }
  }
}
