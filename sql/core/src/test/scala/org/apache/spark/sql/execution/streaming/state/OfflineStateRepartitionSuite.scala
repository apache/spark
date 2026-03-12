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

import scala.util.Try

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.state.metadata.StateMetadataPartitionReader
import org.apache.spark.sql.execution.streaming.checkpointing.{CommitLog, CommitMetadata}
import org.apache.spark.sql.execution.streaming.operators.stateful.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryCheckpointMetadata}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.util.SerializableConfiguration

/**
 * Test for offline state repartitioning. This tests that repartition behaves as expected
 * for different scenarios.
 */
class OfflineStateRepartitionSuite extends StreamTest
  with AlsoTestWithRocksDBFeatures {
  import testImplicits._
  import OfflineStateRepartitionUtils._
  import OfflineStateRepartitionTestUtils._

  test("Fail if empty checkpoint directory") {
    withTempDir { dir =>
      val ex = intercept[StateRepartitionNoCommittedBatchError] {
        spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, 5)
      }

      checkError(
        ex,
        condition = "STATE_REPARTITION_INVALID_CHECKPOINT.NO_COMMITTED_BATCH",
        parameters = Map(
          "checkpointLocation" -> dir.getAbsolutePath
        )
      )
    }
  }

  test("Fail if no batch found in checkpoint directory") {
    withTempDir { dir =>
      // Write commit log but no offset log.
      val commitLog = new CommitLog(spark, dir.getCanonicalPath + "/commits")
      commitLog.add(0, CommitMetadata())

      val ex = intercept[StateRepartitionNoBatchFoundError] {
        spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, 5)
      }

      checkError(
        ex,
        condition = "STATE_REPARTITION_INVALID_CHECKPOINT.NO_BATCH_FOUND",
        parameters = Map(
          "checkpointLocation" -> dir.getAbsolutePath
        )
      )
    }
  }

  test("Fails if repartition parameter is invalid") {
    val ex1 = intercept[StateRepartitionParameterIsNullError] {
      spark.streamingCheckpointManager.repartition(null, 5)
    }

    checkError(
      ex1,
      condition = "STATE_REPARTITION_INVALID_PARAMETER.IS_NULL",
      parameters = Map("parameter" -> "checkpointLocation")
    )

    val ex2 = intercept[StateRepartitionParameterIsEmptyError] {
      spark.streamingCheckpointManager.repartition("", 5)
    }

    checkError(
      ex2,
      condition = "STATE_REPARTITION_INVALID_PARAMETER.IS_EMPTY",
      parameters = Map("parameter" -> "checkpointLocation")
    )

    val ex3 = intercept[StateRepartitionParameterIsNotGreaterThanZeroError] {
      spark.streamingCheckpointManager.repartition("test", 0)
    }

    checkError(
      ex3,
      condition = "STATE_REPARTITION_INVALID_PARAMETER.IS_NOT_GREATER_THAN_ZERO",
      parameters = Map("parameter" -> "numPartitions")
    )
  }

  Seq(1, 2).foreach { ckptVersion =>
    def testWithCheckpointId(testName: String)(testFun: => Unit): Unit = {
      test(s"$testName (enableCkptId = ${ckptVersion >= 2})") {
        withSQLConf(
          SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> ckptVersion.toString) {
          testFun
        }
      }
    }

    testWithCheckpointId("Repartition: success, failure, retry") {
      withTempDir { dir =>
        val originalPartitions = 3
        val input = MemoryStream[Int]
        val batchId = runSimpleStreamQuery(originalPartitions, dir.getAbsolutePath, input)
        val checkpointMetadata = new StreamingQueryCheckpointMetadata(spark, dir.getAbsolutePath)
        // Shouldn't be seen as a repartition batch
        assert(!isRepartitionBatch(batchId, checkpointMetadata.offsetLog))

        // Trying to repartition to the same number should fail
        val ex = intercept[StateRepartitionShufflePartitionsAlreadyMatchError] {
          spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, originalPartitions)
        }
        checkError(
          ex,
          condition = "STATE_REPARTITION_INVALID_CHECKPOINT.SHUFFLE_PARTITIONS_ALREADY_MATCH",
          parameters = Map(
            "checkpointLocation" -> dir.getAbsolutePath,
            "batchId" -> batchId.toString,
            "numPartitions" -> originalPartitions.toString
          )
        )

        // Trying to repartition to a different number should succeed
        val newPartitions = originalPartitions + 1
        spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, newPartitions)
        val repartitionBatchId = batchId + 1
        val hadoopConf = spark.sessionState.newHadoopConf()
        verifyRepartitionBatch(
          repartitionBatchId,
          checkpointMetadata,
          hadoopConf,
          dir.getAbsolutePath,
          newPartitions,
          spark)

        // Now delete the repartition commit to simulate a failed repartition attempt.
        // This will delete all the commits after the batchId.
        checkpointMetadata.commitLog.purgeAfter(batchId)

        // Try to repartition with a different numPartitions should fail,
        // since it will see an uncommitted repartition batch with a different numPartitions.
        val ex2 = intercept[StateRepartitionLastBatchAbandonedRepartitionError] {
          spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, newPartitions + 1)
        }
        checkError(
          ex2,
          condition = "STATE_REPARTITION_INVALID_CHECKPOINT.LAST_BATCH_ABANDONED_REPARTITION",
          parameters = Map(
            "checkpointLocation" -> dir.getAbsolutePath,
            "lastBatchId" -> repartitionBatchId.toString,
            "lastBatchShufflePartitions" -> newPartitions.toString,
            "numPartitions" -> (newPartitions + 1).toString
          )
        )

        // Retrying with the same numPartitions should work
        spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, newPartitions)
        verifyRepartitionBatch(
          repartitionBatchId,
          checkpointMetadata,
          hadoopConf,
          dir.getAbsolutePath,
          newPartitions,
          spark)

        // Repartition with way more partitions, to verify that empty partitions are properly
        // created
        val morePartitions = newPartitions * 3
        spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, morePartitions)
        verifyRepartitionBatch(
          repartitionBatchId + 1, checkpointMetadata, hadoopConf,
          dir.getAbsolutePath, morePartitions, spark)
        // Restart the query to make sure it can start after repartitioning
        runSimpleStreamQuery(morePartitions, dir.getAbsolutePath, input)
      }
    }

    testWithCheckpointId("Query last batch failed before repartitioning") {
      withTempDir { dir =>
        val originalPartitions = 3
        val input = MemoryStream[Int]
        // Run 3 batches
        val firstBatchId = 0
        val lastBatchId = firstBatchId + 2
        (firstBatchId to lastBatchId).foreach { _ =>
          runSimpleStreamQuery(originalPartitions, dir.getAbsolutePath, input)
        }
        val checkpointMetadata = new StreamingQueryCheckpointMetadata(spark, dir.getAbsolutePath)

        // Lets keep only the first commit to simulate multiple failed batches
        checkpointMetadata.commitLog.purgeAfter(firstBatchId)

        // Now repartitioning should fail
        val ex = intercept[StateRepartitionLastBatchFailedError] {
          spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, originalPartitions + 1)
        }
        checkError(
          ex,
          condition = "STATE_REPARTITION_INVALID_CHECKPOINT.LAST_BATCH_FAILED",
          parameters = Map(
            "checkpointLocation" -> dir.getAbsolutePath,
            "lastBatchId" -> lastBatchId.toString
          )
        )

        // Setting enforceExactlyOnceSink to false should allow repartitioning
        spark.streamingCheckpointManager.repartition(
          dir.getAbsolutePath, originalPartitions + 1, enforceExactlyOnceSink = false)
        verifyRepartitionBatch(
          lastBatchId + 1,
          checkpointMetadata,
          spark.sessionState.newHadoopConf(),
          dir.getAbsolutePath,
          originalPartitions + 1,
          spark,
          // Repartition should be based on the first batch, since we skipped the others
          baseBatchId = Some(firstBatchId))
      }
    }

    testWithCheckpointId("Consecutive repartition") {
      withTempDir { dir =>
        val originalPartitions = 5
        val input = MemoryStream[Int]
        val batchId = runSimpleStreamQuery(originalPartitions, dir.getAbsolutePath, input)

        val checkpointMetadata = new StreamingQueryCheckpointMetadata(spark, dir.getAbsolutePath)
        val hadoopConf = spark.sessionState.newHadoopConf()

        // decrease
        spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, originalPartitions - 3)
        verifyRepartitionBatch(
          batchId + 1,
          checkpointMetadata,
          hadoopConf,
          dir.getAbsolutePath,
          originalPartitions - 3,
          spark
        )

        // increase
        spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, originalPartitions + 1)
        verifyRepartitionBatch(
          batchId + 2,
          checkpointMetadata,
          hadoopConf,
          dir.getAbsolutePath,
          originalPartitions + 1,
          spark
        )

        // Restart the query to make sure it can start after repartitioning
        runSimpleStreamQuery(originalPartitions + 1, dir.getAbsolutePath, input)
      }
    }
  }

  private def runSimpleStreamQuery(
      numPartitions: Int,
      checkpointLocation: String,
      input: MemoryStream[Int] = MemoryStream[Int]): Long = {
    val conf = Map(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> numPartitions.toString)

    var committedBatchId: Long = -1
    // Set the confs before starting the stream
    withSQLConf(conf.toSeq: _*) {
      testStream(input.toDF().groupBy("value").count(), outputMode = OutputMode.Update)(
        StartStream(checkpointLocation = checkpointLocation),
        AddData(input, 1, 2, 3),
        ProcessAllAvailable(),
        Execute { query =>
          committedBatchId = Option(query.lastProgress).map(_.batchId).getOrElse(-1)
        }
      )
    }

    assert(committedBatchId >= 0, "No batch was committed in the streaming query")
    committedBatchId
  }
}

object OfflineStateRepartitionTestUtils {
  import OfflineStateRepartitionUtils._

  def verifyRepartitionBatch(
      batchId: Long,
      checkpointMetadata: StreamingQueryCheckpointMetadata,
      hadoopConf: Configuration,
      checkpointLocation: String,
      expectedShufflePartitions: Int,
      spark: SparkSession,
      baseBatchId: Option[Long] = None): Unit = {
    // Should be seen as a repartition batch
    assert(isRepartitionBatch(batchId, checkpointMetadata.offsetLog))

    // When failed batches are skipped, then repartition can be based
    // on an older batch and not batchId - 1.
    val previousBatchId = baseBatchId.getOrElse(batchId - 1)

    verifyOffsetAndCommitLog(
      batchId, previousBatchId, expectedShufflePartitions, checkpointMetadata)
    verifyPartitionDirs(checkpointLocation, expectedShufflePartitions)

    val serializableConf = new SerializableConfiguration(hadoopConf)
    val baseOperatorsMetadata = getOperatorMetadata(
      checkpointLocation, serializableConf, previousBatchId)
    val repartitionOperatorsMetadata = getOperatorMetadata(
      checkpointLocation, serializableConf, batchId)
    verifyOperatorMetadata(
      baseOperatorsMetadata, repartitionOperatorsMetadata, expectedShufflePartitions)
    if (StatefulOperatorStateInfo.enableStateStoreCheckpointIds(spark.sessionState.conf)) {
      verifyCheckpointIds(
        batchId,
        checkpointMetadata,
        expectedShufflePartitions,
        baseOperatorsMetadata)
    }
  }

  private def verifyOffsetAndCommitLog(
      repartitionBatchId: Long,
      previousBatchId: Long,
      expectedShufflePartitions: Int,
      checkpointMetadata: StreamingQueryCheckpointMetadata): Unit = {
    // Verify the repartition batch
    val lastBatchId = checkpointMetadata.offsetLog.getLatestBatchId().get
    assert(lastBatchId == repartitionBatchId,
      "The latest batch in offset log should be the repartition batch")

    val lastBatch = checkpointMetadata.offsetLog.get(lastBatchId).get
    val lastBatchShufflePartitions = getShufflePartitions(lastBatch.metadataOpt.get).get
    assert(lastBatchShufflePartitions == expectedShufflePartitions)

    // Verify the commit log
    val lastCommitId = checkpointMetadata.commitLog.getLatestBatchId().get
    assert(lastCommitId == repartitionBatchId,
      "The latest batch in commit log should be the repartition batch")

    // verify that the offset seq is the same between repartition batch and
    // the batch the repartition is based on except for the shuffle partitions.
    val previousBatch = checkpointMetadata.offsetLog.get(previousBatchId).get

    // Verify offsets are identical
    assert(lastBatch.offsets == previousBatch.offsets,
      s"Offsets should be identical between batch $previousBatchId and $repartitionBatchId")

    // Verify metadata is the same except for shuffle partitions config
    (lastBatch.metadataOpt, previousBatch.metadataOpt) match {
      case (Some(lastMetadata), Some(previousMetadata)) =>
        // Check watermark and timestamp are the same
        assert(lastMetadata.batchWatermarkMs == previousMetadata.batchWatermarkMs,
          "Batch watermark should be the same")
        assert(lastMetadata.batchTimestampMs == previousMetadata.batchTimestampMs,
          "Batch timestamp should be the same")

        // Check all configs are the same except shuffle partitions
        val lastConfWithoutShufflePartitions =
          lastMetadata.conf - SQLConf.SHUFFLE_PARTITIONS.key
        val previousConfWithoutShufflePartitions =
          previousMetadata.conf - SQLConf.SHUFFLE_PARTITIONS.key
        assert(lastConfWithoutShufflePartitions == previousConfWithoutShufflePartitions,
          "All configs except shuffle partitions should be the same")

        // Verify shuffle partitions are different
        assert(
          getShufflePartitions(lastMetadata).get != getShufflePartitions(previousMetadata).get,
          "Shuffle partitions should be different between batches")
      case _ =>
        assert(false, "Both batches should have metadata")
    }
  }

  // verify number of partition dirs in state dir
  private def verifyPartitionDirs(
      checkpointLocation: String,
      expectedShufflePartitions: Int): Unit = {
    val stateDir = new java.io.File(checkpointLocation, "state")

    def numDirs(file: java.io.File): Int = {
      file.listFiles()
        .filter(d => d.isDirectory && Try(d.getName.toInt).isSuccess)
        .length
    }

    val numOperators = numDirs(stateDir)
    for (op <- 0 until numOperators) {
      val partitionsDir = new java.io.File(stateDir, s"$op")
      val numPartitions = numDirs(partitionsDir)
      // Doing <= in case of reduced number of partitions
      assert(expectedShufflePartitions <= numPartitions,
        s"Expected atleast $expectedShufflePartitions partition dirs for operator $op," +
          s" but found $numPartitions")
    }
  }

  private def getOperatorMetadata(
      checkpointLocation: String,
      serializableConf: SerializableConfiguration,
      batchId: Long
    ): Array[OperatorStateMetadata] = {
    val metadataPartitionReader = new StateMetadataPartitionReader(
      checkpointLocation, serializableConf, batchId)
    metadataPartitionReader.allOperatorStateMetadata
  }

  private def verifyOperatorMetadata(
      baseOperatorsMetadata: Array[OperatorStateMetadata],
      repartitionOperatorsMetadata: Array[OperatorStateMetadata],
      expectedShufflePartitions: Int): Unit = {
    assert(baseOperatorsMetadata.nonEmpty, "Base batch should have operator metadata")
    assert(repartitionOperatorsMetadata.nonEmpty, "Repartition batch should have operator metadata")
    assert(baseOperatorsMetadata.length == repartitionOperatorsMetadata.length,
      "Both batches should have the same number of operators")

    // Verify each operator's metadata
    baseOperatorsMetadata.zip(repartitionOperatorsMetadata).foreach {
      case (baseOp, repartitionOp) =>
        // Verify both are of the same type
        assert(baseOp.getClass == repartitionOp.getClass,
          s"Metadata types should match: base=${baseOp.getClass.getSimpleName}, " +
            s"repartition=${repartitionOp.getClass.getSimpleName}")

        (baseOp, repartitionOp) match {
          case (baseV2: OperatorStateMetadataV2, repartitionV2: OperatorStateMetadataV2) =>
            // Verify operator info is the same
            assert(baseV2.operatorInfo == repartitionV2.operatorInfo,
              s"Operator info should match: base=${baseV2.operatorInfo}, " +
                s"repartition=${repartitionV2.operatorInfo}")

            // Verify operator properties JSON is the same
            assert(baseV2.operatorPropertiesJson == repartitionV2.operatorPropertiesJson,
              "Operator properties JSON should match")

            // Verify state store info (except numPartitions)
            assert(baseV2.stateStoreInfo.length == repartitionV2.stateStoreInfo.length,
              "Should have same number of state stores")

            baseV2.stateStoreInfo.zip(repartitionV2.stateStoreInfo).foreach {
              case (baseStore, repartitionStore) =>
                assert(baseStore.storeName == repartitionStore.storeName,
                  s"Store name should match: ${baseStore.storeName} " +
                    s"vs ${repartitionStore.storeName}")
                assert(baseStore.numColsPrefixKey == repartitionStore.numColsPrefixKey,
                  "numColsPrefixKey should match")
                // Schema file paths should be the same (they reference the same schema files)
                assert(baseStore.stateSchemaFilePaths == repartitionStore.stateSchemaFilePaths,
                  "State schema file paths should match")
                assert(baseStore.numPartitions != repartitionStore.numPartitions,
                  "numPartitions shouldn't be the same")
                // Verify numPartitions is updated to expectedShufflePartitions
                assert(repartitionStore.numPartitions == expectedShufflePartitions,
                  s"Repartition batch numPartitions should be $expectedShufflePartitions, " +
                    s"but found ${repartitionStore.numPartitions}")
            }

          case (baseV1: OperatorStateMetadataV1, repartitionV1: OperatorStateMetadataV1) =>
            // For v1, since we didn't update it, then it should be the same.
            // Can't use == directly because Array uses reference equality
            assert(baseV1.operatorInfo == repartitionV1.operatorInfo,
              "V1 operator info should be the same")
            assert(baseV1.stateStoreInfo.sameElements(repartitionV1.stateStoreInfo),
              "V1 state store info should be the same")

          case _ =>
            assert(false,
              s"Unexpected metadata types: base=${baseOp.getClass.getSimpleName}, " +
                s"repartition=${repartitionOp.getClass.getSimpleName}")
        }
    }
  }

  private def verifyCheckpointIds(
      repartitionBatchId: Long,
      checkpointMetadata: StreamingQueryCheckpointMetadata,
      expectedShufflePartitions: Int,
      baseOperatorsMetadata: Array[OperatorStateMetadata]): Unit = {
    val expectedStoreCnts: Map[Long, Int] = baseOperatorsMetadata.map {
      case metadataV2: OperatorStateMetadataV2 =>
        metadataV2.operatorInfo.operatorId -> metadataV2.stateStoreInfo.length
      case metadataV1: OperatorStateMetadataV1 =>
        metadataV1.operatorInfo.operatorId -> metadataV1.stateStoreInfo.length
    }.toMap
    // Verify commit log has the repartition batch with checkpoint IDs
    val commitOpt = checkpointMetadata.commitLog.get(repartitionBatchId)
    assert(commitOpt.isDefined, s"Commit for batch $repartitionBatchId should exist")

    val commitMetadata = commitOpt.get

    // Verify stateUniqueIds is present for checkpoint V2
    assert(commitMetadata.stateUniqueIds.isDefined,
      "stateUniqueIds should be present in commit metadata when checkpoint version >= 2")

    val operatorIdToCkptInfos = commitMetadata.stateUniqueIds.get
    assert(operatorIdToCkptInfos.nonEmpty,
      "operatorIdToCkptInfos should not be empty")

    // Verify structure for each operator
    operatorIdToCkptInfos.foreach { case (operatorId, partitionToCkptIds) =>
      // Should have checkpoint IDs for all partitions
      assert(partitionToCkptIds.length == expectedShufflePartitions,
        s"Operator $operatorId: Expected $expectedShufflePartitions partition checkpoint IDs, " +
          s"but found ${partitionToCkptIds.length}")
      // Each partition should have checkpoint IDs (at least one per store)
      partitionToCkptIds.zipWithIndex.foreach { case (ckptIds, partitionId) =>
        assert(ckptIds.length == expectedStoreCnts(operatorId),
            s"Operator $operatorId, partition $partitionId should" +
            s"has ${expectedStoreCnts(operatorId)} checkpoint Ids")
      }
    }
  }
}
