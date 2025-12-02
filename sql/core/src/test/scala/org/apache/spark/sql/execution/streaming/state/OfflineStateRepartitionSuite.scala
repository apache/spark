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

import org.apache.spark.sql.execution.streaming.checkpointing.{CommitLog, CommitMetadata}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryCheckpointMetadata}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._

/**
 * Test for offline state repartitioning. This tests that repartition behaves as expected
 * for different scenarios.
 */
class OfflineStateRepartitionSuite extends StreamTest {
  import testImplicits._
  import OfflineStateRepartitionUtils._

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

  test("Repartition: success, failure, retry") {
    withTempDir { dir =>
      val originalPartitions = 3
      val batchId = runSimpleStreamQuery(originalPartitions, dir.getAbsolutePath)
      val checkpointMetadata = new StreamingQueryCheckpointMetadata(spark, dir.getAbsolutePath)
      // Shouldn't be seen as a repartition batch
      assert(!isRepartitionBatch(batchId, checkpointMetadata.offsetLog, dir.getAbsolutePath))

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
      verifyRepartitionBatch(
        repartitionBatchId, checkpointMetadata, dir.getAbsolutePath, newPartitions)

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
        repartitionBatchId, checkpointMetadata, dir.getAbsolutePath, newPartitions)
    }
  }

  test("Query last batch failed before repartitioning") {
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
        dir.getAbsolutePath,
        originalPartitions + 1,
        // Repartition should be based on the first batch, since we skipped the others
        baseBatchId = Some(firstBatchId))
    }
  }

  test("Consecutive repartition") {
    withTempDir { dir =>
      val originalPartitions = 3
      val batchId = runSimpleStreamQuery(originalPartitions, dir.getAbsolutePath)

      val checkpointMetadata = new StreamingQueryCheckpointMetadata(spark, dir.getAbsolutePath)

      // decrease
      spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, originalPartitions - 1)
      verifyRepartitionBatch(
        batchId + 1,
        checkpointMetadata,
        dir.getAbsolutePath,
        originalPartitions - 1
      )

      // increase
      spark.streamingCheckpointManager.repartition(dir.getAbsolutePath, originalPartitions + 1)
      verifyRepartitionBatch(
        batchId + 2,
        checkpointMetadata,
        dir.getAbsolutePath,
        originalPartitions + 1
      )
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
    testStream(input.toDF().groupBy().count(), outputMode = OutputMode.Update)(
      StartStream(checkpointLocation = checkpointLocation, additionalConfs = conf),
      AddData(input, 1, 2, 3),
      ProcessAllAvailable(),
      Execute { query =>
        committedBatchId = Option(query.lastProgress).map(_.batchId).getOrElse(-1)
      }
    )

    assert(committedBatchId >= 0, "No batch was committed in the streaming query")
    committedBatchId
  }

  private def verifyRepartitionBatch(
      batchId: Long,
      checkpointMetadata: StreamingQueryCheckpointMetadata,
      checkpointLocation: String,
      expectedShufflePartitions: Int,
      baseBatchId: Option[Long] = None): Unit = {
    // Should be seen as a repartition batch
    assert(isRepartitionBatch(batchId, checkpointMetadata.offsetLog, checkpointLocation))

    // Verify the repartition batch
    val lastBatchId = checkpointMetadata.offsetLog.getLatestBatchId().get
    assert(lastBatchId == batchId)

    val lastBatch = checkpointMetadata.offsetLog.get(lastBatchId).get
    val lastBatchShufflePartitions = getShufflePartitions(lastBatch.metadataOpt.get).get
    assert(lastBatchShufflePartitions == expectedShufflePartitions)

    // Verify the commit log
    val lastCommitId = checkpointMetadata.commitLog.getLatestBatchId().get
    assert(lastCommitId == batchId)

    // verify that the offset seq is the same between repartition batch and
    // the batch the repartition is based on except for the shuffle partitions.
    // When failed batches are skipped, then repartition can be based
    // on an older batch and not batchId - 1.
    val previousBatchId = baseBatchId.getOrElse(batchId - 1)
    val previousBatch = checkpointMetadata.offsetLog.get(previousBatchId).get

    // Verify offsets are identical
    assert(lastBatch.offsets == previousBatch.offsets,
      s"Offsets should be identical between batch $previousBatchId and $batchId")

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
        fail("Both batches should have metadata")
    }
  }
}
