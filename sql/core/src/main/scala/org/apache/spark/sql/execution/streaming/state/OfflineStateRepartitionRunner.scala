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

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming.checkpointing.{CommitMetadata, OffsetMap, OffsetSeq, OffsetSeqLog, OffsetSeqMetadata, OffsetSeqMetadataBase}
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryCheckpointMetadata
import org.apache.spark.sql.execution.streaming.utils.StreamingUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Runs repartitioning for the state stores used by a streaming query.
 *
 * This class handles the process of creating a new microbatch, repartitioning state data
 * across new partitions, and committing the changes to the checkpoint i.e.
 * if the last streaming batch was batch `N`, this will create batch `N+1` with the repartitioned
 * state. Note that this new batch doesn't read input data from sources, it only represents the
 * repartition operation. The next time the streaming query is started, it will pick up from
 * this new batch.
 *
 * @param sparkSession The active Spark session
 * @param checkpointLocation The checkpoint location path
 * @param numPartitions The new number of partitions to repartition to
 * @param enforceExactlyOnceSink if we shouldn't allow skipping failed batches,
 *                               to avoid duplicates in exactly once sinks.
 */
class OfflineStateRepartitionRunner(
    sparkSession: SparkSession,
    checkpointLocation: String,
    numPartitions: Int,
    enforceExactlyOnceSink: Boolean = true) extends Logging {

  import OfflineStateRepartitionUtils._

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  private val resolvedCpLocation = StreamingUtils.resolvedCheckpointLocation(
    hadoopConf, checkpointLocation)

  private val checkpointMetadata = new StreamingQueryCheckpointMetadata(
    sparkSession, resolvedCpLocation)

  /**
   * Runs a repartitioning batch and returns the batch ID.
   * This will only return when the repartitioning is done.
   *
   * @return The repartition batch ID
   */
  def run(): Long = {
    logInfo(log"Starting offline state repartitioning for " +
      log"checkpointLocation=${MDC(CHECKPOINT_LOCATION, checkpointLocation)}, " +
      log"numPartitions=${MDC(NUM_PARTITIONS, numPartitions)}, " +
      log"enforceExactlyOnceSink=${MDC(ENFORCE_EXACTLY_ONCE, enforceExactlyOnceSink)}")

    try {
      val (repartitionBatchId, durationMs) = Utils.timeTakenMs {
        val lastCommittedBatchId = getLastCommittedBatchId()
        val lastBatchId = getLastBatchId()

        val newBatchId = createNewBatchIfNeeded(lastBatchId, lastCommittedBatchId)

        // todo(SPARK-54365): Do the repartitioning here, in subsequent PR

        // todo(SPARK-54365): update operator metadata in subsequent PR.

        // Commit the repartition batch
        commitBatch(newBatchId, lastCommittedBatchId)
        newBatchId
      }

      logInfo(log"Completed state repartitioning for " +
        log"checkpointLocation=${MDC(CHECKPOINT_LOCATION, checkpointLocation)}, " +
        log"numPartitions=${MDC(NUM_PARTITIONS, numPartitions)}, " +
        log"enforceExactlyOnceSink=${MDC(ENFORCE_EXACTLY_ONCE, enforceExactlyOnceSink)}, " +
        log"repartitionBatchId=${MDC(BATCH_ID, repartitionBatchId)}, " +
        log"durationMs=${MDC(DURATION, durationMs)}")

      repartitionBatchId
    } catch {
      case e: Throwable =>
        logError(log"State repartitioning failed for " +
          log"checkpointLocation=${MDC(CHECKPOINT_LOCATION, checkpointLocation)}, " +
          log"numPartitions=${MDC(NUM_PARTITIONS, numPartitions)}", e)
        throw e
    }
  }

  private def getLastCommittedBatchId(): Long = {
    checkpointMetadata.commitLog.getLatestBatchId() match {
      case Some(id) => id
      // Needs at least 1 committed batch to repartition
      case None => throw OfflineStateRepartitionErrors.noCommittedBatchError(checkpointLocation)
    }
  }

  private def getLastBatchId(): Long = {
    checkpointMetadata.offsetLog.getLatestBatchId() match {
      case Some(id) => id
      case None => throw OfflineStateRepartitionErrors.noBatchFoundError(checkpointLocation)
    }
  }

  private def createNewBatchIfNeeded(lastBatchId: Long, lastCommittedBatchId: Long): Long = {
    if (lastBatchId == lastCommittedBatchId) {
      // Means there are no uncommitted batches. So start a new batch.
      createNewBatchFromLastCommitted(lastBatchId, lastCommittedBatchId)
    } else {
      // Means there are uncommitted batches.
      if (isRepartitionBatch(lastBatchId, checkpointMetadata.offsetLog, checkpointLocation)) {
        // If it is a failed repartition batch, lets check if the shuffle partitions
        // is the same as the requested. If same, then we can retry the batch.
        val lastBatch = checkpointMetadata.offsetLog.get(lastBatchId).get
        val lastBatchShufflePartitions = getShufflePartitions(
          lastBatch.metadataOpt.get).get
        if (lastBatchShufflePartitions == numPartitions) {
          // We can retry the repartition batch.
          logInfo(log"The last batch is a failed repartition batch " +
            log"(batchId=${MDC(BATCH_ID, lastBatchId)}). " +
            log"Retrying it since it used the same number of shuffle partitions " +
            log"as the requested ${MDC(NUM_PARTITIONS, numPartitions)}.")
          lastBatchId
        } else {
          // Failed repartition should be retried with the same number of shuffle partitions.
          // Once that completes successfully, then can repartition to another number
          // of shuffle partitions.
          throw OfflineStateRepartitionErrors.lastBatchAbandonedRepartitionError(
            checkpointLocation, lastBatchId, lastBatchShufflePartitions, numPartitions)
        }
      } else {
        if (enforceExactlyOnceSink) {
          // We want the last batch to have committed successfully.
          // Before proceeding with repartitioning, since repartitioning produces a new batch.
          // If we skip the unsuccessful batch, this can cause duplicates in exactly-once sinks
          // which uses the batchId to track already committed data.
          throw OfflineStateRepartitionErrors.lastBatchFailedError(checkpointLocation, lastBatchId)
        } else {
          // We can skip the uncommitted batches. And repartition using the last committed
          // batch state. Note that input data from the skipped failed batch will be reprocessed
          // in the next query run.
          skipUncommittedBatches(lastBatchId, lastCommittedBatchId)
          // Now create a new batch
          createNewBatchFromLastCommitted(lastBatchId, lastCommittedBatchId)
        }
      }
    }
  }

  private def skipUncommittedBatches(lastBatchId: Long, lastCommittedBatchId: Long): Unit = {
    assert(lastBatchId > lastCommittedBatchId,
      "Last batch ID must be greater than last committed batch ID")

    val fromBatchId = lastCommittedBatchId + 1
    for (batchId <- fromBatchId to lastBatchId) {
      // write empty commit for these skipped batches
      if (!checkpointMetadata.commitLog.add(batchId, CommitMetadata())) {
        throw QueryExecutionErrors.concurrentStreamLogUpdate(batchId)
      }
    }

    logInfo(log"Skipped uncommitted batches from batchId " +
      log"${MDC(BATCH_ID, fromBatchId)} to ${MDC(BATCH_ID, lastBatchId)}")
  }

  /**
   * Creates a new offset log entry for the repartition batch using the OffsetSeq
   * of the last committed batch. But with a new number of partitions.
   */
  private def createNewBatchFromLastCommitted(
      lastBatchId: Long,
      lastCommittedBatchId: Long): Long = {
    val newBatchId = lastBatchId + 1
    // We want to repartition the state as of the last committed batch.
    val lastCommittedOffsetSeq = checkpointMetadata.offsetLog.get(lastCommittedBatchId)
      .getOrElse(throw OfflineStateRepartitionErrors
        .offsetSeqNotFoundError(checkpointLocation, lastCommittedBatchId))

    // Missing offset metadata not supported
    val lastCommittedMetadata = lastCommittedOffsetSeq.metadataOpt.getOrElse(
      throw OfflineStateRepartitionErrors.missingOffsetSeqMetadataError(
        checkpointLocation, version = 1, batchId = lastCommittedBatchId)
    )

    // No-op if the number of shuffle partitions in last commit is the same as the requested.
    if (getShufflePartitions(lastCommittedMetadata).get == numPartitions) {
      throw OfflineStateRepartitionErrors.shufflePartitionsAlreadyMatchError(
        checkpointLocation, lastCommittedBatchId, numPartitions)
    }

    // Create a new offset log entry from the last committed but with updated num shuffle partitions
    val newOffsetSeq = lastCommittedOffsetSeq match {
      case v1: OffsetSeq =>
        val metadata = v1.metadataOpt.get.asInstanceOf[OffsetSeqMetadata]
        v1.copy(metadataOpt = Some(metadata.copy(
          conf = metadata.conf + (SQLConf.SHUFFLE_PARTITIONS.key -> numPartitions.toString))))
      case v2: OffsetMap =>
        v2.copy(metadata = v2.metadata.copy(
          conf = v2.metadata.conf + (SQLConf.SHUFFLE_PARTITIONS.key -> numPartitions.toString)))
      case _ => throw OfflineStateRepartitionErrors.unsupportedOffsetSeqVersionError(
        checkpointLocation, version = -1)
    }

    // Will fail if there is a concurrent operation on going
    if (!checkpointMetadata.offsetLog.add(newBatchId, newOffsetSeq)) {
      throw QueryExecutionErrors.concurrentStreamLogUpdate(newBatchId)
    }

    logInfo(log"Created new offset log entry for repartition batch. " +
      log"batchId=${MDC(BATCH_ID, newBatchId)}")

    newBatchId
  }

  private def commitBatch(newBatchId: Long, lastCommittedBatchId: Long): Unit = {
    val latestCommit = checkpointMetadata.commitLog.get(lastCommittedBatchId).get

    // todo: For checkpoint v2, we need to update the stateUniqueIds based on the
    //  newly created state commit. Will be done in subsequent PR.
    if (!checkpointMetadata.commitLog.add(newBatchId, latestCommit)) {
      throw QueryExecutionErrors.concurrentStreamLogUpdate(newBatchId)
    }
  }
}

object OfflineStateRepartitionUtils {
  def isRepartitionBatch(
      batchId: Long, offsetLog: OffsetSeqLog, checkpointLocation: String): Boolean = {
    assert(batchId >= 0, "Batch ID must be non-negative")
    batchId match {
      // first batch can never be a repartition batch since we require at least one committed batch
      case 0 => false
      case _ =>
        // A repartition batch is a batch where the number of shuffle partitions changed
        // compared to the previous batch.
        val batch = offsetLog.get(batchId).getOrElse(throw OfflineStateRepartitionErrors
          .offsetSeqNotFoundError(checkpointLocation, batchId))
        val prevBatchId = batchId - 1
        val previousBatch = offsetLog.get(prevBatchId).getOrElse(
          throw OfflineStateRepartitionErrors
            .offsetSeqNotFoundError(checkpointLocation, prevBatchId))

        // Determine version from the batch type
        val batchVersion = batch match {
          case _: OffsetSeq => 1
          case _: OffsetMap => 2
          case _ => -1
        }
        val batchMetadata = batch.metadataOpt.getOrElse(
          throw OfflineStateRepartitionErrors.missingOffsetSeqMetadataError(
            checkpointLocation, version = batchVersion, batchId = batchId))
        val shufflePartitions = getShufflePartitions(batchMetadata).get

        val prevBatchVersion = previousBatch match {
          case _: OffsetSeq => 1
          case _: OffsetMap => 2
          case _ => -1
        }
        val previousBatchMetadata = previousBatch.metadataOpt.getOrElse(
          throw OfflineStateRepartitionErrors.missingOffsetSeqMetadataError(
            checkpointLocation, version = prevBatchVersion, batchId = prevBatchId))
        val previousShufflePartitions = getShufflePartitions(previousBatchMetadata).get

        previousShufflePartitions != shufflePartitions
    }
  }

  def getShufflePartitions(metadata: OffsetSeqMetadataBase): Option[Int] = {
    metadata.conf.get(SQLConf.SHUFFLE_PARTITIONS.key).map(_.toInt)
  }
}
