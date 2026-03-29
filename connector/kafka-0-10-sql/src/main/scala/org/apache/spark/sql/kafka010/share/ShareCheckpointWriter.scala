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

package org.apache.spark.sql.kafka010.share

import java.io.{BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import scala.io.Source
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, MetadataVersionUtil}

/**
 * Writes and reads share group checkpoint state to/from HDFS-compatible storage.
 *
 * The checkpoint contains:
 * 1. Batch ID for ordering
 * 2. Share group ID
 * 3. Acquired record offsets per partition
 * 4. Acknowledgment state
 *
 * This is used for:
 * - Recovery after driver failure
 * - Exactly-once semantics (checkpoint-based deduplication)
 * - Progress tracking
 */
class ShareCheckpointWriter(metadataPath: String) extends Logging {

  private val VERSION = 1
  private val checkpointDir = s"$metadataPath/share-offsets"

  // Lazy initialization of file system
  private lazy val (hadoopConf, fs) = {
    val spark = SparkSession.active
    val conf = spark.sparkContext.hadoopConfiguration
    val fileSystem = FileSystem.get(new Path(checkpointDir).toUri, conf)
    (conf, fileSystem)
  }

  /**
   * Write the offset for a batch to the checkpoint.
   */
  def write(offset: KafkaShareSourceOffset): Unit = {
    val batchPath = new Path(checkpointDir, offset.batchId.toString)

    try {
      // Ensure parent directory exists
      val parentPath = batchPath.getParent
      if (!fs.exists(parentPath)) {
        fs.mkdirs(parentPath)
      }

      // Write to temporary file first
      val tempPath = new Path(s"${batchPath.toString}.tmp")
      val outputStream = fs.create(tempPath, true)
      val writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))

      try {
        writer.write(s"v$VERSION\n")
        writer.write(offset.json)
        writer.newLine()
      } finally {
        writer.close()
      }

      // Atomic rename
      if (fs.exists(batchPath)) {
        fs.delete(batchPath, false)
      }
      fs.rename(tempPath, batchPath)

      logDebug(s"Wrote checkpoint for batch ${offset.batchId}")
    } catch {
      case e: Exception =>
        logError(s"Failed to write checkpoint for batch ${offset.batchId}: ${e.getMessage}", e)
        throw e
    }
  }

  /**
   * Read the offset for a specific batch from the checkpoint.
   */
  def read(batchId: Long): Option[KafkaShareSourceOffset] = {
    val batchPath = new Path(checkpointDir, batchId.toString)

    if (!fs.exists(batchPath)) {
      return None
    }

    try {
      val inputStream = fs.open(batchPath)
      val reader = Source.fromInputStream(inputStream, StandardCharsets.UTF_8.name())

      try {
        val lines = reader.getLines().toList

        if (lines.isEmpty) {
          logWarning(s"Empty checkpoint file for batch $batchId")
          return None
        }

        // Validate version
        val versionLine = lines.head
        if (!versionLine.startsWith("v")) {
          logWarning(s"Invalid checkpoint version format: $versionLine")
          return None
        }

        val version = versionLine.substring(1).toInt
        if (version > VERSION) {
          logWarning(s"Checkpoint version $version is newer than supported version $VERSION")
          return None
        }

        // Parse offset JSON
        val json = lines.drop(1).mkString("\n")
        Some(KafkaShareSourceOffset(json))
      } finally {
        reader.close()
      }
    } catch {
      case e: Exception =>
        logError(s"Failed to read checkpoint for batch $batchId: ${e.getMessage}", e)
        None
    }
  }

  /**
   * Get the latest committed batch offset.
   */
  def getLatest(): Option[KafkaShareSourceOffset] = {
    try {
      val checkpointPath = new Path(checkpointDir)

      if (!fs.exists(checkpointPath)) {
        return None
      }

      // Find the highest batch ID
      val batchIds = fs.listStatus(checkpointPath)
        .filter(_.isFile)
        .flatMap { status =>
          Try(status.getPath.getName.toLong).toOption
        }
        .sorted
        .reverse

      batchIds.headOption.flatMap(read)
    } catch {
      case e: Exception =>
        logError(s"Failed to get latest checkpoint: ${e.getMessage}", e)
        None
    }
  }

  /**
   * Get all committed batch offsets.
   */
  def getAll(): Seq[KafkaShareSourceOffset] = {
    try {
      val checkpointPath = new Path(checkpointDir)

      if (!fs.exists(checkpointPath)) {
        return Seq.empty
      }

      fs.listStatus(checkpointPath)
        .filter(_.isFile)
        .flatMap { status =>
          Try(status.getPath.getName.toLong).toOption
        }
        .sorted
        .flatMap(read)
        .toSeq
    } catch {
      case e: Exception =>
        logError(s"Failed to get all checkpoints: ${e.getMessage}", e)
        Seq.empty
    }
  }

  /**
   * Purge old checkpoints, keeping only the most recent N.
   */
  def purge(keepCount: Int): Int = {
    try {
      val checkpointPath = new Path(checkpointDir)

      if (!fs.exists(checkpointPath)) {
        return 0
      }

      val batchIds = fs.listStatus(checkpointPath)
        .filter(_.isFile)
        .flatMap { status =>
          Try(status.getPath.getName.toLong).toOption
        }
        .sorted

      val toDelete = batchIds.dropRight(keepCount)

      toDelete.foreach { batchId =>
        val path = new Path(checkpointDir, batchId.toString)
        fs.delete(path, false)
        logDebug(s"Purged checkpoint for batch $batchId")
      }

      toDelete.length
    } catch {
      case e: Exception =>
        logError(s"Failed to purge checkpoints: ${e.getMessage}", e)
        0
    }
  }

  /**
   * Delete checkpoint for a specific batch.
   */
  def delete(batchId: Long): Boolean = {
    try {
      val batchPath = new Path(checkpointDir, batchId.toString)
      if (fs.exists(batchPath)) {
        fs.delete(batchPath, false)
        true
      } else {
        false
      }
    } catch {
      case e: Exception =>
        logError(s"Failed to delete checkpoint for batch $batchId: ${e.getMessage}", e)
        false
    }
  }
}

/**
 * Recovery manager for share group streaming queries.
 *
 * Handles recovery scenarios:
 * 1. Driver restart - resume from last committed batch
 * 2. Task failure - acquire locks expire, records redelivered
 * 3. Executor failure - same as task failure
 */
class ShareRecoveryManager(
    shareGroupId: String,
    checkpointPath: String) extends Logging {

  private val checkpointWriter = new ShareCheckpointWriter(checkpointPath)

  /**
   * Recover the streaming query state after a restart.
   *
   * @return The offset to start from, and any pending records to release
   */
  def recover(): RecoveryState = {
    logInfo(s"Starting recovery for share group $shareGroupId")

    val latestOffset = checkpointWriter.getLatest()

    latestOffset match {
      case Some(offset) =>
        logInfo(s"Recovered from batch ${offset.batchId} with ${offset.totalRecords} records")

        // Any acquired records in the checkpoint were not acknowledged
        // They should be released (Kafka will handle this via lock expiry)
        val pendingRecords = offset.getAllRecordKeys

        if (pendingRecords.nonEmpty) {
          logWarning(s"Found ${pendingRecords.size} unacknowledged records from batch ${offset.batchId}. " +
            "These will be redelivered by Kafka after lock expiry.")
        }

        RecoveryState(
          startBatchId = offset.batchId + 1,
          lastCommittedOffset = Some(offset),
          pendingRecords = pendingRecords,
          isCleanStart = false
        )

      case None =>
        logInfo("No checkpoint found, starting fresh")
        RecoveryState(
          startBatchId = 0,
          lastCommittedOffset = None,
          pendingRecords = Set.empty,
          isCleanStart = true
        )
    }
  }

  /**
   * Get the checkpoint writer for external use.
   */
  def getCheckpointWriter: ShareCheckpointWriter = checkpointWriter
}

/**
 * State recovered from checkpoint.
 */
case class RecoveryState(
    startBatchId: Long,
    lastCommittedOffset: Option[KafkaShareSourceOffset],
    pendingRecords: Set[RecordKey],
    isCleanStart: Boolean)

/**
 * Exception types for share group operations.
 */
object KafkaShareExceptions {

  class ShareGroupException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

  class AcquisitionLockExpiredException(recordKey: RecordKey)
    extends ShareGroupException(s"Acquisition lock expired for record: $recordKey")

  class AcknowledgmentFailedException(message: String, cause: Throwable = null)
    extends ShareGroupException(s"Failed to acknowledge: $message", cause)

  class CheckpointException(message: String, cause: Throwable = null)
    extends ShareGroupException(s"Checkpoint error: $message", cause)

  class RecoveryException(message: String, cause: Throwable = null)
    extends ShareGroupException(s"Recovery error: $message", cause)

  def acquisitionLockExpired(key: RecordKey): AcquisitionLockExpiredException =
    new AcquisitionLockExpiredException(key)

  def acknowledgmentFailed(message: String, cause: Throwable = null): AcknowledgmentFailedException =
    new AcknowledgmentFailedException(message, cause)

  def checkpointFailed(message: String, cause: Throwable = null): CheckpointException =
    new CheckpointException(message, cause)

  def recoveryFailed(message: String, cause: Throwable = null): RecoveryException =
    new RecoveryException(message, cause)
}

