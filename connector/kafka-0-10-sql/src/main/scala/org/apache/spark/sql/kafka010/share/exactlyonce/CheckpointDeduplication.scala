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

package org.apache.spark.sql.kafka010.share.exactlyonce

import java.io.{DataInputStream, DataOutputStream}
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.JsonDSL._

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.kafka010.share._

/**
 * Strategy C: Checkpoint-Based Deduplication for Exactly-Once Semantics
 *
 * This strategy achieves exactly-once semantics by tracking processed record IDs
 * in Spark's checkpoint and skipping redelivered records.
 *
 * How it works:
 * 1. When a record is acquired, check if it's in the processed set
 * 2. If already processed, skip it (and ACCEPT immediately)
 * 3. If not processed, process it and add to processed set
 * 4. Periodically persist processed set to checkpoint
 *
 * State Management:
 * - ProcessedRecords: Records that have been successfully processed
 * - PendingRecords: Records currently being processed (for recovery)
 *
 * Recovery:
 * - On restart, load processed records from checkpoint
 * - Release any pending records (they weren't committed)
 *
 * Requirements:
 * - Checkpoint storage (HDFS, S3, etc.)
 *
 * Advantages:
 * - Works with any sink
 * - Medium complexity
 * - Built on Spark's checkpoint mechanism
 *
 * Disadvantages:
 * - State growth over time (needs cleanup)
 * - Checkpoint overhead
 * - Memory usage for in-memory dedup set
 */
class CheckpointDedupManager(checkpointPath: String) extends Logging {

  implicit val formats: Formats = DefaultFormats

  private val processedRecordsPath = s"$checkpointPath/processed_records"
  private val pendingRecordsPath = s"$checkpointPath/pending_records"
  private val metadataPath = s"$checkpointPath/dedup_metadata"

  // In-memory cache of processed records
  private val processedRecords = ConcurrentHashMap.newKeySet[RecordKey]()

  // Currently pending records (being processed in current batch)
  private val pendingRecords = ConcurrentHashMap.newKeySet[RecordKey]()

  // Batch tracking
  private var currentBatchId: Long = -1
  private var lastCleanupBatchId: Long = -1

  // Configuration
  private val maxProcessedRecords = 1000000 // Max records to keep in memory
  private val cleanupIntervalBatches = 100   // Cleanup every N batches

  /**
   * Initialize the dedup manager by loading state from checkpoint.
   */
  def initialize(): Unit = {
    loadProcessedRecords()
    recoverPendingRecords()
    logInfo(s"Initialized checkpoint dedup manager with ${processedRecords.size()} processed records")
  }

  /**
   * Check if a record has already been processed.
   */
  def isProcessed(key: RecordKey): Boolean = {
    processedRecords.contains(key)
  }

  /**
   * Mark a record as pending (currently being processed).
   */
  def markPending(key: RecordKey): Unit = {
    pendingRecords.add(key)
  }

  /**
   * Mark a record as processed (successfully completed).
   */
  def markProcessed(key: RecordKey): Unit = {
    pendingRecords.remove(key)
    processedRecords.add(key)
  }

  /**
   * Mark multiple records as processed.
   */
  def markProcessed(keys: Iterable[RecordKey]): Unit = {
    keys.foreach { key =>
      pendingRecords.remove(key)
      processedRecords.add(key)
    }
  }

  /**
   * Get pending records that need to be released on failure.
   */
  def getPendingRecords: Set[RecordKey] = {
    pendingRecords.asScala.toSet
  }

  /**
   * Clear pending records (after successful commit).
   */
  def clearPending(): Unit = {
    pendingRecords.clear()
  }

  /**
   * Start a new batch.
   */
  def startBatch(batchId: Long): Unit = {
    currentBatchId = batchId

    // Check if cleanup is needed
    if (batchId - lastCleanupBatchId >= cleanupIntervalBatches) {
      cleanupOldRecords()
      lastCleanupBatchId = batchId
    }
  }

  /**
   * Commit a batch - persist processed records to checkpoint.
   */
  def commitBatch(batchId: Long): Unit = {
    // Move pending to processed
    val pendingSnapshot = pendingRecords.asScala.toSet
    pendingSnapshot.foreach { key =>
      processedRecords.add(key)
      pendingRecords.remove(key)
    }

    // Persist to checkpoint
    persistProcessedRecords()

    logDebug(s"Committed batch $batchId with ${pendingSnapshot.size} records")
  }

  /**
   * Rollback a batch - clear pending records.
   */
  def rollbackBatch(batchId: Long): Set[RecordKey] = {
    val rollbackRecords = pendingRecords.asScala.toSet
    pendingRecords.clear()
    logWarning(s"Rolled back batch $batchId, ${rollbackRecords.size} records to release")
    rollbackRecords
  }

  /**
   * Cleanup old processed records to prevent unbounded growth.
   *
   * Strategy: Keep only records from recent batches based on offset ranges.
   */
  private def cleanupOldRecords(): Unit = {
    val sizeBefore = processedRecords.size()

    if (sizeBefore > maxProcessedRecords) {
      // Group by topic-partition and keep only recent offsets
      val grouped = processedRecords.asScala.groupBy(r => (r.topic, r.partition))
      val cleaned = ConcurrentHashMap.newKeySet[RecordKey]()

      grouped.foreach { case ((topic, partition), records) =>
        // Keep records with highest offsets
        val sorted = records.toSeq.sortBy(_.offset).reverse
        val toKeep = sorted.take(maxProcessedRecords / grouped.size)
        toKeep.foreach(cleaned.add)
      }

      processedRecords.clear()
      cleaned.forEach(processedRecords.add)

      logInfo(s"Cleaned up processed records: $sizeBefore -> ${processedRecords.size()}")
    }
  }

  /**
   * Persist processed records to checkpoint storage.
   */
  private def persistProcessedRecords(): Unit = {
    try {
      val spark = SparkSession.active
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)

      val path = new Path(processedRecordsPath)
      fs.mkdirs(path.getParent)

      val tempPath = new Path(s"$processedRecordsPath.tmp")
      val out = new DataOutputStream(fs.create(tempPath, true))

      try {
        // Write version
        out.writeInt(1)

        // Write count
        out.writeInt(processedRecords.size())

        // Write each record key
        processedRecords.forEach { key =>
          out.writeUTF(key.topic)
          out.writeInt(key.partition)
          out.writeLong(key.offset)
        }
      } finally {
        out.close()
      }

      // Atomic rename
      fs.delete(path, false)
      fs.rename(tempPath, path)

      logDebug(s"Persisted ${processedRecords.size()} processed records to checkpoint")
    } catch {
      case e: Exception =>
        logError(s"Failed to persist processed records: ${e.getMessage}", e)
    }
  }

  /**
   * Load processed records from checkpoint storage.
   */
  private def loadProcessedRecords(): Unit = {
    try {
      val spark = SparkSession.active
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)

      val path = new Path(processedRecordsPath)
      if (!fs.exists(path)) {
        logInfo("No existing processed records checkpoint found")
        return
      }

      val in = new DataInputStream(fs.open(path))
      try {
        // Read version
        val version = in.readInt()
        require(version == 1, s"Unsupported checkpoint version: $version")

        // Read count
        val count = in.readInt()

        // Read each record key
        (0 until count).foreach { _ =>
          val topic = in.readUTF()
          val partition = in.readInt()
          val offset = in.readLong()
          processedRecords.add(RecordKey(topic, partition, offset))
        }

        logInfo(s"Loaded $count processed records from checkpoint")
      } finally {
        in.close()
      }
    } catch {
      case e: Exception =>
        logError(s"Failed to load processed records: ${e.getMessage}", e)
    }
  }

  /**
   * Recover from pending records checkpoint.
   * Pending records from a failed batch should be released.
   */
  private def recoverPendingRecords(): Unit = {
    try {
      val spark = SparkSession.active
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)

      val path = new Path(pendingRecordsPath)
      if (!fs.exists(path)) {
        return
      }

      val in = new DataInputStream(fs.open(path))
      val recoveredPending = scala.collection.mutable.Set[RecordKey]()

      try {
        val version = in.readInt()
        val count = in.readInt()

        (0 until count).foreach { _ =>
          val topic = in.readUTF()
          val partition = in.readInt()
          val offset = in.readLong()
          recoveredPending.add(RecordKey(topic, partition, offset))
        }
      } finally {
        in.close()
      }

      if (recoveredPending.nonEmpty) {
        logWarning(s"Found ${recoveredPending.size} pending records from previous run - " +
          "these will need to be released")
        // Note: The caller should release these records
        recoveredPending.foreach(pendingRecords.add)
      }

      // Delete pending checkpoint after recovery
      fs.delete(path, false)
    } catch {
      case e: Exception =>
        logError(s"Failed to recover pending records: ${e.getMessage}", e)
    }
  }

  /**
   * Persist pending records for crash recovery.
   */
  def persistPendingRecords(): Unit = {
    if (pendingRecords.isEmpty) return

    try {
      val spark = SparkSession.active
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)

      val path = new Path(pendingRecordsPath)
      fs.mkdirs(path.getParent)

      val out = new DataOutputStream(fs.create(path, true))
      try {
        out.writeInt(1) // version
        out.writeInt(pendingRecords.size())

        pendingRecords.forEach { key =>
          out.writeUTF(key.topic)
          out.writeInt(key.partition)
          out.writeLong(key.offset)
        }
      } finally {
        out.close()
      }
    } catch {
      case e: Exception =>
        logError(s"Failed to persist pending records: ${e.getMessage}", e)
    }
  }

  /**
   * Close the manager and persist final state.
   */
  def close(): Unit = {
    persistProcessedRecords()
    persistPendingRecords()
  }

  /**
   * Get statistics about the dedup manager.
   */
  def getStats: DedupStats = DedupStats(
    processedCount = processedRecords.size(),
    pendingCount = pendingRecords.size(),
    currentBatchId = currentBatchId,
    lastCleanupBatchId = lastCleanupBatchId
  )
}

/**
 * Statistics for the checkpoint dedup manager.
 */
case class DedupStats(
    processedCount: Int,
    pendingCount: Int,
    currentBatchId: Long,
    lastCleanupBatchId: Long)

/**
 * Checkpoint state for a share group batch.
 * Stored in Spark's checkpoint for recovery.
 */
case class ShareBatchCheckpoint(
    batchId: Long,
    shareGroupId: String,
    processedRecords: Set[RecordKey],
    pendingRecords: Set[RecordKey],
    timestamp: Long = System.currentTimeMillis()) {

  implicit val formats: Formats = DefaultFormats

  def toJson: String = {
    compact(render(
      ("batchId" -> batchId) ~
      ("shareGroupId" -> shareGroupId) ~
      ("processedRecords" -> processedRecords.map(_.toString).toList) ~
      ("pendingRecords" -> pendingRecords.map(_.toString).toList) ~
      ("timestamp" -> timestamp)
    ))
  }
}

object ShareBatchCheckpoint {
  implicit val formats: Formats = DefaultFormats

  def fromJson(json: String): ShareBatchCheckpoint = {
    val parsed = parse(json)
    val batchId = (parsed \ "batchId").extract[Long]
    val shareGroupId = (parsed \ "shareGroupId").extract[String]
    val processedRecords = (parsed \ "processedRecords").extract[List[String]]
      .map(RecordKey.fromString).toSet
    val pendingRecords = (parsed \ "pendingRecords").extract[List[String]]
      .map(RecordKey.fromString).toSet
    val timestamp = (parsed \ "timestamp").extract[Long]

    ShareBatchCheckpoint(batchId, shareGroupId, processedRecords, pendingRecords, timestamp)
  }
}

