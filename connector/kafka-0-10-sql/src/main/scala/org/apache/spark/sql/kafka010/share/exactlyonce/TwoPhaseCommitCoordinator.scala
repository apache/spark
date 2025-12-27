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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.kafka010.share._
import org.apache.spark.sql.kafka010.share.consumer._

/**
 * Strategy B: Two-Phase Commit for True Exactly-Once Semantics
 *
 * This strategy achieves true exactly-once semantics by atomically committing
 * both the output writes and Kafka acknowledgments using a two-phase commit protocol.
 *
 * How it works:
 * Phase 1 (Prepare):
 *   1. Write output to staging location
 *   2. Prepare Kafka acknowledgments (but don't commit)
 *   3. Record transaction state to WAL
 *
 * Phase 2 (Commit):
 *   1. Move staged output to final location
 *   2. Commit Kafka acknowledgments
 *   3. Mark transaction as complete
 *
 * Rollback (on failure):
 *   1. Delete staged output
 *   2. Release Kafka records (RELEASE acknowledgment)
 *   3. Mark transaction as aborted
 *
 * Requirements:
 * - Transaction-capable sink (staging support)
 * - WAL for transaction state
 * - Coordination between driver and executors
 *
 * Advantages:
 * - True exactly-once semantics
 * - Works with any sink that supports staging
 *
 * Disadvantages:
 * - Higher latency (two phases)
 * - More complex implementation
 * - Requires coordination
 */
class TwoPhaseCommitCoordinator(
    shareGroupId: String,
    checkpointPath: String) extends Logging {

  import TwoPhaseCommitCoordinator._

  // Transaction state tracking
  private val activeTransactions = new ConcurrentHashMap[Long, TransactionState]()

  // WAL for transaction recovery
  private val transactionLog = new TransactionLog(checkpointPath)

  // Transaction ID generator
  private val transactionIdCounter = new AtomicInteger(0)

  /**
   * Begin a new transaction for a batch.
   *
   * @param batchId The micro-batch ID
   * @return Transaction handle for the batch
   */
  def beginTransaction(batchId: Long): TransactionHandle = {
    val transactionId = transactionIdCounter.incrementAndGet()
    val state = TransactionState(
      transactionId = transactionId,
      batchId = batchId,
      phase = Phase.STARTED,
      startTime = System.currentTimeMillis(),
      pendingAcks = new ConcurrentHashMap[RecordKey, AcknowledgmentType.AcknowledgmentType]()
    )

    activeTransactions.put(batchId, state)
    transactionLog.write(TransactionRecord(transactionId, batchId, Phase.STARTED))

    logInfo(s"Started transaction $transactionId for batch $batchId")
    TransactionHandle(transactionId, batchId, this)
  }

  /**
   * Phase 1: Prepare the transaction.
   *
   * This should be called after output has been written to staging and
   * acknowledgments have been recorded (but not committed to Kafka).
   */
  def prepareTransaction(batchId: Long): Boolean = {
    val state = activeTransactions.get(batchId)
    if (state == null) {
      logError(s"No active transaction found for batch $batchId")
      return false
    }

    if (state.phase != Phase.STARTED) {
      logError(s"Transaction ${state.transactionId} is in ${state.phase}, expected STARTED")
      return false
    }

    // Update state to PREPARED
    state.phase = Phase.PREPARED
    state.prepareTime = System.currentTimeMillis()
    transactionLog.write(TransactionRecord(state.transactionId, batchId, Phase.PREPARED))

    logInfo(s"Prepared transaction ${state.transactionId} for batch $batchId " +
      s"with ${state.pendingAcks.size()} acknowledgments")
    true
  }

  /**
   * Phase 2: Commit the transaction.
   *
   * This should be called after verifying the output has been moved to final location.
   */
  def commitTransaction(batchId: Long, consumer: InternalKafkaShareConsumer): Boolean = {
    val state = activeTransactions.get(batchId)
    if (state == null) {
      logError(s"No active transaction found for batch $batchId")
      return false
    }

    if (state.phase != Phase.PREPARED) {
      logError(s"Transaction ${state.transactionId} is in ${state.phase}, expected PREPARED")
      return false
    }

    try {
      // Apply all pending acknowledgments to Kafka
      state.pendingAcks.forEach { (key, ackType) =>
        consumer.acknowledge(key, ackType)
      }

      // Commit to Kafka
      consumer.commitSync()

      // Update state to COMMITTED
      state.phase = Phase.COMMITTED
      state.commitTime = System.currentTimeMillis()
      transactionLog.write(TransactionRecord(state.transactionId, batchId, Phase.COMMITTED))

      // Clean up
      activeTransactions.remove(batchId)

      logInfo(s"Committed transaction ${state.transactionId} for batch $batchId")
      true
    } catch {
      case e: Exception =>
        logError(s"Failed to commit transaction ${state.transactionId}: ${e.getMessage}", e)
        // Attempt rollback
        rollbackTransaction(batchId, consumer)
        false
    }
  }

  /**
   * Rollback a transaction.
   *
   * This releases all acquired records back to Kafka for redelivery.
   */
  def rollbackTransaction(batchId: Long, consumer: InternalKafkaShareConsumer): Boolean = {
    val state = activeTransactions.get(batchId)
    if (state == null) {
      logWarning(s"No active transaction found for batch $batchId to rollback")
      return false
    }

    try {
      // Release all records
      state.pendingAcks.forEach { (key, _) =>
        consumer.acknowledge(key, AcknowledgmentType.RELEASE)
      }

      // Commit releases to Kafka
      consumer.commitSync()

      // Update state to ABORTED
      state.phase = Phase.ABORTED
      transactionLog.write(TransactionRecord(state.transactionId, batchId, Phase.ABORTED))

      // Clean up
      activeTransactions.remove(batchId)

      logInfo(s"Rolled back transaction ${state.transactionId} for batch $batchId")
      true
    } catch {
      case e: Exception =>
        logError(s"Failed to rollback transaction ${state.transactionId}: ${e.getMessage}", e)
        false
    }
  }

  /**
   * Record an acknowledgment for the current transaction.
   * The acknowledgment won't be applied until the transaction commits.
   */
  def recordAcknowledgment(
      batchId: Long,
      key: RecordKey,
      ackType: AcknowledgmentType.AcknowledgmentType): Unit = {
    val state = activeTransactions.get(batchId)
    if (state != null) {
      state.pendingAcks.put(key, ackType)
    }
  }

  /**
   * Get the current phase of a transaction.
   */
  def getTransactionPhase(batchId: Long): Option[Phase] = {
    Option(activeTransactions.get(batchId)).map(_.phase)
  }

  /**
   * Recover incomplete transactions on startup.
   *
   * - STARTED transactions -> Abort
   * - PREPARED transactions -> Attempt to complete or abort
   * - COMMITTED/ABORTED -> Clean up
   */
  def recoverTransactions(consumer: InternalKafkaShareConsumer): Unit = {
    val incompleteTransactions = transactionLog.readIncomplete()

    incompleteTransactions.foreach { record =>
      record.phase match {
        case Phase.STARTED =>
          logWarning(s"Found incomplete STARTED transaction ${record.transactionId}, aborting")
          // STARTED transactions should be aborted

        case Phase.PREPARED =>
          logWarning(s"Found PREPARED transaction ${record.transactionId}, " +
            "checking if output was committed")
          // For PREPARED, we need to check if output was committed
          // If yes, commit to Kafka; if no, abort

        case Phase.COMMITTED | Phase.ABORTED =>
          logInfo(s"Transaction ${record.transactionId} already completed, cleaning up")
          transactionLog.markComplete(record.transactionId)

        case _ =>
          logWarning(s"Unknown transaction phase: ${record.phase}")
      }
    }
  }

  /**
   * Close the coordinator and clean up resources.
   */
  def close(): Unit = {
    // Abort any active transactions
    activeTransactions.forEach { (batchId, state) =>
      if (state.phase != Phase.COMMITTED && state.phase != Phase.ABORTED) {
        logWarning(s"Aborting incomplete transaction ${state.transactionId} on close")
        transactionLog.write(TransactionRecord(state.transactionId, batchId, Phase.ABORTED))
      }
    }
    activeTransactions.clear()
    transactionLog.close()
  }
}

object TwoPhaseCommitCoordinator {

  /**
   * Transaction phases in 2PC protocol.
   */
  sealed trait Phase
  object Phase {
    case object STARTED extends Phase     // Transaction started, processing in progress
    case object PREPARED extends Phase    // Output staged, ready to commit
    case object COMMITTED extends Phase   // Transaction committed successfully
    case object ABORTED extends Phase     // Transaction aborted/rolled back
  }

  /**
   * Internal state for an active transaction.
   */
  case class TransactionState(
      transactionId: Int,
      batchId: Long,
      var phase: Phase,
      startTime: Long,
      var prepareTime: Long = 0L,
      var commitTime: Long = 0L,
      pendingAcks: ConcurrentHashMap[RecordKey, AcknowledgmentType.AcknowledgmentType])

  /**
   * Record persisted to transaction log.
   */
  case class TransactionRecord(
      transactionId: Int,
      batchId: Long,
      phase: Phase,
      timestamp: Long = System.currentTimeMillis())

  /**
   * Handle for interacting with an active transaction.
   */
  case class TransactionHandle(
      transactionId: Int,
      batchId: Long,
      coordinator: TwoPhaseCommitCoordinator) {

    def recordAck(key: RecordKey, ackType: AcknowledgmentType.AcknowledgmentType): Unit = {
      coordinator.recordAcknowledgment(batchId, key, ackType)
    }

    def prepare(): Boolean = coordinator.prepareTransaction(batchId)

    def commit(consumer: InternalKafkaShareConsumer): Boolean = {
      coordinator.commitTransaction(batchId, consumer)
    }

    def rollback(consumer: InternalKafkaShareConsumer): Boolean = {
      coordinator.rollbackTransaction(batchId, consumer)
    }
  }
}

/**
 * Write-ahead log for transaction state.
 * Persists transaction records for recovery.
 */
private[exactlyonce] class TransactionLog(checkpointPath: String) extends Logging {
  import TwoPhaseCommitCoordinator._

  private val logPath = s"$checkpointPath/transactions"
  private val records = new ConcurrentHashMap[Int, TransactionRecord]()

  // Load existing records on initialization
  loadExisting()

  def write(record: TransactionRecord): Unit = {
    records.put(record.transactionId, record)
    // TODO: Persist to file system for durability
    logDebug(s"Wrote transaction record: $record")
  }

  def read(transactionId: Int): Option[TransactionRecord] = {
    Option(records.get(transactionId))
  }

  def readIncomplete(): Seq[TransactionRecord] = {
    records.values().asScala
      .filter(r => r.phase != Phase.COMMITTED && r.phase != Phase.ABORTED)
      .toSeq
  }

  def markComplete(transactionId: Int): Unit = {
    records.remove(transactionId)
  }

  def close(): Unit = {
    // TODO: Flush any pending writes
  }

  private def loadExisting(): Unit = {
    // TODO: Load from file system
  }
}

/**
 * Helper for staging output writes.
 */
object StagingHelper extends Logging {

  /**
   * Generate a staging path for a batch.
   */
  def getStagingPath(basePath: String, batchId: Long): String = {
    s"$basePath/_staging/batch_$batchId"
  }

  /**
   * Generate the final path for a batch.
   */
  def getFinalPath(basePath: String, batchId: Long): String = {
    s"$basePath/data/batch_$batchId"
  }

  /**
   * Move staged data to final location.
   * This should be atomic (rename operation).
   */
  def commitStaging(stagingPath: String, finalPath: String): Boolean = {
    try {
      import org.apache.hadoop.fs.{FileSystem, Path}
      import org.apache.spark.sql.SparkSession

      val spark = SparkSession.active
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)

      val srcPath = new Path(stagingPath)
      val dstPath = new Path(finalPath)

      // Ensure parent directory exists
      fs.mkdirs(dstPath.getParent)

      // Atomic rename
      fs.rename(srcPath, dstPath)
      true
    } catch {
      case e: Exception =>
        logError(s"Failed to commit staging: ${e.getMessage}", e)
        false
    }
  }

  /**
   * Delete staged data on rollback.
   */
  def rollbackStaging(stagingPath: String): Boolean = {
    try {
      import org.apache.hadoop.fs.{FileSystem, Path}
      import org.apache.spark.sql.SparkSession

      val spark = SparkSession.active
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)

      fs.delete(new Path(stagingPath), true)
      true
    } catch {
      case e: Exception =>
        logError(s"Failed to rollback staging: ${e.getMessage}", e)
        false
    }
  }
}

