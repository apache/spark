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

import java.{util => ju}
import java.util.Optional
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Network.NETWORK_TIMEOUT
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming._
import org.apache.spark.sql.kafka010.share.consumer.AcknowledgmentMode
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * A [[MicroBatchStream]] that reads data from Kafka share groups.
 *
 * Unlike [[org.apache.spark.sql.kafka010.KafkaMicroBatchStream]] which tracks sequential offsets
 * per partition, this implementation:
 *
 * 1. Subscribes to topics via share groups (multiple consumers can read from same partition)
 * 2. Uses batch IDs for ordering instead of Kafka offsets
 * 3. Tracks non-sequential acquired record offsets
 * 4. Supports acknowledgment-based commit (ACCEPT/RELEASE/REJECT)
 *
 * The [[KafkaShareSourceOffset]] is the custom [[Offset]] that contains:
 * - Share group ID
 * - Batch ID for ordering
 * - Map of TopicPartition to acquired record ranges (non-sequential offsets)
 *
 * Fault tolerance is achieved via:
 * - Kafka's acquisition locks (auto-release on timeout)
 * - Checkpointing of acknowledgment state
 * - At-least-once semantics by default
 * - Optional exactly-once via idempotent sinks or checkpoint-based dedup
 *
 * @param shareGroupId The Kafka share group identifier
 * @param topics Set of topics to subscribe to
 * @param executorKafkaParams Kafka params for executor-side consumers
 * @param options Source options
 * @param metadataPath Path for storing checkpoint metadata
 * @param acknowledgmentMode Implicit or explicit acknowledgment
 * @param exactlyOnceStrategy Strategy for exactly-once semantics
 */
private[kafka010] class KafkaShareMicroBatchStream(
    val shareGroupId: String,
    val topics: Set[String],
    executorKafkaParams: ju.Map[String, Object],
    options: CaseInsensitiveStringMap,
    metadataPath: String,
    acknowledgmentMode: AcknowledgmentMode,
    exactlyOnceStrategy: ExactlyOnceStrategy)
    extends SupportsTriggerAvailableNow
    with ReportsSourceMetrics
    with MicroBatchStream
    with Logging {

  private val pollTimeoutMs = options.getLong(
    KafkaShareSourceProvider.CONSUMER_POLL_TIMEOUT,
    SparkEnv.get.conf.get(NETWORK_TIMEOUT) * 1000L)

  private val lockTimeoutMs = options.getLong(
    KafkaShareSourceProvider.SHARE_LOCK_TIMEOUT,
    30000L)

  private val maxRecordsPerBatch = Option(options.get(
    KafkaShareSourceProvider.MAX_RECORDS_PER_BATCH)).map(_.toLong)

  private val parallelism = options.getInt(
    KafkaShareSourceProvider.SHARE_PARALLELISM,
    SparkSession.active.sparkContext.defaultParallelism)

  private val includeHeaders = options.getBoolean(
    KafkaShareSourceProvider.INCLUDE_HEADERS, false)

  // Batch ID counter - used for offset ordering since share groups don't have sequential offsets
  private val batchIdCounter = new AtomicLong(0)

  // Track in-flight batches for acknowledgment
  private val inFlightManager = new ShareInFlightManager()

  // Checkpoint writer for exactly-once semantics
  private lazy val checkpointWriter = new ShareCheckpointWriter(metadataPath)

  // State for Trigger.AvailableNow
  private var isTriggerAvailableNow: Boolean = false
  private var allDataForTriggerAvailableNow: Option[KafkaShareSourceOffset] = None

  /**
   * Return the initial offset for the stream.
   * For share groups, this is an empty offset since Kafka manages state.
   */
  override def initialOffset(): Offset = {
    // Check for existing checkpoint
    val existingOffset = checkpointWriter.getLatest()
    existingOffset.getOrElse(KafkaShareSourceOffset.empty(shareGroupId))
  }

  /**
   * Return the latest available offset.
   * For share groups, we create a new batch ID since Kafka doesn't expose "latest" in the same way.
   */
  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = {
    val startOffset = KafkaShareSourceOffset(start)
    val newBatchId = batchIdCounter.incrementAndGet()

    // For share groups, we don't have a concept of "latest offset" like traditional consumers.
    // Instead, we create a placeholder offset that will be populated when we actually poll.
    // The actual records will be determined during planInputPartitions.
    val offset = KafkaShareSourceOffset.forBatch(shareGroupId, newBatchId)

    // Apply rate limiting if configured
    readLimit match {
      case rows: ReadMaxRows =>
        // Rate limiting will be applied during partition planning
        logDebug(s"Rate limit set to ${rows.maxRows()} records per batch")
      case _ => // No rate limiting
    }

    if (isTriggerAvailableNow && allDataForTriggerAvailableNow.isEmpty) {
      allDataForTriggerAvailableNow = Some(offset)
    }

    offset
  }

  /**
   * Plan input partitions for the batch.
   *
   * Unlike traditional Kafka source which creates one partition per TopicPartition,
   * share groups can have multiple consumers reading from the same partition.
   * We create partitions based on configured parallelism.
   */
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset = KafkaShareSourceOffset(start)
    val endOffset = KafkaShareSourceOffset(end)

    logInfo(s"Planning ${parallelism} input partitions for share group $shareGroupId " +
      s"(batch ${startOffset.batchId} to ${endOffset.batchId})")

    // Create input partitions based on parallelism, not Kafka partitions
    // Each Spark task will have its own share consumer that will receive records
    // from any available partition in the subscribed topics
    (0 until parallelism).map { partitionId =>
      KafkaShareInputPartition(
        shareGroupId = shareGroupId,
        sparkPartitionId = partitionId,
        topics = topics,
        kafkaParams = executorKafkaParams,
        pollTimeoutMs = pollTimeoutMs,
        lockTimeoutMs = lockTimeoutMs,
        maxRecordsPerBatch = maxRecordsPerBatch,
        includeHeaders = includeHeaders,
        acknowledgmentMode = acknowledgmentMode,
        exactlyOnceStrategy = exactlyOnceStrategy,
        batchId = endOffset.batchId
      )
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    KafkaShareBatchReaderFactory
  }

  override def deserializeOffset(json: String): Offset = {
    KafkaShareSourceOffset(json)
  }

  /**
   * Commit the batch by acknowledging records to Kafka.
   *
   * This is called after the batch has been successfully processed.
   * For share groups, this commits the acknowledgments (not offsets).
   */
  override def commit(end: Offset): Unit = {
    val offset = KafkaShareSourceOffset(end)
    val batchId = offset.batchId

    logInfo(s"Committing batch $batchId for share group $shareGroupId")

    // In implicit mode, we assume all records are successfully processed
    // and commit ACCEPT acknowledgments
    if (acknowledgmentMode == AcknowledgmentMode.Implicit) {
      inFlightManager.getBatch(batchId).foreach { batch =>
        logDebug(s"Implicitly acknowledging ${batch.pending} pending records as ACCEPT")
        batch.acknowledgeAllAsAccept()
      }
    }

    // Write checkpoint for exactly-once strategies
    exactlyOnceStrategy match {
      case ExactlyOnceStrategy.CheckpointDedup =>
        checkpointWriter.write(offset)
      case _ => // No checkpoint needed for other strategies
    }

    // Clean up completed batch
    inFlightManager.removeBatch(batchId)
  }

  override def stop(): Unit = {
    // Release any pending records before stopping
    val released = inFlightManager.releaseAll()
    if (released > 0) {
      logWarning(s"Released $released pending records during shutdown")
    }
    inFlightManager.clear()
  }

  override def toString: String = s"KafkaShareMicroBatchStream[shareGroup=$shareGroupId, topics=$topics]"

  override def metrics(latestConsumedOffset: Optional[Offset]): ju.Map[String, String] = {
    val offset = Option(latestConsumedOffset.orElse(null))
    offset match {
      case Some(o: KafkaShareSourceOffset) =>
        Map(
          "shareGroupId" -> shareGroupId,
          "batchId" -> o.batchId.toString,
          "totalAcquiredRecords" -> o.totalRecords.toString,
          "partitionsWithRecords" -> o.partitionsWithRecords.size.toString
        ).asJava
      case _ =>
        ju.Collections.emptyMap()
    }
  }

  override def prepareForTriggerAvailableNow(): Unit = {
    isTriggerAvailableNow = true
  }

  /**
   * Register acquired records for acknowledgment tracking.
   * Called by partition readers when they acquire records.
   */
  def registerAcquiredRecords(
      batchId: Long,
      records: Seq[ShareInFlightRecord]): Unit = {
    val batch = inFlightManager.getOrCreateBatch(batchId)
    records.foreach(batch.addRecord)
  }

  /**
   * Get the in-flight manager for external access (e.g., explicit acknowledgment).
   */
  def getInFlightManager: ShareInFlightManager = inFlightManager
}

/**
 * Input partition for share group consumers.
 * Each partition represents a Spark task that will poll from the share group.
 */
case class KafkaShareInputPartition(
    shareGroupId: String,
    sparkPartitionId: Int,
    topics: Set[String],
    kafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    lockTimeoutMs: Long,
    maxRecordsPerBatch: Option[Long],
    includeHeaders: Boolean,
    acknowledgmentMode: AcknowledgmentMode,
    exactlyOnceStrategy: ExactlyOnceStrategy,
    batchId: Long) extends InputPartition {

  // Preferred locations for share groups are not as meaningful as traditional consumers
  // since any consumer can receive records from any partition
  override def preferredLocations(): Array[String] = Array.empty
}

/**
 * Factory for creating share group partition readers.
 */
object KafkaShareBatchReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): KafkaSharePartitionReader = {
    val sharePartition = partition.asInstanceOf[KafkaShareInputPartition]
    new KafkaSharePartitionReader(
      shareGroupId = sharePartition.shareGroupId,
      sparkPartitionId = sharePartition.sparkPartitionId,
      topics = sharePartition.topics,
      kafkaParams = sharePartition.kafkaParams,
      pollTimeoutMs = sharePartition.pollTimeoutMs,
      lockTimeoutMs = sharePartition.lockTimeoutMs,
      maxRecordsPerBatch = sharePartition.maxRecordsPerBatch,
      includeHeaders = sharePartition.includeHeaders,
      acknowledgmentMode = sharePartition.acknowledgmentMode,
      exactlyOnceStrategy = sharePartition.exactlyOnceStrategy,
      batchId = sharePartition.batchId
    )
  }
}

/**
 * Strategy for achieving exactly-once semantics.
 */
sealed trait ExactlyOnceStrategy
object ExactlyOnceStrategy {
  /** No exactly-once guarantees (at-least-once only) */
  case object None extends ExactlyOnceStrategy

  /** Deduplicate at sink using record keys (topic, partition, offset) */
  case object Idempotent extends ExactlyOnceStrategy

  /** Two-phase commit with transaction coordinator */
  case object TwoPhaseCommit extends ExactlyOnceStrategy

  /** Track processed records in checkpoint for deduplication */
  case object CheckpointDedup extends ExactlyOnceStrategy

  def fromString(s: String): ExactlyOnceStrategy = s.toLowerCase match {
    case "none" | "" => None
    case "idempotent" => Idempotent
    case "two-phase-commit" | "2pc" => TwoPhaseCommit
    case "checkpoint-dedup" | "checkpoint" => CheckpointDedup
    case _ => throw new IllegalArgumentException(s"Unknown exactly-once strategy: $s")
  }
}

