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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

/**
 * Acknowledgment types for share consumer records.
 * Mirrors Kafka's AcknowledgeType enum.
 */
object AcknowledgmentType extends Enumeration {
  type AcknowledgmentType = Value
  val ACCEPT = Value("ACCEPT")   // Successfully processed - mark as complete
  val RELEASE = Value("RELEASE") // Release back for redelivery - mark as available
  val REJECT = Value("REJECT")   // Permanently reject - move to dead letter / archive
}

/**
 * Represents a single in-flight record in a share group.
 * Tracks the record data along with its acquisition state and acknowledgment status.
 *
 * @param recordKey Unique identifier for the record
 * @param record The actual Kafka consumer record
 * @param acquiredAt Timestamp when the record was acquired
 * @param lockExpiresAt Timestamp when the acquisition lock expires
 * @param deliveryCount Number of times this record has been delivered
 * @param acknowledgment The pending acknowledgment type (if any)
 */
case class ShareInFlightRecord(
    recordKey: RecordKey,
    record: ConsumerRecord[Array[Byte], Array[Byte]],
    acquiredAt: Long,
    lockExpiresAt: Long,
    deliveryCount: Short,
    acknowledgment: Option[AcknowledgmentType.AcknowledgmentType] = None) {

  def topic: String = record.topic()
  def partition: Int = record.partition()
  def offset: Long = record.offset()
  def topicPartition: TopicPartition = new TopicPartition(topic, partition)

  /** Check if the lock has expired */
  def isLockExpired(currentTimeMs: Long): Boolean = currentTimeMs >= lockExpiresAt

  /** Check if this record has been acknowledged */
  def isAcknowledged: Boolean = acknowledgment.isDefined

  /** Mark this record as acknowledged with the given type */
  def withAcknowledgment(ackType: AcknowledgmentType.AcknowledgmentType): ShareInFlightRecord = {
    copy(acknowledgment = Some(ackType))
  }

  /** Get remaining lock time in milliseconds */
  def remainingLockTimeMs(currentTimeMs: Long): Long = {
    math.max(0, lockExpiresAt - currentTimeMs)
  }

  override def toString: String = {
    s"ShareInFlightRecord[${recordKey}, ack=${acknowledgment.map(_.toString).getOrElse("pending")}, " +
      s"deliveryCount=$deliveryCount, lockExpires=$lockExpiresAt]"
  }
}

object ShareInFlightRecord {
  /** Create from a Kafka consumer record */
  def apply(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      acquiredAt: Long,
      lockTimeoutMs: Long,
      deliveryCount: Short): ShareInFlightRecord = {
    ShareInFlightRecord(
      recordKey = RecordKey(record.topic(), record.partition(), record.offset()),
      record = record,
      acquiredAt = acquiredAt,
      lockExpiresAt = acquiredAt + lockTimeoutMs,
      deliveryCount = deliveryCount
    )
  }
}

/**
 * Tracks in-flight records for a share consumer batch.
 * Manages the lifecycle of records from acquisition through acknowledgment.
 *
 * Thread-safe implementation using ConcurrentHashMap for executor-side usage.
 */
class ShareInFlightBatch(val batchId: Long) {
  // Map of RecordKey -> ShareInFlightRecord for all in-flight records
  private val records = new ConcurrentHashMap[RecordKey, ShareInFlightRecord]()

  // Counters for tracking acknowledgment state
  private val pendingCount = new AtomicInteger(0)
  private val acceptedCount = new AtomicInteger(0)
  private val releasedCount = new AtomicInteger(0)
  private val rejectedCount = new AtomicInteger(0)

  /** Add a record to the in-flight batch */
  def addRecord(record: ShareInFlightRecord): Unit = {
    records.put(record.recordKey, record)
    pendingCount.incrementAndGet()
  }

  /** Get a record by its key */
  def getRecord(key: RecordKey): Option[ShareInFlightRecord] = {
    Option(records.get(key))
  }

  /** Acknowledge a record with the given type */
  def acknowledge(key: RecordKey, ackType: AcknowledgmentType.AcknowledgmentType): Boolean = {
    val record = records.get(key)
    if (record != null && record.acknowledgment.isEmpty) {
      records.put(key, record.withAcknowledgment(ackType))
      pendingCount.decrementAndGet()
      ackType match {
        case AcknowledgmentType.ACCEPT => acceptedCount.incrementAndGet()
        case AcknowledgmentType.RELEASE => releasedCount.incrementAndGet()
        case AcknowledgmentType.REJECT => rejectedCount.incrementAndGet()
      }
      true
    } else {
      false
    }
  }

  /** Acknowledge all pending records as ACCEPT */
  def acknowledgeAllAsAccept(): Int = {
    var count = 0
    records.forEach { (key, record) =>
      if (record.acknowledgment.isEmpty) {
        records.put(key, record.withAcknowledgment(AcknowledgmentType.ACCEPT))
        pendingCount.decrementAndGet()
        acceptedCount.incrementAndGet()
        count += 1
      }
    }
    count
  }

  /** Release all pending records (for failure recovery) */
  def releaseAllPending(): Int = {
    var count = 0
    records.forEach { (key, record) =>
      if (record.acknowledgment.isEmpty) {
        records.put(key, record.withAcknowledgment(AcknowledgmentType.RELEASE))
        pendingCount.decrementAndGet()
        releasedCount.incrementAndGet()
        count += 1
      }
    }
    count
  }

  /** Get all pending (unacknowledged) records */
  def getPendingRecords: Seq[ShareInFlightRecord] = {
    records.values().asScala.filter(_.acknowledgment.isEmpty).toSeq
  }

  /** Get all records with a specific acknowledgment type */
  def getRecordsByAckType(ackType: AcknowledgmentType.AcknowledgmentType): Seq[ShareInFlightRecord] = {
    records.values().asScala.filter(_.acknowledgment.contains(ackType)).toSeq
  }

  /** Get all accepted records */
  def getAcceptedRecords: Seq[ShareInFlightRecord] = getRecordsByAckType(AcknowledgmentType.ACCEPT)

  /** Get all released records */
  def getReleasedRecords: Seq[ShareInFlightRecord] = getRecordsByAckType(AcknowledgmentType.RELEASE)

  /** Get all rejected records */
  def getRejectedRecords: Seq[ShareInFlightRecord] = getRecordsByAckType(AcknowledgmentType.REJECT)

  /** Get records grouped by TopicPartition */
  def getRecordsByPartition: Map[TopicPartition, Seq[ShareInFlightRecord]] = {
    records.values().asScala.groupBy(_.topicPartition).toMap
  }

  /** Get acknowledgments grouped by TopicPartition for commit */
  def getAcknowledgmentsForCommit: Map[TopicPartition, Map[Long, AcknowledgmentType.AcknowledgmentType]] = {
    records.values().asScala
      .filter(_.acknowledgment.isDefined)
      .groupBy(_.topicPartition)
      .map { case (tp, recs) =>
        tp -> recs.map(r => r.offset -> r.acknowledgment.get).toMap
      }
      .toMap
  }

  /** Check if all records have been acknowledged */
  def isComplete: Boolean = pendingCount.get() == 0

  /** Get counts */
  def totalRecords: Int = records.size()
  def pending: Int = pendingCount.get()
  def accepted: Int = acceptedCount.get()
  def released: Int = releasedCount.get()
  def rejected: Int = rejectedCount.get()

  /** Clear all records */
  def clear(): Unit = {
    records.clear()
    pendingCount.set(0)
    acceptedCount.set(0)
    releasedCount.set(0)
    rejectedCount.set(0)
  }

  override def toString: String = {
    s"ShareInFlightBatch[batchId=$batchId, total=${totalRecords}, " +
      s"pending=${pending}, accepted=${accepted}, released=${released}, rejected=${rejected}]"
  }
}

/**
 * Manager for in-flight batches across multiple micro-batches.
 * Used by the driver to track acknowledgment state.
 */
class ShareInFlightManager {
  // Map of batchId -> ShareInFlightBatch
  private val batches = new ConcurrentHashMap[Long, ShareInFlightBatch]()

  /** Create a new in-flight batch for the given batch ID */
  def createBatch(batchId: Long): ShareInFlightBatch = {
    val batch = new ShareInFlightBatch(batchId)
    batches.put(batchId, batch)
    batch
  }

  /** Get an existing batch */
  def getBatch(batchId: Long): Option[ShareInFlightBatch] = {
    Option(batches.get(batchId))
  }

  /** Get or create a batch */
  def getOrCreateBatch(batchId: Long): ShareInFlightBatch = {
    batches.computeIfAbsent(batchId, _ => new ShareInFlightBatch(batchId))
  }

  /** Remove a completed batch */
  def removeBatch(batchId: Long): Option[ShareInFlightBatch] = {
    Option(batches.remove(batchId))
  }

  /** Get all incomplete batches */
  def getIncompleteBatches: Seq[ShareInFlightBatch] = {
    batches.values().asScala.filterNot(_.isComplete).toSeq
  }

  /** Release all pending records in all batches (for shutdown) */
  def releaseAll(): Int = {
    batches.values().asScala.map(_.releaseAllPending()).sum
  }

  /** Clear all batches */
  def clear(): Unit = {
    batches.clear()
  }

  /** Get total pending records across all batches */
  def totalPending: Int = {
    batches.values().asScala.map(_.pending).sum
  }
}

