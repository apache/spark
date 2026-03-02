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
import java.nio.charset.StandardCharsets

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.kafka010.share.consumer._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Partition reader for Kafka share groups.
 *
 * This reader:
 * 1. Polls records from the share group
 * 2. Tracks acquired records for acknowledgment
 * 3. Converts records to Spark InternalRow format
 * 4. Handles acknowledgment based on configured mode
 *
 * Unlike traditional Kafka readers that seek to specific offsets,
 * share group readers receive whatever records Kafka assigns to them.
 * Records can come from any partition in the subscribed topics.
 */
private[kafka010] class KafkaSharePartitionReader(
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
    batchId: Long) extends PartitionReader[InternalRow] with Logging {

  // The share consumer for this reader
  private val shareConsumer = KafkaShareDataConsumer.acquire(
    shareGroupId, topics, kafkaParams, lockTimeoutMs)

  // Buffer for polled records
  private var recordBuffer: Iterator[ShareInFlightRecord] = Iterator.empty

  // Current record being processed
  private var currentRecord: ShareInFlightRecord = _

  // Track all records acquired in this batch for acknowledgment
  private val acquiredRecords = ArrayBuffer[ShareInFlightRecord]()

  // Track processed record keys for checkpoint-based deduplication
  private val processedKeys = new ju.HashSet[RecordKey]()

  // State tracking
  private var recordsRead: Long = 0
  private var hasPolled: Boolean = false
  private var isExhausted: Boolean = false

  /**
   * Move to the next record.
   * Returns false when there are no more records to process.
   */
  override def next(): Boolean = {
    // Check if we've hit the record limit
    if (maxRecordsPerBatch.exists(recordsRead >= _)) {
      logDebug(s"Reached max records per batch: $maxRecordsPerBatch")
      return false
    }

    // Try to get next record from buffer
    if (recordBuffer.hasNext) {
      currentRecord = recordBuffer.next()
      recordsRead += 1
      return true
    }

    // If we haven't polled yet, or buffer is empty, poll for more records
    if (!isExhausted) {
      val polledRecords = pollRecords()
      if (polledRecords.nonEmpty) {
        recordBuffer = polledRecords.iterator
        currentRecord = recordBuffer.next()
        recordsRead += 1
        return true
      } else {
        // No more records available
        isExhausted = true
        return false
      }
    }

    false
  }

  /**
   * Get the current record as an InternalRow.
   */
  override def get(): InternalRow = {
    assert(currentRecord != null, "next() must be called before get()")
    recordToInternalRow(currentRecord.record)
  }

  /**
   * Close the reader and handle acknowledgments.
   */
  override def close(): Unit = {
    try {
      // Handle acknowledgment based on mode
      acknowledgmentMode match {
        case AcknowledgmentMode.Implicit =>
          // Acknowledge all records as ACCEPT
          acknowledgeAllAsAccept()

        case AcknowledgmentMode.Explicit =>
          // In explicit mode, any unacknowledged records are released
          releaseUnacknowledgedRecords()
      }

      // Commit acknowledgments to Kafka
      shareConsumer.commitSync()
    } catch {
      case e: Exception =>
        logError(s"Error during acknowledgment commit: ${e.getMessage}", e)
        // Try to release records on error
        try {
          releaseAllRecords()
          shareConsumer.commitSync()
        } catch {
          case inner: Exception =>
            logError(s"Error releasing records: ${inner.getMessage}", inner)
        }
    } finally {
      shareConsumer.release()
    }

    logInfo(s"Share partition reader closed. " +
      s"Read $recordsRead records, acquired ${acquiredRecords.size} records")
  }

  /**
   * Poll for records from the share consumer.
   */
  private def pollRecords(): Seq[ShareInFlightRecord] = {
    hasPolled = true

    // Calculate how many records we can still fetch
    val remainingCapacity = maxRecordsPerBatch.map(_ - recordsRead).getOrElse(Long.MaxValue)

    if (remainingCapacity <= 0) {
      return Seq.empty
    }

    try {
      val records = shareConsumer.poll(pollTimeoutMs)

      // Apply rate limiting if needed
      val limitedRecords = if (maxRecordsPerBatch.isDefined) {
        records.take(remainingCapacity.toInt)
      } else {
        records
      }

      // Apply deduplication for checkpoint-based strategy
      val dedupedRecords = exactlyOnceStrategy match {
        case ExactlyOnceStrategy.CheckpointDedup =>
          limitedRecords.filterNot(r => processedKeys.contains(r.recordKey))
        case _ =>
          limitedRecords
      }

      // Track acquired records
      acquiredRecords ++= dedupedRecords
      dedupedRecords.foreach(r => processedKeys.add(r.recordKey))

      logDebug(s"Polled ${records.size} records, kept ${dedupedRecords.size} after dedup")
      dedupedRecords
    } catch {
      case e: Exception =>
        logError(s"Error polling from share group: ${e.getMessage}", e)
        Seq.empty
    }
  }

  /**
   * Acknowledge all acquired records as ACCEPT.
   */
  private def acknowledgeAllAsAccept(): Unit = {
    val pending = acquiredRecords.filter(_.acknowledgment.isEmpty)
    if (pending.nonEmpty) {
      logDebug(s"Acknowledging ${pending.size} records as ACCEPT")
      pending.foreach { record =>
        shareConsumer.acknowledge(record.recordKey, AcknowledgmentType.ACCEPT)
      }
    }
  }

  /**
   * Release all unacknowledged records.
   */
  private def releaseUnacknowledgedRecords(): Unit = {
    val unacked = acquiredRecords.filter(_.acknowledgment.isEmpty)
    if (unacked.nonEmpty) {
      logWarning(s"Releasing ${unacked.size} unacknowledged records")
      unacked.foreach { record =>
        shareConsumer.acknowledge(record.recordKey, AcknowledgmentType.RELEASE)
      }
    }
  }

  /**
   * Release all records (for error recovery).
   */
  private def releaseAllRecords(): Unit = {
    logWarning(s"Releasing all ${acquiredRecords.size} records due to error")
    acquiredRecords.foreach { record =>
      shareConsumer.acknowledge(record.recordKey, AcknowledgmentType.RELEASE)
    }
  }

  /**
   * Convert a Kafka ConsumerRecord to Spark InternalRow.
   *
   * Schema: key, value, topic, partition, offset, timestamp, timestampType, headers
   */
  private def recordToInternalRow(
      record: ConsumerRecord[Array[Byte], Array[Byte]]): InternalRow = {
    val row = new GenericInternalRow(
      if (includeHeaders) 8 else 7
    )

    row.update(0, record.key())                                    // key: binary
    row.update(1, record.value())                                  // value: binary
    row.update(2, UTF8String.fromString(record.topic()))           // topic: string
    row.update(3, record.partition())                              // partition: int
    row.update(4, record.offset())                                 // offset: long
    row.update(5, record.timestamp())                              // timestamp: long
    row.update(6, record.timestampType().id)                       // timestampType: int

    if (includeHeaders) {
      row.update(7, headersToArrayData(record.headers()))          // headers: array<struct>
    }

    row
  }

  /**
   * Convert Kafka headers to Spark ArrayData.
   */
  private def headersToArrayData(headers: Headers): GenericArrayData = {
    val headersList = headers.asScala.map { header =>
      val headerRow = new GenericInternalRow(2)
      headerRow.update(0, UTF8String.fromString(header.key()))
      headerRow.update(1, header.value())
      headerRow
    }.toArray
    new GenericArrayData(headersList)
  }
}

/**
 * Schema for Kafka share source records.
 */
object KafkaShareRecordSchema {
  val BASE_SCHEMA: StructType = StructType(Seq(
    StructField("key", BinaryType, nullable = true),
    StructField("value", BinaryType, nullable = true),
    StructField("topic", StringType, nullable = false),
    StructField("partition", IntegerType, nullable = false),
    StructField("offset", LongType, nullable = false),
    StructField("timestamp", LongType, nullable = false),
    StructField("timestampType", IntegerType, nullable = false)
  ))

  val HEADER_SCHEMA: StructType = StructType(Seq(
    StructField("key", StringType, nullable = false),
    StructField("value", BinaryType, nullable = true)
  ))

  val SCHEMA_WITH_HEADERS: StructType = BASE_SCHEMA.add(
    StructField("headers", ArrayType(HEADER_SCHEMA), nullable = true)
  )

  def getSchema(includeHeaders: Boolean): StructType = {
    if (includeHeaders) SCHEMA_WITH_HEADERS else BASE_SCHEMA
  }
}

