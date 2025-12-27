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

import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.JsonAST.{JArray, JObject, JString}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql.connector.read.streaming.{Offset => SparkOffset}
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

/**
 * Represents a unique identifier for a record in a share group.
 * Unlike traditional consumer groups, share groups can have non-sequential offset tracking
 * because records can be:
 * 1. Acquired by different consumers in any order
 * 2. Acknowledged out of order
 * 3. Released and reacquired multiple times
 *
 * @param topic The Kafka topic name
 * @param partition The partition number
 * @param offset The record offset
 */
case class RecordKey(topic: String, partition: Int, offset: Long) {
  def topicPartition: TopicPartition = new TopicPartition(topic, partition)

  override def toString: String = s"$topic-$partition:$offset"
}

object RecordKey {
  def apply(tp: TopicPartition, offset: Long): RecordKey = {
    RecordKey(tp.topic(), tp.partition(), offset)
  }

  def fromString(s: String): RecordKey = {
    val parts = s.split(":")
    require(parts.length == 2, s"Invalid RecordKey string: $s")
    val topicPart = parts(0).lastIndexOf("-")
    require(topicPart > 0, s"Invalid topic-partition format: ${parts(0)}")
    RecordKey(
      parts(0).substring(0, topicPart),
      parts(0).substring(topicPart + 1).toInt,
      parts(1).toLong
    )
  }
}

/**
 * Represents a range of acquired records for checkpointing.
 * This captures the non-sequential nature of share group offset tracking.
 *
 * @param offsets Set of individual offsets acquired in this batch (non-sequential)
 * @param acquiredAt Timestamp when records were acquired
 * @param lockExpiresAt Timestamp when acquisition lock expires
 * @param deliveryCount Number of times these records have been delivered
 */
case class AcquiredRecordRange(
    offsets: Set[Long],
    acquiredAt: Long,
    lockExpiresAt: Long,
    deliveryCount: Short = 1) {

  def size: Int = offsets.size

  def minOffset: Long = if (offsets.isEmpty) -1L else offsets.min

  def maxOffset: Long = if (offsets.isEmpty) -1L else offsets.max

  /** Check if a specific offset is in this range */
  def contains(offset: Long): Boolean = offsets.contains(offset)

  /** Check if the lock has expired */
  def isLockExpired(currentTimeMs: Long): Boolean = currentTimeMs >= lockExpiresAt

  /** Add an offset to this range */
  def withOffset(offset: Long): AcquiredRecordRange = copy(offsets = offsets + offset)

  /** Remove an offset from this range (when acknowledged) */
  def withoutOffset(offset: Long): AcquiredRecordRange = copy(offsets = offsets - offset)
}

object AcquiredRecordRange {
  /** Create an empty range */
  def empty(acquiredAt: Long, lockTimeoutMs: Long): AcquiredRecordRange = {
    AcquiredRecordRange(Set.empty, acquiredAt, acquiredAt + lockTimeoutMs)
  }

  /** Create a range from a contiguous offset range (for initial acquisition) */
  def fromRange(start: Long, end: Long, acquiredAt: Long, lockTimeoutMs: Long): AcquiredRecordRange = {
    AcquiredRecordRange((start to end).toSet, acquiredAt, acquiredAt + lockTimeoutMs)
  }
}

/**
 * Offset for the Kafka Share Source that tracks non-sequential offset acquisition.
 *
 * Unlike [[org.apache.spark.sql.kafka010.KafkaSourceOffset]] which tracks a single offset
 * per partition, this tracks:
 * 1. The batch ID for ordering
 * 2. Per-partition sets of acquired record offsets (can be non-sequential)
 * 3. Acquisition timestamps and lock expiry times
 *
 * This design accounts for the fact that in share groups:
 * - Multiple consumers can acquire records from the same partition concurrently
 * - Records can be acknowledged/released out of order
 * - Kafka broker assigns random available offsets, not sequential ranges
 *
 * @param shareGroupId The share group identifier
 * @param batchId The micro-batch identifier (for ordering)
 * @param acquiredRecords Map of TopicPartition to acquired record ranges
 */
case class KafkaShareSourceOffset(
    shareGroupId: String,
    batchId: Long,
    acquiredRecords: Map[TopicPartition, AcquiredRecordRange]) extends Offset {

  implicit val formats: Formats = DefaultFormats

  override val json: String = {
    val partitions = acquiredRecords.map { case (tp, range) =>
      ("topic" -> tp.topic()) ~
      ("partition" -> tp.partition()) ~
      ("offsets" -> range.offsets.toSeq.sorted) ~
      ("acquiredAt" -> range.acquiredAt) ~
      ("lockExpiresAt" -> range.lockExpiresAt) ~
      ("deliveryCount" -> range.deliveryCount.toInt)
    }

    compact(render(
      ("shareGroupId" -> shareGroupId) ~
      ("batchId" -> batchId) ~
      ("partitions" -> partitions)
    ))
  }

  /** Get all record keys in this offset */
  def getAllRecordKeys: Set[RecordKey] = {
    acquiredRecords.flatMap { case (tp, range) =>
      range.offsets.map(offset => RecordKey(tp, offset))
    }.toSet
  }

  /** Get the total number of acquired records */
  def totalRecords: Int = acquiredRecords.values.map(_.size).sum

  /** Check if any partition has records */
  def hasRecords: Boolean = acquiredRecords.exists(_._2.offsets.nonEmpty)

  /** Get partitions that have records */
  def partitionsWithRecords: Set[TopicPartition] = {
    acquiredRecords.filter(_._2.offsets.nonEmpty).keySet
  }

  /** Create a new offset with an additional acquired record */
  def withAcquiredRecord(tp: TopicPartition, offset: Long, acquiredAt: Long, lockTimeoutMs: Long): KafkaShareSourceOffset = {
    val currentRange = acquiredRecords.getOrElse(tp,
      AcquiredRecordRange.empty(acquiredAt, lockTimeoutMs))
    copy(acquiredRecords = acquiredRecords + (tp -> currentRange.withOffset(offset)))
  }

  /** Create a new offset with a record removed (acknowledged) */
  def withAcknowledgedRecord(tp: TopicPartition, offset: Long): KafkaShareSourceOffset = {
    acquiredRecords.get(tp) match {
      case Some(range) =>
        val newRange = range.withoutOffset(offset)
        if (newRange.offsets.isEmpty) {
          copy(acquiredRecords = acquiredRecords - tp)
        } else {
          copy(acquiredRecords = acquiredRecords + (tp -> newRange))
        }
      case None => this
    }
  }
}

object KafkaShareSourceOffset {
  implicit val formats: Formats = DefaultFormats

  /** Create an empty offset for the start of processing */
  def empty(shareGroupId: String): KafkaShareSourceOffset = {
    KafkaShareSourceOffset(shareGroupId, 0L, Map.empty)
  }

  /** Create an initial offset for a new batch */
  def forBatch(shareGroupId: String, batchId: Long): KafkaShareSourceOffset = {
    KafkaShareSourceOffset(shareGroupId, batchId, Map.empty)
  }

  /** Parse from JSON string */
  def apply(json: String): KafkaShareSourceOffset = {
    val parsed = parse(json)

    val shareGroupId = (parsed \ "shareGroupId").extract[String]
    val batchId = (parsed \ "batchId").extract[Long]
    val partitions = (parsed \ "partitions").extract[List[JValue]]

    val acquiredRecords = partitions.map { p =>
      val topic = (p \ "topic").extract[String]
      val partition = (p \ "partition").extract[Int]
      val offsets = (p \ "offsets").extract[List[Long]].toSet
      val acquiredAt = (p \ "acquiredAt").extract[Long]
      val lockExpiresAt = (p \ "lockExpiresAt").extract[Long]
      val deliveryCount = (p \ "deliveryCount").extract[Int].toShort

      val tp = new TopicPartition(topic, partition)
      val range = AcquiredRecordRange(offsets, acquiredAt, lockExpiresAt, deliveryCount)
      tp -> range
    }.toMap

    KafkaShareSourceOffset(shareGroupId, batchId, acquiredRecords)
  }

  /** Convert from SerializedOffset */
  def apply(offset: SerializedOffset): KafkaShareSourceOffset = {
    apply(offset.json)
  }

  /** Convert from Spark streaming Offset */
  def apply(offset: SparkOffset): KafkaShareSourceOffset = {
    offset match {
      case k: KafkaShareSourceOffset => k
      case so: SerializedOffset => apply(so)
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaShareSourceOffset")
    }
  }

  /** Extract acquired records from an Offset */
  def getAcquiredRecords(offset: Offset): Map[TopicPartition, AcquiredRecordRange] = {
    offset match {
      case o: KafkaShareSourceOffset => o.acquiredRecords
      case so: SerializedOffset => KafkaShareSourceOffset(so).acquiredRecords
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaShareSourceOffset")
    }
  }
}

