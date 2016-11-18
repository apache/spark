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

package org.apache.spark.sql.kafka010

import java.{util => ju}

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer, OffsetOutOfRangeException}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.kafka010.KafkaSource._


/**
 * Consumer of single topicpartition, intended for cached reuse.
 * Underlying consumer is not threadsafe, so neither is this,
 * but processing the same topicpartition and group id in multiple threads is usually bad anyway.
 */
private[kafka010] case class CachedKafkaConsumer private(
    topicPartition: TopicPartition,
    kafkaParams: ju.Map[String, Object]) extends Logging {
  import CachedKafkaConsumer._

  private val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]

  private val consumer = {
    val c = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
    val tps = new ju.ArrayList[TopicPartition]()
    tps.add(topicPartition)
    c.assign(tps)
    c
  }

  /** Iterator to the already fetch data */
  private var fetchedData = ju.Collections.emptyIterator[ConsumerRecord[Array[Byte], Array[Byte]]]
  private var nextOffsetInFetchedData = UNKNOWN_OFFSET

  /**
   * Get the record for the given offset, waiting up to timeout ms if IO is necessary.
   * Sequential forward access will use buffers, but random access will be horribly inefficient.
   *
   * When `failOnDataLoss` is `true`, this will either return record at offset if available, or
   * throw exception.
   *
   * When `failOnDataLoss` is `false`, this will either return record at offset if available, or
   * return the next earliest available record less than untilOffset, or null. It will not throw
   * any exception.
   */
  def get(
      offset: Long,
      untilOffset: Long,
      pollTimeoutMs: Long,
      failOnDataLoss: Boolean): ConsumerRecord[Array[Byte], Array[Byte]] = {
    require(offset < untilOffset,
      s"offset must always be less than untilOffset [offset: $offset, untilOffset: $untilOffset]")
    logDebug(s"Get $groupId $topicPartition nextOffset $nextOffsetInFetchedData requested $offset")
    var toFetchOffset = offset
    while (toFetchOffset != UNKNOWN_OFFSET) {
      try {
        val record = fetchData(toFetchOffset, untilOffset, pollTimeoutMs, failOnDataLoss)
        if (record == null) {
          reset()
        }
        return record
      } catch {
        case e: OffsetOutOfRangeException =>
          val message =
            if (failOnDataLoss) {
              s"Cannot fetch offset $toFetchOffset"
            } else {
              s"Cannot fetch offset $toFetchOffset. Some data may be lost. " +
                "Recovering from the earliest offset"
            }
          reportDataLoss(failOnDataLoss, message, e)
          toFetchOffset = getNextEarliestOffset(toFetchOffset, untilOffset)
      }
    }
    reset()
    null
  }

  /**
   * Return the next earliest available offset in [offset, untilOffset). If all offsets in
   * [offset, untilOffset) are invalid (e.g., the topic is deleted and recreated), it will return
   * `UNKNOWN_OFFSET`.
   */
  private def getNextEarliestOffset(offset: Long, untilOffset: Long): Long = {
    val (earliestOffset, latestOffset) = getAvailableOffsetRange()
    if (offset >= latestOffset || earliestOffset >= untilOffset) {
      // [offset, untilOffset) and [earliestOffset, latestOffset) have no overlap,
      // either
      // --------------------------------------------------------
      //         ^                 ^         ^         ^
      //         |                 |         |         |
      //   earliestOffset   latestOffset   offset   untilOffset
      //
      // or
      // --------------------------------------------------------
      //      ^          ^              ^                ^
      //      |          |              |                |
      //   offset   untilOffset   earliestOffset   latestOffset
      val warningMessage =
        s"""
          |The current available offset range is [$earliestOffset, $latestOffset).
          | Offset ${offset} is out of range, and records in [$offset, $untilOffset) will be
          | skipped ${additionalMessage(failOnDataLoss = false)}
        """.stripMargin
      logWarning(warningMessage)
      UNKNOWN_OFFSET
    } else if (offset >= earliestOffset) {
      // -----------------------------------------------------------------------------
      //         ^            ^                  ^                                 ^
      //         |            |                  |                                 |
      //   earliestOffset   offset   min(untilOffset,latestOffset)   max(untilOffset, latestOffset)
      //
      // This will happen when a topic is deleted and recreated, and new data are pushed very fast,
      // then we will see `offset` disappears first then appears again. Although the parameters
      // are same, the state in Kafka cluster is changed, so the outer loop won't be endless.
      logWarning(s"Found a disappeared offset $offset. " +
        s"Some data may be lost ${additionalMessage(failOnDataLoss = false)}")
      offset
    } else {
      // ------------------------------------------------------------------------------
      //      ^           ^                       ^                                 ^
      //      |           |                       |                                 |
      //   offset   earliestOffset   min(untilOffset,latestOffset)   max(untilOffset, latestOffset)
      val warningMessage =
        s"""
           |The current available offset range is [$earliestOffset, $latestOffset).
           | Offset ${offset} is out of range, and records in [$offset, $earliestOffset) will be
           | skipped ${additionalMessage(failOnDataLoss = false)}
        """.stripMargin
      logWarning(warningMessage)
      earliestOffset
    }
  }

  /**
   * Get the earliest record in [offset, untilOffset). If there is not such record, return null and
   * clear the fetched data.
   */
  private def fetchData(
      offset: Long,
      untilOffset: Long,
      pollTimeoutMs: Long,
      failOnDataLoss: Boolean): ConsumerRecord[Array[Byte], Array[Byte]] = {
    if (offset != nextOffsetInFetchedData || !fetchedData.hasNext()) {
      // This is the first fetch, or the last pre-fetched data has been drained.
      // Seek to the offset because we may call seekToBeginning or seekToEnd before this.
      seek(offset)
      poll(pollTimeoutMs)
    }

    if (!fetchedData.hasNext()) {
      // We cannot fetch anything after `poll`. Two possible cases:
      // - `earliestOffset` is `offset` but there is nothing for `earliestOffset` right now.
      // - Cannot fetch any data before timeout.
      // Because there is no way to distinguish, just skip the rest offsets in the current
      // partition.
      val message =
        if (failOnDataLoss) {
          s"Cannot fetch record for offset $offset"
        } else {
          s"Cannot fetch record for offset $offset. " +
            s"Records in [$offset, $untilOffset) will be skipped"
        }
      reportDataLoss(failOnDataLoss, message)
      null
    } else {
      val record = fetchedData.next()
      nextOffsetInFetchedData = record.offset + 1
      // `seek` is always called before "poll". So "record.offset" must be same as "offset".
      assert(record.offset == offset,
        s"The fetched data has a different offset: expected $offset but was ${record.offset}")
      record
    }
  }

  /**
   * Reset the internal pre-fetched data.
   */
  private def reset(): Unit = {
    nextOffsetInFetchedData = UNKNOWN_OFFSET
    fetchedData = ju.Collections.emptyIterator[ConsumerRecord[Array[Byte], Array[Byte]]]
  }

  /**
   * Return an addition message including useful message and instruction.
   */
  private def additionalMessage(failOnDataLoss: Boolean): String = {
    if (failOnDataLoss) {
      s"(GroupId: $groupId, TopicPartition: $topicPartition). " +
        s"$INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE"
    } else {
      s"(GroupId: $groupId, TopicPartition: $topicPartition). " +
        s"$INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE"
    }
  }

  /**
   * Throw an exception or log a warning as per `failOnDataLoss`.
   */
  private def reportDataLoss(
      failOnDataLoss: Boolean,
      message: String,
      cause: Throwable = null): Unit = {
    val finalMessage = s"$message ${additionalMessage(failOnDataLoss)}"
    if (failOnDataLoss) {
      if (cause != null) {
        throw new IllegalStateException(finalMessage)
      } else {
        throw new IllegalStateException(finalMessage, cause)
      }
    } else {
      if (cause != null) {
        logWarning(finalMessage)
      } else {
        logWarning(finalMessage, cause)
      }
    }
  }

  private def close(): Unit = consumer.close()

  private def seek(offset: Long): Unit = {
    logDebug(s"Seeking to $groupId $topicPartition $offset")
    consumer.seek(topicPartition, offset)
  }

  private def poll(pollTimeoutMs: Long): Unit = {
    val p = consumer.poll(pollTimeoutMs)
    val r = p.records(topicPartition)
    logDebug(s"Polled $groupId ${p.partitions()}  ${r.size}")
    fetchedData = r.iterator
  }

  /**
   * Return the available offset range of the current partition. It's a pair of the earliest offset
   * and the latest offset.
   */
  private def getAvailableOffsetRange(): (Long, Long) = {
    consumer.seekToBeginning(Set(topicPartition).asJava)
    val earliestOffset = consumer.position(topicPartition)
    consumer.seekToEnd(Set(topicPartition).asJava)
    val latestOffset = consumer.position(topicPartition)
    (earliestOffset, latestOffset)
  }
}

private[kafka010] object CachedKafkaConsumer extends Logging {

  private val UNKNOWN_OFFSET = -2L

  private case class CacheKey(groupId: String, topicPartition: TopicPartition)

  private lazy val cache = {
    val conf = SparkEnv.get.conf
    val capacity = conf.getInt("spark.sql.kafkaConsumerCache.capacity", 64)
    new ju.LinkedHashMap[CacheKey, CachedKafkaConsumer](capacity, 0.75f, true) {
      override def removeEldestEntry(
        entry: ju.Map.Entry[CacheKey, CachedKafkaConsumer]): Boolean = {
        if (this.size > capacity) {
          logWarning(s"KafkaConsumer cache hitting max capacity of $capacity, " +
            s"removing consumer for ${entry.getKey}")
          try {
            entry.getValue.close()
          } catch {
            case e: SparkException =>
              logError(s"Error closing earliest Kafka consumer for ${entry.getKey}", e)
          }
          true
        } else {
          false
        }
      }
    }
  }

  /**
   * Get a cached consumer for groupId, assigned to topic and partition.
   * If matching consumer doesn't already exist, will be created using kafkaParams.
   */
  def getOrCreate(
      topic: String,
      partition: Int,
      kafkaParams: ju.Map[String, Object]): CachedKafkaConsumer = synchronized {
    val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]
    val topicPartition = new TopicPartition(topic, partition)
    val key = CacheKey(groupId, topicPartition)

    // If this is reattempt at running the task, then invalidate cache and start with
    // a new consumer
    if (TaskContext.get != null && TaskContext.get.attemptNumber > 1) {
      cache.remove(key)
      new CachedKafkaConsumer(topicPartition, kafkaParams)
    } else {
      if (!cache.containsKey(key)) {
        cache.put(key, new CachedKafkaConsumer(topicPartition, kafkaParams))
      }
      cache.get(key)
    }
  }
}
