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
   * If `failOnDataLoss` is `false`, it will try to get the earliest record in
   * `[offset, untilOffset)` when some illegal state happens. Otherwise, an `IllegalStateException`
   * will be thrown.
   *
   * It returns `null` only when `failOnDataLoss` is `false` and it cannot fetch any record between
   * [offset, untilOffset).
   */
  def get(
      offset: Long,
      untilOffset: Long,
      pollTimeoutMs: Long,
      failOnDataLoss: Boolean): ConsumerRecord[Array[Byte], Array[Byte]] = {
    require(offset < untilOffset, s"offset: $offset, untilOffset: $untilOffset")
    logDebug(s"Get $groupId $topicPartition nextOffset $nextOffsetInFetchedData requested $offset")
    try {
      fetchDataIfNeeded(offset, pollTimeoutMs)
      getRecordFromFetchedData(offset, untilOffset, failOnDataLoss)
    } catch {
      case e: OffsetOutOfRangeException =>
        val message =
          if (failOnDataLoss) {
            s"""Cannot fetch offset $offset. (GroupId: $groupId, TopicPartition: $topicPartition).
               | $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE""".stripMargin
          } else {
            s"""Cannot fetch offset $offset. Some data may be lost. Recovering from the earliest
               | offset (GroupId: $groupId, TopicPartition: $topicPartition).""".stripMargin
          }
        reportDataLoss(failOnDataLoss, message, e)
        advanceToEarliestOffsetAndFetch(offset, untilOffset, pollTimeoutMs)
    }
  }

  /**
   * Check the pre-fetched data with `offset` and try to fetch from Kafka if they don't match.
   */
  private def fetchDataIfNeeded(offset: Long, pollTimeoutMs: Long): Unit = {
    if (offset != nextOffsetInFetchedData) {
      logInfo(s"Initial fetch for $topicPartition $offset")
      seek(offset)
      poll(pollTimeoutMs)
    } else if (!fetchedData.hasNext()) {
      // The last pre-fetched data has been drained.
      // Seek to the offset because we may call seekToBeginning or seekToEnd before this.
      seek(offset)
      poll(pollTimeoutMs)
    }
  }

  /**
   * Try to advance to the beginning offset and fetch again. `earliestOffset` should be in
   * `[offset, untilOffset]`. If not, it will try to fetch `offset` again if it's in
   * `[earliestOffset, latestOffset)`. Otherwise, it will return null and reset the pre-fetched
   * data.
   */
  private def advanceToEarliestOffsetAndFetch(
      offset: Long,
      untilOffset: Long,
      pollTimeoutMs: Long): ConsumerRecord[Array[Byte], Array[Byte]] = {
    val (earliestOffset, latestOffset) = getCurrentOffsetRange()
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
          | skipped (GroupId: $groupId, TopicPartition: $topicPartition).
          | $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE
        """.stripMargin
      logWarning(warningMessage)
      reset()
      null
    } else if (offset >= earliestOffset) {
      // -----------------------------------------------------------------------------
      //         ^            ^                  ^                                 ^
      //         |            |                  |                                 |
      //   earliestOffset   offset   min(untilOffset,latestOffset)   max(untilOffset, latestOffset)
      //
      // This will happen when a topic is deleted and recreated, and new data are pushed very fast,
      // then we will see `offset` disappears first then appears again. Although the parameters
      // are same, the state in Kafka cluster is changed, so it's not an endless loop.
      //
      // In addition, the stack here won't be deep unless the user keeps deleting and creating the
      // topic very fast.
      //
      // Therefore, this recursive call is safe.
      get(offset, untilOffset, pollTimeoutMs, failOnDataLoss = false)
    } else {
      // ------------------------------------------------------------------------------
      //      ^           ^                       ^                                 ^
      //      |           |                       |                                 |
      //   offset   earliestOffset   min(untilOffset,latestOffset)   max(untilOffset, latestOffset)
      val warningMessage =
        s"""
           |The current available offset range is [$earliestOffset, $latestOffset).
           | Offset ${offset} is out of range, and records in [$offset, $earliestOffset) will be
           | skipped (GroupId: $groupId, TopicPartition: $topicPartition).
           | $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE
        """.stripMargin
      logWarning(warningMessage)
      get(earliestOffset, untilOffset, pollTimeoutMs, failOnDataLoss = false)
    }
  }

  /**
   * Get the earliest record in [offset, untilOffset) from the fetched data. If there is no such
   * record, returns null. Must be called after `poll`.
   */
  private def getRecordFromFetchedData(
      offset: Long,
      untilOffset: Long,
      failOnDataLoss: Boolean): ConsumerRecord[Array[Byte], Array[Byte]] = {
    if (!fetchedData.hasNext()) {
      // We cannot fetch anything after `poll`. Two possible cases:
      // - `earliestOffset` is `offset` but there is nothing for `earliestOffset` right now.
      // - Cannot fetch any data before timeout.
      // Because there is no way to distinguish, just skip the rest offsets in the current
      // partition.
      val message =
        if (failOnDataLoss) {
          s"""
             |Cannot fetch record for offset ${offset}
             | (GroupId: $groupId, TopicPartition: $topicPartition).
             | $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE""".stripMargin
        } else {
          s"""
             |Cannot fetch record for offset ${offset}. Records in [$offset, $untilOffset) will be
             | skipped (GroupId: $groupId, TopicPartition: $topicPartition).
             | $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE""".stripMargin
        }
      reportDataLoss(failOnDataLoss, message)
      reset()
      null
    } else {
      val record = fetchedData.next()
      if (record.offset >= untilOffset) {
        val message =
          if (failOnDataLoss) {
            s"""
               |The offset range is [$offset, $untilOffset), but the fetched record offset
               | ${record.offset} is out of range
               | (GroupId: $groupId, TopicPartition: $topicPartition).
               | $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE""".stripMargin
          } else {
            s"""
               |The fetched record offset in fetched data ${record.offset} is out of range. Records
               | in [$offset, $untilOffset) will be skipped
               | (GroupId: $groupId, TopicPartition: $topicPartition).
               | $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE""".stripMargin
          }
        reportDataLoss(failOnDataLoss, message)
        reset()
        null
      } else {
        if (record.offset != offset) {
          val message =
            if (failOnDataLoss) {
              s"""
                 |The fetched record offset ${record.offset()} is not the requested offset $offset
                 | (GroupId: $groupId, TopicPartition: $topicPartition).
                 | $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE""".stripMargin
            } else {
              s"""
                 |The fetched record offset ${record.offset()} is not the requested offset $offset.
                 | Records in [$offset, ${record.offset()}) will be skipped
                 | (GroupId: $groupId, TopicPartition: $topicPartition).
                 | $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE""".stripMargin
            }
          reportDataLoss(failOnDataLoss, message)
        }
        nextOffsetInFetchedData = record.offset + 1
        record
      }
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
   * Throw an exception or log a warning as per `failOnDataLoss`.
   */
  private def reportDataLoss(
      failOnDataLoss: Boolean,
      message: String,
      cause: Throwable = null): Unit = {
    if (failOnDataLoss) {
      if (cause != null) {
        throw new IllegalStateException(message)
      } else {
        throw new IllegalStateException(message, cause)
      }
    } else {
      if (cause != null) {
        logWarning(message)
      } else {
        logWarning(message, cause)
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

  private def getCurrentOffsetRange(): (Long, Long) = {
    consumer.seekToBeginning(Set(topicPartition).asJava)
    val earliestOffset = consumer.position(topicPartition)
    consumer.seekToEnd(Set(topicPartition).asJava)
    val latestOffset = consumer.position(topicPartition)
    (earliestOffset, latestOffset)
  }
}

private[kafka010] object CachedKafkaConsumer extends Logging {

  private val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE =
    """
      |There may have been some data loss because some data may have been aged out in Kafka or
      | the topic has been deleted and is therefore unavailable for processing. If you want your
      | streaming query to fail on such cases, set the source option "failOnDataLoss" to "true".
    """.stripMargin

  private val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE =
    """
      |There may have been some data loss because some data may have been aged out in Kafka or
      | the topic has been deleted and is therefore unavailable for processing. If you don't want
      | your streaming query to fail on such cases, set the source option "failOnDataLoss" to
      | "false".
    """.stripMargin

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
