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

import scala.annotation.tailrec
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
  private var nextOffsetInFetchedData = -2L

  /**
   * Get the record for the given offset, waiting up to timeout ms if IO is necessary.
   * Sequential forward access will use buffers, but random access will be horribly inefficient.
   */
  def get(offset: Long, pollTimeoutMs: Long): ConsumerRecord[Array[Byte], Array[Byte]] = {
    logDebug(s"Get $groupId $topicPartition nextOffset $nextOffsetInFetchedData requested $offset")
    if (offset != nextOffsetInFetchedData) {
      logInfo(s"Initial fetch for $topicPartition $offset")
      seek(offset)
      poll(pollTimeoutMs)
    }

    if (!fetchedData.hasNext()) { poll(pollTimeoutMs) }
    assert(fetchedData.hasNext(),
      s"Failed to get records for $groupId $topicPartition $offset " +
        s"after polling for $pollTimeoutMs")
    var record = fetchedData.next()

    if (record.offset != offset) {
      logInfo(s"Buffer miss for $groupId $topicPartition $offset")
      seek(offset)
      poll(pollTimeoutMs)
      assert(fetchedData.hasNext(),
        s"Failed to get records for $groupId $topicPartition $offset " +
          s"after polling for $pollTimeoutMs")
      record = fetchedData.next()
      assert(record.offset == offset,
        s"Got wrong record for $groupId $topicPartition even after seeking to offset $offset")
    }

    nextOffsetInFetchedData = offset + 1
    record
  }

  @tailrec
  final def getAndIgnoreLostData(
      offset: Long,
      untilOffset: Long,
      pollTimeoutMs: Long): ConsumerRecord[Array[Byte], Array[Byte]] = {
    // scalastyle:off
    // When `failOnDataLoss` is `false`, we need to handle the following cases (note: untilOffset and latestOffset are exclusive):
    // 1. Some data are aged out, and `offset < beginningOffset <= untilOffset - 1 <= latestOffset - 1`
    //      Seek to the beginningOffset and fetch the data.
    // 2. Some data are aged out, and `offset <= untilOffset - 1 < beginningOffset`.
    //      There is nothing to fetch, return null.
    // 3. The topic is deleted.
    //      There is nothing to fetch, return null.
    // 4. The topic is deleted and recreated, and `beginningOffset <= offset <= untilOffset - 1 <= latestOffset - 1`.
    //      We cannot detect this case. We can still fetch data like nothing happens.
    // 5. The topic is deleted and recreated, and `beginningOffset <= offset < latestOffset - 1 < untilOffset - 1`.
    //      Same as 4.
    // 6. The topic is deleted and recreated, and `beginningOffset <= latestOffset - 1 < offset <= untilOffset - 1`.
    //      There is nothing to fetch, return null.
    // 7. The topic is deleted and recreated, and `offset < beginningOffset <= untilOffset - 1`.
    //      Same as 1.
    // 8. The topic is deleted and recreated, and `offset <= untilOffset - 1 < beginningOffset`.
    //      There is nothing to fetch, return null.
    // scalastyle:on
    if (offset >= untilOffset) {
      // Case 2 or 8
      // We seek to beginningOffset but beginningOffset >= untilOffset
      reset()
      return null
    }

    logDebug(s"Get $groupId $topicPartition nextOffset $nextOffsetInFetchedData requested $offset")
    var outOfOffset = false
    if (offset != nextOffsetInFetchedData) {
      logInfo(s"Initial fetch for $topicPartition $offset")
      seek(offset)
      try {
        poll(pollTimeoutMs)
      } catch {
        case e: OffsetOutOfRangeException =>
          logWarning(s"Cannot fetch offset $offset, try to recover from the beginning offset", e)
          outOfOffset = true
      }
    } else if (!fetchedData.hasNext()) {
      // The last pre-fetched data has been drained.
      seek(offset)
      try {
        poll(pollTimeoutMs)
      } catch {
        case e: OffsetOutOfRangeException =>
          logWarning(s"Cannot fetch offset $offset, try to recover from the beginning offset", e)
          outOfOffset = true
      }
    }
    if (outOfOffset) {
      val beginningOffset = getBeginningOffset()
      if (beginningOffset <= offset) {
        val latestOffset = getLatestOffset()
        if (latestOffset <= offset) {
          // Case 3 or 6
          logWarning(s"Offset ${offset} is later than the latest offset $latestOffset. " +
            s"Skipped [$offset, $untilOffset)")
          reset()
          return null
        } else {
          // Case 4 or 5
          getAndIgnoreLostData(offset, untilOffset, pollTimeoutMs)
        }
      } else {
        // Case 1 or 7
        logWarning(s"Buffer miss for $groupId $topicPartition [$offset, $beginningOffset})")
        return getAndIgnoreLostData(beginningOffset, untilOffset, pollTimeoutMs)
      }
    } else {
      if (!fetchedData.hasNext()) {
        // We cannot fetch anything after `polling`. Two possible cases:
        // - `beginningOffset` is `offset` but there is nothing for `beginningOffset` right now.
        // - Cannot fetch any date before timeout.
        // Because there is no way to distinguish, just skip the rest offsets in the current
        // partition.
        logWarning(s"Buffer miss for $groupId $topicPartition [$offset, $untilOffset)")
        reset()
        return null
      }

      val record = fetchedData.next()
      if (record.offset >= untilOffset) {
        // Case 2
        logWarning(s"Buffer miss for $groupId $topicPartition [$offset, $untilOffset)")
        reset()
        return null
      } else {
        if (record.offset != offset) {
          // Case 1
          logWarning(s"Buffer miss for $groupId $topicPartition [$offset, ${record.offset})")
        }
        nextOffsetInFetchedData = record.offset + 1
        return record
      }
    }
  }

  private def reset(): Unit = {
    nextOffsetInFetchedData = -2
    fetchedData = ju.Collections.emptyIterator[ConsumerRecord[Array[Byte], Array[Byte]]]
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

  private def getBeginningOffset(): Long = {
    consumer.seekToBeginning(Set(topicPartition).asJava)
    consumer.position(topicPartition)
  }

  private def getLatestOffset(): Long = {
    consumer.seekToEnd(Set(topicPartition).asJava)
    consumer.position(topicPartition)
  }
}

private[kafka010] object CachedKafkaConsumer extends Logging {

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
