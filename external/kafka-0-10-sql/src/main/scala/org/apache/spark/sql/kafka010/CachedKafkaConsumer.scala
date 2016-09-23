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

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
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

  protected val consumer = {
    val c = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
    val tps = new ju.ArrayList[TopicPartition]()
    tps.add(topicPartition)
    c.assign(tps)
    c
  }

  // Timeout for polls to the consumer can be small as the data is expected to be present in Kafka
  private val pollTimeoutMs = 1000

  // Iterator to the already fetch data
  protected var buffer = ju.Collections.emptyIterator[ConsumerRecord[Array[Byte], Array[Byte]]]
  protected var nextOffset = -2L

  /**
   * Get the record for the given offset, waiting up to timeout ms if IO is necessary.
   * Sequential forward access will use buffers, but random access will be horribly inefficient.
   */
  def get(offset: Long): ConsumerRecord[Array[Byte], Array[Byte]] = {
    logDebug(s"Get $topicPartition nextOffset $nextOffset requested $offset")
    if (offset != nextOffset) {
      logInfo(s"Initial fetch for $topicPartition $offset")
      seek(offset)
      poll()
    }

    if (!buffer.hasNext()) { poll() }
    assert(buffer.hasNext(),
      s"Failed to get records for $topicPartition $offset after polling for $pollTimeoutMs")
    var record = buffer.next()

    if (record.offset != offset) {
      logInfo(s"Buffer miss for $topicPartition $offset")
      seek(offset)
      poll()
      assert(buffer.hasNext(),
        s"Failed to get records for $topicPartition $offset after polling for $pollTimeoutMs")
      record = buffer.next()
      assert(record.offset == offset,
        s"Got wrong record for $topicPartition even after seeking to offset $offset")
    }

    nextOffset = offset + 1
    record
  }

  def close(): Unit = consumer.close()

  private def seek(offset: Long): Unit = {
    logDebug(s"Seeking to $topicPartition $offset")
    consumer.seek(topicPartition, offset)
  }

  private def poll(): Unit = {
    val p = consumer.poll(pollTimeoutMs)
    val r = p.records(topicPartition)
    logDebug(s"Polled ${p.partitions()}  ${r.size}")
    buffer = r.iterator
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
