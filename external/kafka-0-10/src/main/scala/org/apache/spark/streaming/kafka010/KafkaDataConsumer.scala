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

package org.apache.spark.streaming.kafka010

import java.{util => ju}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.consumer.SparkKafkaConsumer

private[kafka010] sealed trait KafkaDataConsumer[K, V] {
  /**
   * Get the record for the given offset if available.
   *
   * @param offset         the offset to fetch.
   */
  def get(offset: Long): ConsumerRecord[K, V] = {
    internalConsumer.get(offset)
  }

  /**
   * Start a batch on a compacted topic
   *
   * @param offset         the offset to fetch.
   */
  def compactedStart(offset: Long): Unit = {
    internalConsumer.compactedStart(offset)
  }

  /**
   * Get the next record in the batch from a compacted topic.
   * Assumes compactedStart has been called first, and ignores gaps.
   */
  def compactedNext(): ConsumerRecord[K, V] = {
    internalConsumer.compactedNext()
  }

  /**
   * Rewind to previous record in the batch from a compacted topic.
   *
   * @throws NoSuchElementException if no previous element
   */
  def compactedPrevious(): ConsumerRecord[K, V] = {
    internalConsumer.compactedPrevious()
  }

  /**
   * Release this consumer from being further used. Depending on its implementation,
   * this consumer will be either finalized, or reset for reuse later.
   */
  def release(): Unit

  /** Reference to the internal implementation that this wrapper delegates to */
  def internalConsumer: InternalKafkaConsumer[K, V]
}


/**
 * A wrapper around Kafka's KafkaConsumer.
 * This is not for direct use outside this file.
 */
private[kafka010] class InternalKafkaConsumer[K, V](
    val topicPartition: TopicPartition,
    val kafkaParams: ju.Map[String, Object],
    val sparkKafkaConsumer: SparkKafkaConsumer[K, V]) extends Logging {

  private[kafka010] val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG)
    .asInstanceOf[String]

  /** indicates whether this consumer is in use or not */
  var inUse = true

  /** indicate whether this consumer is going to be stopped in the next release */
  var markedForClose = false

  override def toString: String = {
    "InternalKafkaConsumer(" +
      s"hash=${Integer.toHexString(hashCode)}, " +
      s"groupId=$groupId, " +
      s"topicPartition=$topicPartition)"
  }

  def close(): Unit = sparkKafkaConsumer.close()

  /**
   * Get the record for the given offset, waiting up to timeout ms if IO is necessary.
   * Sequential forward access will use buffers, but random access will be horribly inefficient.
   */
  def get(offset: Long): ConsumerRecord[K, V] = {
    logTrace(s"Get $groupId $topicPartition nextOffset "
      + sparkKafkaConsumer.getNextOffset()
      + " requested $offset")
    if (offset != sparkKafkaConsumer.getNextOffset()) {
      logInfo(s"Initial fetch for $groupId $topicPartition $offset")
      sparkKafkaConsumer.ensureOffset(offset)
    }

    var record = sparkKafkaConsumer.getNextRecord()
    require(record != null,
      s"Failed to get records for $groupId $topicPartition $offset after polling for "
    + sparkKafkaConsumer.getTimeout())

    if (record.offset != offset) {
      sparkKafkaConsumer.ensureOffset(offset)
      record = sparkKafkaConsumer.getNextRecord()
      require(record != null,
        s"Failed to get records for $groupId $topicPartition $offset after polling for "
          + sparkKafkaConsumer.getTimeout())
    }
    record
  }

  /**
   * Start a batch on a compacted topic
   */
  def compactedStart(offset: Long): Unit = {
    logDebug(s"compacted start $groupId $topicPartition starting $offset")
    // This seek may not be necessary, but it's hard to tell due to gaps in compacted topics
    if (offset != sparkKafkaConsumer.getNextOffset()) {
      logInfo(s"Initial fetch for compacted $groupId $topicPartition $offset")
      sparkKafkaConsumer.ensureOffset(offset)
    }
  }

  /**
   * Get the next record in the batch from a compacted topic.
   * Assumes compactedStart has been called first, and ignores gaps.
   */
  def compactedNext(): ConsumerRecord[K, V] = {
    val record = sparkKafkaConsumer.getNextRecord()
    require(record != null,
      s"Failed to get records for compacted $groupId $topicPartition after polling for "
        + sparkKafkaConsumer.getTimeout())
    record
  }

  /**
   * Rewind to previous record in the batch from a compacted topic.
   * @throws NoSuchElementException if no previous element
   */
  def compactedPrevious(): ConsumerRecord[K, V] = {
    sparkKafkaConsumer.moveToPrevious()
  }
}

private[kafka010] case class CacheKey(groupId: String, topicPartition: TopicPartition)

private[kafka010] object KafkaDataConsumer extends Logging {

  private case class CachedKafkaDataConsumer[K, V](internalConsumer: InternalKafkaConsumer[K, V])
    extends KafkaDataConsumer[K, V] {
    assert(internalConsumer.inUse)
    override def release(): Unit = KafkaDataConsumer.release(internalConsumer)
  }

  private case class NonCachedKafkaDataConsumer[K, V](internalConsumer: InternalKafkaConsumer[K, V])
    extends KafkaDataConsumer[K, V] {
    override def release(): Unit = internalConsumer.close()
  }

  // Don't want to depend on guava, don't want a cleanup thread, use a simple LinkedHashMap
  private[kafka010] var cache: ju.Map[CacheKey, InternalKafkaConsumer[_, _]] = null

  /**
   * Must be called before acquire, once per JVM, to configure the cache.
   * Further calls are ignored.
   */
  def init(
      initialCapacity: Int,
      maxCapacity: Int,
      loadFactor: Float): Unit = synchronized {
    if (null == cache) {
      logInfo(s"Initializing cache $initialCapacity $maxCapacity $loadFactor")
      cache = new ju.LinkedHashMap[CacheKey, InternalKafkaConsumer[_, _]](
        initialCapacity, loadFactor, true) {
        override def removeEldestEntry(
            entry: ju.Map.Entry[CacheKey, InternalKafkaConsumer[_, _]]): Boolean = {

          // Try to remove the least-used entry if its currently not in use.
          //
          // If you cannot remove it, then the cache will keep growing. In the worst case,
          // the cache will grow to the max number of concurrent tasks that can run in the executor,
          // (that is, number of tasks slots) after which it will never reduce. This is unlikely to
          // be a serious problem because an executor with more than 64 (default) tasks slots is
          // likely running on a beefy machine that can handle a large number of simultaneously
          // active consumers.

          if (entry.getValue.inUse == false && this.size > maxCapacity) {
            logWarning(
              s"KafkaConsumer cache hitting max capacity of $maxCapacity, " +
                s"removing consumer for ${entry.getKey}")
            try {
              entry.getValue.close()
            } catch {
              case x: KafkaException =>
                logError("Error closing oldest Kafka consumer", x)
            }
            true
          } else {
            false
          }
        }
      }
    }
  }

  /**
   * Get a cached consumer for groupId, assigned to topic and partition.
   * If matching consumer doesn't already exist, will be created using kafkaParams.
   * The returned consumer must be released explicitly using [[KafkaDataConsumer.release()]].
   *
   * Note: This method guarantees that the consumer returned is not currently in use by anyone
   * else. Within this guarantee, this method will make a best effort attempt to re-use consumers by
   * caching them and tracking when they are in use.
   */
  def acquire[K, V](
      topicPartition: TopicPartition,
      kafkaParams: ju.Map[String, Object],
      context: TaskContext,
      useCache: Boolean,
      sparkKafkaConsumer: SparkKafkaConsumer[K, V]): KafkaDataConsumer[K, V] = synchronized {
    val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]
    val key = new CacheKey(groupId, topicPartition)
    val existingInternalConsumer = cache.get(key)

    lazy val newInternalConsumer = new InternalKafkaConsumer[K, V](
      topicPartition, kafkaParams, sparkKafkaConsumer
    )

    if (context != null && context.attemptNumber >= 1) {
      // If this is reattempt at running the task, then invalidate cached consumers if any and
      // start with a new one. If prior attempt failures were cache related then this way old
      // problematic consumers can be removed.
      logDebug(s"Reattempt detected, invalidating cached consumer $existingInternalConsumer")
      if (existingInternalConsumer != null) {
        // Consumer exists in cache. If its in use, mark it for closing later, or close it now.
        if (existingInternalConsumer.inUse) {
          existingInternalConsumer.markedForClose = true
        } else {
          existingInternalConsumer.close()
          // Remove the consumer from cache only if it's closed.
          // Marked for close consumers will be removed in release function.
          cache.remove(key)
        }
      }

      logDebug("Reattempt detected, new non-cached consumer will be allocated " +
        s"$newInternalConsumer")
      NonCachedKafkaDataConsumer(newInternalConsumer)
    } else if (!useCache) {
      // If consumer reuse turned off, then do not use it, return a new consumer
      logDebug("Cache usage turned off, new non-cached consumer will be allocated " +
        s"$newInternalConsumer")
      NonCachedKafkaDataConsumer(newInternalConsumer)
    } else if (existingInternalConsumer == null) {
      // If consumer is not already cached, then put a new in the cache and return it
      logDebug("No cached consumer, new cached consumer will be allocated " +
        s"$newInternalConsumer")
      cache.put(key, newInternalConsumer)
      CachedKafkaDataConsumer(newInternalConsumer)
    } else if (existingInternalConsumer.inUse) {
      // If consumer is already cached but is currently in use, then return a new consumer
      logDebug("Used cached consumer found, new non-cached consumer will be allocated " +
        s"$newInternalConsumer")
      NonCachedKafkaDataConsumer(newInternalConsumer)
    } else {
      // If consumer is already cached and is currently not in use, then return that consumer
      logDebug(s"Not used cached consumer found, re-using it $existingInternalConsumer")
      existingInternalConsumer.inUse = true
      // Any given TopicPartition should have a consistent key and value type
      CachedKafkaDataConsumer(existingInternalConsumer.asInstanceOf[InternalKafkaConsumer[K, V]])
    }
  }

  private def release(internalConsumer: InternalKafkaConsumer[_, _]): Unit = synchronized {
    // Clear the consumer from the cache if this is indeed the consumer present in the cache
    val key = new CacheKey(internalConsumer.groupId, internalConsumer.topicPartition)
    val cachedInternalConsumer = cache.get(key)
    if (internalConsumer.eq(cachedInternalConsumer)) {
      // The released consumer is the same object as the cached one.
      if (internalConsumer.markedForClose) {
        internalConsumer.close()
        cache.remove(key)
      } else {
        internalConsumer.inUse = false
      }
    } else {
      // The released consumer is either not the same one as in the cache, or not in the cache
      // at all. This may happen if the cache was invalidate while this consumer was being used.
      // Just close this consumer.
      internalConsumer.close()
      logInfo(s"Released a supposedly cached consumer that was not found in the cache " +
        s"$internalConsumer")
    }
  }
}
