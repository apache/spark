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
import java.util.concurrent.{Callable, Executors, ExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.ThreadUtils

private[kafka010] sealed trait KafkaDataConsumer[K, V] {
  /**
   * Get the record for the given offset if available.
   *
   * @param offset         the offset to fetch.
   * @param pollTimeoutMs  timeout in milliseconds to poll data from Kafka.
   */
  def get(offset: Long, pollTimeoutMs: Long): ConsumerRecord[K, V] = {
    internalConsumer.get(offset, pollTimeoutMs)
  }

  /**
   * Start a batch on a compacted topic
   *
   * @param offset         the offset to fetch.
   * @param pollTimeoutMs  timeout in milliseconds to poll data from Kafka.
   */
  def compactedStart(offset: Long, pollTimeoutMs: Long): Unit = {
    internalConsumer.compactedStart(offset, pollTimeoutMs)
  }

  /**
   * Get the next record in the batch from a compacted topic.
   * Assumes compactedStart has been called first, and ignores gaps.
   *
   * @param pollTimeoutMs  timeout in milliseconds to poll data from Kafka.
   */
  def compactedNext(pollTimeoutMs: Long): ConsumerRecord[K, V] = {
    internalConsumer.compactedNext(pollTimeoutMs)
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
    val maintainBufferMin: Int,
    val asyncExecutorContext: ExecutorService) extends Logging {

  private[kafka010] val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG)
    .asInstanceOf[String]

  /** indicates whether this consumer is in use or not */
  var inUse = true

  /** indicate whether this consumer is going to be stopped in the next release */
  var markedForClose = false

  implicit val consumerHelper = new KafkaConsumerHelper[K, V](
    topicPartition, kafkaParams, maintainBufferMin,
    ExecutionContext.fromExecutor(asyncExecutorContext)
  )

  override def toString: String = {
    "InternalKafkaConsumer(" +
      s"hash=${Integer.toHexString(hashCode)}, " +
      s"groupId=$groupId, " +
      s"topicPartition=$topicPartition)"
  }

  def close(): Unit = consumerHelper.close()

  /**
   * Get the record for the given offset, waiting up to timeout ms if IO is necessary.
   * Sequential forward access will use buffers, but random access will be horribly inefficient.
   */
  def get(offset: Long, timeout: Long): ConsumerRecord[K, V] = {
    logTrace(s"Get $groupId $topicPartition nextOffset "
      + consumerHelper.getNextOffset()
      + " requested $offset")
    if (offset != consumerHelper.getNextOffset()) {
      logInfo(s"Initial fetch for $groupId $topicPartition $offset")
      consumerHelper.ensureOffset(offset)
    }

    var record = consumerHelper.getNextRecord(timeout)
    require(record != null,
      s"Failed to get records for $groupId $topicPartition $offset after polling for $timeout")

    if (record.offset != offset) {
      consumerHelper.ensureOffset(offset)
      record = consumerHelper.getNextRecord(timeout)
      require(record != null,
        s"Failed to get records for $groupId $topicPartition $offset after polling for $timeout")
    }
    record
  }

  /**
   * Start a batch on a compacted topic
   */
  def compactedStart(offset: Long, pollTimeoutMs: Long): Unit = {
    logDebug(s"compacted start $groupId $topicPartition starting $offset")
    // This seek may not be necessary, but it's hard to tell due to gaps in compacted topics
    if (offset != consumerHelper.getNextOffset()) {
      logInfo(s"Initial fetch for compacted $groupId $topicPartition $offset")
      consumerHelper.ensureOffset(offset)
    }
    consumerHelper.ensureBuffer(pollTimeoutMs)
  }

  /**
   * Get the next record in the batch from a compacted topic.
   * Assumes compactedStart has been called first, and ignores gaps.
   */
  def compactedNext(pollTimeoutMs: Long): ConsumerRecord[K, V] = {
    val record = consumerHelper.getNextRecord(pollTimeoutMs)
    require(record != null,
      s"Failed to get records for compacted $groupId $topicPartition " +
        s"after polling for $pollTimeoutMs")
    record
  }

  /**
   * Rewind to previous record in the batch from a compacted topic.
   * @throws NoSuchElementException if no previous element
   */
  def compactedPrevious(): ConsumerRecord[K, V] = {
    consumerHelper.moveToPrevious()
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

  private[kafka010] var asyncExecutorContext: ExecutorService = null

  /**
   * Must be called before acquire, once per JVM, to configure the cache.
   * Further calls are ignored.
   */
  def init(
      initialCapacity: Int,
      maxCapacity: Int,
      loadFactor: Float,
      asyncBufferThread: Int): Unit = synchronized {
    if (null == asyncExecutorContext) {
      asyncExecutorContext = Executors.newFixedThreadPool(asyncBufferThread)
    }

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
      useCache: Boolean): KafkaDataConsumer[K, V] = {
    acquire(topicPartition, kafkaParams, context, useCache, 0)
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
      maintainBufferMin: Int): KafkaDataConsumer[K, V] = synchronized {
    val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]
    val key = new CacheKey(groupId, topicPartition)
    val existingInternalConsumer = cache.get(key)

    lazy val newInternalConsumer = new InternalKafkaConsumer[K, V](
      topicPartition, kafkaParams, maintainBufferMin, asyncExecutorContext
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

private[kafka010] class KafkaConsumerHelper[K, V](
    val topicPartition: TopicPartition,
    val kafkaParams: ju.Map[String, Object],
    val maintainBufferMin: Int,
    val asyncExecutorContext: ExecutionContext) extends Logging {

  private val isAsyncBufferEnabled: Boolean = maintainBufferMin > 0

  val consumer = createConsumer
  lazy val asyncConsumer = if (isAsyncBufferEnabled) createConsumer else null

  // TODO if the buffer was kept around as a random-access structure,
  // could possibly optimize re-calculating of an RDD in the same batch
  // Buffer is kept linkedlist to fasten-up read from first and last
  // Add records to buffer could be optimized further
  val buffer = new ju.LinkedList[ConsumerRecord[K, V]]()

  /** We need to maintain lastRecord to revert buffer to previous state
   * in case of compacted iteration
   */
  @volatile private var lastRecord: ConsumerRecord[K, V] = null
  @volatile private var nextOffset = KafkaConsumerHelper.UNKNOWN_OFFSET

  // Pre-initialize async task and output to empty list
  // This tuple will cleaned-up and updated as required
  lazy val asyncPollTask: AsyncPollTask[K, V] = if (isAsyncBufferEnabled) {
    new AsyncPollTask[K, V](topicPartition, asyncExecutorContext, asyncConsumer)
  } else null

  /**
   * Ensure that next record have expected offset.
   * If not clear the buffer and async poll task, and reset seek offset
   */
  def ensureOffset(offset: Long): Boolean = {
    if (buffer.isEmpty || buffer.getFirst.offset() != offset) {
      logDebug(s"$topicPartition $offset No buffer or offset mismatch - Cleaning buffer")
      // clean buffer
      buffer.clear()
      // reset offset
      nextOffset = offset
      // cancel task
      if (isAsyncBufferEnabled) {
        logDebug(s"$topicPartition $offset Canceling async task since offset is getting reset")
        asyncPollTask.cancelTask()
      }
      // seek to offset in consumer
      logDebug(s"$topicPartition $offset Seeking to offset $offset")
      consumer.seek(topicPartition, offset)
    }
    true
  }

  /**
   * Ensure buffer have data for topic and partition.
   * If there is a completed async poll task, add its result to the buffer, and cleanup the task.
   * If buffer is empty cancel async poll task if exists, and make sync poll through consumer.
   * If async buffer is enabled, check if buffer is filled atleast till maintainBufferMin,
   *  if not create a new async task to poll more records.
   */
  def ensureBuffer(timeout: Long): Unit = {
    // If last future is started, wait for completion, populate the buffer, and clean up future
    if (isAsyncBufferEnabled && asyncPollTask.isCompleted) {
      val records = asyncPollTask.getResult(timeout)
      logDebug(s"$topicPartition $nextOffset " +
        s"Async Polled " + records.size())
      buffer.addAll(records)
    }

    // If buffer is empty cancel the future and poll in current thread,
    // since can't wait for its scheduling
    if (buffer.isEmpty) {
      if (isAsyncBufferEnabled && asyncPollTask.isStarted()
        && asyncPollTask.getSeekOffset() == nextOffset) {
        val records = asyncPollTask.getResult(timeout)
        logDebug(s"$topicPartition $nextOffset " + s"Blocked async Polled " + records.size())
        buffer.addAll(records)
      } else {
        val records = consumer.poll(timeout)
        logDebug(s"$topicPartition $nextOffset Sync Polled " + records.count())
        buffer.addAll(records.records(topicPartition))
      }
    }

    if (isAsyncBufferEnabled) {
      // If buffer is less than maintainBufferMin, create new future
      // When this future completes, next ensureBuffer call will populate the buffer
      checkAndFillBuffer(timeout)
    }
  }

  /**
   * If there is no pending async poll task and buffer is less than maintainBufferMin,
   *  create new async task to poll more records
   */
  private def checkAndFillBuffer(timeout: Long) = {
    val bufferSize = buffer.size()
    if (!asyncPollTask.isActive() && bufferSize < maintainBufferMin) {
      logDebug(s"$topicPartition $nextOffset Buffer is less.Creating async task " +
        s"$maintainBufferMin $bufferSize")
      asyncPollTask.startTask(new KafkaPollCallable[ju.List[ConsumerRecord[K, V]]] {
        val offsetToFetch = if (buffer.isEmpty) nextOffset else buffer.getLast.offset() + 1
        override def call(): ju.List[ConsumerRecord[K, V]] = {
          asyncConsumer.seek(
            topicPartition,
            offsetToFetch
          )
          val records = asyncConsumer.poll(timeout).records(topicPartition)
          logDebug(
            s"$topicPartition $nextOffset $offsetToFetch Async Polled records " + records.size()
              + s" from offset : $offsetToFetch"
          )
          records
        }
        override def getSeekOffset(): Long = offsetToFetch
      })
    }
  }

  /**
   * Get next record from buffer.
   *  First ensure buffer have data.
   *  Then return first record from buffer.
   */
  def getNextRecord(timeout: Long): ConsumerRecord[K, V] = {
    ensureBuffer(timeout)
    var record: ConsumerRecord[K, V] = null
    if (!buffer.isEmpty) {
      record = buffer.removeFirst()
      nextOffset = record.offset() + 1
    }
    lastRecord = record;
    record
  }

  /**
   * Add last record back to buffer to revert the state.
   */
  def moveToPrevious(): ConsumerRecord[K, V] = {
    buffer.addFirst(lastRecord)
    lastRecord
  }

  def getNextOffset(): Long = {
    nextOffset
  }

  /** Create a KafkaConsumer to fetch records for `topicPartition` */
  private def createConsumer: KafkaConsumer[K, V] = {
    val c = new KafkaConsumer[K, V](kafkaParams)
    val topics = ju.Arrays.asList(topicPartition)
    c.assign(topics)
    logDebug(s"$topicPartition Created new kafka consumer with assigned topics $topics")
    c
  }

  def close(): Unit = {
    logDebug(s"$topicPartition Closing consumers")
    consumer.close()
    if (isAsyncBufferEnabled) {
      asyncPollTask.close()
    }
  }
}

private[kafka010] class AsyncPollTask[K, V](
                                topicPartition: TopicPartition,
                                executionContext: ExecutionContext,
                                kafkaConsumer: KafkaConsumer[K, V]
                              ) extends Logging {

  private var callable: KafkaPollCallable[ju.List[ConsumerRecord[K, V]]] = null

  private var future: Future[ju.List[ConsumerRecord[K, V]]] = null

  def cancelTask(): Unit = {
    if (callable != null) {
      callable.cancelTask()
    }
    callable = null
    future = null
  }

  def isStarted(): Boolean = {
    callable!=null && callable.started.get()
  }

  def isCompleted(): Boolean = {
    callable!=null && callable.started.get() && future.isCompleted
  }

  def isActive(): Boolean = callable != null

  def getResult(timeout: Long): ju.List[ConsumerRecord[K, V]] = {
    val records = ThreadUtils.awaitResult(future, Duration(timeout, TimeUnit.MILLISECONDS))
    cancelTask()
    records
  }

  def getSeekOffset(): Long = if (!isActive()) {
    KafkaConsumerHelper.UNKNOWN_OFFSET
  } else {
    callable.getSeekOffset()
  }

  def startTask(pollTask: KafkaPollCallable[ju.List[ConsumerRecord[K, V]]]): Unit = {
    callable = pollTask
    future = Future {
      // Check if this task is already canceled
      if (!callable.canceled.get()) {
        callable.startTask()
        callable.call()
      }
      else {
        logDebug(s"$topicPartition Async Tasks canceled. Skipping.")
        null.asInstanceOf[ju.List[ConsumerRecord[K, V]]]
      }
    }(executionContext)
  }

  /**
   * Close async kafka consumer. It uses future to close the consumer
   * since task could be running and throw exception if closed directly
   */
  def close(): Unit = {
    Future {
      ThreadUtils.awaitResult(future, (Duration(5, TimeUnit.MINUTES)))
      cancelTask()
      kafkaConsumer.close()
    } (executionContext)
  }

}

private[kafka010] trait KafkaPollCallable[T] extends Callable[T] {
  val canceled = new AtomicBoolean(false)
  val started = new AtomicBoolean(false)

  def cancelTask(): Unit = {
    canceled.set(true)
  }

  def startTask(): Unit = {
    started.set(true)
  }

  def getSeekOffset(): Long
}

private[kafka010] object KafkaConsumerHelper {
  val UNKNOWN_OFFSET = -2L
}
