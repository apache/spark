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

package org.apache.spark.sql.kafka010.share.consumer

import java.{util => ju}
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.pool2.{BaseKeyedPooledObjectFactory, PooledObject, SwallowedExceptionListener}
import org.apache.commons.pool2.impl.{BaseObjectPoolConfig, DefaultEvictionPolicy, DefaultPooledObject, GenericKeyedObjectPool, GenericKeyedObjectPoolConfig}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.kafka010._

/**
 * Object pool for [[InternalKafkaShareConsumer]] which is keyed by [[ShareConsumerCacheKey]].
 *
 * Unlike traditional Kafka consumers that are keyed by (groupId, topicPartition),
 * share consumers are keyed by (shareGroupId, topics) because share groups:
 * 1. Subscribe to topics, not individual partitions
 * 2. Allow multiple consumers to receive records from the same partition
 *
 * This pool leverages [[GenericKeyedObjectPool]] internally with the same contract:
 * after using a borrowed object, you must either call returnObject() if healthy,
 * or invalidateObject() if the object should be destroyed.
 */
private[consumer] class InternalKafkaShareConsumerPool(
    objectFactory: ShareObjectFactory,
    poolConfig: SharePoolConfig) extends Logging {

  def this(conf: SparkConf) = {
    this(new ShareObjectFactory, new SharePoolConfig(conf))
  }

  // Pool is intended to have soft capacity only
  assert(poolConfig.getMaxTotal < 0)

  private val pool = {
    val internalPool = new GenericKeyedObjectPool[ShareConsumerCacheKey, InternalKafkaShareConsumer](
      objectFactory, poolConfig)
    internalPool.setSwallowedExceptionListener(ShareSwallowedExceptionListener)
    internalPool
  }

  /**
   * Borrow a [[InternalKafkaShareConsumer]] from the pool.
   * If there's no idle consumer for the key, a new one will be created.
   *
   * @param key The cache key (shareGroupId, topics)
   * @param kafkaParams Kafka configuration parameters
   * @param lockTimeoutMs Acquisition lock timeout
   * @return A borrowed InternalKafkaShareConsumer
   */
  def borrowObject(
      key: ShareConsumerCacheKey,
      kafkaParams: ju.Map[String, Object],
      lockTimeoutMs: Long): InternalKafkaShareConsumer = {
    updateParamsForKey(key, kafkaParams, lockTimeoutMs)

    if (size >= poolConfig.softMaxSize) {
      logWarning("Share consumer pool exceeds its soft max size, cleaning up idle objects...")
      pool.clearOldest()
    }

    pool.borrowObject(key)
  }

  /** Return borrowed consumer to the pool. */
  def returnObject(consumer: InternalKafkaShareConsumer): Unit = {
    val key = extractCacheKey(consumer)
    pool.returnObject(key, consumer)
  }

  /** Invalidate (destroy) a borrowed consumer. */
  def invalidateObject(consumer: InternalKafkaShareConsumer): Unit = {
    val key = extractCacheKey(consumer)
    pool.invalidateObject(key, consumer)
  }

  /** Invalidate all idle consumers for the given key. */
  def invalidateKey(key: ShareConsumerCacheKey): Unit = {
    pool.clear(key)
  }

  /**
   * Close the pool. Once closed, borrowObject will fail.
   * returnObject and invalidateObject will continue to work.
   */
  def close(): Unit = {
    pool.close()
  }

  def reset(): Unit = {
    pool.clear()
  }

  def numIdle: Int = pool.getNumIdle
  def numIdle(key: ShareConsumerCacheKey): Int = pool.getNumIdle(key)
  def numActive: Int = pool.getNumActive
  def numActive(key: ShareConsumerCacheKey): Int = pool.getNumActive(key)
  def size: Int = numIdle + numActive
  def size(key: ShareConsumerCacheKey): Int = numIdle(key) + numActive(key)

  private def updateParamsForKey(
      key: ShareConsumerCacheKey,
      kafkaParams: ju.Map[String, Object],
      lockTimeoutMs: Long): Unit = {
    objectFactory.keyToParams.putIfAbsent(key, ShareConsumerParams(kafkaParams, lockTimeoutMs))
  }

  private def extractCacheKey(consumer: InternalKafkaShareConsumer): ShareConsumerCacheKey = {
    ShareConsumerCacheKey(consumer.shareGroupId, consumer.topics)
  }
}

/**
 * Parameters needed to create a share consumer.
 */
private[consumer] case class ShareConsumerParams(
    kafkaParams: ju.Map[String, Object],
    lockTimeoutMs: Long)

/**
 * Exception listener for the pool.
 */
private[consumer] object ShareSwallowedExceptionListener
    extends SwallowedExceptionListener with Logging {
  override def onSwallowException(e: Exception): Unit = {
    logError(s"Error closing Kafka share consumer", e)
  }
}

/**
 * Pool configuration for share consumers.
 */
private[consumer] class SharePoolConfig(conf: SparkConf)
    extends GenericKeyedObjectPoolConfig[InternalKafkaShareConsumer] {

  private var _softMaxSize = Int.MaxValue

  def softMaxSize: Int = _softMaxSize

  init()

  private def init(): Unit = {
    // Use the same configuration keys as regular consumers
    _softMaxSize = conf.get(CONSUMER_CACHE_CAPACITY)

    val jmxEnabled = conf.get(CONSUMER_CACHE_JMX_ENABLED)
    val minEvictableIdleTimeMillis = conf.get(CONSUMER_CACHE_TIMEOUT)
    val evictorThreadRunIntervalMillis = conf.get(CONSUMER_CACHE_EVICTOR_THREAD_RUN_INTERVAL)

    // Behavior configuration:
    // 1. Min idle per key = 0: don't create unnecessary consumers
    // 2. Max idle per key = 3: keep a few idle for reuse
    // 3. Max total per key = infinite: don't restrict borrowing
    // 4. Max total = infinite: all objects managed in pool
    setMinIdlePerKey(0)
    setMaxIdlePerKey(3)
    setMaxTotalPerKey(-1)
    setMaxTotal(-1)

    // Eviction configuration
    setMinEvictableIdleDuration(Duration.ofMillis(minEvictableIdleTimeMillis))
    setSoftMinEvictableIdleDuration(BaseObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_DURATION)
    setTimeBetweenEvictionRuns(Duration.ofMillis(evictorThreadRunIntervalMillis))
    setNumTestsPerEvictionRun(10)
    setEvictionPolicy(new DefaultEvictionPolicy[InternalKafkaShareConsumer]())

    // Fail immediately on exhausted pool
    setBlockWhenExhausted(false)

    setJmxEnabled(jmxEnabled)
    setJmxNamePrefix("kafka010-cached-share-consumer-pool")
  }
}

/**
 * Factory for creating and destroying share consumers.
 */
private[consumer] class ShareObjectFactory
    extends BaseKeyedPooledObjectFactory[ShareConsumerCacheKey, InternalKafkaShareConsumer] {

  val keyToParams = new ConcurrentHashMap[ShareConsumerCacheKey, ShareConsumerParams]()

  override def create(key: ShareConsumerCacheKey): InternalKafkaShareConsumer = {
    Option(keyToParams.get(key)) match {
      case Some(params) =>
        new InternalKafkaShareConsumer(
          shareGroupId = key.shareGroupId,
          topics = key.topics,
          kafkaParams = params.kafkaParams,
          lockTimeoutMs = params.lockTimeoutMs
        )
      case None =>
        throw new IllegalStateException(
          "Share consumer params should be set before borrowing object.")
    }
  }

  override def wrap(value: InternalKafkaShareConsumer): PooledObject[InternalKafkaShareConsumer] = {
    new DefaultPooledObject[InternalKafkaShareConsumer](value)
  }

  override def destroyObject(
      key: ShareConsumerCacheKey,
      p: PooledObject[InternalKafkaShareConsumer]): Unit = {
    p.getObject.close()
  }
}

