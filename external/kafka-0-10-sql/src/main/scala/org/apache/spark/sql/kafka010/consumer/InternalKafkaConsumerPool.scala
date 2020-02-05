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

package org.apache.spark.sql.kafka010.consumer

import java.{util => ju}
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.pool2.{BaseKeyedPooledObjectFactory, PooledObject, SwallowedExceptionListener}
import org.apache.commons.pool2.impl.{DefaultEvictionPolicy, DefaultPooledObject, GenericKeyedObjectPool, GenericKeyedObjectPoolConfig}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.kafka010._
import org.apache.spark.sql.kafka010.consumer.InternalKafkaConsumerPool._
import org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer.CacheKey

/**
 * Provides object pool for [[InternalKafkaConsumer]] which is grouped by [[CacheKey]].
 *
 * This class leverages [[GenericKeyedObjectPool]] internally, hence providing methods based on
 * the class, and same contract applies: after using the borrowed object, you must either call
 * returnObject() if the object is healthy to return to pool, or invalidateObject() if the object
 * should be destroyed.
 *
 * The soft capacity of pool is determined by "spark.kafka.consumer.cache.capacity" config value,
 * and the pool will have reasonable default value if the value is not provided.
 * (The instance will do its best effort to respect soft capacity but it can exceed when there's
 * a borrowing request and there's neither free space nor idle object to clear.)
 *
 * This class guarantees that no caller will get pooled object once the object is borrowed and
 * not yet returned, hence provide thread-safety usage of non-thread-safe [[InternalKafkaConsumer]]
 * unless caller shares the object to multiple threads.
 */
private[consumer] class InternalKafkaConsumerPool(
    objectFactory: ObjectFactory,
    poolConfig: PoolConfig) extends Logging {
  def this(conf: SparkConf) = {
    this(new ObjectFactory, new PoolConfig(conf))
  }

  // the class is intended to have only soft capacity
  assert(poolConfig.getMaxTotal < 0)

  private val pool = {
    val internalPool = new GenericKeyedObjectPool[CacheKey, InternalKafkaConsumer](
      objectFactory, poolConfig)
    internalPool.setSwallowedExceptionListener(CustomSwallowedExceptionListener)
    internalPool
  }

  /**
   * Borrows [[InternalKafkaConsumer]] object from the pool. If there's no idle object for the key,
   * the pool will create the [[InternalKafkaConsumer]] object.
   *
   * If the pool doesn't have idle object for the key and also exceeds the soft capacity,
   * pool will try to clear some of idle objects.
   *
   * Borrowed object must be returned by either calling returnObject or invalidateObject, otherwise
   * the object will be kept in pool as active object.
   */
  def borrowObject(key: CacheKey, kafkaParams: ju.Map[String, Object]): InternalKafkaConsumer = {
    updateKafkaParamForKey(key, kafkaParams)

    if (size >= poolConfig.softMaxSize) {
      logWarning("Pool exceeds its soft max size, cleaning up idle objects...")
      pool.clearOldest()
    }

    pool.borrowObject(key)
  }

  /** Returns borrowed object to the pool. */
  def returnObject(consumer: InternalKafkaConsumer): Unit = {
    pool.returnObject(extractCacheKey(consumer), consumer)
  }

  /** Invalidates (destroy) borrowed object to the pool. */
  def invalidateObject(consumer: InternalKafkaConsumer): Unit = {
    pool.invalidateObject(extractCacheKey(consumer), consumer)
  }

  /** Invalidates all idle consumers for the key */
  def invalidateKey(key: CacheKey): Unit = {
    pool.clear(key)
  }

  /**
   * Closes the keyed object pool. Once the pool is closed,
   * borrowObject will fail with [[IllegalStateException]], but returnObject and invalidateObject
   * will continue to work, with returned objects destroyed on return.
   *
   * Also destroys idle instances in the pool.
   */
  def close(): Unit = {
    pool.close()
  }

  def reset(): Unit = {
    // this is the best-effort of clearing up. otherwise we should close the pool and create again
    // but we don't want to make it "var" only because of tests.
    pool.clear()
  }

  def numIdle: Int = pool.getNumIdle

  def numIdle(key: CacheKey): Int = pool.getNumIdle(key)

  def numActive: Int = pool.getNumActive

  def numActive(key: CacheKey): Int = pool.getNumActive(key)

  def size: Int = numIdle + numActive

  def size(key: CacheKey): Int = numIdle(key) + numActive(key)

  // TODO: revisit the relation between CacheKey and kafkaParams - for now it looks a bit weird
  //   as we force all consumers having same (groupId, topicPartition) to have same kafkaParams
  //   which might be viable in performance perspective (kafkaParams might be too huge to use
  //   as a part of key), but there might be the case kafkaParams could be different -
  //   cache key should be differentiated for both kafkaParams.
  private def updateKafkaParamForKey(key: CacheKey, kafkaParams: ju.Map[String, Object]): Unit = {
    // We can assume that kafkaParam should not be different for same cache key,
    // otherwise we can't reuse the cached object and cache key should contain kafkaParam.
    // So it should be safe to put the key/value pair only when the key doesn't exist.
    val oldKafkaParams = objectFactory.keyToKafkaParams.putIfAbsent(key, kafkaParams)
    require(oldKafkaParams == null || kafkaParams == oldKafkaParams, "Kafka parameters for same " +
      s"cache key should be equal. old parameters: $oldKafkaParams new parameters: $kafkaParams")
  }

  private def extractCacheKey(consumer: InternalKafkaConsumer): CacheKey = {
    new CacheKey(consumer.topicPartition, consumer.kafkaParams)
  }
}

private[consumer] object InternalKafkaConsumerPool {
  object CustomSwallowedExceptionListener extends SwallowedExceptionListener with Logging {
    override def onSwallowException(e: Exception): Unit = {
      logError(s"Error closing Kafka consumer", e)
    }
  }

  class PoolConfig(conf: SparkConf) extends GenericKeyedObjectPoolConfig[InternalKafkaConsumer] {
    private var _softMaxSize = Int.MaxValue

    def softMaxSize: Int = _softMaxSize

    init()

    def init(): Unit = {
      _softMaxSize = conf.get(CONSUMER_CACHE_CAPACITY)

      val jmxEnabled = conf.get(CONSUMER_CACHE_JMX_ENABLED)
      val minEvictableIdleTimeMillis = conf.get(CONSUMER_CACHE_TIMEOUT)
      val evictorThreadRunIntervalMillis = conf.get(
        CONSUMER_CACHE_EVICTOR_THREAD_RUN_INTERVAL)

      // NOTE: Below lines define the behavior, so do not modify unless you know what you are
      // doing, and update the class doc accordingly if necessary when you modify.

      // 1. Set min idle objects per key to 0 to avoid creating unnecessary object.
      // 2. Set max idle objects per key to 3 but set total objects per key to infinite
      // which ensures borrowing per key is not restricted.
      // 3. Set max total objects to infinite which ensures all objects are managed in this pool.
      setMinIdlePerKey(0)
      setMaxIdlePerKey(3)
      setMaxTotalPerKey(-1)
      setMaxTotal(-1)

      // Set minimum evictable idle time which will be referred from evictor thread
      setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis)
      setSoftMinEvictableIdleTimeMillis(-1)

      // evictor thread will run test with ten idle objects
      setTimeBetweenEvictionRunsMillis(evictorThreadRunIntervalMillis)
      setNumTestsPerEvictionRun(10)
      setEvictionPolicy(new DefaultEvictionPolicy[InternalKafkaConsumer]())

      // Immediately fail on exhausted pool while borrowing
      setBlockWhenExhausted(false)

      setJmxEnabled(jmxEnabled)
      setJmxNamePrefix("kafka010-cached-simple-kafka-consumer-pool")
    }
  }

  class ObjectFactory extends BaseKeyedPooledObjectFactory[CacheKey, InternalKafkaConsumer] {
    val keyToKafkaParams = new ConcurrentHashMap[CacheKey, ju.Map[String, Object]]()

    override def create(key: CacheKey): InternalKafkaConsumer = {
      Option(keyToKafkaParams.get(key)) match {
        case Some(kafkaParams) => new InternalKafkaConsumer(key.topicPartition, kafkaParams)
        case None => throw new IllegalStateException("Kafka params should be set before " +
          "borrowing object.")
      }
    }

    override def wrap(value: InternalKafkaConsumer): PooledObject[InternalKafkaConsumer] = {
      new DefaultPooledObject[InternalKafkaConsumer](value)
    }

    override def destroyObject(key: CacheKey, p: PooledObject[InternalKafkaConsumer]): Unit = {
      p.getObject.close()
    }
  }
}
