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

package org.apache.spark.sql.kafka010.producer

import java.{util => ju}
import java.util.concurrent.{Executors, TimeUnit}

import scala.util.Random

import org.apache.kafka.common.serialization.ByteArraySerializer
import org.jmock.lib.concurrent.DeterministicScheduler

import org.apache.spark.SparkConf
import org.apache.spark.sql.kafka010.{PRODUCER_CACHE_EVICTOR_THREAD_RUN_INTERVAL, PRODUCER_CACHE_TIMEOUT}
import org.apache.spark.sql.kafka010.producer.InternalKafkaProducerPool.CachedProducerEntry
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ManualClock

class InternalKafkaProducerPoolSuite extends SharedSparkSession {

  private var pool: InternalKafkaProducerPool = _

  protected override def afterEach(): Unit = {
    if (pool != null) {
      try {
        pool.shutdown()
        pool = null
      } catch {
        // ignore as it's known issue, DeterministicScheduler doesn't support shutdown
        case _: UnsupportedOperationException =>
      }
    }
  }

  test("Should return same cached instance on calling acquire with same params.") {
    pool = new InternalKafkaProducerPool(new SparkConf())

    val kafkaParams = getTestKafkaParams()
    val producer = pool.acquire(kafkaParams)
    val producer2 = pool.acquire(kafkaParams)
    assert(producer eq producer2)

    val map = pool.getAsMap
    assert(map.size === 1)
    val cacheEntry = map.head._2
    assertCacheEntry(pool, cacheEntry, 2L)

    pool.release(producer)
    assertCacheEntry(pool, cacheEntry, 1L)

    pool.release(producer2)
    assertCacheEntry(pool, cacheEntry, 0L)

    val producer3 = pool.acquire(kafkaParams)
    assertCacheEntry(pool, cacheEntry, 1L)
    assert(producer eq producer3)
  }

  test("Should return different cached instances on calling acquire with different params.") {
    pool = new InternalKafkaProducerPool(new SparkConf())

    val kafkaParams = getTestKafkaParams()
    val producer = pool.acquire(kafkaParams)
    kafkaParams.put("acks", "1")
    val producer2 = pool.acquire(kafkaParams)
    // With updated conf, a new producer instance should be created.
    assert(producer ne producer2)

    val map = pool.getAsMap
    assert(map.size === 2)
    val cacheEntry = map.find(_._2.producer.id == producer.id).get._2
    assertCacheEntry(pool, cacheEntry, 1L)
    val cacheEntry2 = map.find(_._2.producer.id == producer2.id).get._2
    assertCacheEntry(pool, cacheEntry2, 1L)
  }

  test("expire instances") {
    val minEvictableIdleTimeMillis = 2000L
    val evictorThreadRunIntervalMillis = 500L

    val conf = new SparkConf()
    conf.set(PRODUCER_CACHE_TIMEOUT, minEvictableIdleTimeMillis)
    conf.set(PRODUCER_CACHE_EVICTOR_THREAD_RUN_INTERVAL, evictorThreadRunIntervalMillis)

    val scheduler = new DeterministicScheduler()
    val clock = new ManualClock()
    pool = new InternalKafkaProducerPool(scheduler, clock, conf)

    val kafkaParams = getTestKafkaParams()

    var map = pool.getAsMap
    assert(map.isEmpty)

    val producer = pool.acquire(kafkaParams)
    map = pool.getAsMap
    assert(map.size === 1)

    clock.advance(minEvictableIdleTimeMillis + 100)
    scheduler.tick(evictorThreadRunIntervalMillis + 100, TimeUnit.MILLISECONDS)
    map = pool.getAsMap
    assert(map.size === 1)

    pool.release(producer)

    // This will clean up expired instance from cache.
    clock.advance(minEvictableIdleTimeMillis + 100)
    scheduler.tick(evictorThreadRunIntervalMillis + 100, TimeUnit.MILLISECONDS)

    map = pool.getAsMap
    assert(map.size === 0)
  }

  test("reference counting with concurrent access") {
    pool = new InternalKafkaProducerPool(new SparkConf())

    val kafkaParams = getTestKafkaParams()

    val numThreads = 100
    val numProducerUsages = 500

    def produce(i: Int): Unit = {
      val producer = pool.acquire(kafkaParams)
      try {
        val map = pool.getAsMap
        assert(map.size === 1)
        val cacheEntry = map.head._2
        assert(cacheEntry.refCount > 0L)
        assert(cacheEntry.expireAt === Long.MaxValue)

        Thread.sleep(Random.nextInt(100))
      } finally {
        pool.release(producer)
      }
    }

    val threadpool = Executors.newFixedThreadPool(numThreads)
    try {
      val futures = (1 to numProducerUsages).map { i =>
        threadpool.submit(new Runnable {
          override def run(): Unit = { produce(i) }
        })
      }
      futures.foreach(_.get(1, TimeUnit.MINUTES))
    } finally {
      threadpool.shutdown()
    }

    val map = pool.getAsMap
    assert(map.size === 1)

    val cacheEntry = map.head._2
    assertCacheEntry(pool, cacheEntry, 0L)
  }

  private def getTestKafkaParams(): ju.HashMap[String, Object] = {
    val kafkaParams = new ju.HashMap[String, Object]()
    kafkaParams.put("acks", "0")
    // Here only host should be resolvable, it does not need a running instance of kafka server.
    kafkaParams.put("bootstrap.servers", "127.0.0.1:9022")
    kafkaParams.put("key.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams.put("value.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams
  }

  private def assertCacheEntry(
      pool: InternalKafkaProducerPool,
      cacheEntry: CachedProducerEntry,
      expectedRefCount: Long): Unit = {
    val timeoutVal = TimeUnit.MILLISECONDS.toNanos(pool.cacheExpireTimeoutMillis)
    assert(cacheEntry.refCount === expectedRefCount)
    if (expectedRefCount > 0) {
      assert(cacheEntry.expireAt === Long.MaxValue)
    } else {
      assert(cacheEntry.expireAt <= pool.clock.nanoTime() + timeoutVal)
    }
  }
}
