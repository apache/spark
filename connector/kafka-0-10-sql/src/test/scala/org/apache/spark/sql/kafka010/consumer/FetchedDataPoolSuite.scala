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
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.jmock.lib.concurrent.DeterministicScheduler
import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkConf
import org.apache.spark.sql.kafka010.{FETCHED_DATA_CACHE_EVICTOR_THREAD_RUN_INTERVAL, FETCHED_DATA_CACHE_TIMEOUT}
import org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer.{AvailableOffsetRange, CacheKey}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ManualClock

class FetchedDataPoolSuite extends SharedSparkSession with PrivateMethodTester {
  import FetchedDataPool._
  type Record = ConsumerRecord[Array[Byte], Array[Byte]]

  private val dummyBytes = "dummy".getBytes

  // Helper private method accessors for FetchedDataPool
  private type PoolCacheType = mutable.Map[CacheKey, CachedFetchedDataList]
  private val _cache = PrivateMethod[PoolCacheType](Symbol("cache"))

  def getCache(pool: FetchedDataPool): PoolCacheType = {
    pool.invokePrivate(_cache())
  }

  test("acquire fetched data from multiple keys") {
    val dataPool = new FetchedDataPool(new SparkConf())

    val cacheKeys = (0 until 10).map { partId =>
      CacheKey("testgroup", new TopicPartition("topic", partId))
    }

    assert(getCache(dataPool).size === 0)
    cacheKeys.foreach { key => assert(getCache(dataPool).get(key).isEmpty) }

    val dataList = cacheKeys.map(key => (key, dataPool.acquire(key, 0)))

    assert(getCache(dataPool).size === cacheKeys.size)
    cacheKeys.map { key =>
      assert(getCache(dataPool)(key).size === 1)
      assert(getCache(dataPool)(key).head.inUse)
    }

    assertFetchedDataPoolStatistic(dataPool, expectedNumCreated = 10, expectedNumTotal = 10)

    dataList.map { case (_, data) =>
      data.withNewPoll(testRecords(0, 5).listIterator, 5, AvailableOffsetRange(0, 4))
    }

    dataList.foreach { case (key, data) =>
      dataPool.release(key, data)
    }

    assert(getCache(dataPool).size === cacheKeys.size)
    cacheKeys.map { key =>
      assert(getCache(dataPool)(key).size === 1)
      assert(!getCache(dataPool)(key).head.inUse)
    }

    dataPool.shutdown()
  }

  test("continuous use of fetched data from single key") {
    val dataPool = new FetchedDataPool(new SparkConf())

    val cacheKey = CacheKey("testgroup", new TopicPartition("topic", 0))

    val data = dataPool.acquire(cacheKey, 0)
    data.withNewPoll(testRecords(0, 5).listIterator, 5, AvailableOffsetRange(0, 4))

    (0 to 3).foreach { _ => data.next() }

    dataPool.release(cacheKey, data)

    // suppose next batch

    val data2 = dataPool.acquire(cacheKey, data.nextOffsetInFetchedData)

    assert(data.eq(data2))

    assertFetchedDataPoolStatistic(dataPool, expectedNumCreated = 1, expectedNumTotal = 1)
    assert(getCache(dataPool)(cacheKey).size === 1)
    assert(getCache(dataPool)(cacheKey).head.inUse)

    dataPool.release(cacheKey, data2)

    assert(getCache(dataPool)(cacheKey).size === 1)
    assert(!getCache(dataPool)(cacheKey).head.inUse)

    dataPool.shutdown()
  }

  test("multiple tasks referring same key continuously using fetched data") {
    val dataPool = new FetchedDataPool(new SparkConf())

    val cacheKey = CacheKey("testgroup", new TopicPartition("topic", 0))

    val dataFromTask1 = dataPool.acquire(cacheKey, 0)
    val dataFromTask2 = dataPool.acquire(cacheKey, 0)

    // it shouldn't give same object as dataFromTask1 though it asks same offset
    // it definitely works when offsets are not overlapped: skip adding test for that
    assertFetchedDataPoolStatistic(dataPool, expectedNumCreated = 2, expectedNumTotal = 2)
    assert(getCache(dataPool)(cacheKey).size === 2)
    assert(getCache(dataPool)(cacheKey)(1).inUse)

    // reading from task 1
    dataFromTask1.withNewPoll(testRecords(0, 5).listIterator, 5, AvailableOffsetRange(0, 4))

    (0 to 3).foreach { _ => dataFromTask1.next() }

    dataPool.release(cacheKey, dataFromTask1)

    // reading from task 2
    dataFromTask2.withNewPoll(testRecords(0, 30).listIterator, 30, AvailableOffsetRange(0, 29))

    (0 to 5).foreach { _ => dataFromTask2.next() }

    dataPool.release(cacheKey, dataFromTask2)

    // suppose next batch for task 1
    val data2FromTask1 = dataPool.acquire(cacheKey, dataFromTask1.nextOffsetInFetchedData)
    assert(data2FromTask1.eq(dataFromTask1))

    assertFetchedDataPoolStatistic(dataPool, expectedNumCreated = 2, expectedNumTotal = 2)
    assert(getCache(dataPool)(cacheKey).head.inUse)

    // suppose next batch for task 2
    val data2FromTask2 = dataPool.acquire(cacheKey, dataFromTask2.nextOffsetInFetchedData)
    assert(data2FromTask2.eq(dataFromTask2))

    assertFetchedDataPoolStatistic(dataPool, expectedNumCreated = 2, expectedNumTotal = 2)
    assert(getCache(dataPool)(cacheKey)(1).inUse)

    // release from task 2
    dataPool.release(cacheKey, data2FromTask2)
    assert(!getCache(dataPool)(cacheKey)(1).inUse)

    // release from task 1
    dataPool.release(cacheKey, data2FromTask1)
    assert(!getCache(dataPool)(cacheKey).head.inUse)

    dataPool.shutdown()
  }

  test("evict idle fetched data") {
    val minEvictableIdleTimeMillis = 2000L
    val evictorThreadRunIntervalMillis = 500L

    val conf = new SparkConf()
    conf.set(FETCHED_DATA_CACHE_TIMEOUT, minEvictableIdleTimeMillis)
    conf.set(FETCHED_DATA_CACHE_EVICTOR_THREAD_RUN_INTERVAL, evictorThreadRunIntervalMillis)

    val scheduler = new DeterministicScheduler()
    val clock = new ManualClock()
    val dataPool = new FetchedDataPool(scheduler, clock, conf)

    val cacheKeys = (0 until 10).map { partId =>
      CacheKey("testgroup", new TopicPartition("topic", partId))
    }

    val dataList = cacheKeys.map(key => (key, dataPool.acquire(key, 0)))

    assertFetchedDataPoolStatistic(dataPool, expectedNumCreated = 10, expectedNumTotal = 10)

    dataList.map { case (_, data) =>
      data.withNewPoll(testRecords(0, 5).listIterator, 5, AvailableOffsetRange(0, 4))
    }

    val dataToEvict = dataList.take(3)
    // release key with around 500 ms delay, so that we can check eviction per key
    dataToEvict.foreach { case (key, data) =>
      dataPool.release(key, data)
      clock.advance(500)
    }

    // time elapsed after releasing
    // first key: 1500ms, second key: 1000 ms, third key: 500 ms

    // advancing - first key: 2100ms, second key: 1600 ms, third key: 1100 ms
    clock.advance(600)

    scheduler.tick(minEvictableIdleTimeMillis + 100, TimeUnit.MILLISECONDS)
    assert(getCache(dataPool)(dataToEvict(0)._1).isEmpty)
    assert(getCache(dataPool)(dataToEvict(1)._1).nonEmpty)
    assert(getCache(dataPool)(dataToEvict(2)._1).nonEmpty)

    // advancing - second key: 2100 ms, third key: 1600 ms
    clock.advance(500)

    scheduler.tick(minEvictableIdleTimeMillis + 100, TimeUnit.MILLISECONDS)
    assert(getCache(dataPool)(dataToEvict(0)._1).isEmpty)
    assert(getCache(dataPool)(dataToEvict(1)._1).isEmpty)
    assert(getCache(dataPool)(dataToEvict(2)._1).nonEmpty)

    // advancing - third key: 2300 ms
    clock.advance(500)

    scheduler.tick(minEvictableIdleTimeMillis + 100, TimeUnit.MILLISECONDS)
    assert(getCache(dataPool)(dataToEvict(0)._1).isEmpty)
    assert(getCache(dataPool)(dataToEvict(1)._1).isEmpty)
    assert(getCache(dataPool)(dataToEvict(2)._1).isEmpty)

    assertFetchedDataPoolStatistic(dataPool, expectedNumCreated = 10, expectedNumTotal = 7)
    assert(getCache(dataPool).values.map(_.size).sum === dataList.size - dataToEvict.size)

    dataList.takeRight(3).foreach { case (key, data) =>
      dataPool.release(key, data)
    }

    // add objects to be candidates for eviction
    clock.advance(minEvictableIdleTimeMillis + 100)

    // ensure releasing more objects don't trigger eviction unless evictor runs
    assertFetchedDataPoolStatistic(dataPool, expectedNumCreated = 10, expectedNumTotal = 7)
    assert(getCache(dataPool).values.map(_.size).sum === dataList.size - dataToEvict.size)

    try {
      dataPool.shutdown()
    } catch {
      // ignore as it's known issue, DeterministicScheduler doesn't support shutdown
      case _: UnsupportedOperationException =>
    }
  }

  test("invalidate key") {
    val dataPool = new FetchedDataPool(new SparkConf())

    val cacheKey = CacheKey("testgroup", new TopicPartition("topic", 0))

    val dataFromTask1 = dataPool.acquire(cacheKey, 0)
    val dataFromTask2 = dataPool.acquire(cacheKey, 0)

    assertFetchedDataPoolStatistic(dataPool, expectedNumCreated = 2, expectedNumTotal = 2)

    // 1 idle, 1 active
    dataPool.release(cacheKey, dataFromTask1)

    val cacheKey2 = CacheKey("testgroup", new TopicPartition("topic", 1))

    dataPool.acquire(cacheKey2, 0)

    assertFetchedDataPoolStatistic(dataPool, expectedNumCreated = 3, expectedNumTotal = 3)
    assert(getCache(dataPool).size === 2)
    assert(getCache(dataPool)(cacheKey).size === 2)
    assert(getCache(dataPool)(cacheKey2).size === 1)

    dataPool.invalidate(cacheKey)

    assertFetchedDataPoolStatistic(dataPool, expectedNumCreated = 3, expectedNumTotal = 1)
    assert(getCache(dataPool).size === 1)
    assert(getCache(dataPool).get(cacheKey).isEmpty)

    // it doesn't affect other keys
    assert(getCache(dataPool)(cacheKey2).size === 1)

    dataPool.release(cacheKey, dataFromTask2)

    // it doesn't throw error on invalidated objects, but it doesn't cache them again
    assert(getCache(dataPool).size === 1)
    assert(getCache(dataPool).get(cacheKey).isEmpty)

    dataPool.shutdown()
  }


  private def testRecords(startOffset: Long, count: Int): ju.List[Record] = {
    (0 until count).map { offset =>
      new Record("topic", 0, startOffset + offset, dummyBytes, dummyBytes)
    }.toList.asJava
  }

  private def assertFetchedDataPoolStatistic(
      fetchedDataPool: FetchedDataPool,
      expectedNumCreated: Long,
      expectedNumTotal: Long): Unit = {
    assert(fetchedDataPool.numCreated === expectedNumCreated)
    assert(fetchedDataPool.numTotal === expectedNumTotal)
  }
}
