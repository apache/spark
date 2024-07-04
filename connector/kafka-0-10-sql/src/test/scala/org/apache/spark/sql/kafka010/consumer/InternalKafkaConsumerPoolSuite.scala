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

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.SparkConf
import org.apache.spark.sql.kafka010.{CONSUMER_CACHE_CAPACITY, CONSUMER_CACHE_EVICTOR_THREAD_RUN_INTERVAL, CONSUMER_CACHE_TIMEOUT}
import org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer.CacheKey
import org.apache.spark.sql.test.SharedSparkSession

class InternalKafkaConsumerPoolSuite extends SharedSparkSession {

  test("basic multiple borrows and returns for single key") {
    val pool = new InternalKafkaConsumerPool(new SparkConf())

    val topic = "topic"
    val partitionId = 0
    val topicPartition = new TopicPartition(topic, partitionId)

    val kafkaParams: ju.Map[String, Object] = getTestKafkaParams

    val key = new CacheKey(topicPartition, kafkaParams)

    val pooledObjects = (0 to 2).map { _ =>
      val pooledObject = pool.borrowObject(key, kafkaParams)
      assertPooledObject(pooledObject, topicPartition, kafkaParams)
      pooledObject
    }

    assertPoolStateForKey(pool, key, numIdle = 0, numActive = 3, numTotal = 3)
    assertPoolState(pool, numIdle = 0, numActive = 3, numTotal = 3)

    val pooledObject2 = pool.borrowObject(key, kafkaParams)

    assertPooledObject(pooledObject2, topicPartition, kafkaParams)
    assertPoolStateForKey(pool, key, numIdle = 0, numActive = 4, numTotal = 4)
    assertPoolState(pool, numIdle = 0, numActive = 4, numTotal = 4)

    pooledObjects.foreach(pool.returnObject)

    assertPoolStateForKey(pool, key, numIdle = 3, numActive = 1, numTotal = 4)
    assertPoolState(pool, numIdle = 3, numActive = 1, numTotal = 4)

    pool.returnObject(pooledObject2)

    // we only allow three idle objects per key
    assertPoolStateForKey(pool, key, numIdle = 3, numActive = 0, numTotal = 3)
    assertPoolState(pool, numIdle = 3, numActive = 0, numTotal = 3)

    pool.close()
  }

  test("basic borrow and return for multiple keys") {
    val pool = new InternalKafkaConsumerPool(new SparkConf())

    val kafkaParams = getTestKafkaParams
    val topicPartitions = createTopicPartitions(Seq("topic", "topic2"), 6)
    val keys = createCacheKeys(topicPartitions, kafkaParams)

    // while in loop pool doesn't still exceed total pool size
    val keyToPooledObjectPairs = borrowObjectsPerKey(pool, kafkaParams, keys)

    assertPoolState(pool, numIdle = 0, numActive = keyToPooledObjectPairs.length,
      numTotal = keyToPooledObjectPairs.length)

    returnObjects(pool, keyToPooledObjectPairs)

    assertPoolState(pool, numIdle = keyToPooledObjectPairs.length, numActive = 0,
      numTotal = keyToPooledObjectPairs.length)

    pool.close()
  }

  test("borrow more than soft max capacity from pool which is neither free space nor idle object") {
    testWithPoolBorrowedSoftMaxCapacity { (pool, kafkaParams, keyToPooledObjectPairs) =>
      val moreTopicPartition = new TopicPartition("topic2", 0)
      val newCacheKey = new CacheKey(moreTopicPartition, kafkaParams)

      // exceeds soft max pool size, and also no idle object for cleaning up
      // but pool will borrow a new object
      pool.borrowObject(newCacheKey, kafkaParams)

      assertPoolState(pool, numIdle = 0, numActive = keyToPooledObjectPairs.length + 1,
        numTotal = keyToPooledObjectPairs.length + 1)
    }
  }

  test("borrow more than soft max capacity from pool frees up idle objects automatically") {
    testWithPoolBorrowedSoftMaxCapacity { (pool, kafkaParams, keyToPooledObjectPairs) =>
      // return 20% of objects to ensure there're some idle objects to free up later
      val numToReturn = (keyToPooledObjectPairs.length * 0.2).toInt
      returnObjects(pool, keyToPooledObjectPairs.take(numToReturn))

      assertPoolState(pool, numIdle = numToReturn,
        numActive = keyToPooledObjectPairs.length - numToReturn,
        numTotal = keyToPooledObjectPairs.length)

      // borrow a new object: there should be some idle objects to clean up
      val moreTopicPartition = new TopicPartition("topic2", 0)
      val newCacheKey = new CacheKey(moreTopicPartition, kafkaParams)

      val newObject = pool.borrowObject(newCacheKey, kafkaParams)
      assertPooledObject(newObject, moreTopicPartition, kafkaParams)
      assertPoolStateForKey(pool, newCacheKey, numIdle = 0, numActive = 1, numTotal = 1)

      // at least one of idle object should be freed up
      assert(pool.numIdle < numToReturn)
      // we can determine number of active objects correctly
      assert(pool.numActive === keyToPooledObjectPairs.length - numToReturn + 1)
      // total objects should be more than number of active + 1 but can't expect exact number
      assert(pool.size > keyToPooledObjectPairs.length - numToReturn + 1)
    }
  }


  private def testWithPoolBorrowedSoftMaxCapacity(
      testFn: (InternalKafkaConsumerPool,
        ju.Map[String, Object],
        Seq[(CacheKey, InternalKafkaConsumer)]) => Unit): Unit = {
    val capacity = 16

    val conf = new SparkConf()
    conf.set(CONSUMER_CACHE_CAPACITY, capacity)
    conf.set(CONSUMER_CACHE_TIMEOUT, -1L)
    conf.set(CONSUMER_CACHE_EVICTOR_THREAD_RUN_INTERVAL, -1L)

    val pool = new InternalKafkaConsumerPool(conf)

    try {
      val kafkaParams = getTestKafkaParams
      val topicPartitions = createTopicPartitions(Seq("topic"), capacity)
      val keys = createCacheKeys(topicPartitions, kafkaParams)

      // borrow objects which makes pool reaching soft capacity
      val keyToPooledObjectPairs = borrowObjectsPerKey(pool, kafkaParams, keys)

      testFn(pool, kafkaParams, keyToPooledObjectPairs)
    } finally {
      pool.close()
    }
  }

  test("evicting idle objects on background") {
    import org.scalatest.time.SpanSugar._

    val minEvictableIdleTimeMillis = 3 * 1000L // 3 seconds
    val evictorThreadRunIntervalMillis = 500L // triggering multiple evictions by intention

    val conf = new SparkConf()
    conf.set(CONSUMER_CACHE_TIMEOUT, minEvictableIdleTimeMillis)
    conf.set(CONSUMER_CACHE_EVICTOR_THREAD_RUN_INTERVAL, evictorThreadRunIntervalMillis)

    val pool = new InternalKafkaConsumerPool(conf)

    val kafkaParams = getTestKafkaParams
    val topicPartitions = createTopicPartitions(Seq("topic"), 10)
    val keys = createCacheKeys(topicPartitions, kafkaParams)

    // borrow and return some consumers to ensure some partitions are being idle
    // this test covers the use cases: rebalance / topic removal happens while running query
    val keyToPooledObjectPairs = borrowObjectsPerKey(pool, kafkaParams, keys)
    val objectsToReturn = keyToPooledObjectPairs.filter(_._1.topicPartition.partition() % 2 == 0)
    returnObjects(pool, objectsToReturn)

    // wait up to twice than minEvictableIdleTimeMillis to ensure evictor thread to clear up
    // idle objects
    eventually(timeout((minEvictableIdleTimeMillis.toLong * 2).seconds),
      interval(evictorThreadRunIntervalMillis.milliseconds)) {
      assertPoolState(pool, numIdle = 0, numActive = 5, numTotal = 5)
    }

    pool.close()
  }

  private def createTopicPartitions(
      topicNames: Seq[String],
      countPartition: Int): List[TopicPartition] = {
    for (
      topic <- topicNames.toList;
      partitionId <- 0 until countPartition
    ) yield new TopicPartition(topic, partitionId)
  }

  private def createCacheKeys(
      topicPartitions: List[TopicPartition],
      kafkaParams: ju.Map[String, Object]): List[CacheKey] = {
    topicPartitions.map(new CacheKey(_, kafkaParams))
  }

  private def assertPooledObject(
      pooledObject: InternalKafkaConsumer,
      expectedTopicPartition: TopicPartition,
      expectedKafkaParams: ju.Map[String, Object]): Unit = {
    assert(pooledObject != null)
    assert(pooledObject.kafkaParams === expectedKafkaParams)
    assert(pooledObject.topicPartition === expectedTopicPartition)
  }

  private def assertPoolState(
      pool: InternalKafkaConsumerPool,
      numIdle: Int,
      numActive: Int,
      numTotal: Int): Unit = {
    assert(pool.numIdle === numIdle)
    assert(pool.numActive === numActive)
    assert(pool.size === numTotal)
  }

  private def assertPoolStateForKey(
      pool: InternalKafkaConsumerPool,
      key: CacheKey,
      numIdle: Int,
      numActive: Int,
      numTotal: Int): Unit = {
    assert(pool.numIdle(key) === numIdle)
    assert(pool.numActive(key) === numActive)
    assert(pool.size(key) === numTotal)
  }

  private def getTestKafkaParams: ju.Map[String, Object] = Map[String, Object](
    GROUP_ID_CONFIG -> "groupId",
    BOOTSTRAP_SERVERS_CONFIG -> "PLAINTEXT://localhost:9092",
    KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ENABLE_AUTO_COMMIT_CONFIG -> "false"
  ).asJava

  private def borrowObjectsPerKey(
      pool: InternalKafkaConsumerPool,
      kafkaParams: ju.Map[String, Object],
      keys: List[CacheKey]): Seq[(CacheKey, InternalKafkaConsumer)] = {
    keys.map { key =>
      val numActiveBeforeBorrowing = pool.numActive
      val numIdleBeforeBorrowing = pool.numIdle
      val numTotalBeforeBorrowing = pool.size

      val pooledObj = pool.borrowObject(key, kafkaParams)

      assertPoolStateForKey(pool, key, numIdle = 0, numActive = 1, numTotal = 1)
      assertPoolState(pool, numIdle = numIdleBeforeBorrowing,
        numActive = numActiveBeforeBorrowing + 1, numTotal = numTotalBeforeBorrowing + 1)

      (key, pooledObj)
    }
  }

  private def returnObjects(
      pool: InternalKafkaConsumerPool,
      objects: Seq[(CacheKey, InternalKafkaConsumer)]): Unit = {
    objects.foreach { case (key, pooledObj) =>
      val numActiveBeforeReturning = pool.numActive
      val numIdleBeforeReturning = pool.numIdle
      val numTotalBeforeReturning = pool.size

      pool.returnObject(pooledObj)

      // we only allow one idle object per key
      assertPoolStateForKey(pool, key, numIdle = 1, numActive = 0, numTotal = 1)
      assertPoolState(pool, numIdle = numIdleBeforeReturning + 1,
        numActive = numActiveBeforeReturning - 1, numTotal = numTotalBeforeReturning)
    }
  }
}
