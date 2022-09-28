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
import java.nio.charset.StandardCharsets
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.util.Random

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.scalatest.PrivateMethodTester

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.kafka010.KafkaDelegationTokenTest
import org.apache.spark.sql.kafka010.{KafkaTestUtils, RecordBuilder}
import org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer.CacheKey
import org.apache.spark.sql.test.SharedSparkSession

class KafkaDataConsumerSuite
  extends SharedSparkSession
  with PrivateMethodTester
  with KafkaDelegationTokenTest {

  protected var testUtils: KafkaTestUtils = _
  private val topic = "topic" + Random.nextInt()
  private val topicPartition = new TopicPartition(topic, 0)
  private val groupId = "groupId"

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(Map[String, Object]())
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    super.afterAll()
  }

  private def getKafkaParams() = Map[String, Object](
    GROUP_ID_CONFIG -> "groupId",
    BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress,
    KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ENABLE_AUTO_COMMIT_CONFIG -> "false"
  ).asJava
  private var fetchedDataPool: FetchedDataPool = _
  private var consumerPool: InternalKafkaConsumerPool = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    fetchedDataPool = {
      val fetchedDataPoolMethod = PrivateMethod[FetchedDataPool](Symbol("fetchedDataPool"))
      KafkaDataConsumer.invokePrivate(fetchedDataPoolMethod())
    }

    consumerPool = {
      val internalKafkaConsumerPoolMethod =
        PrivateMethod[InternalKafkaConsumerPool](Symbol("consumerPool"))
      KafkaDataConsumer.invokePrivate(internalKafkaConsumerPoolMethod())
    }

    fetchedDataPool.reset()
    consumerPool.reset()
  }

  test("SPARK-19886: Report error cause correctly in reportDataLoss") {
    val cause = new Exception("D'oh!")
    val reportDataLoss = PrivateMethod[Unit](Symbol("reportDataLoss0"))
    val e = intercept[IllegalStateException] {
      KafkaDataConsumer.invokePrivate(reportDataLoss(true, "message", cause))
    }
    assert(e.getCause === cause)
  }

  test("new KafkaDataConsumer instance in case of Task retry") {
    try {
      val kafkaParams = getKafkaParams()
      val key = CacheKey(groupId, topicPartition)

      val context1 = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      TaskContext.setTaskContext(context1)
      val consumer1Underlying = initSingleConsumer(kafkaParams, key)

      val context2 = new TaskContextImpl(0, 0, 0, 0, 1, 1, null, null, null)
      TaskContext.setTaskContext(context2)
      val consumer2Underlying = initSingleConsumer(kafkaParams, key)

      // here we expect different consumer as pool will invalidate for task reattempt
      assert(consumer2Underlying.ne(consumer1Underlying))
    } finally {
      TaskContext.unset()
    }
  }

  test("same KafkaDataConsumer instance in case of same token") {
    try {
      val kafkaParams = getKafkaParams()
      val key = new CacheKey(groupId, topicPartition)

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      TaskContext.setTaskContext(context)
      setSparkEnv(
        Map(
          s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers" -> bootStrapServers
        )
      )
      addTokenToUGI(tokenService1, tokenId1, tokenPassword1)
      val consumer1Underlying = initSingleConsumer(kafkaParams, key)
      val consumer2Underlying = initSingleConsumer(kafkaParams, key)

      assert(consumer2Underlying.eq(consumer1Underlying))
    } finally {
      TaskContext.unset()
    }
  }

  test("new KafkaDataConsumer instance in case of token renewal") {
    try {
      val kafkaParams = getKafkaParams()
      val key = new CacheKey(groupId, topicPartition)

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      TaskContext.setTaskContext(context)
      setSparkEnv(
        Map(
          s"spark.kafka.clusters.$identifier1.auth.bootstrap.servers" -> bootStrapServers
        )
      )
      addTokenToUGI(tokenService1, tokenId1, tokenPassword1)
      val consumer1Underlying = initSingleConsumer(kafkaParams, key)
      addTokenToUGI(tokenService1, tokenId2, tokenPassword2)
      val consumer2Underlying = initSingleConsumer(kafkaParams, key)

      assert(consumer2Underlying.ne(consumer1Underlying))
    } finally {
      TaskContext.unset()
    }
  }

  private def initSingleConsumer(
      kafkaParams: ju.Map[String, Object],
      key: CacheKey): InternalKafkaConsumer = {
    val consumer = KafkaDataConsumer.acquire(topicPartition, kafkaParams)

    // any method call which requires consumer is necessary
    consumer.getOrRetrieveConsumer()

    val consumerUnderlying = consumer._consumer
    assert(consumerUnderlying.isDefined)

    consumer.release()

    assert(consumerPool.size(key) === 1)
    // check whether acquired object is available in pool
    val pooledObj = consumerPool.borrowObject(key, kafkaParams)
    assert(consumerUnderlying.get.eq(pooledObj))
    consumerPool.returnObject(pooledObj)

    consumerUnderlying.get
  }

  test("SPARK-23623: concurrent use of KafkaDataConsumer") {
    val data: immutable.IndexedSeq[(String, Seq[(String, Array[Byte])])] =
      prepareTestTopicHavingTestMessages(topic)

    val topicPartition = new TopicPartition(topic, 0)
    val kafkaParams = getKafkaParams()
    val numThreads = 100
    val numConsumerUsages = 500

    @volatile var error: Throwable = null

    def consume(i: Int): Unit = {
      val taskContext = if (Random.nextBoolean) {
        new TaskContextImpl(0, 0, 0, 0, attemptNumber = Random.nextInt(2), 1,
          null, null, null)
      } else {
        null
      }
      TaskContext.setTaskContext(taskContext)
      val consumer = KafkaDataConsumer.acquire(topicPartition, kafkaParams)
      try {
        val range = consumer.getAvailableOffsetRange()
        val rcvd = range.earliest until range.latest map { offset =>
          val record = consumer.get(offset, Long.MaxValue, 10000, failOnDataLoss = false)
          val value = new String(record.value(), StandardCharsets.UTF_8)
          val headers = record.headers().toArray.map(header => (header.key(), header.value())).toSeq
          (value, headers)
        }
        data.zip(rcvd).foreach { case (expected, actual) =>
          // value
          assert(expected._1 === actual._1)
          // headers
          expected._2.zip(actual._2).foreach { case (l, r) =>
            // header key
            assert(l._1 === r._1)
            // header value
            assert(l._2 === r._2)
          }
        }
      } catch {
        case e: Throwable =>
          error = e
          throw e
      } finally {
        consumer.release()
      }
    }

    val threadpool = Executors.newFixedThreadPool(numThreads)
    try {
      val futures = (1 to numConsumerUsages).map { i =>
        threadpool.submit(new Runnable {
          override def run(): Unit = { consume(i) }
        })
      }
      futures.foreach(_.get(1, TimeUnit.MINUTES))
      assert(error == null)
    } finally {
      threadpool.shutdown()
    }
  }

  test("SPARK-25151 Handles multiple tasks in executor fetching same (topic, partition) pair") {
    prepareTestTopicHavingTestMessages(topic)
    val topicPartition = new TopicPartition(topic, 0)

    val kafkaParams = getKafkaParams()

    withTaskContext(TaskContext.empty()) {
      // task A trying to fetch offset 0 to 100, and read 5 records
      val consumer1 = KafkaDataConsumer.acquire(topicPartition, kafkaParams)
      val lastOffsetForConsumer1 = readAndGetLastOffset(consumer1, 0, 100, 5)
      consumer1.release()

      assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 1, expectedNumTotal = 1)

      // task B trying to fetch offset 300 to 500, and read 5 records
      val consumer2 = KafkaDataConsumer.acquire(topicPartition, kafkaParams)
      val lastOffsetForConsumer2 = readAndGetLastOffset(consumer2, 300, 500, 5)
      consumer2.release()

      assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 2, expectedNumTotal = 2)

      // task A continue reading from the last offset + 1, with upper bound 100 again
      val consumer1a = KafkaDataConsumer.acquire(topicPartition, kafkaParams)

      consumer1a.get(lastOffsetForConsumer1 + 1, 100, 10000, failOnDataLoss = false)
      consumer1a.release()

      // pool should succeed to provide cached data instead of creating one
      assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 2, expectedNumTotal = 2)

      // task B also continue reading from the last offset + 1, with upper bound 500 again
      val consumer2a = KafkaDataConsumer.acquire(topicPartition, kafkaParams)

      consumer2a.get(lastOffsetForConsumer2 + 1, 500, 10000, failOnDataLoss = false)
      consumer2a.release()

      // same expectation: pool should succeed to provide cached data instead of creating one
      assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 2, expectedNumTotal = 2)
    }
  }

  test("SPARK-25151 Handles multiple tasks in executor fetching same (topic, partition) pair " +
    "and same offset (edge-case) - data in use") {
    prepareTestTopicHavingTestMessages(topic)
    val topicPartition = new TopicPartition(topic, 0)

    val kafkaParams = getKafkaParams()

    withTaskContext(TaskContext.empty()) {
      // task A trying to fetch offset 0 to 100, and read 5 records (still reading)
      val consumer1 = KafkaDataConsumer.acquire(topicPartition, kafkaParams)
      val lastOffsetForConsumer1 = readAndGetLastOffset(consumer1, 0, 100, 5)

      assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 1, expectedNumTotal = 1)

      // task B trying to fetch offset the last offset task A is reading so far + 1 to 500
      // this is a condition for edge case
      val consumer2 = KafkaDataConsumer.acquire(topicPartition, kafkaParams)
      consumer2.get(lastOffsetForConsumer1 + 1, 100, 10000, failOnDataLoss = false)

      // Pool must create a new fetched data instead of returning existing on now in use even
      // there's fetched data matching start offset.
      assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 2, expectedNumTotal = 2)

      consumer1.release()
      consumer2.release()
    }
  }

  test("SPARK-25151 Handles multiple tasks in executor fetching same (topic, partition) pair " +
    "and same offset (edge-case) - data not in use") {
    prepareTestTopicHavingTestMessages(topic)
    val topicPartition = new TopicPartition(topic, 0)

    val kafkaParams = getKafkaParams()

    withTaskContext(TaskContext.empty()) {
      // task A trying to fetch offset 0 to 100, and read 5 records (still reading)
      val consumer1 = KafkaDataConsumer.acquire(topicPartition, kafkaParams)
      val lastOffsetForConsumer1 = readAndGetLastOffset(consumer1, 0, 100, 5)
      consumer1.release()

      assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 1, expectedNumTotal = 1)

      // task B trying to fetch offset the last offset task A is reading so far + 1 to 500
      // this is a condition for edge case
      val consumer2 = KafkaDataConsumer.acquire(topicPartition, kafkaParams)
      consumer2.get(lastOffsetForConsumer1 + 1, 100, 10000, failOnDataLoss = false)

      // Pool cannot determine the origin task, so it has to just provide matching one.
      // task A may come back and try to fetch, and cannot find previous data
      // (or the data is in use).
      // If then task A may have to fetch from Kafka, but we already avoided fetching from Kafka in
      // task B, so it is not a big deal in overall.
      assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 1, expectedNumTotal = 1)

      consumer2.release()
    }
  }

  private def assertFetchedDataPoolStatistic(
      fetchedDataPool: FetchedDataPool,
      expectedNumCreated: Long,
      expectedNumTotal: Long): Unit = {
    assert(fetchedDataPool.numCreated === expectedNumCreated)
    assert(fetchedDataPool.numTotal === expectedNumTotal)
  }

  private def readAndGetLastOffset(
      consumer: KafkaDataConsumer,
      startOffset: Long,
      untilOffset: Long,
      numToRead: Int): Long = {
    var lastOffset: Long = startOffset - 1
    (0 until numToRead).foreach { _ =>
      val record = consumer.get(lastOffset + 1, untilOffset, 10000, failOnDataLoss = false)
      // validation for fetched record is covered by other tests, so skip on validating
      lastOffset = record.offset()
    }
    lastOffset
  }

  private def prepareTestTopicHavingTestMessages(topic: String) = {
    val data = (1 to 1000).map(i => (i.toString, Seq[(String, Array[Byte])]()))
    testUtils.createTopic(topic, 1)
    val messages = data.map { case (value, hdrs) =>
      new RecordBuilder(topic, value).headers(hdrs).build()
    }
    testUtils.sendMessages(messages)
    data
  }

  private def withTaskContext(context: TaskContext)(task: => Unit): Unit = {
    try {
      TaskContext.setTaskContext(context)
      task
    } finally {
      TaskContext.unset()
    }
  }

}
