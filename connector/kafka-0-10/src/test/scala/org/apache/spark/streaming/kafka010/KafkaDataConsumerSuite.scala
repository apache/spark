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

import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._

class KafkaDataConsumerSuite extends SparkFunSuite with MockitoSugar {
  private var testUtils: KafkaTestUtils = _
  private val topic = "topic" + Random.nextInt()
  private val topicPartition = new TopicPartition(topic, 0)
  private val groupId = "groupId"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
    val env = mock[SparkEnv]
    SparkEnv.set(env)
    when(env.conf).thenReturn(conf)

    testUtils = new KafkaTestUtils
    testUtils.setup()
    KafkaDataConsumer.init(16, 64, 0.75f)
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    SparkEnv.set(null)
    super.afterAll()
  }

  private def getKafkaParams() = Map[String, Object](
    GROUP_ID_CONFIG -> groupId,
    BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress,
    KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ENABLE_AUTO_COMMIT_CONFIG -> "false"
  ).asJava

  test("KafkaDataConsumer reuse in case of same groupId and TopicPartition") {
    KafkaDataConsumer.cache.clear()

    val kafkaParams = getKafkaParams()

    val consumer1 = KafkaDataConsumer.acquire[Array[Byte], Array[Byte]](
      topicPartition, kafkaParams, null, true)
    consumer1.release()

    val consumer2 = KafkaDataConsumer.acquire[Array[Byte], Array[Byte]](
      topicPartition, kafkaParams, null, true)
    consumer2.release()

    assert(KafkaDataConsumer.cache.size() == 1)
    val key = new CacheKey(groupId, topicPartition)
    val existingInternalConsumer = KafkaDataConsumer.cache.get(key)
    assert(existingInternalConsumer.eq(consumer1.internalConsumer))
    assert(existingInternalConsumer.eq(consumer2.internalConsumer))
  }

  test("new KafkaDataConsumer instance in case of Task retry") {
    KafkaDataConsumer.cache.clear()

    val kafkaParams = getKafkaParams()
    val key = new CacheKey(groupId, topicPartition)

    val context1 = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
    val consumer1 = KafkaDataConsumer.acquire[Array[Byte], Array[Byte]](
      topicPartition, kafkaParams, context1, true)
    consumer1.release()

    assert(KafkaDataConsumer.cache.size() == 1)
    assert(KafkaDataConsumer.cache.get(key).eq(consumer1.internalConsumer))

    val context2 = new TaskContextImpl(0, 0, 0, 0, 1, 1, null, null, null)
    val consumer2 = KafkaDataConsumer.acquire[Array[Byte], Array[Byte]](
      topicPartition, kafkaParams, context2, true)
    consumer2.release()

    // The first consumer should be removed from cache and new non-cached should be returned
    assert(KafkaDataConsumer.cache.size() == 0)
    assert(consumer1.internalConsumer.ne(consumer2.internalConsumer))
  }

  test("concurrent use of KafkaDataConsumer") {
    val data = (1 to 1000).map(_.toString)
    testUtils.createTopic(topic)
    testUtils.sendMessages(topic, data.toArray)

    val kafkaParams = getKafkaParams()

    val numThreads = 100
    val numConsumerUsages = 500

    @volatile var error: Throwable = null

    def consume(i: Int): Unit = {
      val useCache = Random.nextBoolean
      val taskContext = if (Random.nextBoolean) {
        new TaskContextImpl(0, 0, 0, 0, attemptNumber = Random.nextInt(2), 1, null, null, null)
      } else {
        null
      }
      val consumer = KafkaDataConsumer.acquire[Array[Byte], Array[Byte]](
        topicPartition, kafkaParams, taskContext, useCache)
      try {
        val rcvd = data.indices.map { offset =>
          val bytes = consumer.get(offset, 10000).value()
          new String(bytes)
        }
        assert(rcvd == data)
      } catch {
        case e: Throwable =>
          error = e
          throw e
      } finally {
        consumer.release()
      }
    }

    val threadPool = Executors.newFixedThreadPool(numThreads)
    try {
      val futures = (1 to numConsumerUsages).map { i =>
        threadPool.submit(new Runnable {
          override def run(): Unit = { consume(i) }
        })
      }
      futures.foreach(_.get(1, TimeUnit.MINUTES))
      assert(error == null)
    } finally {
      threadPool.shutdown()
    }
  }
}
