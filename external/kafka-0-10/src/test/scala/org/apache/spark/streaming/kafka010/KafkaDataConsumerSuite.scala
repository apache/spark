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
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.scalatest.BeforeAndAfterAll

import org.apache.spark._
import org.apache.spark.streaming.kafka010.consumer.async.AsyncSparkKafkaConsumerBuilder
import org.apache.spark.streaming.kafka010.consumer.sync.SyncSparkKafkaConsumerBuilder

class KafkaDataConsumerSuite extends SparkFunSuite with BeforeAndAfterAll {
  private var testUtils: KafkaTestUtils = _
  private val groupId = "groupId"

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
    KafkaDataConsumer.init(16, 64, 0.75f)
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    super.afterAll()
  }

  private def getKafkaParams() = Map[String, Object](
    GROUP_ID_CONFIG -> groupId,
    BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress,
    KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    MAX_POLL_RECORDS_CONFIG -> 100.toString,
    AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ENABLE_AUTO_COMMIT_CONFIG -> "false"
  ).asJava

  val consumersToTest = List(
    ("Sync Consumer",
      (tp: TopicPartition, kafkaParams: ju.Map[String, Object]) => {
        val consumerParams = new ju.HashMap[String, String]
        new SyncSparkKafkaConsumerBuilder[Array[Byte], Array[Byte]]
          .init(tp, kafkaParams, consumerParams, TimeUnit.MINUTES.toMillis(2))
      }
    ),
    ("Async Consumer with default setting",
      (tp: TopicPartition, kafkaParams: ju.Map[String, Object]) => {
        val consumerParams = new ju.HashMap[String, String]
        new AsyncSparkKafkaConsumerBuilder[Array[Byte], Array[Byte]]
          .init(tp, kafkaParams, consumerParams, TimeUnit.MINUTES.toMillis(2))
      }
    ),
    ("Async Consumer with buffer",
      (tp: TopicPartition, kafkaParams: ju.Map[String, Object]) => {
        val consumerParams = new ju.HashMap[String, String]
        consumerParams.put("maintainBufferMin", "100")
        new AsyncSparkKafkaConsumerBuilder[Array[Byte], Array[Byte]]
          .init(tp, kafkaParams, consumerParams, TimeUnit.MINUTES.toMillis(2))
      }
    )
  )

  test("KafkaDataConsumer reuse in case of same groupId and TopicPartition") {
    KafkaDataConsumer.cache.clear()

    val kafkaParams = getKafkaParams()

    consumersToTest.foreach { consumerProducer =>
      var testName = consumerProducer._1
      val suiteName = this.getClass.getName
      val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")

      try {
        logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")

        KafkaDataConsumer.cache.clear()

        val topic = "topic" + Random.nextInt()
        val topicPartition = new TopicPartition(topic, 0)

        val consumer1 = KafkaDataConsumer.acquire[Array[Byte], Array[Byte]](
          topicPartition, kafkaParams, null, true,
          consumerProducer._2(topicPartition, kafkaParams)
        )
        consumer1.release()

        val consumer2 = KafkaDataConsumer.acquire[Array[Byte], Array[Byte]](
          topicPartition, kafkaParams, null, true,
          consumerProducer._2(topicPartition, kafkaParams)
        )
        consumer2.release()

        assert(KafkaDataConsumer.cache.size() == 1)
        val key = new CacheKey(groupId, topicPartition)
        val existingInternalConsumer = KafkaDataConsumer.cache.get(key)
        assert(existingInternalConsumer.eq(consumer1.internalConsumer))
        assert(existingInternalConsumer.eq(consumer2.internalConsumer))
      } finally {
        logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
      }
    }
  }

  test("concurrent use of KafkaDataConsumer") {
    val kafkaParams = getKafkaParams()

    consumersToTest.foreach { consumerProducer =>
      var testName = consumerProducer._1
      val suiteName = this.getClass.getName
      val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")

      try {
        logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")

        KafkaDataConsumer.cache.clear()

        val data = (1 to 1000).map(_.toString)

        val topic = "topic" + Random.nextInt()
        val topicPartition = new TopicPartition(topic, 0)

        testUtils.createTopic(topic)
        testUtils.sendMessages(topic, data.toArray)

        val kafkaParams = getKafkaParams()

        val numThreads = 100
        val numConsumerUsages = 500

        @volatile var error: Throwable = null

        def consume(i: Int): Unit = {
          val useCache = Random.nextBoolean
          val taskContext = if (Random.nextBoolean) {
            new TaskContextImpl(0, 0, 0, 0, attemptNumber = Random.nextInt(2), null, null, null)
          } else {
            null
          }

          val consumerParams = new ju.HashMap[String, String]
          consumerParams.put("maintainBufferMin", "100");
          val consumerBuilder = new AsyncSparkKafkaConsumerBuilder[Array[Byte], Array[Byte]]
            .init(topicPartition, kafkaParams, consumerParams, TimeUnit.MINUTES.toMillis(1))

          val consumer = KafkaDataConsumer.acquire[Array[Byte], Array[Byte]](
            topicPartition, kafkaParams, taskContext, useCache, consumerBuilder)
          try {
            val rcvd = (0 until data.length).map { offset =>
              val bytes = consumer.get(offset).value()
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
              override def run(): Unit = {
                consume(i)
              }
            })
          }
          futures.foreach(_.get(1, TimeUnit.MINUTES))
          assert(error == null)
        } finally {
          threadPool.shutdown()
        }
      } finally {
        logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
      }

    }
  }
}
