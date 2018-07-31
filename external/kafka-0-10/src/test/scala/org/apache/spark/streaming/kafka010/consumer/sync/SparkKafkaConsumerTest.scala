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

package org.apache.spark.streaming.kafka010.consumer.sync

import java.{util => ju}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.scalatest.BeforeAndAfterAll

import org.apache.spark._
import org.apache.spark.streaming.kafka010.{KafkaDataConsumer, KafkaTestUtils}
import org.apache.spark.streaming.kafka010.consumer.async.AsyncSparkKafkaConsumerBuilder

class SparkKafkaConsumerTest extends SparkFunSuite with BeforeAndAfterAll {

  private var testUtils: KafkaTestUtils = _
  private val topic = "topic" + Random.nextInt()
  private val topicPartition = new TopicPartition(topic, 0)
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
    AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ENABLE_AUTO_COMMIT_CONFIG -> "false"
  ).asJava

  test("Spark Kafka Consumer Test") {
    val kafkaParams = getKafkaParams()

    val consumersToTest = List(
      ("Sync Consumer", (tn: String) => {
        val consumerParams = new ju.HashMap[String, String]
        new SyncSparkKafkaConsumerBuilder[Array[Byte], Array[Byte]]
          .init(
            new TopicPartition(tn, 0), kafkaParams, consumerParams,
            TimeUnit.SECONDS.toMillis(10)
          )
      }),
      ("Async Consumer with default setting", (tn: String) => {
        val consumerParams = new ju.HashMap[String, String]
        new AsyncSparkKafkaConsumerBuilder[Array[Byte], Array[Byte]]
          .init(
            new TopicPartition(tn, 0), kafkaParams, consumerParams,
            TimeUnit.SECONDS.toMillis(10)
          )
      }),
      ("Async Consumer with buffer", (tn: String) => {
        val consumerParams = new ju.HashMap[String, String]
        consumerParams.put("maintainBufferMin", "100")
        new AsyncSparkKafkaConsumerBuilder[Array[Byte], Array[Byte]]
          .init(
            new TopicPartition(tn, 0), kafkaParams, consumerParams,
            TimeUnit.SECONDS.toMillis(10)
          )
      })
    )
    consumersToTest.foreach{consumerProducer =>
      var testName = consumerProducer._1
      val suiteName = this.getClass.getName
      val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")
      try {
        logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")

        val data = (1 to 1000).map(_.toString)
        val currTopic = topic + Random.nextInt()
        testUtils.createTopic(currTopic)
        testUtils.sendMessages(currTopic, data.toArray)
        var consumer = consumerProducer._2(currTopic)

        consumer.ensureOffset(0)
        (1 to 200).foreach(i => {
          assert(consumer.getNextOffset() === i-1)
          val record = consumer.getNextRecord()
          assert(record.offset() === i-1)
          assert(new String(record.value()) === i.toString)
        })

        consumer.ensureOffset(135)
        (136 to 789).foreach(i => {
          assert(consumer.getNextOffset() === i-1)
          val record = consumer.getNextRecord()
          assert(record.offset() === i-1)
          assert(new String(record.value()) === i.toString)
        })

        assert(consumer.getNextOffset() === 789)
        consumer.ensureOffset(238)
        assert(consumer.getNextOffset() === 238)
        assert(consumer.getNextRecord().offset() === 238)
      } finally {
        logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
      }
    }
  }
}
