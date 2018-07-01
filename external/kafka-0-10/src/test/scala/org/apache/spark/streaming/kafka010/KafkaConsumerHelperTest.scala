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

import java.util
import java.util.concurrent.Executor

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.Random

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll

import org.apache.spark._

class KafkaConsumerHelperTest extends SparkFunSuite with BeforeAndAfterAll {

  private var testUtils: KafkaTestUtils = _
  private val topic = "topic" + Random.nextInt()
  private val topicPartition = new TopicPartition(topic, 0)
  private val groupId = "groupId"
  private val maxPollRecords: Int = 3

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
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
    BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress,
    KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ENABLE_AUTO_COMMIT_CONFIG -> "false",
    MAX_POLL_RECORDS_CONFIG -> maxPollRecords.toString
  ).asJava

  test("KafkaDataConsumer prefetch records using async thread") {
    testUtils.createTopic(topic)
    val messages = (0 to 99).map(num => "message-" + num).toArray
    testUtils.sendMessages(topic, messages)

    val executor: Executor = java.util.concurrent.Executors.newSingleThreadExecutor()
    var kafkaParams = new util.HashMap[String, Object](getKafkaParams())
    kafkaParams.put(GROUP_ID_CONFIG, groupId + "Async")
    val kafkaConsumerHelperAsync: KafkaConsumerHelper[String, String] = new KafkaConsumerHelper(
      new TopicPartition(topic, 0),
      kafkaParams,
      10,
      ExecutionContext.fromExecutor(executor)
    )

    kafkaConsumerHelperAsync.ensureOffset(0);
    val timeout = 10000
    var record = kafkaConsumerHelperAsync.getNextRecord(timeout)
    assert(record.value() == "message-0")
    assert(kafkaConsumerHelperAsync.buffer.size() == 2)
    Thread.sleep(timeout)
    record = kafkaConsumerHelperAsync.getNextRecord(timeout)
    assert(record.value() == "message-1")
    assert(kafkaConsumerHelperAsync.buffer.size() > maxPollRecords)


    kafkaParams = new util.HashMap[String, Object](getKafkaParams())
    kafkaParams.put(GROUP_ID_CONFIG, groupId + "Sync")
    val kafkaConsumerHelperSync: KafkaConsumerHelper[String, String] = new KafkaConsumerHelper(
      new TopicPartition(topic, 0),
      kafkaParams,
      0,
      ExecutionContext.fromExecutor(executor)
    )

    kafkaConsumerHelperSync.ensureOffset(0);
    record = kafkaConsumerHelperSync.getNextRecord(timeout)
    assert(record.value() == "message-0")
    assert(kafkaConsumerHelperSync.buffer.size() == 2)
    Thread.sleep(timeout)
    record = kafkaConsumerHelperSync.getNextRecord(timeout)
    assert(record.value() == "message-1")
    assert(kafkaConsumerHelperSync.buffer.size() < maxPollRecords)




  }

}

