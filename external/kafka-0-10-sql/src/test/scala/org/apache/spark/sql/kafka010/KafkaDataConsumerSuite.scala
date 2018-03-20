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

package org.apache.spark.sql.kafka010

import java.util.ArrayList
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.scalatest.PrivateMethodTester

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.sql.test.SharedSQLContext

class KafkaDataConsumerSuite extends SharedSQLContext with PrivateMethodTester {

  protected var testUtils: KafkaTestUtils = _

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

  test("SPARK-19886: Report error cause correctly in reportDataLoss") {
    val cause = new Exception("D'oh!")
    val reportDataLoss = PrivateMethod[Unit]('reportDataLoss0)
    val e = intercept[IllegalStateException] {
      InternalKafkaConsumer.invokePrivate(reportDataLoss(true, "message", cause))
    }
    assert(e.getCause === cause)
  }

  test("SPARK-23623: concurrent use of KafkaDataConsumer") {
    val topic = "topic" + Random.nextInt()
    val data = (1 to 1000).map(_.toString)
    testUtils.createTopic(topic, 1)
    testUtils.sendMessages(topic, data.toArray)
    val topicPartition = new TopicPartition(topic, 0)

    import ConsumerConfig._
    val kafkaParams = Map[String, Object](
      GROUP_ID_CONFIG -> "groupId",
      BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress,
      KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

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
      TaskContext.setTaskContext(taskContext)
      val consumer = KafkaDataConsumer.acquire(
        topicPartition, kafkaParams.asJava, useCache)
      try {
        val range = consumer.getAvailableOffsetRange()
        val rcvd = range.earliest until range.latest map { offset =>
          val bytes = consumer.get(offset, Long.MaxValue, 10000, failOnDataLoss = false).value()
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


  test("SPARK-23685: returning record and not throwing exception in fetchData when a " +
    "specific offset in the user request range is not found ") {
    val topicPartition = new TopicPartition("testTopic", 0)
    val data = new ArrayList[ConsumerRecord[Array[Byte], Array[Byte]]]();
    data.add(new ConsumerRecord("testTopic", 0, 1L, "mykey".getBytes(), "myvalue1".getBytes()))
    data.add(new ConsumerRecord("testTopic", 0, 2L, "mykey".getBytes(), "myvalue2".getBytes()))
    data.add(new ConsumerRecord("testTopic", 0, 3L, "mykey".getBytes(), "myvalue3".getBytes()))
    data.add(new ConsumerRecord("testTopic", 0, 5L, "mykey".getBytes(), "myvalue5".getBytes()))
    data.add(new ConsumerRecord("testTopic", 0, 6L, "mykey".getBytes(), "myvalue6".getBytes()))

    var recordsMap = new java.util.HashMap[TopicPartition,
      java.util.List[ConsumerRecord[Array[Byte], Array[Byte]]]]
    recordsMap.put(topicPartition, data)
    var consumerRecords = new ConsumerRecords(recordsMap)
    var fetchedData = consumerRecords.iterator

    var map = new java.util.HashMap[String, Object]
    map.put("group.id", "groupid")
    map.put("bootstrap.servers", "http://127.0.0.1:8080")
    map.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    map.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = InternalKafkaConsumer(topicPartition, map)
    consumer.nextOffsetInFetchedData = 0L
    consumer.fetchedData = fetchedData
    // set value for the fetchedData
    val record = consumer.get(0L, 4L, 100L, true)

    assert(data.get(0) === record)

  }
}
