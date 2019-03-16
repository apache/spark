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

import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.Random

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.scalatest.PrivateMethodTester

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.ThreadUtils

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
}
