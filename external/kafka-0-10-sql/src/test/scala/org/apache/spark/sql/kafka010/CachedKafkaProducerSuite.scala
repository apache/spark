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
import scala.util.Random

import org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.kafka.clients.producer.ProducerConfig.{KEY_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_CONFIG}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.PrivateMethodTester

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.sql.kafka010.InternalKafkaProducerPool._
import org.apache.spark.sql.test.SharedSparkSession

class CachedKafkaProducerSuite extends SharedSparkSession with PrivateMethodTester with KafkaTest {

  private var testUtils: KafkaTestUtils = _
  private val topic = "topic" + Random.nextInt()
  private var producerPool: InternalKafkaProducerPool = _

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

  override def beforeEach(): Unit = {
    super.beforeEach()

    producerPool = {
      val internalKafkaConsumerPoolMethod = PrivateMethod[InternalKafkaProducerPool]('producerPool)
      CachedKafkaProducer.invokePrivate(internalKafkaConsumerPoolMethod())
    }

    producerPool.reset()
  }

  private def getKafkaParams(acks: Int = 0) = Map[String, Object](
    "acks" -> acks.toString,
    // Here only host should be resolvable, it does not need a running instance of kafka server.
    BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress,
    KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName
  ).asJava

  test("acquire should return the cached instance with same params") {
    val kafkaParams = getKafkaParams()

    val producer1 = CachedKafkaProducer.acquire(kafkaParams)
    CachedKafkaProducer.release(producer1)
    val producer2 = CachedKafkaProducer.acquire(kafkaParams)
    CachedKafkaProducer.release(producer2)

    assert(producer1 === producer2)
    assert(producerPool.size(toCacheKey(kafkaParams)) === 1)
  }

  test("acquire should return a new instance with different params") {
    val kafkaParams1 = getKafkaParams()
    val kafkaParams2 = getKafkaParams(1)

    val producer1 = CachedKafkaProducer.acquire(kafkaParams1)
    CachedKafkaProducer.release(producer1)
    val producer2 = CachedKafkaProducer.acquire(kafkaParams2)
    CachedKafkaProducer.release(producer2)

    assert(producer1 !== producer2)
    assert(producerPool.size(toCacheKey(kafkaParams1)) === 1)
    assert(producerPool.size(toCacheKey(kafkaParams2)) === 1)
  }

  test("acquire should return a new instance with Task retry") {
    try {
      val kafkaParams = getKafkaParams()

      val context1 = new TaskContextImpl(0, 0, 0, 0, 0, null, null, null)
      TaskContext.setTaskContext(context1)
      val producer1 = CachedKafkaProducer.acquire(kafkaParams)
      CachedKafkaProducer.release(producer1)

      val context2 = new TaskContextImpl(0, 0, 0, 0, 1, null, null, null)
      TaskContext.setTaskContext(context2)
      val producer2 = CachedKafkaProducer.acquire(kafkaParams)
      CachedKafkaProducer.release(producer2)

      assert(producer1 !== producer2)
      assert(producerPool.size(toCacheKey(kafkaParams)) === 1)
    } finally {
      TaskContext.unset()
    }
  }

  test("Concurrent use of CachedKafkaProducer") {
    val data = (1 to 1000).map(_.toString)
    testUtils.createTopic(topic, 1)

    val kafkaParams = getKafkaParams()
    val numThreads = 100
    val numProducerUsages = 500

    @volatile var error: Throwable = null

    val callback = new Callback() {
      override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
        if (error == null && e != null) {
          error = e
        }
      }
    }

    def produce(): Unit = {
      val taskContext = if (Random.nextBoolean) {
        new TaskContextImpl(0, 0, 0, 0, attemptNumber = Random.nextInt(2), null, null, null)
      } else {
        null
      }
      TaskContext.setTaskContext(taskContext)
      val producer = CachedKafkaProducer.acquire(kafkaParams)
      try {
        data.foreach { d =>
          val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, null, d.getBytes)
          producer.send(record, callback)
        }
      } finally {
        CachedKafkaProducer.release(producer)
      }
    }

    val threadpool = Executors.newFixedThreadPool(numThreads)
    try {
      val futures = (1 to numProducerUsages).map { i =>
        threadpool.submit(new Runnable {
          override def run(): Unit = { produce() }
        })
      }
      futures.foreach(_.get(1, TimeUnit.MINUTES))
      assert(error == null)
    } finally {
      threadpool.shutdown()
    }
  }
}
