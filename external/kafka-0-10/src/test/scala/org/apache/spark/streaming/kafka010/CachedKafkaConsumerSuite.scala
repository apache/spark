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

import java.util.concurrent.CountDownLatch

import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll
import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite

class CachedKafkaConsumerSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeAll {
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll {
    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  test("[SPARK-22562] unsafe eviction from cache") {
    val topic = "test-topic"
    val groupId = "test.group"

    kafkaTestUtils.createTopic(topic)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaTestUtils.brokerAddress,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId
    ).asJava

    CachedKafkaConsumer.init(1, 1, 0.75f)

    val consumer = CachedKafkaConsumer.get(groupId, topic, 0, kafkaParams)
    val latch = new CountDownLatch(1)
    new Thread(new Runnable {
      override def run(): Unit = {
        try {
          consumer.get(0, 500)
          consumer.close()
        } finally {
          latch.countDown()
        }
      }
    }).start()

    CachedKafkaConsumer.get(groupId, "another-test-topic", 0, kafkaParams)
    latch.await()
  }
}
