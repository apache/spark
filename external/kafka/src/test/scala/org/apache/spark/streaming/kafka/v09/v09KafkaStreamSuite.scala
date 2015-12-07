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

package org.apache.spark.streaming.kafka.v09

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.{KafkaTestUtils, KafkaUtils}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

class v09KafkaStreamSuite extends SparkFunSuite with Eventually with BeforeAndAfterAll {
  private var ssc: StreamingContext = _
  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeAll(): Unit = {
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll(): Unit = {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }

    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  test("Kafka input stream") {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)
    ssc = new StreamingContext(sparkConf, Milliseconds(500))
    val topic = "new_topic_NewKafkaStreamSuite"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, sent)

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaTestUtils.brokerAddress,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.GROUP_ID_CONFIG -> s"test-consumer-${Random.nextInt(10000)}",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "spark.kafka.poll.time" -> "100"
    )

    val stream = KafkaUtils.createStream[String, String](
      ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)
    val result = new mutable.HashMap[String, Long]() with mutable.SynchronizedMap[String, Long]
    stream.map(_._2).countByValue().foreachRDD { r =>
      val ret = r.collect()
      ret.toMap.foreach { kv =>
        val count = result.getOrElseUpdate(kv._1, 0) + kv._2
        result.put(kv._1, count)
      }
    }

    ssc.start()

    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(sent === result)
    }
  }
}
