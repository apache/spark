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

package org.apache.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

class KafkaStreamSuite extends SparkFunSuite
    with BeforeAndAfterAll with BeforeAndAfter with Eventually {

  private val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName(this.getClass.getSimpleName)
  private val data = Map("a" -> 5, "b" -> 3, "c" -> 10)
  private val data2 = Map("d" -> 8, "e" -> 1, "f" -> 9)

  private var kafkaTestUtils: KafkaTestUtils = _

  private var groupId: String = _
  private var kafkaParams: Map[String, String] = _
  private var ssc: StreamingContext = _

  override def beforeAll() : Unit = {
    groupId = s"test-consumer-${Random.nextInt(10000)}"
  }

  override def afterAll(): Unit = {
    // Nothing to be done here
  }

  before {
    ssc = new StreamingContext(sparkConf, Milliseconds(500))

    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()

    kafkaParams = Map(
      "zookeeper.connect" -> kafkaTestUtils.zkAddress,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )
  }

  after {
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
    val topic = "topic1"
    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, data)

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
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
      assert(data === result)
    }
  }

  test("Kafka input stream with WhiteList") {
    val topic = "topic1"
    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, data)

    val topicFilter = KafkaRegexTopicFilter("topic1", 1, true)

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicFilter, StorageLevel.MEMORY_ONLY)
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
      assert(data === result)
    }
  }

  test("Kafka input stream with WhiteList 2") {
    val topics = Map("topic1" -> data, "topic2" -> data2)
    topics.foreach { case (t, d) =>
      kafkaTestUtils.createTopic(t)
      kafkaTestUtils.sendMessages(t, d)
    }

    val topicFilter = KafkaRegexTopicFilter("topic1", 1, true)

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicFilter, StorageLevel.MEMORY_ONLY)
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
      assert(data === result)
    }
  }
}
