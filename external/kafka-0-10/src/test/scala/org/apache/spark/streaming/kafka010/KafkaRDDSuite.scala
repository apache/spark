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

import java.{ util => ju }

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll

import org.apache.spark._
import org.apache.spark.scheduler.ExecutorCacheTaskLocation

class KafkaRDDSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var kafkaTestUtils: KafkaTestUtils = _

  private val sparkConf = new SparkConf().setMaster("local[4]")
    .setAppName(this.getClass.getSimpleName)
  private var sc: SparkContext = _

  override def beforeAll {
    sc = new SparkContext(sparkConf)
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll {
    if (sc != null) {
      sc.stop
      sc = null
    }

    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  private def getKafkaParams() = Map[String, Object](
    "bootstrap.servers" -> kafkaTestUtils.brokerAddress,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> s"test-consumer-${Random.nextInt}-${System.currentTimeMillis}"
  ).asJava

  private val preferredHosts = LocationStrategies.PreferConsistent

  test("basic usage") {
    val topic = s"topicbasic-${Random.nextInt}-${System.currentTimeMillis}"
    kafkaTestUtils.createTopic(topic)
    val messages = Array("the", "quick", "brown", "fox")
    kafkaTestUtils.sendMessages(topic, messages)

    val kafkaParams = getKafkaParams()

    val offsetRanges = Array(OffsetRange(topic, 0, 0, messages.size))

    val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, preferredHosts)
      .map(_.value)

    val received = rdd.collect.toSet
    assert(received === messages.toSet)

    // size-related method optimizations return sane results
    assert(rdd.count === messages.size)
    assert(rdd.countApprox(0).getFinalValue.mean === messages.size)
    assert(!rdd.isEmpty)
    assert(rdd.take(1).size === 1)
    assert(rdd.take(1).head === messages.head)
    assert(rdd.take(messages.size + 10).size === messages.size)

    val emptyRdd = KafkaUtils.createRDD[String, String](
      sc, kafkaParams, Array(OffsetRange(topic, 0, 0, 0)), preferredHosts)

    assert(emptyRdd.isEmpty)

    // invalid offset ranges throw exceptions
    val badRanges = Array(OffsetRange(topic, 0, 0, messages.size + 1))
    intercept[SparkException] {
      val result = KafkaUtils.createRDD[String, String](sc, kafkaParams, badRanges, preferredHosts)
        .map(_.value)
        .collect()
    }
  }

  test("iterator boundary conditions") {
    // the idea is to find e.g. off-by-one errors between what kafka has available and the rdd
    val topic = s"topicboundary-${Random.nextInt}-${System.currentTimeMillis}"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    kafkaTestUtils.createTopic(topic)

    val kafkaParams = getKafkaParams()

    // this is the "lots of messages" case
    kafkaTestUtils.sendMessages(topic, sent)
    var sentCount = sent.values.sum

    val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams,
      Array(OffsetRange(topic, 0, 0, sentCount)), preferredHosts)

    val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val rangeCount = ranges.map(o => o.untilOffset - o.fromOffset).sum

    assert(rangeCount === sentCount, "offset range didn't include all sent messages")
    assert(rdd.map(_.offset).collect.sorted === (0 until sentCount).toArray,
      "didn't get all sent messages")

    // this is the "0 messages" case
    val rdd2 = KafkaUtils.createRDD[String, String](sc, kafkaParams,
      Array(OffsetRange(topic, 0, sentCount, sentCount)), preferredHosts)

    // shouldn't get anything, since message is sent after rdd was defined
    val sentOnlyOne = Map("d" -> 1)

    kafkaTestUtils.sendMessages(topic, sentOnlyOne)

    assert(rdd2.map(_.value).collect.size === 0, "got messages when there shouldn't be any")

    // this is the "exactly 1 message" case, namely the single message from sentOnlyOne above
    val rdd3 = KafkaUtils.createRDD[String, String](sc, kafkaParams,
      Array(OffsetRange(topic, 0, sentCount, sentCount + 1)), preferredHosts)

    // send lots of messages after rdd was defined, they shouldn't show up
    kafkaTestUtils.sendMessages(topic, Map("extra" -> 22))

    assert(rdd3.map(_.value).collect.head === sentOnlyOne.keys.head,
      "didn't get exactly one message")
  }

  test("executor sorting") {
    val kafkaParams = new ju.HashMap[String, Object](getKafkaParams())
    kafkaParams.put("auto.offset.reset", "none")
    val rdd = new KafkaRDD[String, String](
      sc,
      kafkaParams,
      Array(OffsetRange("unused", 0, 1, 2)),
      ju.Collections.emptyMap[TopicPartition, String](),
      true)
    val a3 = ExecutorCacheTaskLocation("a", "3")
    val a4 = ExecutorCacheTaskLocation("a", "4")
    val b1 = ExecutorCacheTaskLocation("b", "1")
    val b2 = ExecutorCacheTaskLocation("b", "2")

    val correct = Array(b2, b1, a4, a3)

    correct.permutations.foreach { p =>
      assert(p.sortWith(rdd.compareExecutors) === correct)
    }
  }
}
