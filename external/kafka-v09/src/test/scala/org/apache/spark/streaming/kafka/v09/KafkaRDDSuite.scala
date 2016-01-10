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

import scala.util.Random
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfterAll
import org.apache.spark._

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

  test("basic usage") {
    val topic = s"new_topicbasic-${Random.nextInt}"
    kafkaTestUtils.createTopic(topic)
    val messages = Array("the", "quick", "brown", "fox")
    kafkaTestUtils.sendMessages(topic, messages)

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaTestUtils.brokerAddress,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.GROUP_ID_CONFIG -> s"test-consumer-${Random.nextInt}",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "spark.kafka.poll.time" -> "10000")

    val offsetRanges = Array(OffsetRange(topic, 0, 0, messages.size))

    val rdd = KafkaUtils.createRDD[String, String](
      sc, kafkaParams, offsetRanges)

    val received = rdd.map(_._2).collect.toSet
    assert(received === messages.toSet)

    // size-related method optimizations return sane results
    assert(rdd.count === messages.size)
    assert(rdd.countApprox(0).getFinalValue.mean === messages.size)
    assert(!rdd.isEmpty)
    assert(rdd.take(1).size === 1)
    assert(rdd.take(1).head._2 === messages.head)
    assert(rdd.take(messages.size + 10).size === messages.size)

    val emptyRdd = KafkaUtils.createRDD[String, String](
      sc, kafkaParams, Array(OffsetRange(topic, 0, 0, 0)))

    assert(emptyRdd.isEmpty)

    // invalid offset ranges throw exceptions
    val badRanges = Array(OffsetRange(topic, 0, 0, messages.size + 1))
    intercept[SparkException] {
      KafkaUtils.createRDD[String, String](
        sc, kafkaParams, badRanges)
    }
  }

  test("iterator boundary conditions") {
    // the idea is to find e.g. off-by-one errors between what kafka has available and the rdd
    val topic = s"new_topicboundary-${Random.nextInt}"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    kafkaTestUtils.createTopic(topic)

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaTestUtils.brokerAddress,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.GROUP_ID_CONFIG -> s"test-consumer-${Random.nextInt}",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "spark.kafka.poll.time" -> "1000")

    val kc = new KafkaCluster(kafkaParams)

    // this is the "lots of messages" case
    kafkaTestUtils.sendMessages(topic, sent)
    val sentCount = sent.values.sum

    // rdd defined from leaders after sending messages, should get the number sent
    val rdd = getRdd(kc, Set(topic))

    assert(rdd.isDefined)

    val ranges = rdd.get.asInstanceOf[HasOffsetRanges].offsetRanges
    val rangeCount = ranges.map(o => o.untilOffset - o.fromOffset).sum

    assert(rangeCount === sentCount, "offset range didn't include all sent messages")
    assert(rdd.get.count === sentCount, "didn't get all sent messages")

    val rangesMap = ranges.map(o => new TopicPartition(o.topic, o.partition) -> o.untilOffset).toMap

    // make sure consumer offsets are committed before the next getRdd call
    kc.setConsumerOffsets(rangesMap)

    // this is the "0 messages" case
    val rdd2 = getRdd(kc, Set(topic))
    // shouldn't get anything, since message is sent after rdd was defined
    val sentOnlyOne = Map("d" -> 1)

    kafkaTestUtils.sendMessages(topic, sentOnlyOne)

    assert(rdd2.isDefined)
    assert(rdd2.get.count === 0, "got messages when there shouldn't be any")

    // this is the "exactly 1 message" case, namely the single message from sentOnlyOne above
    val rdd3 = getRdd(kc, Set(topic))
    // send lots of messages after rdd was defined, they shouldn't show up
    kafkaTestUtils.sendMessages(topic, Map("extra" -> 22))

    assert(rdd3.isDefined)
    assert(rdd3.get.count === sentOnlyOne.values.sum, "didn't get exactly one message")

  }

  // get an rdd from the committed consumer offsets until the latest leader offsets,
  private def getRdd(kc: KafkaCluster[_, _], topics: Set[String]) = {
    val groupId = kc.kafkaParams("group.id")
    val topicPartitions = kc.getPartitions(topics)
    val consumerOffsets = try {
      kc.getCommittedOffsets(topicPartitions)
    } catch {
      case e: SparkException => kc.getEarliestOffsets(topicPartitions)
    }
    val latestOffsets = kc.getLatestOffsets(topicPartitions)

    val offsetRanges = consumerOffsets.map { case (tp: TopicPartition, fromOffset: Long) =>
      OffsetRange(tp.topic, tp.partition, fromOffset, latestOffsets(tp))
    }.toArray

    Option(KafkaUtils.createRDD[String, String, String](
      sc, kc.kafkaParams, offsetRanges,
      (cr: ConsumerRecord[String, String]) => s"${cr.offset()} ${cr.value()}"))
    }

}
