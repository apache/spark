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

import scala.util.Random

import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.SparkContext._

class KafkaRDDSuite extends KafkaStreamSuiteBase with BeforeAndAfter {
  var sc: SparkContext = _
  before {
    setupKafka()
  }

  after {
    if (sc != null) {
      sc.stop
      sc = null
    }
    tearDownKafka()
  }

  test("Kafka RDD basic usage") {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)
    sc = new SparkContext(sparkConf)
    val topic = "topicbasic"
    createTopic(topic)
    val messages = Set("the", "quick", "brown", "fox")
    sendMessages(topic, messages.toArray)


    val kafkaParams = Map("metadata.broker.list" -> brokerAddress,
      "group.id" -> s"test-consumer-${Random.nextInt(10000)}")

    val offsetRanges = Array(OffsetRange(topic, 0, 0, messages.size))

    val rdd =  KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, offsetRanges)

    val received = rdd.map(_._2).collect.toSet
    assert(received === messages)
  }

  test("Kafka RDD integration") {
    // the idea is to find e.g. off-by-one errors between what kafka has available and the rdd

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)
    sc = new SparkContext(sparkConf)
    val topic = "topic1"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    createTopic(topic)

    val kafkaParams = Map("metadata.broker.list" -> brokerAddress,
      "group.id" -> s"test-consumer-${Random.nextInt(10000)}")

    val kc = new KafkaCluster(kafkaParams)

    // this is the "lots of messages" case
    sendMessages(topic, sent)
    // rdd defined from leaders after sending messages, should get the number sent
    val rdd = getRdd(kc, Set(topic))

    assert(rdd.isDefined)
    assert(rdd.get.count === sent.values.sum, "didn't get all sent messages")

    val ranges = rdd.get.asInstanceOf[HasOffsetRanges]
      .offsetRanges.map(o => TopicAndPartition(o.topic, o.partition) -> o.untilOffset).toMap

    kc.setConsumerOffsets(kafkaParams("group.id"), ranges)

    // this is the "0 messages" case
    val rdd2 = getRdd(kc, Set(topic))
    // shouldn't get anything, since message is sent after rdd was defined
    val sentOnlyOne = Map("d" -> 1)

    sendMessages(topic, sentOnlyOne)
    assert(rdd2.isDefined)
    assert(rdd2.get.count === 0, "got messages when there shouldn't be any")

    // this is the "exactly 1 message" case, namely the single message from sentOnlyOne above
    val rdd3 = getRdd(kc, Set(topic))
    // send lots of messages after rdd was defined, they shouldn't show up
    sendMessages(topic, Map("extra" -> 22))

    assert(rdd3.isDefined)
    assert(rdd3.get.count === sentOnlyOne.values.sum, "didn't get exactly one message")

  }

  // get an rdd from the committed consumer offsets until the latest leader offsets,
  private def getRdd(kc: KafkaCluster, topics: Set[String]) = {
    val groupId = kc.kafkaParams("group.id")
    for {
      topicPartitions <- kc.getPartitions(topics).right.toOption
      from <- kc.getConsumerOffsets(groupId, topicPartitions).right.toOption.orElse(
        kc.getEarliestLeaderOffsets(topicPartitions).right.toOption.map { offs =>
          offs.map(kv => kv._1 -> kv._2.offset)
        }
      )
      until <- kc.getLatestLeaderOffsets(topicPartitions).right.toOption
    } yield {
      val leaders = until.map { case (tp, lo) =>
          tp -> Broker(lo.host, lo.port)
      }.toMap
      val offsetRanges = from.map { case (tp, f) =>
          val u = until(tp)
          OffsetRange(tp.topic, tp.partition, f, u.offset)
      }.toArray

      KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder, String](
        sc, kc.kafkaParams, offsetRanges, leaders,
        (mmd: MessageAndMetadata[String, String]) => s"${mmd.offset} ${mmd.message}")
    }
  }
}
