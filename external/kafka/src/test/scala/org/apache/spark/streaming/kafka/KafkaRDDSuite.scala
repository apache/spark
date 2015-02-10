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

  test("Kafka RDD") {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)
    sc = new SparkContext(sparkConf)
    val topic = "topic1"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    createTopic(topic)
    sendMessages(topic, sent)

    val kafkaParams = Map("metadata.broker.list" -> brokerAddress,
      "group.id" -> s"test-consumer-${Random.nextInt(10000)}")

    val kc = new KafkaCluster(kafkaParams)

    val rdd = getRdd(kc, Set(topic))
    // this is the "lots of messages" case
    // make sure we get all of them
    assert(rdd.isDefined)
    assert(rdd.get.count === sent.values.sum)

    kc.setConsumerOffsets(
      kafkaParams("group.id"),
      rdd.get.offsetRanges.map(o => TopicAndPartition(o.topic, o.partition) -> o.untilOffset).toMap)

    val rdd2 = getRdd(kc, Set(topic))
    val sent2 = Map("d" -> 1)
    sendMessages(topic, sent2)
    // this is the "0 messages" case
    // make sure we dont get anything, since messages were sent after rdd was defined
    assert(rdd2.isDefined)
    assert(rdd2.get.count === 0)

    val rdd3 = getRdd(kc, Set(topic))
    sendMessages(topic, Map("extra" -> 22))
    // this is the "exactly 1 message" case
    // make sure we get exactly one message, despite there being lots more available
    assert(rdd3.isDefined)
    assert(rdd3.get.count === sent2.values.sum)

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
      KafkaRDD[String, String, StringDecoder, StringDecoder, String](
        sc, kc.kafkaParams, from, until, mmd => s"${mmd.offset} ${mmd.message}")
    }
  }
}
