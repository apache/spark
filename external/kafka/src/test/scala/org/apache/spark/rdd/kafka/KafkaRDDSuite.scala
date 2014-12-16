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

package org.apache.spark.rdd.kafka

import scala.util.Random

import kafka.serializer.StringDecoder
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.kafka.KafkaStreamSuiteBase

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
    produceAndSendMessage(topic, sent)

    val kafkaParams = Map("metadata.broker.list" -> s"localhost:$brokerPort",
      "group.id" -> s"test-consumer-${Random.nextInt(10000)}")

    val kc = new KafkaCluster(kafkaParams)

    val rdd = getRdd(kc, Set(topic))
    assert(rdd.isDefined)
    assert(rdd.get.countByValue.size === sent.size)

    kc.setConsumerOffsets(kafkaParams("group.id"), rdd.get.untilOffsets)

    val rdd2 = getRdd(kc, Set(topic))
    assert(rdd2.isDefined)
    assert(rdd2.get.count === 0)
  }

  private def getRdd(kc: KafkaCluster, topics: Set[String]) = {
    val groupId = kc.kafkaParams("group.id")
    for {
      topicPartitions <- kc.getPartitions(topics).right.toOption
      from <- kc.getConsumerOffsets(groupId, topicPartitions).right.toOption.orElse(
        kc.getEarliestLeaderOffsets(topicPartitions).right.toOption)
      until <- kc.getLatestLeaderOffsets(topicPartitions).right.toOption
    } yield {
      new KafkaRDD[String, String, StringDecoder, StringDecoder, String](
        sc, kc.kafkaParams, from, until, mmd => mmd.message)
    }
  }
}
