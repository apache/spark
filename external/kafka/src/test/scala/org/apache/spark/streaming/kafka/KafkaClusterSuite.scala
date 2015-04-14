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

import kafka.common.TopicAndPartition
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class KafkaClusterSuite extends FunSuite with BeforeAndAfterAll {
  private val topic = "kcsuitetopic" + Random.nextInt(10000)
  private val topicAndPartition = TopicAndPartition(topic, 0)
  private var kc: KafkaCluster = null

  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeAll() {
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()

    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, Map("a" -> 1))
    kc = new KafkaCluster(Map("metadata.broker.list" -> kafkaTestUtils.brokerAddress))
  }

  override def afterAll() {
    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  test("metadata apis") {
    val leader = kc.findLeaders(Set(topicAndPartition)).right.get(topicAndPartition)
    val leaderAddress = s"${leader._1}:${leader._2}"
    assert(leaderAddress === kafkaTestUtils.brokerAddress, "didn't get leader")

    val parts = kc.getPartitions(Set(topic)).right.get
    assert(parts(topicAndPartition), "didn't get partitions")

    val err = kc.getPartitions(Set(topic + "BAD"))
    assert(err.isLeft, "getPartitions for a nonexistant topic should be an error")
  }

  test("leader offset apis") {
    val earliest = kc.getEarliestLeaderOffsets(Set(topicAndPartition)).right.get
    assert(earliest(topicAndPartition).offset === 0, "didn't get earliest")

    val latest = kc.getLatestLeaderOffsets(Set(topicAndPartition)).right.get
    assert(latest(topicAndPartition).offset === 1, "didn't get latest")
  }

  test("consumer offset apis") {
    val group = "kcsuitegroup" + Random.nextInt(10000)

    val offset = Random.nextInt(10000)

    val set = kc.setConsumerOffsets(group, Map(topicAndPartition -> offset))
    assert(set.isRight, "didn't set consumer offsets")

    val get = kc.getConsumerOffsets(group, Set(topicAndPartition)).right.get
    assert(get(topicAndPartition) === offset, "didn't get consumer offsets")
  }
}
