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
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkFunSuite

class KafkaClusterSuite extends SparkFunSuite with BeforeAndAfterAll {
  private val topic = "new_kcsuitetopic" + Random.nextInt(10000)
  private val topicPartition = new TopicPartition(topic, 0)
  private var newKc: KafkaCluster[_, _] = null

  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeAll() {
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()

    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, Map("a" -> 1))
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaTestUtils.brokerAddress,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer")
    newKc = new KafkaCluster(kafkaParams)
  }

  override def afterAll() {
    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  test("leader offset apis") {
    val earliest = newKc.getEarliestOffsets(Set(topicPartition))
    assert(earliest(topicPartition) === 0, "didn't get earliest")

    val latest = newKc.getLatestOffsets(Set(topicPartition))
    assert(latest(topicPartition) === 1, "didn't get latest")
  }

}
