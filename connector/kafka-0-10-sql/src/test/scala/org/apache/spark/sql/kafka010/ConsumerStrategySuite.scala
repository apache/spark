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

package org.apache.spark.sql.kafka010

import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito.mock

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.util.ResetSystemProperties

class ConsumerStrategySuite extends SparkFunSuite with ResetSystemProperties {
  private var testUtils: KafkaTestUtils = _

  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  protected def newTopic(prefix: String = "topic") = s"$prefix-${UUID.randomUUID().toString}"

  private def setSparkEnv(settings: Iterable[(String, String)]): Unit = {
    val conf = new SparkConf().setAll(settings)
    val env = mock(classOf[SparkEnv])
    doReturn(conf).when(env).conf
    SparkEnv.set(env)
  }

  private def adminProps = {
    Map[String, Object](
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress
    ).asJava
  }

  private def admin(strategy: ConsumerStrategy): Admin = {
    strategy.createAdmin(adminProps)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(Map.empty)
    testUtils.setup()
    setSparkEnv(Map.empty)
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    super.afterAll()
  }

  test("createAdmin must create admin properly") {
    val strategy = AssignStrategy(Array.empty)
    assert(strategy.createAdmin(adminProps) != null)
  }

  test("AssignStrategy.assignedTopicPartitions must give back all assigned") {
    val assignedTopic = newTopic()
    testUtils.createTopic(assignedTopic, partitions = 3)
    val otherExistingTopic = newTopic()
    testUtils.createTopic(otherExistingTopic, partitions = 2)

    val partitions = Array(
      new TopicPartition(assignedTopic, 0),
      new TopicPartition(assignedTopic, 2)
    )
    val strategy = AssignStrategy(partitions)
    assert(strategy.assignedTopicPartitions(admin(strategy)) === partitions.toSet)

    testUtils.deleteTopic(assignedTopic)
    testUtils.deleteTopic(otherExistingTopic)
  }

  test("AssignStrategy.assignedTopicPartitions must skip invalid partitions") {
    val assignedTopic = newTopic()
    testUtils.createTopic(assignedTopic, partitions = 1)

    val partitions = Array(new TopicPartition(assignedTopic, 1))
    val strategy = AssignStrategy(partitions)
    assert(strategy.assignedTopicPartitions(admin(strategy)) === Set.empty)

    testUtils.deleteTopic(assignedTopic)
  }

  test("SubscribeStrategy.assignedTopicPartitions must give back all assigned") {
    val subscribedTopic1 = newTopic()
    testUtils.createTopic(subscribedTopic1, partitions = 2)
    val subscribedTopic2 = newTopic()
    testUtils.createTopic(subscribedTopic2, partitions = 2)
    val otherExistingTopic = newTopic()
    testUtils.createTopic(otherExistingTopic, partitions = 2)

    val partitions = Set(
      new TopicPartition(subscribedTopic1, 0),
      new TopicPartition(subscribedTopic1, 1),
      new TopicPartition(subscribedTopic2, 0),
      new TopicPartition(subscribedTopic2, 1)
    )
    val strategy = SubscribeStrategy(Seq(subscribedTopic1, subscribedTopic2))
    assert(strategy.assignedTopicPartitions(admin(strategy)) === partitions)

    testUtils.deleteTopic(subscribedTopic1)
    testUtils.deleteTopic(subscribedTopic2)
    testUtils.deleteTopic(otherExistingTopic)
  }

  test("SubscribePatternStrategy.assignedTopicPartitions must give back all assigned") {
    val subscribePattern = "subscribePattern"
    val subscribedTopic1 = newTopic(subscribePattern)
    testUtils.createTopic(subscribedTopic1, partitions = 2)
    val subscribedTopic2 = newTopic(subscribePattern)
    testUtils.createTopic(subscribedTopic2, partitions = 2)
    val otherExistingTopic = newTopic("other")
    testUtils.createTopic(otherExistingTopic, partitions = 2)

    val partitions = Set(
      new TopicPartition(subscribedTopic1, 0),
      new TopicPartition(subscribedTopic1, 1),
      new TopicPartition(subscribedTopic2, 0),
      new TopicPartition(subscribedTopic2, 1)
    )
    val strategy = SubscribePatternStrategy(s"$subscribePattern.*")
    assert(strategy.assignedTopicPartitions(admin(strategy)) === partitions)

    testUtils.deleteTopic(subscribedTopic1)
    testUtils.deleteTopic(subscribedTopic2)
    testUtils.deleteTopic(otherExistingTopic)
  }
}
