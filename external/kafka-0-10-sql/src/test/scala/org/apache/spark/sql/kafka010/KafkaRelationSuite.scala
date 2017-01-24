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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext

class KafkaRelationSuite extends SparkFunSuite with BeforeAndAfter with SharedSQLContext {

  import testImplicits._

  private val topicId = new AtomicInteger(0)

  private var testUtils: KafkaTestUtils = _

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def assignString(topic: String, partitions: Iterable[Int]): String = {
    JsonUtils.partitions(partitions.map(p => new TopicPartition(topic, p)))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  test("batch processing earliest to latest") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("20"), Some(2))

    // Specify explicit earliest and latest offset values
    var reader = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
    assert(reader.count() === 21)
    testUtils.sendMessages(topic, (21 to 29).map(_.toString).toArray, Some(2))
    assert(reader.count() === 30)


    // Implicit offset values, should default to earliest and latest
    reader = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .load()
    assert(reader.count() === 30)

    // Test explicitly specified offsets
    val startPartitionOffsets = Map(
      new TopicPartition(topic, 0) -> -2L,
      new TopicPartition(topic, 1) -> -2L,
      new TopicPartition(topic, 2) -> 0L
    )
    val startingOffsets = JsonUtils.partitionOffsets(startPartitionOffsets)

    val endPartitionOffsets = Map(
      new TopicPartition(topic, 0) -> -1L,
      new TopicPartition(topic, 1) -> -1L,
      new TopicPartition(topic, 2) -> 10L
    )
    val endingOffsets = JsonUtils.partitionOffsets(endPartitionOffsets)
    reader = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("endingOffsets", endingOffsets)
      .load()
    assert(reader.count() === 30)
    testUtils.sendMessages(topic, (30 to 39).map(_.toString).toArray, Some(2))
    assert(reader.count() === 30) // static offset partition 2, nothing should change
    testUtils.sendMessages(topic, (30 to 39).map(_.toString).toArray, Some(1))
    assert(reader.count() === 40) // latest offset partition 1, should change
  }

  test("bad source options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark
          .read
          .format("kafka")
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase.contains(m.toLowerCase))
      }
    }

    // Specifying an ending offset as the starting point
    testBadOptions("startingOffsets" -> "latest")("starting relation offset can't be latest")

    // Now do it with an explicit json offset
    val startPartitionOffsets = Map(
      new TopicPartition("t", 0) -> -1L // specify latest
    )
    val startingOffsets = JsonUtils.partitionOffsets(startPartitionOffsets)
    testBadOptions("subscribe" -> "t", "startingOffsets" -> startingOffsets)(
      "startingoffsets for t-0 can't be latest")


    // Make sure we catch ending offsets that indicate earliest
    testBadOptions("endingOffsets" -> "earliest")("ending relation offset can't be earliest")

    val endPartitionOffsets = Map(
      new TopicPartition("t", 0) -> -2L // specify earliest
    )
    val endingOffsets = JsonUtils.partitionOffsets(endPartitionOffsets)
    testBadOptions("subscribe" -> "t", "endingOffsets" -> endingOffsets)(
      "ending offset for t-0 can't be earliest")

    // No strategy specified
    testBadOptions()("options must be specified", "subscribe", "subscribePattern")

    // Multiple strategies specified
    testBadOptions("subscribe" -> "t", "subscribePattern" -> "t.*")(
      "only one", "options can be specified")

    testBadOptions("subscribe" -> "t", "assign" -> """{"a":[0]}""")(
      "only one", "options can be specified")

    testBadOptions("assign" -> "")("no topicpartitions to assign")
    testBadOptions("subscribe" -> "")("no topics to subscribe")
    testBadOptions("subscribePattern" -> "")("pattern to subscribe is empty")
  }
}
