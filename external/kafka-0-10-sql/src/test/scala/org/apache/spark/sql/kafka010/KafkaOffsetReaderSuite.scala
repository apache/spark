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
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.kafka010.KafkaOffsetRangeLimit.{EARLIEST, LATEST}
import org.apache.spark.sql.test.SharedSparkSession

class KafkaOffsetReaderSuite extends QueryTest with SharedSparkSession with KafkaTest {

  protected var testUtils: KafkaTestUtils = _

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    try {
      if (testUtils != null) {
        testUtils.teardown()
        testUtils = null
      }
    } finally {
      super.afterAll()
    }
  }

  private def createKafkaReader(topic: String, minPartitions: Option[Int]): KafkaOffsetReader = {
    new KafkaOffsetReader(
      SubscribeStrategy(Seq(topic)),
      org.apache.spark.sql.kafka010.KafkaSourceProvider.kafkaParamsForDriver(
        Map(
        "bootstrap.servers" ->
         testUtils.brokerAddress
      )),
      CaseInsensitiveMap(
        minPartitions.map(m => Map("minPartitions" -> m.toString)).getOrElse(Map.empty)),
      UUID.randomUUID().toString
    )
  }

  test("SPARK-30656: getOffsetRangesFromUnresolvedOffsets - using specific offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, (0 until 10).map(_.toString).toArray, Some(0))
    val tp = new TopicPartition(topic, 0)
    val reader = createKafkaReader(topic, minPartitions = Some(3))
    val startingOffsets = SpecificOffsetRangeLimit(Map(tp -> 1))
    val endingOffsets = SpecificOffsetRangeLimit(Map(tp -> 4))
    val offsetRanges = reader.getOffsetRangesFromUnresolvedOffsets(startingOffsets, endingOffsets)
    assert(offsetRanges === Seq(
      KafkaOffsetRange(tp, 1, 2, None),
      KafkaOffsetRange(tp, 2, 3, None),
      KafkaOffsetRange(tp, 3, 4, None)))
  }

  test("SPARK-30656: getOffsetRangesFromUnresolvedOffsets - using special offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, (0 until 4).map(_.toString).toArray, Some(0))
    val tp = new TopicPartition(topic, 0)
    val reader = createKafkaReader(topic, minPartitions = Some(3))
    val startingOffsets = EarliestOffsetRangeLimit
    val endingOffsets = LatestOffsetRangeLimit
    val offsetRanges = reader.getOffsetRangesFromUnresolvedOffsets(startingOffsets, endingOffsets)
    assert(offsetRanges === Seq(
      KafkaOffsetRange(tp, EARLIEST, 1, None),
      KafkaOffsetRange(tp, 1, 2, None),
      KafkaOffsetRange(tp, 2, LATEST, None)))
  }

  test("SPARK-30656: getOffsetRangesFromUnresolvedOffsets - multiple topic partitions") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)
    testUtils.sendMessages(topic, (0 until 100).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (0 until 4).map(_.toString).toArray, Some(1))
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val reader = createKafkaReader(topic, minPartitions = Some(3))

    val startingOffsets = SpecificOffsetRangeLimit(Map(tp1 -> EARLIEST, tp2 -> EARLIEST))
    val endingOffsets = SpecificOffsetRangeLimit(Map(tp1 -> LATEST, tp2 -> 3))
    val offsetRanges = reader.getOffsetRangesFromUnresolvedOffsets(startingOffsets, endingOffsets)
    assert(offsetRanges === Seq(
      KafkaOffsetRange(tp2, EARLIEST, 3, None),
      KafkaOffsetRange(tp1, EARLIEST, 33, None),
      KafkaOffsetRange(tp1, 33, 66, None),
      KafkaOffsetRange(tp1, 66, LATEST, None)))
  }

  test("SPARK-30656: getOffsetRangesFromResolvedOffsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)
    testUtils.sendMessages(topic, (0 until 100).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (0 until 4).map(_.toString).toArray, Some(1))
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val reader = createKafkaReader(topic, minPartitions = Some(3))

    val fromPartitionOffsets = Map(tp1 -> 0L, tp2 -> 0L)
    val untilPartitionOffsets = Map(tp1 -> 100L, tp2 -> 3L)
    val offsetRanges = reader.getOffsetRangesFromResolvedOffsets(
      fromPartitionOffsets,
      untilPartitionOffsets,
      _ => {})
    assert(offsetRanges === Seq(
      KafkaOffsetRange(tp1, 0, 33, None),
      KafkaOffsetRange(tp1, 33, 66, None),
      KafkaOffsetRange(tp1, 66, 100, None),
      KafkaOffsetRange(tp2, 0, 3, None)))
  }
}
