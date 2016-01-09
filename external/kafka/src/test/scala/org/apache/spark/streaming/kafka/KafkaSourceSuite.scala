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

import scala.util.Try

import kafka.common.TopicAndPartition
import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.StreamTest
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.streaming.OffsetSuite
import org.apache.spark.sql.test.SharedSQLContext


class KafkaSourceSuite extends StreamTest with SharedSQLContext with OffsetSuite {

  import testImplicits._

  private var testUtils: KafkaTestUtils = _
  private var kafkaParams: Map[String, String] = _

  override val streamingTimout = 10.seconds

  case class AddKafkaData(
      kafkaSource: KafkaSource,
      topic: String, data: Int*)(implicit multiPartitionCheck: Boolean = true) extends AddData {

    override def addData(): Offset = {
      val sentMetadata = testUtils.sendMessages(topic, data.map{ _.toString}.toArray)
      val latestOffsetMap = sentMetadata
        .groupBy { _._2.partition }
        .map { case (partition, msgAndMetadata) =>
          val maxOffsetInPartition = msgAndMetadata.map(_._2).maxBy(_.offset).offset()
          (TopicAndPartition(topic, partition), maxOffsetInPartition + 1)
        }

      def metadataToStr(m: (String, RecordMetadata)): String = {
        s"Sent ${m._1} to partition ${m._2.partition()}, offset ${m._2.offset()}"
      }

      assert(latestOffsetMap.size > 1,
        s"Added data does not test multiple partitions: " + sentMetadata.map(metadataToStr))

      // Expected offset to ensure this data is read is last offset of this data + 1
      val offset = KafkaSourceOffset(latestOffsetMap)
      logInfo(s"Added data, expected offset $offset")
      offset
    }

    override def source: Source = kafkaSource
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
    kafkaParams = Map("metadata.broker.list" -> testUtils.brokerAddress)
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  test("basic receiving from latest offset with 1 topic and 2 partitions") {
    val topic = "topic1"
    testUtils.createTopic(topic, partitions = 2)

    // Add data in multiple rounds to the same topic and test whether
    for (i <- 0 until 5) {
      logInfo(s"Round $i")

      // Create Kafka source that reads from latest offset
      val kafkaSource = KafkaSource(Set(topic), kafkaParams)

      val mapped =
        kafkaSource
          .toDS()
          .map[Int]((kv: (Array[Byte], Array[Byte])) => new String(kv._2).toInt + 1)

      logInfo(s"Initial offsets: ${kafkaSource.initialOffsets}")

      testStream(mapped)(
        AddKafkaData(kafkaSource, topic, 1, 2, 3),
        CheckAnswer(2, 3, 4),
        StopStream ,
        DropBatches(1),         // Lose last batch in the sink
        StartStream,
        CheckAnswer(2, 3, 4),   // Should get the data back on recovery
        StopStream,
        AddKafkaData(kafkaSource, topic, 4, 5, 6),    // Add data when stream is stopped
        StartStream,
        CheckAnswer(2, 3, 4, 5, 6, 7),                // Should get the added data
        AddKafkaData(kafkaSource, topic, 7, 8),
        CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9)
      )
    }
  }

  compare(
    one = KafkaSourceOffset(("t", 0, 1L)),
    two = KafkaSourceOffset(("t", 0, 2L)))

  compare(
    one = KafkaSourceOffset(("t", 0, 1L), ("t", 1, 0L)),
    two = KafkaSourceOffset(("t", 0, 2L), ("t", 1, 1L)))

  compare(
    one = KafkaSourceOffset(("t", 0, 1L), ("T", 0, 0L)),
    two = KafkaSourceOffset(("t", 0, 2L), ("T", 0, 1L)))

  compare(
    one = KafkaSourceOffset(("t", 0, 1L)),
    two = KafkaSourceOffset(("t", 0, 2L), ("t", 1, 1L)))

  compareInvalid(
    one = KafkaSourceOffset(("t", 1, 1L)),
    two = KafkaSourceOffset(("t", 0, 2L)))

  compareInvalid(
    one = KafkaSourceOffset(("t", 0, 1L)),
    two = KafkaSourceOffset(("T", 0, 2L)))
}
