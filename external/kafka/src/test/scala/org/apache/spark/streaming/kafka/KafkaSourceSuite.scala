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

import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.StreamTest
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.test.SharedSQLContext


class KafkaSourceSuite extends StreamTest with SharedSQLContext {

  import testImplicits._

  private var testUtils: KafkaTestUtils = _
  private var kafkaParams: Map[String, String] = _

  override val streamingTimout = 10.seconds

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
          .map((kv: (Array[Byte], Array[Byte])) => new String(kv._2).toInt + 1)

      logInfo(s"Initial offsets: ${kafkaSource.initialOffsets}")

      testStream(mapped)(
        AddKafkaData(kafkaSource, Set(topic), 1, 2, 3),
        CheckAnswer(2, 3, 4),
        StopStream ,
        DropBatches(1),         // Lose last batch in the sink
        StartStream,
        CheckAnswer(2, 3, 4),   // Should get the data back on recovery
        StopStream,
        AddKafkaData(kafkaSource, Set(topic), 4, 5, 6),    // Add data when stream is stopped
        StartStream,
        CheckAnswer(2, 3, 4, 5, 6, 7),                // Should get the added data
        AddKafkaData(kafkaSource, Set(topic), 7, 8),
        CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9)
      )
    }
  }

  test("stress test with multiple topics and partitions") {
    val topics = (1 to 5).map (i => s"stress$i").toSet
    topics.foreach(testUtils.createTopic(_, partitions = Random.nextInt(10)))

    for (i <- 1 to 5) {
      // Create Kafka source that reads from latest offset
      val kafkaSource = KafkaSource(topics, kafkaParams)

      val mapped =
        kafkaSource
          .toDS()
          .map(r => new String(r._2).toInt + 1)

      createStressTest(
        mapped,
        d => AddKafkaData(kafkaSource, topics, d: _*)(ensureDataInMultiplePartition = false),
        iterations = 30)
    }
  }


  case class AddKafkaData(
      kafkaSource: KafkaSource,
      topics: Set[String], data: Int*)(implicit ensureDataInMultiplePartition: Boolean = true)
    extends AddData {

    override def addData(): Offset = {
      val topic = topics.toSeq(Random.nextInt(topics.size))
      val sentMetadata = testUtils.sendMessages(topic, data.map{ _.toString}.toArray)

      def metadataToStr(m: (String, RecordMetadata)): String = {
        s"Sent ${m._1} to partition ${m._2.partition()}, offset ${m._2.offset()}"
      }

      // Verify that the test data gets inserted into multiple partitions
      if (ensureDataInMultiplePartition) {
        require(sentMetadata.groupBy(_._2.partition).size > 1,
          s"Added data does not test multiple partitions: ${sentMetadata.map(metadataToStr)}")
      }

      val offset = KafkaSourceOffset(testUtils.getLatestOffsets(topics))
      logInfo(s"Added data, expected offset $offset")
      offset
    }

    override def source: Source = kafkaSource
  }
}
