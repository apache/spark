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

import scala.util.Random

import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSQLContext


class KafkaSourceSuite extends StreamTest with SharedSQLContext {

  import testImplicits._

  private val topicId = new AtomicInteger(0)
  private var testUtils: KafkaTestUtils = _

  override val streamingTimeout = 10.seconds

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

  test("subscribing topic by name from latest offsets") {
    val topic = newTopic()
    testFromLatestOffsets(topic, "subscribe" -> topic)
  }

  test("subscribing topic by name from earliest offsets") {
    val topic = newTopic()
    testFromEarliestOffsets(topic, "subscribe" -> topic)
  }

  test("subscribing topic by pattern from latest offsets") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-suffix"
    testFromLatestOffsets(topic, "subscribePattern" -> s"$topicPrefix-.*")
  }

  test("subscribing topic by pattern from earliest offsets") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-suffix"
    testFromEarliestOffsets(topic, "subscribePattern" -> s"$topicPrefix-.*")
  }

  test("stress test with multiple topics and partitions") {
    val topics = (1 to 5).map(i => s"stress$i").toSet
    topics.foreach { topic =>
      testUtils.createTopic(topic, partitions = Random.nextInt(5) + 1)
      testUtils.sendMessages(topic, (101 to 105).map { _.toString }.toArray)
    }

      // Create Kafka source that reads from latest offset
    val kafka =
      spark.readStream
        .format(classOf[KafkaSourceProvider].getCanonicalName.stripSuffix("$"))
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("kafka.group.id", s"group-stress-test")
        .option("subscribe", topics.mkString(","))
        .load()
        .select("key", "value")
        .as[(Array[Byte], Array[Byte])]

    val mapped = kafka.map(kv => new String(kv._2).toInt + 1)

    runStressTest(
      mapped,
      d => AddKafkaData(topics, d: _*)(ensureDataInMultiplePartition = false),
      iterations = 50)
  }

  test("bad source options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark
          .readStream
          .format("kafka")
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase.contains(m.toLowerCase))
      }
    }

    // No strategy specified
    testBadOptions()("options must be specified", "subscribe", "subscribePattern")

    // Multiple strategies specified
    testBadOptions("subscribe" -> "t", "subscribePattern" -> "t.*")(
      "only one", "options can be specified")

    testBadOptions("subscribe" -> "")("no topics to subscribe")
    testBadOptions("subscribePattern" -> "")("pattern to subscribe is empty")
  }


  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def testFromLatestOffsets(topic: String, options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, Array("-1"))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.group.id", s"group-$topic")
      .option("kafka.auto.offset.reset", s"latest")
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load().select("key", "value").as[(Array[Byte], Array[Byte])]
    val mapped = kafka.map(kv => new String(kv._2).toInt + 1)

    testStream(mapped)(
      AddKafkaData(Set(topic), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4), // Should get the data back on recovery
      StopStream,
      AddKafkaData(Set(topic), 4, 5, 6), // Add data when stream is stopped
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7), // Should get the added data
      AddKafkaData(Set(topic), 7, 8),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9)
    )
  }

  private def testFromEarliestOffsets(topic: String, options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, (1 to 3).map { _.toString }.toArray)
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark.readStream
    reader
      .format(classOf[KafkaSourceProvider].getCanonicalName.stripSuffix("$"))
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.group.id", s"group-$topic")
      .option("kafka.auto.offset.reset", s"earliest")
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load().select("key", "value").as[(Array[Byte], Array[Byte])]
    val mapped = kafka.map(kv => new String(kv._2).toInt + 1)

    testStream(mapped)(
      AddKafkaData(Set(topic), 4, 5, 6), // Add data when stream is stopped
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      AddKafkaData(Set(topic), 7, 8),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9)
    )
  }

  case class AddKafkaData(topics: Set[String], data: Int*)
    (implicit ensureDataInMultiplePartition: Boolean = false) extends AddData {

    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active kafka source")

      val sources = query.get.logicalPlan.collect {
        case StreamingExecutionRelation(source, _) if source.isInstanceOf[KafkaSource] =>
          source.asInstanceOf[KafkaSource]
      }
      if (sources.isEmpty) {
        throw new Exception(
          "Could not find Kafka source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the Kafka source in the StreamExecution logical plan as there" +
            "are multiple Kafka sources:\n\t" + sources.mkString("\n\t"))
      }
      val kafkaSource = sources.head
      val topic = topics.toSeq(Random.nextInt(topics.size))
      val sentMetadata = testUtils.sendMessages(topic, data.map { _.toString }.toArray)

      def metadataToStr(m: (String, RecordMetadata)): String = {
        s"Sent ${m._1} to partition ${m._2.partition()}, offset ${m._2.offset()}"
      }
      // Verify that the test data gets inserted into multiple partitions
      if (ensureDataInMultiplePartition) {
        require(
          sentMetadata.groupBy(_._2.partition).size > 1,
          s"Added data does not test multiple partitions: ${sentMetadata.map(metadataToStr)}")
      }

      val offset = KafkaSourceOffset(testUtils.getLatestOffsets(topics))
      logInfo(s"Added data, expected offset $offset")
      (kafkaSource, offset)
    }
  }
}
