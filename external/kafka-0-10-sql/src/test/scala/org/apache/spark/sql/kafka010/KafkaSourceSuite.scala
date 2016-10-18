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

import java.io.StringWriter
import java.util.concurrent.atomic.AtomicInteger

import scala.util.Random

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.{ ProcessingTime, StreamTest }
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.ManualClock

abstract class KafkaSourceTest extends StreamTest with SharedSQLContext {

  protected var testUtils: KafkaTestUtils = _

  override val streamingTimeout = 30.seconds

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

  protected def makeSureGetOffsetCalled = AssertOnQuery { q =>
    // Because KafkaSource's initialPartitionOffsets is set lazily, we need to make sure
    // its "getOffset" is called before pushing any data. Otherwise, because of the race contion,
    // we don't know which data should be fetched when `startingOffsets` is latest.
    q.processAllAvailable()
    true
  }

  /**
   * Add data to Kafka.
   *
   * `topicAction` can be used to run actions for each topic before inserting data.
   */
  case class AddKafkaData(topics: Set[String], data: Int*)
    (implicit ensureDataInMultiplePartition: Boolean = false,
      concurrent: Boolean = false,
      message: String = "",
      topicAction: (String, Option[Int]) => Unit = (_, _) => {}) extends AddData {

    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      if (query.get.isActive) {
        // Make sure no Spark job is running when deleting a topic
        query.get.processAllAvailable()
      }

      val existingTopics = testUtils.getAllTopicsAndPartitionSize().toMap
      val newTopics = topics.diff(existingTopics.keySet)
      for (newTopic <- newTopics) {
        topicAction(newTopic, None)
      }
      for (existingTopicPartitions <- existingTopics) {
        topicAction(existingTopicPartitions._1, Some(existingTopicPartitions._2))
      }

      // Read all topics again in case some topics are delete.
      val allTopics = testUtils.getAllTopicsAndPartitionSize().toMap.keys
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

    override def toString: String =
      s"AddKafkaData(topics = $topics, data = $data, message = $message)"
  }
}


class KafkaSourceSuite extends KafkaSourceTest {

  import testImplicits._

  private val topicId = new AtomicInteger(0)

  test("maxOffsetsPerTrigger") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (100 to 200).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 20).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("1"), Some(2))

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("maxOffsetsPerTrigger", 10)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    val clock = new ManualClock
    testStream(mapped)(
      StartStream(ProcessingTime(100), clock),
      AdvanceManualClock(100),
      // 1 from smallest, 1 from middle, 8 from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107),
      AdvanceManualClock(100),
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111, 112, 113, 114, 115, 116
      ),
      StopStream
    )
  }

  test("cannot stop Kafka stream") {
    val topic = newTopic()
    testUtils.createTopic(newTopic(), partitions = 5)
    testUtils.sendMessages(topic, (101 to 105).map { _.toString }.toArray)

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("subscribePattern", s"topic-.*")

    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = kafka.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      StopStream
    )
  }

  test("assign from latest offsets") {
    val topic = newTopic()
    testFromLatestOffsets(topic, false, "assign" -> assignString(topic, 0 to 4))
  }

  test("assign from earliest offsets") {
    val topic = newTopic()
    testFromEarliestOffsets(topic, false, "assign" -> assignString(topic, 0 to 4))
  }

  test("assign from specific offsets") {
    val topic = newTopic()
    testFromSpecificOffsets(topic, "assign" -> assignString(topic, 0 to 4))
  }

  test("subscribing topic by name from latest offsets") {
    val topic = newTopic()
    testFromLatestOffsets(topic, true, "subscribe" -> topic)
  }

  test("subscribing topic by name from earliest offsets") {
    val topic = newTopic()
    testFromEarliestOffsets(topic, true, "subscribe" -> topic)
  }

  test("subscribing topic by name from specific offsets") {
    val topic = newTopic()
    testFromSpecificOffsets(topic, "subscribe" -> topic)
  }

  test("subscribing topic by pattern from latest offsets") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-suffix"
    testFromLatestOffsets(topic, true, "subscribePattern" -> s"$topicPrefix-.*")
  }

  test("subscribing topic by pattern from earliest offsets") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-suffix"
    testFromEarliestOffsets(topic, true, "subscribePattern" -> s"$topicPrefix-.*")
  }

  test("subscribing topic by pattern from specific offsets") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-suffix"
    testFromSpecificOffsets(topic, "subscribePattern" -> s"$topicPrefix-.*")
  }

  test("subscribing topic by pattern with topic deletions") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-seems"
    val topic2 = topicPrefix + "-bad"
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, Array("-1"))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("subscribePattern", s"$topicPrefix-.*")
      .option("failOnDataLoss", "false")

    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = kafka.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddKafkaData(Set(topic), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      Assert {
        testUtils.deleteTopic(topic)
        testUtils.createTopic(topic2, partitions = 5)
        true
      },
      AddKafkaData(Set(topic2), 4, 5, 6),
      CheckAnswer(2, 3, 4, 5, 6, 7)
    )
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

    testBadOptions("subscribe" -> "t", "assign" -> """{"a":[0]}""")(
      "only one", "options can be specified")

    testBadOptions("assign" -> "")("no topicpartitions to assign")
    testBadOptions("subscribe" -> "")("no topics to subscribe")
    testBadOptions("subscribePattern" -> "")("pattern to subscribe is empty")
  }

  test("unsupported kafka configs") {
    def testUnsupportedConfig(key: String, value: String = "someValue"): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark
          .readStream
          .format("kafka")
          .option("subscribe", "topic")
          .option("kafka.bootstrap.servers", "somehost")
          .option(s"$key", value)
        reader.load()
      }
      assert(ex.getMessage.toLowerCase.contains("not supported"))
    }

    testUnsupportedConfig("kafka.group.id")
    testUnsupportedConfig("kafka.auto.offset.reset")
    testUnsupportedConfig("kafka.enable.auto.commit")
    testUnsupportedConfig("kafka.interceptor.classes")
    testUnsupportedConfig("kafka.key.deserializer")
    testUnsupportedConfig("kafka.value.deserializer")

    testUnsupportedConfig("kafka.auto.offset.reset", "none")
    testUnsupportedConfig("kafka.auto.offset.reset", "someValue")
    testUnsupportedConfig("kafka.auto.offset.reset", "earliest")
    testUnsupportedConfig("kafka.auto.offset.reset", "latest")
  }

  test("input row metrics") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, Array("-1"))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val kafka = spark
      .readStream
      .format("kafka")
      .option("subscribe", topic)
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val mapped = kafka.map(kv => kv._2.toInt + 1)
    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddKafkaData(Set(topic), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      AssertOnLastQueryStatus { status =>
        assert(status.triggerDetails.get("numRows.input.total").toInt > 0)
        assert(status.sourceStatuses(0).processingRate > 0.0)
      }
    )
  }

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def assignString(topic: String, partitions: Iterable[Int]): String = {
    val writer = new StringWriter
    JsonUtils.partitions(
      partitions.map(p => new TopicPartition(topic, p)),
      writer)
    writer.toString
  }

  private def testFromSpecificOffsets(topic: String, options: (String, String)*): Unit = {
    val writer = new StringWriter
    val partitionOffsets = Map(
      new TopicPartition(topic, 0) -> -2L,
      new TopicPartition(topic, 1) -> -1L,
      new TopicPartition(topic, 2) -> 0L,
      new TopicPartition(topic, 3) -> 1L,
      new TopicPartition(topic, 4) -> 2L
    )
    JsonUtils.partitionOffsets(partitionOffsets, writer)
    val startingOffsets = writer.toString

    testUtils.createTopic(topic, partitions = 5)
    // part 0 starts at earliest, these should all be seen
    testUtils.sendMessages(topic, Array(-20, -21, -22).map(_.toString), Some(0))
    // part 1 starts at latest, these should all be skipped
    testUtils.sendMessages(topic, Array(-10, -11, -12).map(_.toString), Some(1))
    // part 2 starts at 0, these should all be seen
    testUtils.sendMessages(topic, Array(0, 1, 2).map(_.toString), Some(2))
    // part 3 starts at 1, first should be skipped
    testUtils.sendMessages(topic, Array(10, 11, 12).map(_.toString), Some(3))
    // part 4 starts at 2, first and second should be skipped
    testUtils.sendMessages(topic, Array(20, 21, 22).map(_.toString), Some(4))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark
      .readStream
      .format("kafka")
      .option("startingOffsets", startingOffsets)
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22),
      StopStream,
      StartStream(),
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22), // Should get the data back on recovery
      AddKafkaData(Set(topic), 30, 31, 32, 33, 34)(ensureDataInMultiplePartition = true),
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22, 30, 31, 32, 33, 34),
      StopStream
    )
  }

  private def testFromLatestOffsets(
      topic: String,
      addPartitions: Boolean,
      options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, Array("-1"))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark
      .readStream
      .format("kafka")
      .option("startingOffsets", s"latest")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = kafka.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
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
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AssertOnQuery("Add partitions") { query: StreamExecution =>
        if (addPartitions) {
          testUtils.addPartitions(topic, 10)
        }
        true
      },
      AddKafkaData(Set(topic), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromEarliestOffsets(
      topic: String,
      addPartitions: Boolean,
      options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, (1 to 3).map { _.toString }.toArray)
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark.readStream
    reader
      .format(classOf[KafkaSourceProvider].getCanonicalName.stripSuffix("$"))
      .option("startingOffsets", s"earliest")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = kafka.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      AddKafkaData(Set(topic), 4, 5, 6), // Add data when stream is stopped
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      AddKafkaData(Set(topic), 7, 8),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AssertOnQuery("Add partitions") { query: StreamExecution =>
        if (addPartitions) {
          testUtils.addPartitions(topic, 10)
        }
        true
      },
      AddKafkaData(Set(topic), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }
}


class KafkaSourceStressSuite extends KafkaSourceTest {

  import testImplicits._

  val topicId = new AtomicInteger(1)

  @volatile var topics: Seq[String] = (1 to 5).map(_ => newStressTopic)

  def newStressTopic: String = s"stress${topicId.getAndIncrement()}"

  private def nextInt(start: Int, end: Int): Int = {
    start + Random.nextInt(start + end - 1)
  }

  test("stress test with multiple topics and partitions")  {
    topics.foreach { topic =>
      testUtils.createTopic(topic, partitions = nextInt(1, 6))
      testUtils.sendMessages(topic, (101 to 105).map { _.toString }.toArray)
    }

    // Create Kafka source that reads from latest offset
    val kafka =
      spark.readStream
        .format(classOf[KafkaSourceProvider].getCanonicalName.stripSuffix("$"))
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("kafka.metadata.max.age.ms", "1")
        .option("subscribePattern", "stress.*")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]

    val mapped = kafka.map(kv => kv._2.toInt + 1)

    runStressTest(
      mapped,
      Seq(makeSureGetOffsetCalled),
      (d, running) => {
        Random.nextInt(5) match {
          case 0 => // Add a new topic
            topics = topics ++ Seq(newStressTopic)
            AddKafkaData(topics.toSet, d: _*)(message = s"Add topic $newStressTopic",
              topicAction = (topic, partition) => {
                if (partition.isEmpty) {
                  testUtils.createTopic(topic, partitions = nextInt(1, 6))
                }
              })
          case 1 if running =>
            // Only delete a topic when the query is running. Otherwise, we may lost data and
            // cannot check the correctness.
            val deletedTopic = topics(Random.nextInt(topics.size))
            if (deletedTopic != topics.head) {
              topics = topics.filterNot(_ == deletedTopic)
            }
            AddKafkaData(topics.toSet, d: _*)(message = s"Delete topic $deletedTopic",
              topicAction = (topic, partition) => {
                // Never remove the first topic to make sure we have at least one topic
                if (topic == deletedTopic && deletedTopic != topics.head) {
                  testUtils.deleteTopic(deletedTopic)
                }
              })
          case 2 => // Add new partitions
            AddKafkaData(topics.toSet, d: _*)(message = "Add partitiosn",
              topicAction = (topic, partition) => {
                testUtils.addPartitions(topic, partition.get + nextInt(1, 6))
              })
          case _ => // Just add new data
            AddKafkaData(topics.toSet, d: _*)
        }
      },
      iterations = 50)
  }
}
