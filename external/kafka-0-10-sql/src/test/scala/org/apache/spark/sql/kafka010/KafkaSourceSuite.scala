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
import org.scalatest.BeforeAndAfter
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSQLContext


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
    // we don't know which data should be fetched when `startingOffset` is latest.
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

    val kafka = reader.load().select("key", "value").as[(Array[Byte], Array[Byte])]
    val mapped = kafka.map(kv => new String(kv._2).toInt + 1)

    testStream(mapped)(
      StopStream
    )
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

    val kafka = reader.load().select("key", "value").as[(Array[Byte], Array[Byte])]
    val mapped = kafka.map(kv => new String(kv._2).toInt + 1)

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

    // only earliest and latest is supported
    testUnsupportedConfig("kafka.auto.offset.reset", "none")
    testUnsupportedConfig("kafka.auto.offset.reset", "someValue")
  }

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def testFromLatestOffsets(topic: String, options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, Array("-1"))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark
      .readStream
      .format("kafka")
      .option("startingOffset", s"latest")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load().select("key", "value").as[(Array[Byte], Array[Byte])]
    val mapped = kafka.map(kv => new String(kv._2).toInt + 1)

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
        testUtils.addPartitions(topic, 10)
        true
      },
      AddKafkaData(Set(topic), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromEarliestOffsets(topic: String, options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, (1 to 3).map { _.toString }.toArray)
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark.readStream
    reader
      .format(classOf[KafkaSourceProvider].getCanonicalName.stripSuffix("$"))
      .option("startingOffset", s"earliest")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
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
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AssertOnQuery("Add partitions") { query: StreamExecution =>
        testUtils.addPartitions(topic, 10)
        true
      },
      AddKafkaData(Set(topic), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }
}


class KafkaSourceStressSuite extends KafkaSourceTest with BeforeAndAfter {

  import testImplicits._

  val topicId = new AtomicInteger(1)

  @volatile var topics: Seq[String] = (1 to 5).map(_ => newStressTopic)

  def newStressTopic: String = s"stress${topicId.getAndIncrement()}"

  private def nextInt(start: Int, end: Int): Int = {
    start + Random.nextInt(start + end - 1)
  }

  after {
    for (topic <- testUtils.getAllTopicsAndPartitionSize().toMap.keys) {
      testUtils.deleteTopic(topic)
    }
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
        .load()
        .select("key", "value")
        .as[(Array[Byte], Array[Byte])]

    val mapped = kafka.map(kv => new String(kv._2).toInt + 1)

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
