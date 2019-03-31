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

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class KafkaRelationSuite extends QueryTest with SharedSQLContext with KafkaTest {

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
    try {
      if (testUtils != null) {
        testUtils.teardown()
        testUtils = null
      }
    } finally {
      super.afterAll()
    }
  }

  private def createDF(
      topic: String,
      withOptions: Map[String, String] = Map.empty[String, String],
      brokerAddress: Option[String] = None) = {
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers",
        brokerAddress.getOrElse(testUtils.brokerAddress))
      .option("subscribe", topic)
    withOptions.foreach {
      case (key, value) => df.option(key, value)
    }
    df.load().selectExpr("CAST(value AS STRING)")
  }


  test("explicit earliest to latest offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("20"), Some(2))

    // Specify explicit earliest and latest offset values
    val df = createDF(topic,
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    checkAnswer(df, (0 to 20).map(_.toString).toDF)

    // "latest" should late bind to the current (latest) offset in the df
    testUtils.sendMessages(topic, (21 to 29).map(_.toString).toArray, Some(2))
    checkAnswer(df, (0 to 29).map(_.toString).toDF)
  }

  test("default starting and ending offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("20"), Some(2))

    // Implicit offset values, should default to earliest and latest
    val df = createDF(topic)
    // Test that we default to "earliest" and "latest"
    checkAnswer(df, (0 to 20).map(_.toString).toDF)
  }

  test("explicit offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("20"), Some(2))

    // Test explicitly specified offsets
    val startPartitionOffsets = Map(
      new TopicPartition(topic, 0) -> -2L, // -2 => earliest
      new TopicPartition(topic, 1) -> -2L,
      new TopicPartition(topic, 2) -> 0L   // explicit earliest
    )
    val startingOffsets = JsonUtils.partitionOffsets(startPartitionOffsets)

    val endPartitionOffsets = Map(
      new TopicPartition(topic, 0) -> -1L, // -1 => latest
      new TopicPartition(topic, 1) -> -1L,
      new TopicPartition(topic, 2) -> 1L  // explicit offset happens to = the latest
    )
    val endingOffsets = JsonUtils.partitionOffsets(endPartitionOffsets)
    val df = createDF(topic,
        withOptions = Map("startingOffsets" -> startingOffsets, "endingOffsets" -> endingOffsets))
    checkAnswer(df, (0 to 20).map(_.toString).toDF)

    // static offset partition 2, nothing should change
    testUtils.sendMessages(topic, (31 to 39).map(_.toString).toArray, Some(2))
    checkAnswer(df, (0 to 20).map(_.toString).toDF)

    // latest offset partition 1, should change
    testUtils.sendMessages(topic, (21 to 30).map(_.toString).toArray, Some(1))
    checkAnswer(df, (0 to 30).map(_.toString).toDF)
  }

  test("reuse same dataframe in query") {
    // This test ensures that we do not cache the Kafka Consumer in KafkaRelation
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, (0 to 10).map(_.toString).toArray, Some(0))

    // Specify explicit earliest and latest offset values
    val df = createDF(topic,
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    checkAnswer(df.union(df), ((0 to 10) ++ (0 to 10)).map(_.toString).toDF)
  }

  test("test late binding start offsets") {
    // Kafka fails to remove the logs on Windows. See KAFKA-1194.
    assume(!Utils.isWindows)

    var kafkaUtils: KafkaTestUtils = null
    try {
      /**
       * The following settings will ensure that all log entries
       * are removed following a call to cleanupLogs
       */
      val brokerProps = Map[String, Object](
        "log.retention.bytes" -> 1.asInstanceOf[AnyRef], // retain nothing
        "log.retention.ms" -> 1.asInstanceOf[AnyRef]     // no wait time
      )
      kafkaUtils = new KafkaTestUtils(withBrokerProps = brokerProps)
      kafkaUtils.setup()

      val topic = newTopic()
      kafkaUtils.createTopic(topic, partitions = 1)
      kafkaUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
      // Specify explicit earliest and latest offset values
      val df = createDF(topic,
        withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"),
        Some(kafkaUtils.brokerAddress))
      checkAnswer(df, (0 to 9).map(_.toString).toDF)
      // Blow away current set of messages.
      kafkaUtils.cleanupLogs()
      // Add some more data, but do not call cleanup
      kafkaUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(0))
      // Ensure that we late bind to the new starting position
      checkAnswer(df, (10 to 19).map(_.toString).toDF)
    } finally {
      if (kafkaUtils != null) {
        kafkaUtils.teardown()
      }
    }
  }

  test("bad batch query options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark
          .read
          .format("kafka")
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    // Specifying an ending offset as the starting point
    testBadOptions("startingOffsets" -> "latest")("starting offset can't be latest " +
      "for batch queries on Kafka")

    // Now do it with an explicit json start offset indicating latest
    val startPartitionOffsets = Map( new TopicPartition("t", 0) -> -1L)
    val startingOffsets = JsonUtils.partitionOffsets(startPartitionOffsets)
    testBadOptions("subscribe" -> "t", "startingOffsets" -> startingOffsets)(
      "startingOffsets for t-0 can't be latest for batch queries on Kafka")


    // Make sure we catch ending offsets that indicate earliest
    testBadOptions("endingOffsets" -> "earliest")("ending offset can't be earliest " +
      "for batch queries on Kafka")

    // Make sure we catch ending offsets that indicating earliest
    val endPartitionOffsets = Map(new TopicPartition("t", 0) -> -2L)
    val endingOffsets = JsonUtils.partitionOffsets(endPartitionOffsets)
    testBadOptions("subscribe" -> "t", "endingOffsets" -> endingOffsets)(
      "ending offset for t-0 can't be earliest for batch queries on Kafka")

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

  test("allow group.id overriding") {
    // Tests code path KafkaSourceProvider.createRelation(.)
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (1 to 10).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (11 to 20).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, (21 to 30).map(_.toString).toArray, Some(2))

    val customGroupId = "id-" + Random.nextInt()
    val df = createDF(topic, withOptions = Map("kafka.group.id" -> customGroupId))
    checkAnswer(df, (1 to 30).map(_.toString).toDF())

    val consumerGroups = testUtils.listConsumerGroups()
    val validGroups = consumerGroups.valid().get()
    val validGroupsId = validGroups.asScala.map(_.groupId())
    assert(validGroupsId.exists(_ === customGroupId), "Valid consumer groups don't " +
      s"contain the expected group id - Valid consumer groups: $validGroupsId / " +
      s"expected group id: $customGroupId")
  }

  test("read Kafka transactional messages: read_committed") {
    val topic = newTopic()
    testUtils.createTopic(topic)
    testUtils.withTranscationalProducer { producer =>
      val df = spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("kafka.isolation.level", "read_committed")
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST(value AS STRING)")

      producer.beginTransaction()
      (1 to 5).foreach { i =>
        producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
      }

      // Should not read any messages before they are committed
      assert(df.isEmpty)

      producer.commitTransaction()

      // Should read all committed messages
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 6)
      checkAnswer(df, (1 to 5).map(_.toString).toDF)

      producer.beginTransaction()
      (6 to 10).foreach { i =>
        producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
      }
      producer.abortTransaction()

      // Should not read aborted messages
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 12)
      checkAnswer(df, (1 to 5).map(_.toString).toDF)

      producer.beginTransaction()
      (11 to 15).foreach { i =>
        producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
      }
      producer.commitTransaction()

      // Should skip aborted messages and read new committed ones.
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 18)
      checkAnswer(df, ((1 to 5) ++ (11 to 15)).map(_.toString).toDF)
    }
  }

  test("read Kafka transactional messages: read_uncommitted") {
    val topic = newTopic()
    testUtils.createTopic(topic)
    testUtils.withTranscationalProducer { producer =>
      val df = spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("kafka.isolation.level", "read_uncommitted")
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST(value AS STRING)")

      producer.beginTransaction()
      (1 to 5).foreach { i =>
        producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
      }

      // "read_uncommitted" should see all messages including uncommitted ones
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 5)
      checkAnswer(df, (1 to 5).map(_.toString).toDF)

      producer.commitTransaction()

      // Should read all committed messages
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 6)
      checkAnswer(df, (1 to 5).map(_.toString).toDF)

      producer.beginTransaction()
      (6 to 10).foreach { i =>
        producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
      }
      producer.abortTransaction()

      // "read_uncommitted" should see all messages including uncommitted or aborted ones
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 12)
      checkAnswer(df, (1 to 10).map(_.toString).toDF)

      producer.beginTransaction()
      (11 to 15).foreach { i =>
        producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
      }
      producer.commitTransaction()

      // Should read all messages
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 18)
      checkAnswer(df, (1 to 15).map(_.toString).toDF)
    }
  }
}
