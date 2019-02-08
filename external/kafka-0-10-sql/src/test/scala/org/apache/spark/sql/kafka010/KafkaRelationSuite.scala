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

  private def buildOffsetRestriction(topic: String, offsetMap: Map[Int, Long]) = {
    val startPartitionOffsets = offsetMap.map{case(k, v) => new TopicPartition(topic, k) -> v }
    JsonUtils.partitionOffsets(startPartitionOffsets)
  }

  private def buildDDLOptions(topic: String, start: Map[Int, Long], end: Map[Int, Long]) = {
    val startingOffsets: String = buildOffsetRestriction(topic, start)
    val endingOffsets: String = buildOffsetRestriction(topic, end)
    Map("startingOffsets" -> startingOffsets, "endingOffsets" -> endingOffsets)
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

  private def createDF(
      topic: String,
      withOptions: Map[String, String] = Map.empty[String, String],
      brokerAddress: Option[String] = None) = {
    createKafkaDF(topic, withOptions, brokerAddress)
      .selectExpr("CAST(value AS STRING)")
  }


  private def createKafkaDF(
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
    df.load()
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

  test("specific columns projection") {
    val topic = newTopic()
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    val df = createKafkaDF(topic)
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic")
    checkAnswer(df, (0 to 9).map(x => (null, x.toString, topic)).toDF("key", "value", "topic"))
  }

  test("timestamp pushdown greaterThan") {
    val topic = newTopic()
    testUtils.createTopic(topic, 4)

    testUtils.sendMessagesOverPartitions(topic, 0 to 9, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 10 to 19, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 20 to 24, 3, 5001001)
    testUtils.sendMessagesOverPartitions(topic, 25 to 29, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 30 to 39, 3, 100000000)
    // Lets leave partition 4 with no data > 5001
    testUtils.sendMessages(topic, (100 to 110).map(_.toString).toArray, Some(3), Some(4000000))

    val df = createDF(topic).where("timestamp > cast(5001 as TIMESTAMP)")
    checkAnswer(df, (20 to 39).map(_.toString).toDF())
  }

  test("timestamp pushdown greaterThanEquals") {
    val topic = newTopic()
    testUtils.createTopic(topic, 3)
    testUtils.sendMessagesOverPartitions(topic, 0 to 9, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 10 to 19, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 20 to 29, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 30 to 39, 3, 100000000)

    val df = createDF(topic).where("timestamp >= cast(5001 as TIMESTAMP)")
    checkAnswer(df, (10 to 39).map(_.toString).toDF())
  }

  test("timestamp pushdown lessThan") {
    val topic = newTopic()
    testUtils.createTopic(topic, 4)
    testUtils.sendMessagesOverPartitions(topic, 0 to 9, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 10 to 14, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 15 to 19, 3, 5001999)
    testUtils.sendMessagesOverPartitions(topic, 20 to 29, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 30 to 39, 3, 100000000)
    // Lets leave partition 4 with no data < 5002000
    testUtils.sendMessages(topic, (100 to 110).map(_.toString).toArray, Some(3), Some(8000000))

    val df = createDF(topic).where("timestamp < cast(5002 as TIMESTAMP)")
    checkAnswer(df, (0 to 19).map(_.toString).toDF())
  }

  test("timestamp pushdown lessThanEquals") {
    val topic = newTopic()
    testUtils.createTopic(topic, 3)
    testUtils.sendMessagesOverPartitions(topic, 0 to 9, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 10 to 19, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 20 to 29, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 30 to 39, 3, 100000000)
    val df = createDF(topic).where("timestamp <= cast(5002 as TIMESTAMP)")
    checkAnswer(df, (0 to 29).map(_.toString).toDF())
  }

  test("timestamp pushdown lessThan and greaterThan") {
    val topic = newTopic()
    testUtils.createTopic(topic, 4)
    testUtils.sendMessagesOverPartitions(topic, 0 to 9, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 10 to 19, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 20 to 29, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 30 to 39, 3, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 40 to 49, 3, 100000000)

    // Lets leave partition 4 with missing data for the query
    testUtils.sendMessages(topic, (110 to 119).map(_.toString).toArray, Some(3), Some(4000000))
    val df = createDF(topic).where(
      "timestamp > cast(5000 as TIMESTAMP) and timestamp < cast(5003 as TIMESTAMP)")
    checkAnswer(df, (10 to 29).map(_.toString).toDF())
  }

  test("timestamp pushdown multiple conditions") {
    val topic = newTopic()
    testUtils.createTopic(topic, 3)
    testUtils.sendMessagesOverPartitions(topic, 0 to 9, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 10 to 19, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 20 to 29, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 30 to 39, 3, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 40 to 49, 3, 100000000)
    val df = createDF(topic).where(
      """timestamp > cast(4000 as TIMESTAMP) and
        |timestamp < cast(8000 as TIMESTAMP) and
        |timestamp > cast(5000 as TIMESTAMP) and
        |timestamp < cast(5003 as TIMESTAMP) and
        |timestamp < cast(5002 as TIMESTAMP)""".stripMargin)
    checkAnswer(df, (10 to 19).map(_.toString).toDF())
  }

  test("timestamp pushdown with contradictory condition") {
    val topic = newTopic()
    testUtils.createTopic(topic, 3)
    testUtils.sendMessagesOverPartitions(topic, 0 to 9, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 10 to 19, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 20 to 29, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 30 to 39, 3, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 40 to 49, 3, 100000000)
    val df = createDF(topic).where(
      """timestamp < cast(5002 as TIMESTAMP) and
        |timestamp > cast(5002 as TIMESTAMP)""".stripMargin)
    checkAnswer(df, spark.emptyDataFrame)
  }

  test("timestamp pushdown equals") {
    val topic = newTopic()
    testUtils.createTopic(topic, 3)
    testUtils.sendMessagesOverPartitions(topic, 0 to 9, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 10 to 19, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 20 to 29, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 30 to 39, 3, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 40 to 49, 3, 100000000)
    val df = createDF(topic).where("timestamp = cast(5002 as TIMESTAMP)")
    checkAnswer(df, (20 to 29).map(_.toString).toDF())
  }

  test("timestamp pushdown on unevenly distributed partitions") {
    val topic = newTopic()
    testUtils.createTopic(topic, 4)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0), Some(5000000))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1), Some(5005000))
    testUtils.sendMessages(topic, (20 to 24).map(_.toString).toArray, Some(2), Some(5002000))
    testUtils.sendMessages(topic, (30 to 34).map(_.toString).toArray, Some(3), Some(5003000))
    testUtils.sendMessages(topic, (35 to 39).map(_.toString).toArray, Some(3), Some(5004000))
    testUtils.sendMessages(topic, (40 to 49).map(_.toString).toArray, Some(0), Some(100000000))
    // Equals operator doesn't find the last element of the partition => we need to add 1 msg on top
    testUtils.sendMessages(topic, (25 to 25).map(_.toString).toArray, Some(2), Some(5003000))

    val df = createDF(topic).where("timestamp = cast(5002 as TIMESTAMP)")
    checkAnswer(df, (20 to 24).map(_.toString).toDF())
  }

  test("timestamp pushdown with specific lower bound DDL offset limit") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)

    testUtils.sendMessagesOverPartitions(topic, 0 to 14, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 15 to 29, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 30 to 44, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 45 to 59, 3, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 60 to 74, 3, 100000000)

    val options = buildDDLOptions(topic,
      Map(0 -> 15L, 1 -> 15L, 2 -> 15L),
      Map(0 -> -1L, 1 -> -1L, 2 -> -1L) // latest
    )
    val df = createDF(topic, withOptions = options)
      .where("timestamp > cast(5000 as TIMESTAMP) and timestamp < cast(5004 as TIMESTAMP)")
    // 45 (15 offset * 3 partitions) to 59 (constrained by timestamp < 5004000)
    checkAnswer(df, (45 to 59).map(_.toString).toDF())
  }


  test("timestamp pushdown on latest range") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)

    testUtils.sendMessagesOverPartitions(topic, 0 to 14, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 15 to 29, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 30 to 44, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 45 to 59, 3, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 60 to 74, 3, 100000000)
    testUtils.sendMessagesOverPartitions(topic, 75 to 200, 3, 5005000)

    val options = buildDDLOptions(topic,
      Map(0 -> 10L, 1 -> 10L, 2 -> 10L),
      Map(0 -> -1L, 1 -> -1L, 2 -> -1L) // latest
    )
    val df = createDF(topic, withOptions = options)
      .where("timestamp > cast(5002 as TIMESTAMP)")
    // 45 (by timestamp > 5002000) to 150 unbounded
    checkAnswer(df, (45 to 200).map(_.toString).toDF())
  }


  test("timestamp pushdown with specific upper bound DDL offset limit") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 4)

    val options = buildDDLOptions(topic,
      Map(0 -> -2L, 1 -> -2L, 2 -> -2L, 3 -> -2L), // earliest
      Map(0 -> 22L, 1 -> 22L, 2 -> 22L, 3 -> 22L)
    )
    val df = createDF(topic, withOptions = options)
      .where("timestamp > cast(5001 as TIMESTAMP)")
    testUtils.sendMessagesOverPartitions(topic, 0 to 19, 4, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 20 to 39, 4, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 40 to 59, 4, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 60 to 79, 4, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 80 to 99, 4, 100000000)
    // from 40 (by timestamp > 5001000)
    // to 79 (20 offset * 4 partitions) + 2 remaining offset from each partition
    checkAnswer(df, ((40 to 79) ++ (80 to 81) ++ (85 to 86) ++ (90 to 91) ++ (95 to 96))
      .map(_.toString).toDF())
  }

  test("timestamp pushdown on earliest range") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 4)

    val options = buildDDLOptions(topic,
      Map(0 -> -2L, 1 -> -2L, 2 -> -2L, 3 -> -2L), // earliest
      Map(0 -> 22L, 1 -> 22L, 2 -> 22L, 3 -> 22L)
    )
    val df = createDF(topic, withOptions = options)
      .where("timestamp < cast(5003 as TIMESTAMP)")
    testUtils.sendMessagesOverPartitions(topic, 0 to 19, 4, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 20 to 39, 4, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 40 to 59, 4, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 60 to 79, 4, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 80 to 99, 4, 100000000)
    // earliest to 59 (by timestamp < 5003000)
    checkAnswer(df, (0 to 59).map(_.toString).toDF())
  }

  test("timestamp pushdown with specific offsets upper & lower bound") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 4)
    val options = buildDDLOptions(topic,
      Map(0 -> 7, 1 -> 7, 2 -> 7, 3 -> 7),
      Map(0 -> 18L, 1 -> 18L, 2 -> 18L, 3 -> 18L)
    )
    val df = createDF(topic, withOptions = options)
      .where("timestamp > cast(5000 as TIMESTAMP) and timestamp < cast(5003 as TIMESTAMP)")
    testUtils.sendMessagesOverPartitions(topic, 0 to 19, 4, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 20 to 39, 4, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 40 to 59, 4, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 60 to 79, 4, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 80 to 99, 4, 100000000)

    // start with 3 from each partition (by offset) to 59 (by timestamp < 5003000)
    checkAnswer(df, ((22 to 24) ++ (27 to 29) ++ (32 to 34) ++ (37 to 39) ++ (40 to 59))
      .map(_.toString).toDF())
  }

  test("timestamp pushdown out of offset range") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 4)
    val options = buildDDLOptions(topic,
      Map(0 -> 7, 1 -> 7, 2 -> 7, 3 -> 7),
      Map(0 -> 18L, 1 -> 18L, 2 -> 18L, 3 -> 18L)
    )
    val df = createDF(topic, withOptions = options)
      .where("timestamp > cast(5003 as TIMESTAMP) and timestamp < cast(5005 as TIMESTAMP)")
    testUtils.sendMessagesOverPartitions(topic, 0 to 19, 4, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 20 to 39, 4, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 40 to 59, 4, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 60 to 79, 4, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 80 to 99, 4, 100000000)

    checkAnswer(df, spark.emptyDataFrame)
  }

  test("where query on partition condition") {
    val topic = newTopic()
    testUtils.createTopic(topic, 3)
    testUtils.sendMessagesOverPartitions(topic, 0 to 8, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 9 to 17, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 18 to 26, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 27 to 35, 3, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 36 to 44, 3, 100000000)
    createKafkaDF(topic).selectExpr("CAST(value AS STRING)", "partition", "offset", "timestamp")
      .show(100)
    val df = createDF(topic).where("timestamp > cast(5001 as TIMESTAMP) and partition = 0")
    // first 3 elements from each batch
    checkAnswer(df, ((18 to 20) ++ (27 to 29) ++ (36 to 38)).map(_.toString).toDF())
  }

  test("where query on offset condition") {
    val topic = newTopic()
    testUtils.createTopic(topic, 3)
    testUtils.sendMessagesOverPartitions(topic, 0 to 8, 3, 5000000)
    testUtils.sendMessagesOverPartitions(topic, 9 to 17, 3, 5001000)
    testUtils.sendMessagesOverPartitions(topic, 18 to 26, 3, 5002000)
    testUtils.sendMessagesOverPartitions(topic, 27 to 35, 3, 5003000)
    testUtils.sendMessagesOverPartitions(topic, 36 to 44, 3, 100000000)
    val df = createDF(topic).where("offset = 7")
    checkAnswer(df, ((19 to 19) ++ (22 to 22) ++ (25 to 25)).map(_.toString).toDF())
  }
}
