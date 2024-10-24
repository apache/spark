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

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{SparkConf, TestUtils}
import org.apache.spark.sql.{DataFrameReader, QueryTest}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

abstract class KafkaRelationSuiteBase extends QueryTest with SharedSparkSession with KafkaTest {

  import testImplicits._

  private val topicId = new AtomicInteger(0)

  protected var testUtils: KafkaTestUtils = _

  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "kafka")

  protected def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

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

  protected def createDF(
      topic: String,
      withOptions: Map[String, String] = Map.empty[String, String],
      brokerAddress: Option[String] = None,
      includeHeaders: Boolean = false) = {
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers",
        brokerAddress.getOrElse(testUtils.brokerAddress))
      .option("subscribe", topic)
    withOptions.foreach {
      case (key, value) => df.option(key, value)
    }
    if (includeHeaders) {
      df.option("includeHeaders", "true")
      df.load()
        .selectExpr("CAST(value AS STRING)", "headers")
    } else {
      df.load().selectExpr("CAST(value AS STRING)")
    }
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
    checkAnswer(df, (0 to 20).map(_.toString).toDF())

    // "latest" should late bind to the current (latest) offset in the df
    testUtils.sendMessages(topic, (21 to 29).map(_.toString).toArray, Some(2))
    checkAnswer(df, (0 to 29).map(_.toString).toDF())
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
    checkAnswer(df, (0 to 20).map(_.toString).toDF())
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
    checkAnswer(df, (0 to 20).map(_.toString).toDF())

    // static offset partition 2, nothing should change
    testUtils.sendMessages(topic, (31 to 39).map(_.toString).toArray, Some(2))
    checkAnswer(df, (0 to 20).map(_.toString).toDF())

    // latest offset partition 1, should change
    testUtils.sendMessages(topic, (21 to 30).map(_.toString).toArray, Some(1))
    checkAnswer(df, (0 to 30).map(_.toString).toDF())
  }

  test("default starting and ending offsets with headers") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessage(
      new RecordBuilder(topic, "1").headers(Seq()).partition(0).build()
    )
    testUtils.sendMessage(
      new RecordBuilder(topic, "2").headers(
        Seq(("a", "b".getBytes(UTF_8)), ("c", "d".getBytes(UTF_8)))).partition(1).build()
    )
    testUtils.sendMessage(
      new RecordBuilder(topic, "3").headers(
        Seq(("e", "f".getBytes(UTF_8)), ("e", "g".getBytes(UTF_8)))).partition(2).build()
    )

    // Implicit offset values, should default to earliest and latest
    val df = createDF(topic, includeHeaders = true)
    // Test that we default to "earliest" and "latest"
    checkAnswer(df, Seq(("1", null),
      ("2", Seq(("a", "b".getBytes(UTF_8)), ("c", "d".getBytes(UTF_8)))),
      ("3", Seq(("e", "f".getBytes(UTF_8)), ("e", "g".getBytes(UTF_8))))).toDF())
  }

  test("timestamp provided for starting and ending") {
    val (topic, timestamps) = prepareTimestampRelatedUnitTest

    // timestamp both presented: starting "first" ending "finalized"
    verifyTimestampRelatedQueryResult({ df =>
      val startPartitionTimestamps: Map[TopicPartition, Long] = Map(
        (0 to 2).map(new TopicPartition(topic, _) -> timestamps(1)): _*)
      val startingTimestamps = JsonUtils.partitionTimestamps(startPartitionTimestamps)

      val endPartitionTimestamps = Map(
        (0 to 2).map(new TopicPartition(topic, _) -> timestamps(2)): _*)
      val endingTimestamps = JsonUtils.partitionTimestamps(endPartitionTimestamps)

      df.option("startingOffsetsByTimestamp", startingTimestamps)
        .option("endingOffsetsByTimestamp", endingTimestamps)
    }, topic, 10 to 19)
  }

  test("timestamp provided for starting, offset provided for ending") {
    val (topic, timestamps) = prepareTimestampRelatedUnitTest

    // starting only presented as "first", and ending presented as endingOffsets
    verifyTimestampRelatedQueryResult({ df =>
      val startTopicTimestamps = Map(
        (0 to 2).map(new TopicPartition(topic, _) -> timestamps.head): _*)
      val startingTimestamps = JsonUtils.partitionTimestamps(startTopicTimestamps)

      val endPartitionOffsets = Map(
        new TopicPartition(topic, 0) -> -1L, // -1 => latest
        new TopicPartition(topic, 1) -> -1L,
        new TopicPartition(topic, 2) -> 1L  // explicit offset - take only first one
      )
      val endingOffsets = JsonUtils.partitionOffsets(endPartitionOffsets)

      // so we here expect full of records from partition 0 and 1, and only the first record
      // from partition 2 which is "2"

      df.option("startingOffsetsByTimestamp", startingTimestamps)
        .option("endingOffsets", endingOffsets)
    }, topic, (0 to 29).filterNot(_ % 3 == 2) ++ Seq(2))
  }

  test("timestamp provided for ending, offset provided for starting") {
    val (topic, timestamps) = prepareTimestampRelatedUnitTest

    // ending only presented as "third", and starting presented as startingOffsets
    verifyTimestampRelatedQueryResult({ df =>
      val startPartitionOffsets = Map(
        new TopicPartition(topic, 0) -> -2L, // -2 => earliest
        new TopicPartition(topic, 1) -> -2L,
        new TopicPartition(topic, 2) -> 0L   // explicit earliest
      )
      val startingOffsets = JsonUtils.partitionOffsets(startPartitionOffsets)

      val endTopicTimestamps = Map(
        (0 to 2).map(new TopicPartition(topic, _) -> timestamps(2)): _*)
      val endingTimestamps = JsonUtils.partitionTimestamps(endTopicTimestamps)

      df.option("startingOffsets", startingOffsets)
        .option("endingOffsetsByTimestamp", endingTimestamps)
    }, topic, 0 to 19)
  }

  test("timestamp provided for starting, ending not provided") {
    val (topic, timestamps) = prepareTimestampRelatedUnitTest

    // starting only presented as "second", and ending not presented
    verifyTimestampRelatedQueryResult({ df =>
      val startTopicTimestamps = Map(
        (0 to 2).map(new TopicPartition(topic, _) -> timestamps(1)): _*)
      val startingTimestamps = JsonUtils.partitionTimestamps(startTopicTimestamps)

      df.option("startingOffsetsByTimestamp", startingTimestamps)
    }, topic, 10 to 29)
  }

  test("timestamp provided for ending, starting not provided") {
    val (topic, timestamps) = prepareTimestampRelatedUnitTest

    // ending only presented as "third", and starting not presented
    verifyTimestampRelatedQueryResult({ df =>
      val endTopicTimestamps = Map(
        (0 to 2).map(new TopicPartition(topic, _) -> timestamps(2)): _*)
      val endingTimestamps = JsonUtils.partitionTimestamps(endTopicTimestamps)

      df.option("endingOffsetsByTimestamp", endingTimestamps)
    }, topic, 0 to 19)
  }

  test("global timestamp provided for starting and ending") {
    val (topic, timestamps) = prepareTimestampRelatedUnitTest

    // timestamp both presented: starting "first" ending "finalized"
    verifyTimestampRelatedQueryResult({ df =>
      df.option("startingTimestamp", timestamps(1)).option("endingTimestamp", timestamps(2))
    }, topic, 10 to 19)
  }

  test("no matched offset for timestamp - startingOffsets") {
    val (topic, timestamps) = prepareTimestampRelatedUnitTest

    // KafkaOffsetReaderConsumer and KafkaOffsetReaderAdmin both throws AssertionError
    // but the UninterruptibleThread used by KafkaOffsetReaderConsumer wraps it with SparkException
    val e = intercept[Throwable] {
      verifyTimestampRelatedQueryResult({ df =>
        // partition 2 will make query fail
        val startTopicTimestamps = Map(
          (0 to 1).map(new TopicPartition(topic, _) -> timestamps(1)): _*) ++
          Map(new TopicPartition(topic, 2) -> Long.MaxValue)

        val startingTimestamps = JsonUtils.partitionTimestamps(startTopicTimestamps)

        df.option("startingOffsetsByTimestamp", startingTimestamps)
      }, topic, Seq.empty)
    }

    TestUtils.assertExceptionMsg(e, "No offset matched from request")
  }

  test("preferences on offset related options") {
    val (topic, timestamps) = prepareTimestampRelatedUnitTest

    /*
    The test will set both configs differently:

    * global timestamp
    starting only presented as "third", and ending not presented

    * specific timestamp for partition
    starting only presented as "second", and ending not presented

    * offsets
    starting only presented as "earliest", and ending not presented

    The preference goes to global timestamp -> timestamp for partition -> offsets
     */

    val startTopicTimestamps = Map(
      (0 to 2).map(new TopicPartition(topic, _) -> timestamps(1)): _*)
    val startingTimestamps = JsonUtils.partitionTimestamps(startTopicTimestamps)

    // all options are specified: global timestamp
    verifyTimestampRelatedQueryResult({ df =>
      df
        .option("startingTimestamp", timestamps(2))
        .option("startingOffsetsByTimestamp", startingTimestamps)
        .option("startingOffsets", "earliest")
    }, topic, 20 to 29)

    // timestamp for partition and offsets are specified: timestamp for partition
    verifyTimestampRelatedQueryResult({ df =>
      df
        .option("startingOffsetsByTimestamp", startingTimestamps)
        .option("startingOffsets", "earliest")
    }, topic, 10 to 29)
  }

  test("no matched offset for timestamp - endingOffsets") {
    val (topic, timestamps) = prepareTimestampRelatedUnitTest

    // the query will run fine, since we allow no matching offset for timestamp
    // if it's endingOffsets
    // for partition 0 and 1, it only takes records between first and second timestamp
    // for partition 2, it will take all records
    verifyTimestampRelatedQueryResult({ df =>
      val endTopicTimestamps = Map(
        (0 to 1).map(new TopicPartition(topic, _) -> timestamps(1)): _*) ++
        Map(new TopicPartition(topic, 2) -> Long.MaxValue)

      val endingTimestamps = JsonUtils.partitionTimestamps(endTopicTimestamps)

      df.option("endingOffsetsByTimestamp", endingTimestamps)
    }, topic, (0 to 9) ++ (10 to 29).filter(_ % 3 == 2))
  }

  private def prepareTimestampRelatedUnitTest: (String, Seq[Long]) = {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)

    def sendMessages(topic: String, msgs: Array[String], part: Int, ts: Long): Unit = {
      val records = msgs.map { msg =>
        new RecordBuilder(topic, msg).partition(part).timestamp(ts).build()
      }
      testUtils.sendMessages(records)
    }

    val firstTimestamp = System.currentTimeMillis() - 5000
    (0 to 2).foreach { partNum =>
      sendMessages(topic, (0 to 9).filter(_ % 3 == partNum)
        .map(_.toString).toArray, partNum, firstTimestamp)
    }

    val secondTimestamp = firstTimestamp + 1000
    (0 to 2).foreach { partNum =>
      sendMessages(topic, (10 to 19).filter(_ % 3 == partNum)
        .map(_.toString).toArray, partNum, secondTimestamp)
    }

    val thirdTimestamp = secondTimestamp + 1000
    (0 to 2).foreach { partNum =>
      sendMessages(topic, (20 to 29).filter(_ % 3 == partNum)
        .map(_.toString).toArray, partNum, thirdTimestamp)
    }

    val finalizedTimestamp = thirdTimestamp + 1000

    (topic, Seq(firstTimestamp, secondTimestamp, thirdTimestamp, finalizedTimestamp))
  }

  private def verifyTimestampRelatedQueryResult(
      optionFn: DataFrameReader => DataFrameReader,
      topic: String,
      expectation: Seq[Int]): Unit = {
    val df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)

    val df2 = optionFn(df).load().selectExpr("CAST(value AS STRING)")
    checkAnswer(df2, expectation.map(_.toString).toDF())
  }

  test("reuse same dataframe in query") {
    // This test ensures that we do not cache the Kafka Consumer in KafkaRelation
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, (0 to 10).map(_.toString).toArray, Some(0))

    // Specify explicit earliest and latest offset values
    val df = createDF(topic,
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    checkAnswer(df.union(df), ((0 to 10) ++ (0 to 10)).map(_.toString).toDF())
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
      checkAnswer(df, (0 to 9).map(_.toString).toDF())
      // Blow away current set of messages.
      kafkaUtils.cleanupLogs()
      // Add some more data, but do not call cleanup
      kafkaUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(0))
      // Ensure that we late bind to the new starting position
      checkAnswer(df, (10 to 19).map(_.toString).toDF())
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
        reader.load().collect()
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
    testUtils.withTransactionalProducer { producer =>
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
      checkAnswer(df, (1 to 5).map(_.toString).toDF())

      producer.beginTransaction()
      (6 to 10).foreach { i =>
        producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
      }
      producer.abortTransaction()

      // Should not read aborted messages
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 12)
      checkAnswer(df, (1 to 5).map(_.toString).toDF())

      producer.beginTransaction()
      (11 to 15).foreach { i =>
        producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
      }
      producer.commitTransaction()

      // Should skip aborted messages and read new committed ones.
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 18)
      checkAnswer(df, ((1 to 5) ++ (11 to 15)).map(_.toString).toDF())
    }
  }

  test("read Kafka transactional messages: read_uncommitted") {
    val topic = newTopic()
    testUtils.createTopic(topic)
    testUtils.withTransactionalProducer { producer =>
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
      checkAnswer(df, (1 to 5).map(_.toString).toDF())

      producer.commitTransaction()

      // Should read all committed messages
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 6)
      checkAnswer(df, (1 to 5).map(_.toString).toDF())

      producer.beginTransaction()
      (6 to 10).foreach { i =>
        producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
      }
      producer.abortTransaction()

      // "read_uncommitted" should see all messages including uncommitted or aborted ones
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 12)
      checkAnswer(df, (1 to 10).map(_.toString).toDF())

      producer.beginTransaction()
      (11 to 15).foreach { i =>
        producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
      }
      producer.commitTransaction()

      // Should read all messages
      testUtils.waitUntilOffsetAppears(new TopicPartition(topic, 0), 18)
      checkAnswer(df, (1 to 15).map(_.toString).toDF())
    }
  }

  test("SPARK-30656: minPartitions") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("20"), Some(2))

    // Implicit offset values, should default to earliest and latest
    val df = createDF(topic, Map("minPartitions" -> "6"))
    val rdd = df.rdd
    val partitions = rdd.collectPartitions()
    assert(partitions.length >= 6)
    assert(partitions.flatMap(_.map(_.getString(0))).toSet === (0 to 20).map(_.toString).toSet)

    // Because of late binding, reused `rdd` and `df` should see the new data.
    testUtils.sendMessages(topic, (21 to 30).map(_.toString).toArray)
    assert(rdd.collectPartitions().flatMap(_.map(_.getString(0))).toSet
      === (0 to 30).map(_.toString).toSet)
    assert(df.rdd.collectPartitions().flatMap(_.map(_.getString(0))).toSet
      === (0 to 30).map(_.toString).toSet)
  }
}

class KafkaRelationSuiteWithAdminV1 extends KafkaRelationSuiteV1 {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_DEPRECATED_KAFKA_OFFSET_FETCHING.key, "false")
}

class KafkaRelationSuiteWithAdminV2 extends KafkaRelationSuiteV2 {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_DEPRECATED_KAFKA_OFFSET_FETCHING.key, "false")
}

class KafkaRelationSuiteV1 extends KafkaRelationSuiteBase {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "kafka")

  test("V1 Source is used when set through SQLConf") {
    val topic = newTopic()
    val df = createDF(topic)
    assert(df.logicalPlan.collect {
      case _: LogicalRelation => true
    }.nonEmpty)
  }
}

class KafkaRelationSuiteV2 extends KafkaRelationSuiteBase {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("V2 Source is used when set through SQLConf") {
    val topic = newTopic()
    val df = createDF(topic)
    assert(df.logicalPlan.collect {
      case _: DataSourceV2Relation => true
    }.nonEmpty)
  }
}
