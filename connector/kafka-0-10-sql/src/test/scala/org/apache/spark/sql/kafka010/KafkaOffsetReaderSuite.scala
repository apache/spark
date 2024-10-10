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
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.{IsolationLevel, TopicPartition}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf
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

  private def createKafkaReader(
    topic: String,
    minPartitions: Option[Int])
      : KafkaOffsetReader = {
    KafkaOffsetReader.build(
      SubscribeStrategy(Seq(topic)),
      KafkaSourceProvider.kafkaParamsForDriver(
        Map(
        "bootstrap.servers" ->
         testUtils.brokerAddress
      )),
      CaseInsensitiveMap(
        minPartitions.map(m => Map("minPartitions" -> m.toString)).getOrElse(Map.empty)),
      UUID.randomUUID().toString,
      DefaultKafkaPartitionLocationAssigner
    )
  }

  test("isolationLevel must give back default isolation level when not set") {
    testIsolationLevel(None,
      IsolationLevel.valueOf(ConsumerConfig.DEFAULT_ISOLATION_LEVEL.toUpperCase(Locale.ROOT)))
  }

  test("isolationLevel must give back READ_UNCOMMITTED when set") {
    testIsolationLevel(Some("read_uncommitted"), IsolationLevel.READ_UNCOMMITTED)
  }

  test("isolationLevel must give back READ_COMMITTED when set") {
    testIsolationLevel(Some("read_committed"), IsolationLevel.READ_COMMITTED)
  }

  test("isolationLevel must throw exception when invalid isolation level set") {
    intercept[IllegalArgumentException] {
      testIsolationLevel(Some("intentionally_invalid"), IsolationLevel.READ_COMMITTED)
    }
  }

  private def testIsolationLevel(kafkaParam: Option[String], isolationLevel: IsolationLevel) = {
    var kafkaParams = Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress)
    kafkaParam.foreach(p => kafkaParams ++= Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> p))
    val reader = new KafkaOffsetReaderAdmin(
      SubscribeStrategy(Seq()),
      KafkaSourceProvider.kafkaParamsForDriver(kafkaParams),
      CaseInsensitiveMap(Map.empty),
      "",
      DefaultKafkaPartitionLocationAssigner
    )
    assert(reader.isolationLevel === isolationLevel)
  }

  testWithAllOffsetFetchingSQLConf("SPARK-30656: getOffsetRangesFromUnresolvedOffsets - " +
    "using specific offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, (0 until 10).map(_.toString).toArray, Some(0))
    val tp = new TopicPartition(topic, 0)
    val reader = createKafkaReader(topic, minPartitions = Some(3))
    val startingOffsets = SpecificOffsetRangeLimit(Map(tp -> 1))
    val endingOffsets = SpecificOffsetRangeLimit(Map(tp -> 4))
    val offsetRanges = reader.getOffsetRangesFromUnresolvedOffsets(startingOffsets,
      endingOffsets)
    assert(offsetRanges.sortBy(_.topicPartition.toString) === Seq(
      KafkaOffsetRange(tp, 1, 2, None),
      KafkaOffsetRange(tp, 2, 3, None),
      KafkaOffsetRange(tp, 3, 4, None)).sortBy(_.topicPartition.toString))
  }

  testWithAllOffsetFetchingSQLConf("SPARK-30656: getOffsetRangesFromUnresolvedOffsets - " +
    "using special offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, (0 until 4).map(_.toString).toArray, Some(0))
    val tp = new TopicPartition(topic, 0)
    val reader = createKafkaReader(topic, minPartitions = Some(3))
    val startingOffsets = EarliestOffsetRangeLimit
    val endingOffsets = LatestOffsetRangeLimit
    val offsetRanges = reader.getOffsetRangesFromUnresolvedOffsets(startingOffsets,
      endingOffsets)
    assert(offsetRanges.sortBy(_.topicPartition.toString) === Seq(
      KafkaOffsetRange(tp, EARLIEST, 1, None),
      KafkaOffsetRange(tp, 1, 2, None),
      KafkaOffsetRange(tp, 2, LATEST, None)).sortBy(_.topicPartition.toString))
  }

  testWithAllOffsetFetchingSQLConf(
    "SPARK-48383: START_OFFSET_DOES_NOT_MATCH_ASSIGNED error class"
  ) {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    val reader = createKafkaReader(topic, minPartitions = Some(4))

    // There are three topic partitions, but we only include two in offsets.
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val startingOffsets = SpecificOffsetRangeLimit(Map(tp1 -> EARLIEST, tp2 -> EARLIEST))
    val endingOffsets = SpecificOffsetRangeLimit(Map(tp1 -> LATEST, tp2 -> 3))

    val ex = intercept[KafkaIllegalStateException] {
      reader.getOffsetRangesFromUnresolvedOffsets(startingOffsets, endingOffsets)
    }
    checkError(
      exception = ex,
      condition = "KAFKA_START_OFFSET_DOES_NOT_MATCH_ASSIGNED",
      parameters = Map(
        "specifiedPartitions" -> "Set\\(.*,.*\\)",
        "assignedPartitions" -> "Set\\(.*,.*,.*\\)"),
      matchPVals = true)
  }

  testWithAllOffsetFetchingSQLConf("SPARK-30656: getOffsetRangesFromUnresolvedOffsets - " +
    "multiple topic partitions") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)
    testUtils.sendMessages(topic, (0 until 100).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (0 until 4).map(_.toString).toArray, Some(1))
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val reader = createKafkaReader(topic, minPartitions = Some(4))

    val startingOffsets = SpecificOffsetRangeLimit(Map(tp1 -> EARLIEST, tp2 -> EARLIEST))
    val endingOffsets = SpecificOffsetRangeLimit(Map(tp1 -> LATEST, tp2 -> 3))
    val offsetRanges = reader.getOffsetRangesFromUnresolvedOffsets(startingOffsets,
      endingOffsets)
    assert(offsetRanges.sortBy(_.topicPartition.toString) === Seq(
      KafkaOffsetRange(tp2, EARLIEST, 3, None),
      KafkaOffsetRange(tp1, EARLIEST, 33, None),
      KafkaOffsetRange(tp1, 33, 66, None),
      KafkaOffsetRange(tp1, 66, LATEST, None)).sortBy(_.topicPartition.toString))
  }

  testWithAllOffsetFetchingSQLConf("SPARK-30656: getOffsetRangesFromResolvedOffsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)
    testUtils.sendMessages(topic, (0 until 100).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (0 until 4).map(_.toString).toArray, Some(1))
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val reader = createKafkaReader(topic, minPartitions = Some(4))

    val fromPartitionOffsets = Map(tp1 -> 0L, tp2 -> 0L)
    val untilPartitionOffsets = Map(tp1 -> 100L, tp2 -> 3L)
    val offsetRanges = reader.getOffsetRangesFromResolvedOffsets(
      fromPartitionOffsets,
      untilPartitionOffsets,
      (_, _) => {})
    assert(offsetRanges.sortBy(_.topicPartition.toString) === Seq(
      KafkaOffsetRange(tp1, 0, 33, None),
      KafkaOffsetRange(tp1, 33, 66, None),
      KafkaOffsetRange(tp1, 66, 100, None),
      KafkaOffsetRange(tp2, 0, 3, None)).sortBy(_.topicPartition.toString))
  }

  testSPARK46798("getOffsetRangesFromUnresolvedOffsets") { (createKafkaReader: ReaderMaker) =>
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    Seq(0, 1, 2).foreach { partitionNumber =>
      testUtils.sendMessages(topic, (0 until 10).map(_.toString).toArray, Some(partitionNumber))
    }
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val tp3 = new TopicPartition(topic, 2)
    val reader = createKafkaReader(topic, Some(3))
    val startingOffsets = SpecificOffsetRangeLimit(Map(tp1 -> 1, tp2 -> 1, tp3 -> 1))
    val endingOffsets = SpecificOffsetRangeLimit(Map(tp1 -> 4, tp2 -> 4, tp3 -> 4))
    val offsetRanges = reader.getOffsetRangesFromUnresolvedOffsets(startingOffsets,
      endingOffsets)
    assert(offsetRanges.sortBy(_.topicPartition.toString) === Seq(
      KafkaOffsetRange(tp1, 1, 4, Some("exec0")),
      KafkaOffsetRange(tp2, 1, 4, Some("exec1")),
      KafkaOffsetRange(tp3, 1, 4, Some("exec2"))).sortBy(_.topicPartition.toString))
  }

  testSPARK46798("getOffsetRangesFromUnresolvedOffsetsWithMinPartition") {
    (createKafkaReader: ReaderMaker) =>
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    Seq(0, 1, 2).foreach { partitionNumber =>
      testUtils.sendMessages(topic, (0 until 10).map(_.toString).toArray, Some(partitionNumber))
    }
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val tp3 = new TopicPartition(topic, 2)
    val reader = createKafkaReader(topic, Some(4))
    val startingOffsets = SpecificOffsetRangeLimit(Map(tp1 -> 1, tp2 -> 1, tp3 -> 1))
    val endingOffsets = SpecificOffsetRangeLimit(Map(tp1 -> 5, tp2 -> 4, tp3 -> 4))
    val offsetRanges = reader.getOffsetRangesFromUnresolvedOffsets(startingOffsets,
      endingOffsets)
    assert(offsetRanges.sortBy(_.topicPartition.toString) === Seq(
      KafkaOffsetRange(tp1, 1, 3, None),
      KafkaOffsetRange(tp1, 3, 5, None),
      KafkaOffsetRange(tp2, 1, 4, None),
      KafkaOffsetRange(tp3, 1, 4, None)).sortBy(_.topicPartition.toString))
  }

  testSPARK46798("getOffsetRangesFromResolvedOffsets") { (createKafkaReader: ReaderMaker) =>
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)
    testUtils.sendMessages(topic, (0 until 4).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (0 until 4).map(_.toString).toArray, Some(1))
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val reader = createKafkaReader(topic, Some(2))

    val fromPartitionOffsets = Map(tp1 -> 0L, tp2 -> 0L)
    val untilPartitionOffsets = Map(tp1 -> 3L, tp2 -> 3L)
    val offsetRanges = reader.getOffsetRangesFromResolvedOffsets(
      fromPartitionOffsets,
      untilPartitionOffsets,
      (_, _) => ())
    assert(offsetRanges.sortBy(_.topicPartition.toString) === Seq(
      KafkaOffsetRange(tp1, 0, 3, Some("exec0")),
      KafkaOffsetRange(tp2, 0, 3, Some("exec1"))).sortBy(_.topicPartition.toString))
  }

  testSPARK46798("getOffsetRangesFromResolvedOffsetsWithMinPartition") {
    (createKafkaReader: ReaderMaker) =>
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)
    testUtils.sendMessages(topic, (0 until 4).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (0 until 4).map(_.toString).toArray, Some(1))
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val reader = createKafkaReader(topic, Some(4))

    val fromPartitionOffsets = Map(tp1 -> 0L, tp2 -> 0L)
    val untilPartitionOffsets = Map(tp1 -> 3L, tp2 -> 3L)
    val offsetRanges = reader.getOffsetRangesFromResolvedOffsets(
      fromPartitionOffsets,
      untilPartitionOffsets,
      (_, _) => ())
    assert(offsetRanges.sortBy(_.topicPartition.toString) === Seq(
      KafkaOffsetRange(tp1, 0, 1, None),
      KafkaOffsetRange(tp1, 1, 3, None),
      KafkaOffsetRange(tp2, 0, 1, None),
      KafkaOffsetRange(tp2, 1, 3, None)).sortBy(_.topicPartition.toString))
  }

  private def testWithAllOffsetFetchingSQLConf(name: String)(func: => Any): Unit = {
    Seq("true", "false").foreach { useDeprecatedOffsetFetching =>
      val testName = s"$name with useDeprecatedOffsetFetching $useDeprecatedOffsetFetching"
      executeFuncWithSQLConf(testName, useDeprecatedOffsetFetching, func)
    }
  }

  type ReaderMaker = (String, Option[Int]) => KafkaOffsetReader

  private def testSPARK46798(name: String)(func: ReaderMaker => Any): Unit = {
    val execs = Array("exec0", "exec1", "exec2")
    test("SPARK-46798: Partition location " + name + " (Admin offset reader)") {
      val readerMaker: ReaderMaker = (topic: String, minPartitions: Option[Int]) => {
        new KafkaOffsetReaderAdmin(
          SubscribeStrategy(Seq(topic)),
          KafkaSourceProvider.kafkaParamsForDriver(
            Map("bootstrap.servers" -> testUtils.brokerAddress)),
          CaseInsensitiveMap(
            minPartitions.map(m => Map("minPartitions" -> m.toString)).getOrElse(Map.empty)),
          UUID.randomUUID().toString,
          TestKPLAssigner) {

          override protected def getSortedExecutorList: Array[ExecutorDescription] =
            execs.map(e => ExecutorDescription(e, e))
        }
      }
      func(readerMaker)
    }

    test(name + " (Consumer offset reader)") {
      val readerMaker: ReaderMaker = (topic: String, minPartitions: Option[Int]) => {
        new KafkaOffsetReaderConsumer(
          SubscribeStrategy(Seq(topic)),
          KafkaSourceProvider.kafkaParamsForDriver(
            Map("bootstrap.servers" -> testUtils.brokerAddress)),
          CaseInsensitiveMap(
            minPartitions.map(m => Map("minPartitions" -> m.toString)).getOrElse(Map.empty)),
          UUID.randomUUID().toString,
          TestKPLAssigner) {

          override protected def getSortedExecutorList: Array[ExecutorDescription] =
            execs.map(e => ExecutorDescription(e, e))
        }
      }
      func(readerMaker)
    }

  }
  private def executeFuncWithSQLConf(
      name: String,
      useDeprecatedOffsetFetching: String,
      func: => Any): Unit = {
    test(name) {
      withSQLConf(SQLConf.USE_DEPRECATED_KAFKA_OFFSET_FETCHING.key -> useDeprecatedOffsetFetching) {
        func
      }
    }
  }
}

object TestKPLAssigner extends KafkaPartitionLocationAssigner {
  type LocationMap = Map[PartitionDescription, Array[ExecutorDescription]]
  def getLocationPreferences(
    partDescrs: Array[PartitionDescription],
    knownExecutors: Array[ExecutorDescription]): LocationMap = {
    partDescrs.map { partitionDescription =>
      val execs = knownExecutors
        .filter(exec => exec.id.contains(partitionDescription.partition.toString))
      (partitionDescription -> execs)
    }.toMap
  }
}
