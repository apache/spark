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

package org.apache.spark.streaming.kafka010

import java.{ util => ju }
import java.io.File

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

import kafka.log.{CleanerConfig, LogCleaner, LogConfig, UnifiedLog}
import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.utils.Pool
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import org.apache.spark._
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.streaming.kafka010.mocks.MockTime

class KafkaRDDSuite extends SparkFunSuite {

  private var kafkaTestUtils: KafkaTestUtils = _

  private val sparkConf = new SparkConf().setMaster("local[4]")
    .setAppName(this.getClass.getSimpleName)
    // Set a timeout of 10 seconds that's going to be used to fetch topics/partitions from kafka.
    // Otherwise the poll timeout defaults to 2 minutes and causes test cases to run longer.
    .set("spark.streaming.kafka.consumer.poll.ms", "10000")

  private var sc: SparkContext = _

  override def beforeAll: Unit = {
    super.beforeAll()
    sc = new SparkContext(sparkConf)
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll: Unit = {
    try {
      try {
        if (sc != null) {
          sc.stop
          sc = null
        }
      } finally {
        if (kafkaTestUtils != null) {
          kafkaTestUtils.teardown()
          kafkaTestUtils = null
        }
      }
    } finally {
      super.afterAll()
    }
  }

  private def getKafkaParams() = Map[String, Object](
    "bootstrap.servers" -> kafkaTestUtils.brokerAddress,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> s"test-consumer-${Random.nextInt}-${System.currentTimeMillis}"
  ).asJava

  private val preferredHosts = LocationStrategies.PreferConsistent

  private def compactLogs(topic: String, partition: Int,
      messages: Array[(String, String)]): Unit = {
    val mockTime = new MockTime()
    val logs = new Pool[TopicPartition, UnifiedLog]()
    val logDir = kafkaTestUtils.brokerLogDir
    val dir = new File(logDir, topic + "-" + partition)
    dir.mkdirs()
    val logProps = new ju.Properties()
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    logProps.put(LogConfig.MinCleanableDirtyRatioProp, java.lang.Float.valueOf(0.1f))
    val logDirFailureChannel = new LogDirFailureChannel(1)
    val topicPartition = new TopicPartition(topic, partition)
    val log = UnifiedLog(
      dir,
      LogConfig(logProps),
      0L,
      0L,
      mockTime.scheduler,
      new BrokerTopicStats(),
      mockTime,
      maxTransactionTimeoutMs = 5 * 60 * 1000, // KAFKA-13221
      Int.MaxValue,
      Int.MaxValue,
      logDirFailureChannel,
      lastShutdownClean = false,
      topicId = None,
      keepPartitionMetadataFile = false
    )
    messages.foreach { case (k, v) =>
      val record = new SimpleRecord(k.getBytes, v.getBytes)
      log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, record), 0);
    }
    log.roll()
    logs.put(topicPartition, log)

    val cleaner = new LogCleaner(CleanerConfig(), Array(dir), logs, logDirFailureChannel)
    cleaner.startup()
    cleaner.awaitCleaned(new TopicPartition(topic, partition), log.activeSegment.baseOffset, 1000)

    cleaner.shutdown()
    mockTime.scheduler.shutdown()
  }


  test("basic usage") {
    val topic = s"topicbasic-${Random.nextInt}-${System.currentTimeMillis}"
    kafkaTestUtils.createTopic(topic)
    val messages = Array("the", "quick", "brown", "fox")
    kafkaTestUtils.sendMessages(topic, messages)

    val kafkaParams = getKafkaParams()

    val offsetRanges = Array(OffsetRange(topic, 0, 0, messages.size))

    val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, preferredHosts)
      .map(_.value)

    val received = rdd.collect.toSet
    assert(received === messages.toSet)

    // size-related method optimizations return sane results
    assert(rdd.count === messages.size)
    assert(rdd.countApprox(0).getFinalValue.mean === messages.size)
    assert(!rdd.isEmpty)
    assert(rdd.take(1).size === 1)
    assert(rdd.take(1).head === messages.head)
    assert(rdd.take(messages.size + 10).size === messages.size)

    val emptyRdd = KafkaUtils.createRDD[String, String](
      sc, kafkaParams, Array(OffsetRange(topic, 0, 0, 0)), preferredHosts)

    assert(emptyRdd.isEmpty)

    // invalid offset ranges throw exceptions
    val badRanges = Array(OffsetRange(topic, 0, 0, messages.size + 1))
    intercept[SparkException] {
      val result = KafkaUtils.createRDD[String, String](sc, kafkaParams, badRanges, preferredHosts)
        .map(_.value)
        .collect()
    }
  }

  test("compacted topic") {
    val compactConf = sparkConf.clone()
    compactConf.set("spark.streaming.kafka.allowNonConsecutiveOffsets", "true")
    sc.stop()
    sc = new SparkContext(compactConf)
    val topic = s"topiccompacted-${Random.nextInt}-${System.currentTimeMillis}"

    val messages = Array(
      ("a", "1"),
      ("a", "2"),
      ("b", "1"),
      ("c", "1"),
      ("c", "2"),
      ("b", "2"),
      ("b", "3")
    )
    val compactedMessages = Array(
      ("a", "2"),
      ("b", "3"),
      ("c", "2")
    )

    compactLogs(topic, 0, messages)

    val props = new ju.Properties()
    props.put("cleanup.policy", "compact")
    props.put("flush.messages", "1")
    props.put("segment.ms", "1")
    props.put("segment.bytes", "256")
    kafkaTestUtils.createTopic(topic, 1, props)


    val kafkaParams = getKafkaParams()

    val offsetRanges = Array(OffsetRange(topic, 0, 0, messages.size))

    val rdd = KafkaUtils.createRDD[String, String](
      sc, kafkaParams, offsetRanges, preferredHosts
    ).map(m => m.key -> m.value)

    // To make it sure that the compaction happens
    eventually(timeout(20.second), interval(1.seconds)) {
      val dir = new File(kafkaTestUtils.brokerLogDir, topic + "-0")
      assert(dir.listFiles().exists(_.getName.endsWith(".deleted")))
    }
    val received = rdd.collect.toSet
    assert(received === compactedMessages.toSet)

    // size-related method optimizations return sane results
    assert(rdd.count === compactedMessages.size)
    assert(rdd.countApprox(0).getFinalValue.mean === compactedMessages.size)
    assert(!rdd.isEmpty)
    assert(rdd.take(1).size === 1)
    assert(rdd.take(1).head === compactedMessages.head)
    assert(rdd.take(messages.size + 10).size === compactedMessages.size)

    val emptyRdd = KafkaUtils.createRDD[String, String](
      sc, kafkaParams, Array(OffsetRange(topic, 0, 0, 0)), preferredHosts)

    assert(emptyRdd.isEmpty)

    // invalid offset ranges throw exceptions
    val badRanges = Array(OffsetRange(topic, 0, 0, messages.size + 1))
    intercept[SparkException] {
      val result = KafkaUtils.createRDD[String, String](sc, kafkaParams, badRanges, preferredHosts)
        .map(_.value)
        .collect()
    }
  }

  test("iterator boundary conditions") {
    // the idea is to find e.g. off-by-one errors between what kafka has available and the rdd
    val topic = s"topicboundary-${Random.nextInt}-${System.currentTimeMillis}"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    kafkaTestUtils.createTopic(topic)

    val kafkaParams = getKafkaParams()

    // this is the "lots of messages" case
    kafkaTestUtils.sendMessages(topic, sent)
    val sentCount = sent.values.sum

    val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams,
      Array(OffsetRange(topic, 0, 0, sentCount)), preferredHosts)

    val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val rangeCount = ranges.map(o => o.untilOffset - o.fromOffset).sum

    assert(rangeCount === sentCount, "offset range didn't include all sent messages")
    assert(rdd.map(_.offset).collect.sorted === (0 until sentCount).toArray,
      "didn't get all sent messages")

    // this is the "0 messages" case
    val rdd2 = KafkaUtils.createRDD[String, String](sc, kafkaParams,
      Array(OffsetRange(topic, 0, sentCount, sentCount)), preferredHosts)

    // shouldn't get anything, since message is sent after rdd was defined
    val sentOnlyOne = Map("d" -> 1)

    kafkaTestUtils.sendMessages(topic, sentOnlyOne)

    assert(rdd2.map(_.value).collect.size === 0, "got messages when there shouldn't be any")

    // this is the "exactly 1 message" case, namely the single message from sentOnlyOne above
    val rdd3 = KafkaUtils.createRDD[String, String](sc, kafkaParams,
      Array(OffsetRange(topic, 0, sentCount, sentCount + 1)), preferredHosts)

    // send lots of messages after rdd was defined, they shouldn't show up
    kafkaTestUtils.sendMessages(topic, Map("extra" -> 22))

    assert(rdd3.map(_.value).collect.head === sentOnlyOne.keys.head,
      "didn't get exactly one message")
  }

  test("executor sorting") {
    val kafkaParams = new ju.HashMap[String, Object](getKafkaParams())
    kafkaParams.put("auto.offset.reset", "none")
    val rdd = new KafkaRDD[String, String](
      sc,
      kafkaParams,
      Array(OffsetRange("unused", 0, 1, 2)),
      ju.Collections.emptyMap[TopicPartition, String](),
      true)
    val a3 = ExecutorCacheTaskLocation("a", "3")
    val a4 = ExecutorCacheTaskLocation("a", "4")
    val b1 = ExecutorCacheTaskLocation("b", "1")
    val b2 = ExecutorCacheTaskLocation("b", "2")

    val correct = Array(b2, b1, a4, a3)

    correct.permutations.foreach { p =>
      assert(p.sortWith(rdd.compareExecutors) === correct)
    }
  }
}
