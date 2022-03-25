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

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.{Locale, Optional}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.kafka010.KafkaSourceProvider._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

abstract class KafkaSourceTest extends StreamTest with SharedSparkSession with KafkaTest {

  protected var testUtils: KafkaTestUtils = _

  override val streamingTimeout = 30.seconds

  protected val brokerProps = Map[String, Object]()

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(brokerProps)
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    super.afterAll()
  }

  protected def makeSureGetOffsetCalled = AssertOnQuery { q =>
    // Because KafkaSource's initialPartitionOffsets is set lazily, we need to make sure
    // its "getOffset" is called before pushing any data. Otherwise, because of the race condition,
    // we don't know which data should be fetched when `startingOffsets` is latest.
    q match {
      case c: ContinuousExecution => c.awaitEpoch(0)
      case m: MicroBatchExecution => m.processAllAvailable()
    }
    true
  }

  protected def setTopicPartitions(topic: String, newCount: Int, query: StreamExecution) : Unit = {
    testUtils.addPartitions(topic, newCount)
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

    override def addData(query: Option[StreamExecution]): (SparkDataStream, Offset) = {
      query match {
        // Make sure no Spark job is running when deleting a topic
        case Some(m: MicroBatchExecution) => m.processAllAvailable()
        case _ =>
      }

      val existingTopics = testUtils.getAllTopicsAndPartitionSize().toMap
      val newTopics = topics.diff(existingTopics.keySet)
      for (newTopic <- newTopics) {
        topicAction(newTopic, None)
      }
      for (existingTopicPartitions <- existingTopics) {
        topicAction(existingTopicPartitions._1, Some(existingTopicPartitions._2))
      }

      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active kafka source")

      val sources: Seq[SparkDataStream] = {
        query.get.logicalPlan.collect {
          case StreamingExecutionRelation(source: KafkaSource, _) => source
          case r: StreamingDataSourceV2Relation if r.stream.isInstanceOf[KafkaMicroBatchStream] ||
              r.stream.isInstanceOf[KafkaContinuousStream] =>
            r.stream
        }
      }.distinct

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

  object WithOffsetSync {
    /**
     * Run `func` to write some Kafka messages and wait until the latest offset of the given
     * `TopicPartition` is not less than `expectedOffset`.
     */
    def apply(
        topicPartition: TopicPartition,
        expectedOffset: Long)(func: () => Unit): StreamAction = {
      Execute("Run Kafka Producer")(_ => {
        func()
        // This is a hack for the race condition that the committed message may be not visible to
        // consumer for a short time.
        testUtils.waitUntilOffsetAppears(topicPartition, expectedOffset)
      })
    }
  }

  private val topicId = new AtomicInteger(0)
  protected def newTopic(): String = s"topic-${topicId.getAndIncrement()}"
}

abstract class KafkaMicroBatchSourceSuiteBase extends KafkaSourceSuiteBase {

  import testImplicits._

  private def waitUntilBatchProcessed(clock: StreamManualClock) = AssertOnQuery { q =>
    eventually(Timeout(streamingTimeout)) {
      if (!q.exception.isDefined) {
        assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
      }
    }
    if (q.exception.isDefined) {
      throw q.exception.get
    }
    true
  }

  test("Trigger.AvailableNow") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 5)

    testUtils.sendMessages(topic, (0 until 15).map { case x =>
      s"foo-$x"
    }.toArray, Some(0))

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("maxOffsetsPerTrigger", 5)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    var index: Int = 0
    def startTriggerAvailableNowQuery(): StreamingQuery = {
      reader.writeStream
        .foreachBatch((_: Dataset[Row], _: Long) => {
          index += 1
        })
        .trigger(Trigger.AvailableNow)
        .start()
    }

    val query = startTriggerAvailableNowQuery()
    try {
      assert(query.awaitTermination(streamingTimeout.toMillis))
    } finally {
      query.stop()
    }

    // should have 3 batches now i.e. 15 / 5 = 3
    assert(index == 3)
  }

  test("(de)serialization of initial offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 5)

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)

    testStream(reader.load)(
      makeSureGetOffsetCalled,
      StopStream,
      StartStream(),
      StopStream)
  }

  test("SPARK-26718 Rate limit set to Long.Max should not overflow integer " +
    "during end offset calculation") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    // fill in 5 messages to trigger potential integer overflow
    testUtils.sendMessages(topic, (0 until 5).map(_.toString).toArray, Some(0))

    val partitionOffsets = Map(
      new TopicPartition(topic, 0) -> 5L
    )
    val startingOffsets = JsonUtils.partitionOffsets(partitionOffsets)

    val kafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      // use latest to force begin to be 5
      .option("startingOffsets", startingOffsets)
      // use Long.Max to try to trigger overflow
      .option("maxOffsetsPerTrigger", Long.MaxValue)
      .option("subscribe", topic)
      .option("kafka.metadata.max.age.ms", "1")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddKafkaData(Set(topic), 30, 31, 32, 33, 34),
      CheckAnswer(30, 31, 32, 33, 34),
      StopStream
    )
  }

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

    val clock = new StreamManualClock

    testStream(mapped)(
      StartStream(Trigger.ProcessingTime(100), clock),
      waitUntilBatchProcessed(clock),
      // 1 from smallest, 1 from middle, 8 from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107),
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111, 112, 113, 114, 115, 116
      ),
      StopStream,
      StartStream(Trigger.ProcessingTime(100), clock),
      waitUntilBatchProcessed(clock),
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111, 112, 113, 114, 115, 116,
        12, 117, 118, 119, 120, 121, 122, 123, 124, 125
      ),
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111, 112, 113, 114, 115, 116,
        12, 117, 118, 119, 120, 121, 122, 123, 124, 125,
        13, 126, 127, 128, 129, 130, 131, 132, 133, 134
      )
    )

    // When Trigger.Once() is used, the read limit should be ignored
    val allData = Seq(1) ++ (10 to 20) ++ (100 to 200)
    withTempDir { dir =>
      testStream(mapped)(
        StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(allData: _*),
        StopStream,

        AddKafkaData(Set(topic), 1000 to 1010: _*),
        StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer((allData ++ 1000.to(1010)): _*)
      )
    }
  }

  test("minOffsetsPerTrigger") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (100 to 109).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 14).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("1"), Some(2))

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("minOffsetsPerTrigger", 15)
      .option("maxTriggerDelay", "5s")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    val clock = new StreamManualClock

    testStream(mapped)(
      StartStream(Trigger.ProcessingTime(100), clock),
      waitUntilBatchProcessed(clock),
      // First Batch is always processed
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 12, 13, 14),
      // Adding more data but less than minOffsetsPerTrigger
      Assert {
        testUtils.sendMessages(topic, (15 to 20).map(_.toString).toArray, Some(1))
        true
      },
      // No data is processed for next batch as data is less than minOffsetsPerTrigger
      // and maxTriggerDelay is not expired
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckNewAnswer(),
      Assert {
        testUtils.sendMessages(topic, (110 to 120).map(_.toString).toArray, Some(0))
        testUtils.sendMessages(topic, Array("2"), Some(2))
        true
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      // Running batch now as number of records is greater than minOffsetsPerTrigger
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111, 112, 113, 114, 115, 116,
        12, 117, 118, 119, 120, 13, 14, 15, 16, 17, 18, 19, 2, 20),
      // Testing maxTriggerDelay
      // Adding more data but less than minOffsetsPerTrigger
      Assert {
        testUtils.sendMessages(topic, (121 to 125).map(_.toString).toArray, Some(0))
        testUtils.sendMessages(topic, (21 to 25).map(_.toString).toArray, Some(1))
        true
      },
      // No data is processed for next batch till maxTriggerDelay is expired
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckNewAnswer(),
      // Sleeping for 5s to let maxTriggerDelay expire
      Assert {
        Thread.sleep(5 * 1000)
        true
      },
      AdvanceManualClock(100),
      // Running batch as maxTriggerDelay is expired
      waitUntilBatchProcessed(clock),
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111, 112, 113, 114, 115, 116,
        12, 117, 118, 119, 120, 121, 122, 123, 124, 125,
        13, 14, 15, 16, 17, 18, 19, 2, 20, 21, 22, 23, 24, 25)
    )
    // When Trigger.Once() is used, the read limit should be ignored
    val allData = Seq(1, 2) ++ (10 to 25) ++ (100 to 125)
    withTempDir { dir =>
      testStream(mapped)(
        StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(allData: _*),
        StopStream,

        AddKafkaData(Set(topic), 1000 to 1010: _*),
        StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer((allData ++ 1000.to(1010)): _*)
      )
    }
  }

  test("compositeReadLimit") {
    MockedSystemClock.reset()

    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (100 to 120).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 20).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("1"), Some(2))

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("minOffsetsPerTrigger", 15)
      .option("maxTriggerDelay", "5s")
      .option("maxOffsetsPerTrigger", 20)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      // mock system time to ensure deterministic behavior
      // in determining if maxOffsetsPerTrigger is satisfied
      .option("_mockSystemTime", "")
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    val clock = new StreamManualClock

    def advanceSystemClock(mills: Long): ExternalAction = () => {
      MockedSystemClock.advanceCurrentSystemTime(mills)
    }

    testStream(mapped)(
      StartStream(Trigger.ProcessingTime(100), clock),
      waitUntilBatchProcessed(clock),
      // First Batch is always processed but it will process only 20
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111,
        12, 13, 14, 15),
      // Pending data is less than minOffsetsPerTrigger
      // No data is processed for next batch as data is less than minOffsetsPerTrigger
      // and maxTriggerDelay is not expired
      AdvanceManualClock(100),
      advanceSystemClock(100),
      waitUntilBatchProcessed(clock),
      CheckNewAnswer(),
      Assert {
        testUtils.sendMessages(topic, (121 to 128).map(_.toString).toArray, Some(0))
        testUtils.sendMessages(topic, (21 to 30).map(_.toString).toArray, Some(1))
        testUtils.sendMessages(topic, Array("2"), Some(2))
        true
      },
      AdvanceManualClock(100),
      advanceSystemClock(100),
      waitUntilBatchProcessed(clock),
      // Running batch now as number of new records is greater than minOffsetsPerTrigger
      // but reading limited data as per maxOffsetsPerTrigger
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111, 112, 113, 114, 115, 116,
        12, 117, 118, 119, 120, 121,
        13, 14, 15, 16, 17, 18, 19, 2, 20, 21, 22, 23, 24),

      // Testing maxTriggerDelay
      // No data is processed for next batch till maxTriggerDelay is expired
      AdvanceManualClock(100),
      advanceSystemClock(100),
      waitUntilBatchProcessed(clock),
      CheckNewAnswer(),
      AdvanceManualClock(100),
      advanceSystemClock(5000),
      // Running batch as maxTriggerDelay is expired
      waitUntilBatchProcessed(clock),
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
        12, 120, 121, 122, 123, 124, 125, 126, 127, 128,
        13, 14, 15, 16, 17, 18, 19, 2, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30)
    )

    // When Trigger.Once() is used, the read limit should be ignored
    val allData = Seq(1, 2) ++ (10 to 30) ++ (100 to 128)
    withTempDir { dir =>
      testStream(mapped)(
        StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(allData: _*),
        StopStream,

        AddKafkaData(Set(topic), 1000 to 1010: _*),
        StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer((allData ++ 1000.to(1010)): _*)
      )
    }
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
      StartStream(trigger = Trigger.ProcessingTime(1)),
      makeSureGetOffsetCalled,
      AddKafkaData(Set(topic), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      AssertOnQuery { query =>
        val recordsRead = query.recentProgress.map(_.numInputRows).sum
        recordsRead == 3
      }
    )
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
      .option("kafka.request.timeout.ms", "3000")
      .option("kafka.default.api.timeout.ms", "3000")
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

  test("subscribe topic by pattern with topic recreation between batches") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-good"
    val topic2 = topicPrefix + "-bad"
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, Array("1", "3"))
    testUtils.createTopic(topic2, partitions = 1)
    testUtils.sendMessages(topic2, Array("2", "4"))

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("kafka.request.timeout.ms", "3000")
      .option("kafka.default.api.timeout.ms", "3000")
      .option("startingOffsets", "earliest")
      .option("subscribePattern", s"$topicPrefix-.*")

    val ds = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .map(kv => kv._2.toInt)

    testStream(ds)(
      StartStream(),
      AssertOnQuery { q =>
        q.processAllAvailable()
        true
      },
      CheckAnswer(1, 2, 3, 4),
      // Restart the stream in this test to make the test stable. When recreating a topic when a
      // consumer is alive, it may not be able to see the recreated topic even if a fresh consumer
      // has seen it.
      StopStream,
      // Recreate `topic2` and wait until it's available
      WithOffsetSync(new TopicPartition(topic2, 0), expectedOffset = 1) { () =>
        testUtils.deleteTopic(topic2)
        testUtils.createTopic(topic2)
        testUtils.sendMessages(topic2, Array("6"))
      },
      StartStream(),
      ExpectFailure[IllegalStateException](e => {
        // The offset of `topic2` should be changed from 2 to 1
        assert(e.getMessage.contains("was changed from 2 to 1"))
      })
    )
  }

  test("ensure that initial offset are written with an extra byte in the beginning (SPARK-19517)") {
    withTempDir { metadataPath =>
      val topic = "kafka-initial-offset-current"
      testUtils.createTopic(topic, partitions = 1)

      val initialOffsetFile = Paths.get(s"${metadataPath.getAbsolutePath}/sources/0/0").toFile

      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", s"earliest")
        .load()

      // Test the written initial offset file has 0 byte in the beginning, so that
      // Spark 2.1.0 can read the offsets (see SPARK-19517)
      testStream(df)(
        StartStream(checkpointLocation = metadataPath.getAbsolutePath),
        makeSureGetOffsetCalled)

      val binarySource = Source.fromFile(initialOffsetFile)
      try {
        assert(binarySource.next().toInt == 0)  // first byte is binary 0
      } finally {
        binarySource.close()
      }
    }
  }

  test("deserialization of initial offset written by Spark 2.1.0 (SPARK-19517)") {
    withTempDir { metadataPath =>
      val topic = "kafka-initial-offset-2-1-0"
      testUtils.createTopic(topic, partitions = 3)
      testUtils.sendMessages(topic, Array("0", "1", "2"), Some(0))
      testUtils.sendMessages(topic, Array("0", "10", "20"), Some(1))
      testUtils.sendMessages(topic, Array("0", "100", "200"), Some(2))

      // Copy the initial offset file into the right location inside the checkpoint root directory
      // such that the Kafka source can read it for initial offsets.
      val from = new File(
        getClass.getResource("/kafka-source-initial-offset-version-2.1.0.bin").toURI).toPath
      val to = Paths.get(s"${metadataPath.getAbsolutePath}/sources/0/0")
      Files.createDirectories(to.getParent)
      Files.copy(from, to)

      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", s"earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.toInt)

      // Test that the query starts from the expected initial offset (i.e. read older offsets,
      // even though startingOffsets is latest).
      testStream(df)(
        StartStream(checkpointLocation = metadataPath.getAbsolutePath),
        AddKafkaData(Set(topic), 1000),
        CheckAnswer(0, 1, 2, 10, 20, 200, 1000))
    }
  }

  test("deserialization of initial offset written by future version") {
    withTempDir { metadataPath =>
      val topic = "kafka-initial-offset-future-version"
      testUtils.createTopic(topic, partitions = 3)

      // Copy the initial offset file into the right location inside the checkpoint root directory
      // such that the Kafka source can read it for initial offsets.
      val from = new File(
        getClass.getResource("/kafka-source-initial-offset-future-version.bin").toURI).toPath
      val to = Paths.get(s"${metadataPath.getAbsolutePath}/sources/0/0")
      Files.createDirectories(to.getParent)
      Files.copy(from, to)

      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.toInt)

      testStream(df)(
        StartStream(checkpointLocation = metadataPath.getAbsolutePath),
        ExpectFailure[IllegalStateException](e => {
          Seq(
            s"maximum supported log version is v1, but encountered v99999",
            "produced by a newer version of Spark and cannot be read by this version"
          ).foreach { message =>
            assert(e.toString.contains(message))
          }
        }))
    }
  }

  test("KafkaSource with watermark") {
    val now = System.currentTimeMillis()
    val topic = newTopic()
    testUtils.createTopic(newTopic(), partitions = 1)
    testUtils.sendMessages(topic, Array(1).map(_.toString))

    val kafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("startingOffsets", s"earliest")
      .option("subscribe", topic)
      .load()

    val windowedAggregation = kafka
      .withWatermark("timestamp", "10 seconds")
      .groupBy(window($"timestamp", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start") as 'window, $"count")

    val query = windowedAggregation
      .writeStream
      .format("memory")
      .outputMode("complete")
      .queryName("kafkaWatermark")
      .start()
    query.processAllAvailable()
    val rows = spark.table("kafkaWatermark").collect()
    assert(rows.length === 1, s"Unexpected results: ${rows.toList}")
    val row = rows(0)
    // We cannot check the exact window start time as it depends on the time that messages were
    // inserted by the producer. So here we just use a low bound to make sure the internal
    // conversion works.
    assert(
      row.getAs[java.sql.Timestamp]("window").getTime >= now - 5 * 1000,
      s"Unexpected results: $row")
    assert(row.getAs[Int]("count") === 1, s"Unexpected results: $row")
    query.stop()
  }

  test("delete a topic when a Spark job is running") {
    KafkaSourceSuite.collectedData.clear()

    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, (1 to 10).map(_.toString).toArray)

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("kafka.request.timeout.ms", "3000")
      .option("kafka.default.api.timeout.ms", "3000")
      .option("subscribe", topic)
      // If a topic is deleted and we try to poll data starting from offset 0,
      // the Kafka consumer will just block until timeout and return an empty result.
      // So set the timeout to 1 second to make this test fast.
      .option("kafkaConsumer.pollTimeoutMs", "1000")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    KafkaSourceSuite.globalTestUtils = testUtils
    // The following ForeachWriter will delete the topic before fetching data from Kafka
    // in executors.
    val query = kafka.map(kv => kv._2.toInt).writeStream.foreach(new ForeachWriter[Int] {
      override def open(partitionId: Long, version: Long): Boolean = {
        // Re-create topic since Kafka auto topic creation is not supported by Spark
        KafkaSourceSuite.globalTestUtils.deleteTopic(topic)
        KafkaSourceSuite.globalTestUtils.createTopic(topic)
        true
      }

      override def process(value: Int): Unit = {
        KafkaSourceSuite.collectedData.add(value)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }).start()
    query.processAllAvailable()
    query.stop()
    // `failOnDataLoss` is `false`, we should not fail the query
    assert(query.exception.isEmpty)
  }

  test("SPARK-22956: currentPartitionOffsets should be set when no new data comes in") {
    def getSpecificDF(range: Range.Inclusive): org.apache.spark.sql.Dataset[Int] = {
      val topic = newTopic()
      testUtils.createTopic(topic, partitions = 1)
      testUtils.sendMessages(topic, range.map(_.toString).toArray, Some(0))

      val reader = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("kafka.metadata.max.age.ms", "1")
        .option("maxOffsetsPerTrigger", 5)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")

      reader.load()
        .selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(k => k.toInt)
    }

    val df1 = getSpecificDF(0 to 9)
    val df2 = getSpecificDF(100 to 199)

    val kafka = df1.union(df2)

    val clock = new StreamManualClock

    testStream(kafka)(
      StartStream(Trigger.ProcessingTime(100), clock),
      waitUntilBatchProcessed(clock),
      // 5 from smaller topic, 5 from bigger one
      CheckLastBatch((0 to 4) ++ (100 to 104): _*),
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      // 5 from smaller topic, 5 from bigger one
      CheckLastBatch((5 to 9) ++ (105 to 109): _*),
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      // smaller topic empty, 5 from bigger one
      CheckLastBatch(110 to 114: _*),
      StopStream,
      StartStream(Trigger.ProcessingTime(100), clock),
      waitUntilBatchProcessed(clock),
      // smallest now empty, 5 from bigger one
      CheckLastBatch(115 to 119: _*),
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      // smallest now empty, 5 from bigger one
      CheckLastBatch(120 to 124: _*)
    )
  }

  test("allow group.id prefix") {
    // Group ID prefix is only supported by consumer based offset reader
    if (spark.conf.get(SQLConf.USE_DEPRECATED_KAFKA_OFFSET_FETCHING)) {
      testGroupId("groupIdPrefix", (expected, actual) => {
        assert(actual.exists(_.startsWith(expected)) && !actual.exists(_ === expected),
          "Valid consumer groups don't contain the expected group id - " +
            s"Valid consumer groups: $actual / expected group id: $expected")
      })
    }
  }

  test("allow group.id override") {
    // Group ID override is only supported by consumer based offset reader
    if (spark.conf.get(SQLConf.USE_DEPRECATED_KAFKA_OFFSET_FETCHING)) {
      testGroupId("kafka.group.id", (expected, actual) => {
        assert(actual.exists(_ === expected), "Valid consumer groups don't " +
          s"contain the expected group id - Valid consumer groups: $actual / " +
          s"expected group id: $expected")
      })
    }
  }

  private def testGroupId(groupIdKey: String,
      validateGroupId: (String, Iterable[String]) => Unit): Unit = {
    // Tests code path KafkaSourceProvider.{sourceSchema(.), createSource(.)}
    // as well as KafkaOffsetReader.createConsumer(.)
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (1 to 10).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (11 to 20).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, (21 to 30).map(_.toString).toArray, Some(2))

    val customGroupId = "id-" + Random.nextInt()
    val dsKafka = spark
      .readStream
      .format("kafka")
      .option(groupIdKey, customGroupId)
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)

    testStream(dsKafka)(
      makeSureGetOffsetCalled,
      CheckAnswer(1 to 30: _*),
      Execute { _ =>
        val consumerGroups = testUtils.listConsumerGroups()
        val validGroups = consumerGroups.valid().get()
        val validGroupsId = validGroups.asScala.map(_.groupId())
        validateGroupId(customGroupId, validGroupsId)
      }
    )
  }

  test("ensure stream-stream self-join generates only one offset in log and correct metrics") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)
    require(testUtils.getLatestOffsets(Set(topic)).size === 2)

    val kafka = spark
      .readStream
      .format("kafka")
      .option("subscribe", topic)
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .load()

    val values = kafka
      .selectExpr("CAST(CAST(value AS STRING) AS INT) AS value",
        "CAST(CAST(value AS STRING) AS INT) % 5 AS key")

    val join = values.join(values, "key")

    def checkQuery(check: AssertOnQuery): Unit = {
      testStream(join)(
        makeSureGetOffsetCalled,
        AddKafkaData(Set(topic), 1, 2),
        CheckAnswer((1, 1, 1), (2, 2, 2)),
        AddKafkaData(Set(topic), 6, 3),
        CheckAnswer((1, 1, 1), (2, 2, 2), (3, 3, 3), (1, 6, 1), (1, 1, 6), (1, 6, 6)),
        check
      )
    }

    withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      checkQuery(AssertOnQuery { q =>
        assert(q.availableOffsets.iterator.size == 1)
        // The kafka source is scanned twice because of self-join
        assert(q.recentProgress.map(_.numInputRows).sum == 8)
        true
      })
    }

    withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true") {
      checkQuery(AssertOnQuery { q =>
        assert(q.availableOffsets.iterator.size == 1)
        assert(q.lastExecution.executedPlan.collect {
          case r: ReusedExchangeExec => r
        }.length == 1)
        // The kafka source is scanned only once because of exchange reuse.
        assert(q.recentProgress.map(_.numInputRows).sum == 4)
        true
      })
    }
  }

  test("read Kafka transactional messages: read_committed") {
    // This test will cover the following cases:
    // 1. the whole batch contains no data messages
    // 2. the first offset in a batch is not a committed data message
    // 3. the last offset in a batch is not a committed data message
    // 4. there is a gap in the middle of a batch

    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("kafka.isolation.level", "read_committed")
      .option("maxOffsetsPerTrigger", 3)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      // Set a short timeout to make the test fast. When a batch doesn't contain any visible data
      // messages, "poll" will wait until timeout.
      .option("kafkaConsumer.pollTimeoutMs", 5000)
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    val clock = new StreamManualClock

    // Wait until the manual clock is waiting on further instructions to move forward. Then we can
    // ensure all batches we are waiting for have been processed.
    val waitUntilBatchProcessed = Execute { q =>
      eventually(Timeout(streamingTimeout)) {
        if (!q.exception.isDefined) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
    }

    val topicPartition = new TopicPartition(topic, 0)
    // The message values are the same as their offsets to make the test easy to follow
    testUtils.withTransactionalProducer { producer =>
      testStream(mapped)(
        StartStream(Trigger.ProcessingTime(100), clock),
        waitUntilBatchProcessed,
        CheckAnswer(),
        WithOffsetSync(topicPartition, expectedOffset = 5) { () =>
          // Send 5 messages. They should be visible only after being committed.
          producer.beginTransaction()
          (0 to 4).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        // Should not see any uncommitted messages
        CheckNewAnswer(),
        WithOffsetSync(topicPartition, expectedOffset = 6) { () =>
          producer.commitTransaction()
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(0, 1, 2), // offset 0, 1, 2
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(3, 4), // offset: 3, 4, 5* [* means it's not a committed data message]
        WithOffsetSync(topicPartition, expectedOffset = 12) { () =>
          // Send 5 messages and abort the transaction. They should not be read.
          producer.beginTransaction()
          (6 to 10).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }
          producer.abortTransaction()
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(), // offset: 6*, 7*, 8*
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(), // offset: 9*, 10*, 11*
        WithOffsetSync(topicPartition, expectedOffset = 18) { () =>
          // Send 5 messages again. The consumer should skip the above aborted messages and read
          // them.
          producer.beginTransaction()
          (12 to 16).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }
          producer.commitTransaction()
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(12, 13, 14), // offset: 12, 13, 14
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(15, 16),  // offset: 15, 16, 17*
        WithOffsetSync(topicPartition, expectedOffset = 25) { () =>
          producer.beginTransaction()
          producer.send(new ProducerRecord[String, String](topic, "18")).get()
          producer.commitTransaction()
          producer.beginTransaction()
          producer.send(new ProducerRecord[String, String](topic, "20")).get()
          producer.commitTransaction()
          producer.beginTransaction()
          producer.send(new ProducerRecord[String, String](topic, "22")).get()
          producer.send(new ProducerRecord[String, String](topic, "23")).get()
          producer.commitTransaction()
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(18, 20), // offset: 18, 19*, 20
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(22, 23), // offset: 21*, 22, 23
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer() // offset: 24*
      )
    }
  }

  test("read Kafka transactional messages: read_uncommitted") {
    // This test will cover the following cases:
    // 1. the whole batch contains no data messages
    // 2. the first offset in a batch is not a committed data message
    // 3. the last offset in a batch is not a committed data message
    // 4. there is a gap in the middle of a batch

    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("kafka.isolation.level", "read_uncommitted")
      .option("maxOffsetsPerTrigger", 3)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      // Set a short timeout to make the test fast. When a batch doesn't contain any visible data
      // messages, "poll" will wait until timeout.
      .option("kafkaConsumer.pollTimeoutMs", 5000)
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    val clock = new StreamManualClock

    // Wait until the manual clock is waiting on further instructions to move forward. Then we can
    // ensure all batches we are waiting for have been processed.
    val waitUntilBatchProcessed = Execute { q =>
      eventually(Timeout(streamingTimeout)) {
        if (!q.exception.isDefined) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
    }

    val topicPartition = new TopicPartition(topic, 0)
    // The message values are the same as their offsets to make the test easy to follow
    testUtils.withTransactionalProducer { producer =>
      testStream(mapped)(
        StartStream(Trigger.ProcessingTime(100), clock),
        waitUntilBatchProcessed,
        CheckNewAnswer(),
        WithOffsetSync(topicPartition, expectedOffset = 5) { () =>
          // Send 5 messages. They should be visible only after being committed.
          producer.beginTransaction()
          (0 to 4).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(0, 1, 2), // offset 0, 1, 2
        WithOffsetSync(topicPartition, expectedOffset = 6) { () =>
          producer.commitTransaction()
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(3, 4), // offset: 3, 4, 5* [* means it's not a committed data message]
        WithOffsetSync(topicPartition, expectedOffset = 12) { () =>
          // Send 5 messages and abort the transaction. They should not be read.
          producer.beginTransaction()
          (6 to 10).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }
          producer.abortTransaction()
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(6, 7, 8), // offset: 6, 7, 8
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(9, 10), // offset: 9, 10, 11*
        WithOffsetSync(topicPartition, expectedOffset = 18) { () =>
          // Send 5 messages again. The consumer should skip the above aborted messages and read
          // them.
          producer.beginTransaction()
          (12 to 16).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }
          producer.commitTransaction()
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(12, 13, 14), // offset: 12, 13, 14
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(15, 16),  // offset: 15, 16, 17*
        WithOffsetSync(topicPartition, expectedOffset = 25) { () =>
          producer.beginTransaction()
          producer.send(new ProducerRecord[String, String](topic, "18")).get()
          producer.commitTransaction()
          producer.beginTransaction()
          producer.send(new ProducerRecord[String, String](topic, "20")).get()
          producer.commitTransaction()
          producer.beginTransaction()
          producer.send(new ProducerRecord[String, String](topic, "22")).get()
          producer.send(new ProducerRecord[String, String](topic, "23")).get()
          producer.commitTransaction()
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(18, 20), // offset: 18, 19*, 20
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer(22, 23), // offset: 21*, 22, 23
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckNewAnswer() // offset: 24*
      )
    }
  }

  test("SPARK-25495: FetchedData.reset should reset all fields") {
    val topic = newTopic()
    val topicPartition = new TopicPartition(topic, 0)
    testUtils.createTopic(topic, partitions = 1)

    val ds = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("kafka.isolation.level", "read_committed")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .select($"value".as[String])

    testUtils.withTransactionalProducer { producer =>
      producer.beginTransaction()
      (0 to 3).foreach { i =>
        producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
      }
      producer.commitTransaction()
    }
    testUtils.waitUntilOffsetAppears(topicPartition, 5)

    val q = ds.writeStream.foreachBatch { (ds: Dataset[String], epochId: Long) =>
      if (epochId == 0) {
        // Send more message before the tasks of the current batch start reading the current batch
        // data, so that the executors will prefetch messages in the next batch and drop them. In
        // this case, if we forget to reset `FetchedData._nextOffsetInFetchedData` or
        // `FetchedData._offsetAfterPoll` (See SPARK-25495), the next batch will see incorrect
        // values and return wrong results hence fail the test.
        testUtils.withTransactionalProducer { producer =>
          producer.beginTransaction()
          (4 to 7).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }
          producer.commitTransaction()
        }
        testUtils.waitUntilOffsetAppears(topicPartition, 10)
        checkDatasetUnorderly(ds, (0 to 3).map(_.toString): _*)
      } else {
        checkDatasetUnorderly(ds, (4 to 7).map(_.toString): _*)
      }
    }.start()
    try {
      q.processAllAvailable()
    } finally {
      q.stop()
    }
  }

  test("SPARK-27494: read kafka record containing null key/values.") {
    testNullableKeyValue(Trigger.ProcessingTime(100))
  }

  test("SPARK-30656: minPartitions") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("20"), Some(2))

    val ds = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("minPartitions", "6")
      .load()
      .select($"value".as[String])
    val q = ds.writeStream.foreachBatch { (batch: Dataset[String], _: Long) =>
      val partitions = batch.rdd.collectPartitions()
      assert(partitions.length >= 6)
      assert(partitions.flatten.toSet === (0 to 20).map(_.toString).toSet): Unit
    }.start()
    try {
      q.processAllAvailable()
    } finally {
      q.stop()
    }
  }
}


class KafkaMicroBatchV1SourceWithAdminSuite extends KafkaMicroBatchV1SourceSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.USE_DEPRECATED_KAFKA_OFFSET_FETCHING.key, "false")
  }
}

class KafkaMicroBatchV2SourceWithAdminSuite extends KafkaMicroBatchV2SourceSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.USE_DEPRECATED_KAFKA_OFFSET_FETCHING.key, "false")
  }
}

class KafkaMicroBatchV1SourceSuite extends KafkaMicroBatchSourceSuiteBase {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      SQLConf.DISABLED_V2_STREAMING_MICROBATCH_READERS.key,
      classOf[KafkaSourceProvider].getCanonicalName)
  }

  test("V1 Source is used when disabled through SQLConf") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 5)

    val kafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("subscribePattern", s"$topic.*")
      .load()

    testStream(kafka)(
      makeSureGetOffsetCalled,
      AssertOnQuery { query =>
        query.logicalPlan.collect {
          case StreamingExecutionRelation(_: KafkaSource, _) => true
        }.nonEmpty
      }
    )
  }
}

class KafkaMicroBatchV2SourceSuite extends KafkaMicroBatchSourceSuiteBase {

  test("V2 Source is used by default") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 5)

    val kafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("subscribePattern", s"$topic.*")
      .load()

    testStream(kafka)(
      makeSureGetOffsetCalled,
      AssertOnQuery { query =>
        query.logicalPlan.exists {
          case r: StreamingDataSourceV2Relation => r.stream.isInstanceOf[KafkaMicroBatchStream]
          case _ => false
        }
      }
    )
  }

  testWithUninterruptibleThread("minPartitions is supported") {

    val topic = newTopic()
    val tp = new TopicPartition(topic, 0)
    testUtils.createTopic(topic, partitions = 1)

    def test(
        minPartitions: String,
        numPartitionsGenerated: Int,
        reusesConsumers: Boolean): Unit = {

      SparkSession.setActiveSession(spark)
      withTempDir { dir =>
        val provider = new KafkaSourceProvider()
        val options = Map(
          "kafka.bootstrap.servers" -> testUtils.brokerAddress,
          "subscribe" -> topic
        ) ++ Option(minPartitions).map { p => "minPartitions" -> p}
        val dsOptions = new CaseInsensitiveStringMap(options.asJava)
        val table = provider.getTable(dsOptions)
        val stream = table.newScanBuilder(dsOptions).build().toMicroBatchStream(dir.getAbsolutePath)
        val inputPartitions = stream.planInputPartitions(
          KafkaSourceOffset(Map(tp -> 0L)),
          KafkaSourceOffset(Map(tp -> 100L))).map(_.asInstanceOf[KafkaBatchInputPartition])
        withClue(s"minPartitions = $minPartitions generated factories $inputPartitions\n\t") {
          assert(inputPartitions.size == numPartitionsGenerated)
        }
      }
    }

    // Test cases when minPartitions is used and not used
    test(minPartitions = null, numPartitionsGenerated = 1, reusesConsumers = true)
    test(minPartitions = "1", numPartitionsGenerated = 1, reusesConsumers = true)
    test(minPartitions = "4", numPartitionsGenerated = 4, reusesConsumers = false)

    // Test illegal minPartitions values
    intercept[IllegalArgumentException] { test(minPartitions = "a", 1, true) }
    intercept[IllegalArgumentException] { test(minPartitions = "1.0", 1, true) }
    intercept[IllegalArgumentException] { test(minPartitions = "0", 1, true) }
    intercept[IllegalArgumentException] { test(minPartitions = "-1", 1, true) }
  }

  test("default config of includeHeader doesn't break existing query from Spark 2.4") {
    import testImplicits._

    // This topic name is migrated from Spark 2.4.3 test run
    val topic = "spark-test-topic-2b8619f5-d3c4-4c2d-b5d1-8d9d9458aa62"
    // create same topic and messages as test run
    testUtils.createTopic(topic, partitions = 5, overwrite = true)
    testUtils.sendMessages(topic, Array(-20, -21, -22).map(_.toString), Some(0))
    testUtils.sendMessages(topic, Array(-10, -11, -12).map(_.toString), Some(1))
    testUtils.sendMessages(topic, Array(0, 1, 2).map(_.toString), Some(2))
    testUtils.sendMessages(topic, Array(10, 11, 12).map(_.toString), Some(3))
    testUtils.sendMessages(topic, Array(20, 21, 22).map(_.toString), Some(4))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val headers = Seq(("a", "b".getBytes(UTF_8)), ("c", "d".getBytes(UTF_8)))
    (31 to 35).map { num =>
      new RecordBuilder(topic, num.toString).partition(num - 31).headers(headers).build()
    }.foreach { rec => testUtils.sendMessage(rec) }

    val kafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("subscribePattern", topic)
      .option("startingOffsets", "earliest")
      .load()

    val query = kafka.dropDuplicates()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .map(kv => kv._2.toInt + 1)

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-2.4.3-kafka-include-headers-default/").toURI

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

    testStream(query)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
      /*
        Note: The checkpoint was generated using the following input in Spark version 2.4.3
        testUtils.createTopic(topic, partitions = 5, overwrite = true)

        testUtils.sendMessages(topic, Array(-20, -21, -22).map(_.toString), Some(0))
        testUtils.sendMessages(topic, Array(-10, -11, -12).map(_.toString), Some(1))
        testUtils.sendMessages(topic, Array(0, 1, 2).map(_.toString), Some(2))
        testUtils.sendMessages(topic, Array(10, 11, 12).map(_.toString), Some(3))
        testUtils.sendMessages(topic, Array(20, 21, 22).map(_.toString), Some(4))
        */
      makeSureGetOffsetCalled,
      CheckNewAnswer(32, 33, 34, 35, 36)
    )
  }

  test("test custom metrics - with rate limit") {
    import testImplicits._

    val topic = newTopic()
    val data = 1 to 10
    testUtils.createTopic(topic, partitions = 2)
    testUtils.sendMessages(topic, (1 to 5).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (6 to 10).map(_.toString).toArray, Some(1))

    val kafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("maxOffsetsPerTrigger", 1)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)

    testStream(kafka)(
      StartStream(),
      makeSureGetOffsetCalled,
      CheckAnswer(data: _*),
      Execute { query =>
        // The rate limit is 1, so there must be some delay in offsets per partition.
        val progressWithDelay = query.recentProgress.map(_.sources.head).reverse.find { progress =>
          // find the metrics that has non-zero average offsetsBehindLatest greater than 0.
          !progress.metrics.isEmpty && progress.metrics.get("avgOffsetsBehindLatest").toDouble > 0
        }
        assert(progressWithDelay.nonEmpty)
        val metrics = progressWithDelay.get.metrics
        assert(metrics.keySet() ===
          Set("minOffsetsBehindLatest",
            "maxOffsetsBehindLatest",
            "avgOffsetsBehindLatest").asJava)
        assert(metrics.get("minOffsetsBehindLatest").toLong > 0)
        assert(metrics.get("maxOffsetsBehindLatest").toLong > 0)
        assert(metrics.get("avgOffsetsBehindLatest").toDouble > 0)
      }
    )
  }

  test("test custom metrics - no rate limit") {
    import testImplicits._

    val topic = newTopic()
    val data = 1 to 10
    testUtils.createTopic(topic, partitions = 2)
    testUtils.sendMessages(topic, (1 to 5).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (6 to 10).map(_.toString).toArray, Some(1))

    val kafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)

    testStream(kafka)(
      StartStream(),
      makeSureGetOffsetCalled,
      CheckAnswer(data: _*),
      Execute { query =>
        val progress = query.recentProgress.map(_.sources.head).lastOption
        assert(progress.nonEmpty)
        val metrics = progress.get.metrics
        // When there is no rate limit, there shouldn't be any delay in the current stream.
        assert(metrics.keySet() ===
          Set("minOffsetsBehindLatest",
            "maxOffsetsBehindLatest",
            "avgOffsetsBehindLatest").asJava)
        assert(metrics.get("minOffsetsBehindLatest").toLong === 0)
        assert(metrics.get("maxOffsetsBehindLatest").toLong === 0)
        assert(metrics.get("avgOffsetsBehindLatest").toDouble === 0)
      }
    )
  }

  test("test custom metrics - corner cases") {
    val topicPartition1 = new TopicPartition(newTopic(), 0)
    val topicPartition2 = new TopicPartition(newTopic(), 0)
    val latestOffset = Map[TopicPartition, Long]((topicPartition1, 3L), (topicPartition2, 6L))

    // test empty offset.
    assert(KafkaMicroBatchStream.metrics(Optional.ofNullable(null), latestOffset).isEmpty)

    // test valid offsetsBehindLatest
    val offset = KafkaSourceOffset(
      Map[TopicPartition, Long]((topicPartition1, 1L), (topicPartition2, 2L)))
    assert(
      KafkaMicroBatchStream.metrics(Optional.ofNullable(offset), latestOffset) ===
        Map[String, String](
          "minOffsetsBehindLatest" -> "2",
          "maxOffsetsBehindLatest" -> "4",
          "avgOffsetsBehindLatest" -> "3.0").asJava)

    // test null latestAvailablePartitionOffsets
    assert(KafkaMicroBatchStream.metrics(Optional.ofNullable(offset), null).isEmpty)
  }
}

abstract class KafkaSourceSuiteBase extends KafkaSourceTest {

  import testImplicits._

  test("cannot stop Kafka stream") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, (101 to 105).map { _.toString }.toArray)

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("subscribePattern", s"$topic.*")

    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = kafka.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      StopStream
    )
  }

  for (failOnDataLoss <- Seq(true, false)) {
    test(s"assign from latest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromLatestOffsets(
        topic,
        addPartitions = false,
        failOnDataLoss = failOnDataLoss,
        "assign" -> assignString(topic, 0 to 4))
    }

    test(s"assign from earliest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromEarliestOffsets(
        topic,
        addPartitions = false,
        failOnDataLoss = failOnDataLoss,
        "assign" -> assignString(topic, 0 to 4))
    }

    test(s"assign from specific offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromSpecificOffsets(
        topic,
        failOnDataLoss = failOnDataLoss,
        "assign" -> assignString(topic, 0 to 4))
    }

    test(s"assign from specific timestamps (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromSpecificTimestamps(
        topic,
        failOnDataLoss = failOnDataLoss,
        addPartitions = false,
        "assign" -> assignString(topic, 0 to 4))
    }

    test(s"assign from global timestamp per topic (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromGlobalTimestamp(
        topic,
        failOnDataLoss = failOnDataLoss,
        addPartitions = false,
        "assign" -> assignString(topic, 0 to 4))
    }

    test(s"subscribing topic by name from latest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromLatestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        "subscribe" -> topic)
    }

    test(s"subscribing topic by name from earliest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromEarliestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        "subscribe" -> topic)
    }

    test(s"subscribing topic by name from specific offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromSpecificOffsets(topic, failOnDataLoss = failOnDataLoss, "subscribe" -> topic)
    }

    test(s"subscribing topic by name from specific timestamps (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromSpecificTimestamps(topic, failOnDataLoss = failOnDataLoss, addPartitions = true,
        "subscribe" -> topic)
    }

    test(s"subscribing topic by name from global timestamp per topic" +
      s" (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromGlobalTimestamp(topic, failOnDataLoss = failOnDataLoss, addPartitions = true,
        "subscribe" -> topic)
    }

    test(s"subscribing topic by pattern from latest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topicPrefix = newTopic()
      val topic = topicPrefix + "-suffix"
      testFromLatestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        "subscribePattern" -> s"$topicPrefix-.*")
    }

    test(s"subscribing topic by pattern from earliest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topicPrefix = newTopic()
      val topic = topicPrefix + "-suffix"
      testFromEarliestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        "subscribePattern" -> s"$topicPrefix-.*")
    }

    test(s"subscribing topic by pattern from specific offsets (failOnDataLoss: $failOnDataLoss)") {
      val topicPrefix = newTopic()
      val topic = topicPrefix + "-suffix"
      testFromSpecificOffsets(
        topic,
        failOnDataLoss = failOnDataLoss,
        "subscribePattern" -> s"$topicPrefix-.*")
    }

    test(s"subscribing topic by pattern from specific timestamps " +
      s"(failOnDataLoss: $failOnDataLoss)") {
      val topicPrefix = newTopic()
      val topic = topicPrefix + "-suffix"
      testFromSpecificTimestamps(
        topic,
        failOnDataLoss = failOnDataLoss,
        addPartitions = true,
        "subscribePattern" -> s"$topicPrefix-.*")
    }

    test(s"subscribing topic by pattern from global timestamp per topic " +
      s"(failOnDataLoss: $failOnDataLoss)") {
      val topicPrefix = newTopic()
      val topic = topicPrefix + "-suffix"
      testFromGlobalTimestamp(
        topic,
        failOnDataLoss = failOnDataLoss,
        addPartitions = true,
        "subscribePattern" -> s"$topicPrefix-.*")
    }
  }

  test("subscribing topic by name from specific timestamps with non-matching starting offset") {
    val topic = newTopic()
    testFromSpecificTimestampsWithNoMatchingStartingOffset(topic, "subscribe" -> topic)
  }

  test("subscribing topic by name from global timestamp per topic with " +
    "non-matching starting offset") {
    val topic = newTopic()
    testFromGlobalTimestampWithNoMatchingStartingOffset(topic, "subscribe" -> topic)
  }

  test("subscribing topic by pattern from specific timestamps with " +
    "non-matching starting offset") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-suffix"
    testFromSpecificTimestampsWithNoMatchingStartingOffset(topic,
      "subscribePattern" -> s"$topicPrefix-.*")
  }

  test("subscribing topic by pattern from global timestamp per topic with " +
    "non-matching starting offset") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-suffix"
    testFromGlobalTimestampWithNoMatchingStartingOffset(topic,
      "subscribePattern" -> s"$topicPrefix-.*")
  }

  private def testFromSpecificTimestampsWithNoMatchingStartingOffset(
      topic: String,
      options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)

    val firstTimestamp = System.currentTimeMillis() - 5000
    val secondTimestamp = firstTimestamp + 1000
    setupTestMessagesForTestOnTimestampOffsets(topic, firstTimestamp, secondTimestamp)
    // no data after second timestamp for partition 4

    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    // here we starts from second timestamp for all partitions, whereas we know there's
    // no data in partition 4 matching second timestamp
    val startPartitionTimestamps: Map[TopicPartition, Long] =
    (0 to 4).map(new TopicPartition(topic, _) -> secondTimestamp).toMap
    val startingTimestamps = JsonUtils.partitionTimestamps(startPartitionTimestamps)

    val mapped = setupDataFrameForTestOnTimestampOffsets(startingTimestamps, failOnDataLoss = true,
      options: _*)
    assertQueryFailOnStartOffsetStrategyAsError(mapped)

    val mapped2 = setupDataFrameForTestOnTimestampOffsets(startingTimestamps, failOnDataLoss = true,
      options :+ ("startingoffsetsbytimestampstrategy", "error"): _*)
    assertQueryFailOnStartOffsetStrategyAsError(mapped2)

    val mapped3 = setupDataFrameForTestOnTimestampOffsets(startingTimestamps, failOnDataLoss = true,
      options :+ ("startingoffsetsbytimestampstrategy", "latest"): _*)

    testStream(mapped3)(
      makeSureGetOffsetCalled,
      Execute { q =>
        val partitions = (0 to 4).map(new TopicPartition(topic, _))
        // wait to reach the last offset in every partition
        q.awaitOffset(
          0, KafkaSourceOffset(partitions.map(tp => tp -> 3L).toMap), streamingTimeout.toMillis)
      },
      CheckAnswer(-21, -22, -11, -12, 2, 12),
      Execute { q =>
        sendMessagesWithTimestamp(topic, Array(23, 24, 25).map(_.toString), 4, secondTimestamp)
        // wait to reach the new last offset in every partition
        val partitions = (0 to 3).map(new TopicPartition(topic, _)).map(tp => tp -> 3L) ++
          Seq(new TopicPartition(topic, 4) -> 6L)
        q.awaitOffset(
          0, KafkaSourceOffset(partitions.toMap), streamingTimeout.toMillis)
      },
      CheckNewAnswer(23, 24, 25)
    )
  }

  private def testFromGlobalTimestampWithNoMatchingStartingOffset(
      topic: String,
      options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)

    val firstTimestamp = System.currentTimeMillis() - 5000
    val secondTimestamp = firstTimestamp + 1000
    setupTestMessagesForTestOnTimestampOffsets(topic, firstTimestamp, secondTimestamp)

    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    // here we starts from second timestamp for all partitions, whereas we know there's
    // no data in partition 4 matching second timestamp

    val mapped = setupDataFrameForTestOnGlobalTimestamp(secondTimestamp, failOnDataLoss = true,
      options: _*)
    assertQueryFailOnStartOffsetStrategyAsError(mapped)

    val mapped2 = setupDataFrameForTestOnGlobalTimestamp(secondTimestamp, failOnDataLoss = true,
      options :+ ("startingoffsetsbytimestampstrategy", "error"): _*)
    assertQueryFailOnStartOffsetStrategyAsError(mapped2)

    val mapped3 = setupDataFrameForTestOnGlobalTimestamp(secondTimestamp, failOnDataLoss = true,
      options :+ ("startingoffsetsbytimestampstrategy", "latest"): _*)

    testStream(mapped3)(
      makeSureGetOffsetCalled,
      Execute { q =>
        val partitions = (0 to 4).map(new TopicPartition(topic, _))
        // wait to reach the last offset in every partition
        q.awaitOffset(
          0, KafkaSourceOffset(partitions.map(tp => tp -> 3L).toMap), streamingTimeout.toMillis)
      },
      CheckAnswer(-21, -22, -11, -12, 2, 12),
      Execute { q =>
        sendMessagesWithTimestamp(topic, Array(23, 24, 25).map(_.toString), 4, secondTimestamp)
        // wait to reach the new last offset in every partition
        val partitions = (0 to 3).map(new TopicPartition(topic, _)).map(tp => tp -> 3L) ++
          Seq(new TopicPartition(topic, 4) -> 6L)
        q.awaitOffset(
          0, KafkaSourceOffset(partitions.toMap), streamingTimeout.toMillis)
      },
      CheckNewAnswer(23, 24, 25)
    )
  }

  private def assertQueryFailOnStartOffsetStrategyAsError(df: Dataset[_]): Unit = {
    // In continuous mode, the origin exception is not caught here unfortunately, so we have to
    // stick with checking general exception instead of verifying IllegalArgumentException.
    intercept[Exception] {
      testStream(df)(makeSureGetOffsetCalled)
    }
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
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    // Specifying an ending offset
    testBadOptions("endingOffsets" -> "latest")("Ending offset not valid in streaming queries")

    testBadOptions("subscribe" -> "t", "endingOffsetsByTimestamp" -> "{\"t\": {\"0\": 1000}}")(
      "Ending timestamp not valid in streaming queries")

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
    testBadOptions(
      "kafka.bootstrap.servers" -> "fake", "subscribe" -> "t", "minOffsetsPerTrigger" -> "20",
      "maxOffsetsPerTrigger" -> "15")(
      "value of minOffsetPerTrigger(20) is higher than the maxOffsetsPerTrigger(15)")
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
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("not supported"))
    }

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

  test("get offsets from case insensitive parameters") {
    for ((optionKey, optionValue, answer) <- Seq(
      (STARTING_OFFSETS_OPTION_KEY, "earLiEst", EarliestOffsetRangeLimit),
      (ENDING_OFFSETS_OPTION_KEY, "laTest", LatestOffsetRangeLimit),
      (STARTING_OFFSETS_OPTION_KEY, """{"topic-A":{"0":23}}""",
        SpecificOffsetRangeLimit(Map(new TopicPartition("topic-A", 0) -> 23))))) {
      val offset = getKafkaOffsetRangeLimit(
        CaseInsensitiveMap[String](Map(optionKey -> optionValue)), "dummy", "dummy", optionKey,
        answer)
      assert(offset === answer)
    }

    for ((optionKey, answer) <- Seq(
      (STARTING_OFFSETS_OPTION_KEY, EarliestOffsetRangeLimit),
      (ENDING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit))) {
      val offset = getKafkaOffsetRangeLimit(
        CaseInsensitiveMap[String](Map.empty), "dummy", "dummy", optionKey, answer)
      assert(offset === answer)
    }
  }

  private def assignString(topic: String, partitions: Iterable[Int]): String = {
    JsonUtils.partitions(partitions.map(p => new TopicPartition(topic, p)))
  }

  private def testFromSpecificOffsets(
      topic: String,
      failOnDataLoss: Boolean,
      options: (String, String)*): Unit = {
    val partitionOffsets = Map(
      new TopicPartition(topic, 0) -> -2L,
      new TopicPartition(topic, 1) -> -1L,
      new TopicPartition(topic, 2) -> 0L,
      new TopicPartition(topic, 3) -> 1L,
      new TopicPartition(topic, 4) -> 2L
    )
    val startingOffsets = JsonUtils.partitionOffsets(partitionOffsets)

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
      .option("failOnDataLoss", failOnDataLoss.toString)
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      Execute { q =>
        // wait to reach the last offset in every partition
        q.awaitOffset(0,
          KafkaSourceOffset(partitionOffsets.mapValues(_ => 3L).toMap), streamingTimeout.toMillis)
      },
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22),
      StopStream,
      StartStream(),
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22), // Should get the data back on recovery
      AddKafkaData(Set(topic), 30, 31, 32, 33, 34)(ensureDataInMultiplePartition = true),
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22, 30, 31, 32, 33, 34),
      StopStream
    )
  }

  private def testFromSpecificTimestamps(
      topic: String,
      failOnDataLoss: Boolean,
      addPartitions: Boolean,
      options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)

    val firstTimestamp = System.currentTimeMillis() - 5000
    val secondTimestamp = firstTimestamp + 1000
    setupTestMessagesForTestOnTimestampOffsets(topic, firstTimestamp, secondTimestamp)
    // no data after second timestamp for partition 4

    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    // we intentionally starts from second timestamp,
    // except for partition 4 - it starts from first timestamp
    val startPartitionTimestamps: Map[TopicPartition, Long] = Map(
      (0 to 3).map(new TopicPartition(topic, _) -> secondTimestamp): _*
    ) ++ Map(new TopicPartition(topic, 4) -> firstTimestamp)
    val startingTimestamps = JsonUtils.partitionTimestamps(startPartitionTimestamps)

    val mapped = setupDataFrameForTestOnTimestampOffsets(startingTimestamps, failOnDataLoss,
      options: _*)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      Execute { q =>
        val partitions = (0 to 4).map(new TopicPartition(topic, _))
        // wait to reach the last offset in every partition
        q.awaitOffset(
          0, KafkaSourceOffset(partitions.map(tp => tp -> 3L).toMap), streamingTimeout.toMillis)
      },
      CheckAnswer(-21, -22, -11, -12, 2, 12, 20, 21, 22),
      StopStream,
      StartStream(),
      CheckAnswer(-21, -22, -11, -12, 2, 12, 20, 21, 22), // Should get the data back on recovery
      StopStream,
      AddKafkaData(Set(topic), 30, 31, 32), // Add data when stream is stopped
      StartStream(),
      CheckAnswer(-21, -22, -11, -12, 2, 12, 20, 21, 22, 30, 31, 32), // Should get the added data
      AssertOnQuery("Add partitions") { query: StreamExecution =>
        if (addPartitions) setTopicPartitions(topic, 10, query)
        true
      },
      AddKafkaData(Set(topic), 40, 41, 42, 43, 44)(ensureDataInMultiplePartition = true),
      CheckAnswer(-21, -22, -11, -12, 2, 12, 20, 21, 22, 30, 31, 32, 40, 41, 42, 43, 44),
      StopStream
    )
  }

  private def testFromGlobalTimestamp(
      topic: String,
      failOnDataLoss: Boolean,
      addPartitions: Boolean,
      options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)

    val firstTimestamp = System.currentTimeMillis() - 5000
    val secondTimestamp = firstTimestamp + 1000
    setupTestMessagesForTestOnTimestampOffsets(topic, firstTimestamp, secondTimestamp)
    // here we should add records in partition 4 which match with second timestamp
    // as the query will break if there's no matching records
    sendMessagesWithTimestamp(topic, Array(23, 24).map(_.toString), 4, secondTimestamp)

    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    // we intentionally starts from second timestamp for all partitions
    // via setting global partition
    val mapped = setupDataFrameForTestOnGlobalTimestamp(secondTimestamp, failOnDataLoss,
      options: _*)
    testStream(mapped)(
      makeSureGetOffsetCalled,
      Execute { q =>
        // wait to reach the last offset in every partition
        val partAndOffsets = (0 to 4).map(new TopicPartition(topic, _)).map { tp =>
          if (tp.partition() < 4) {
            tp -> 3L
          } else {
            tp -> 5L // we added 2 more records to partition 4
          }
        }.toMap
        q.awaitOffset(0, KafkaSourceOffset(partAndOffsets), streamingTimeout.toMillis)
      },
      CheckAnswer(-21, -22, -11, -12, 2, 12, 23, 24),
      StopStream,
      StartStream(),
      CheckAnswer(-21, -22, -11, -12, 2, 12, 23, 24), // Should get the data back on recovery
      StopStream,
      AddKafkaData(Set(topic), 30, 31, 32), // Add data when stream is stopped
      StartStream(),
      CheckAnswer(-21, -22, -11, -12, 2, 12, 23, 24, 30, 31, 32), // Should get the added data
      AssertOnQuery("Add partitions") { query: StreamExecution =>
        if (addPartitions) setTopicPartitions(topic, 10, query)
        true
      },
      AddKafkaData(Set(topic), 40, 41, 42, 43, 44)(ensureDataInMultiplePartition = true),
      CheckAnswer(-21, -22, -11, -12, 2, 12, 23, 24, 30, 31, 32, 40, 41, 42, 43, 44),
      StopStream
    )
  }

  private def sendMessagesWithTimestamp(
      topic: String,
      msgs: Seq[String],
      part: Int,
      ts: Long): Unit = {
    val records = msgs.map { msg =>
      new RecordBuilder(topic, msg).partition(part).timestamp(ts).build()
    }
    testUtils.sendMessages(records)
  }

  private def setupTestMessagesForTestOnTimestampOffsets(
      topic: String,
      firstTimestamp: Long,
      secondTimestamp: Long): Unit = {
    sendMessagesWithTimestamp(topic, Array(-20).map(_.toString), 0, firstTimestamp)
    sendMessagesWithTimestamp(topic, Array(-10).map(_.toString), 1, firstTimestamp)
    sendMessagesWithTimestamp(topic, Array(0, 1).map(_.toString), 2, firstTimestamp)
    sendMessagesWithTimestamp(topic, Array(10, 11).map(_.toString), 3, firstTimestamp)
    sendMessagesWithTimestamp(topic, Array(20, 21, 22).map(_.toString), 4, firstTimestamp)

    sendMessagesWithTimestamp(topic, Array(-21, -22).map(_.toString), 0, secondTimestamp)
    sendMessagesWithTimestamp(topic, Array(-11, -12).map(_.toString), 1, secondTimestamp)
    sendMessagesWithTimestamp(topic, Array(2).map(_.toString), 2, secondTimestamp)
    sendMessagesWithTimestamp(topic, Array(12).map(_.toString), 3, secondTimestamp)
    // no data after second timestamp for partition 4
  }

  private def setupDataFrameForTestOnTimestampOffsets(
      startingTimestamps: String,
      failOnDataLoss: Boolean,
      options: (String, String)*): Dataset[_] = {
    val reader = spark
      .readStream
      .format("kafka")
      .option("startingOffsetsByTimestamp", startingTimestamps)
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("failOnDataLoss", failOnDataLoss.toString)
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)
    mapped
  }

  private def setupDataFrameForTestOnGlobalTimestamp(
      startingTimestamp: Long,
      failOnDataLoss: Boolean,
      options: (String, String)*): Dataset[_] = {
    val reader = spark
      .readStream
      .format("kafka")
      .option("startingTimestamp", startingTimestamp)
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("failOnDataLoss", failOnDataLoss.toString)
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)
    mapped
  }

  test("Kafka column types") {
    val now = System.currentTimeMillis()
    val topic = newTopic()
    testUtils.createTopic(newTopic(), partitions = 1)
    testUtils.sendMessage(
      new RecordBuilder(topic, "1")
        .headers(Seq(("a", "b".getBytes(UTF_8)), ("c", "d".getBytes(UTF_8)))).build()
    )

    val kafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("startingOffsets", s"earliest")
      .option("subscribe", topic)
      .option("includeHeaders", "true")
      .load()

    val query = kafka
      .writeStream
      .format("memory")
      .queryName("kafkaColumnTypes")
      .trigger(defaultTrigger)
      .start()
    eventually(timeout(streamingTimeout)) {
      assert(spark.table("kafkaColumnTypes").count == 1,
        s"Unexpected results: ${spark.table("kafkaColumnTypes").collectAsList()}")
    }
    val row = spark.table("kafkaColumnTypes").head()
    assert(row.getAs[Array[Byte]]("key") === null, s"Unexpected results: $row")
    assert(row.getAs[Array[Byte]]("value") === "1".getBytes(UTF_8), s"Unexpected results: $row")
    assert(row.getAs[String]("topic") === topic, s"Unexpected results: $row")
    assert(row.getAs[Int]("partition") === 0, s"Unexpected results: $row")
    assert(row.getAs[Long]("offset") === 0L, s"Unexpected results: $row")
    // We cannot check the exact timestamp as it's the time that messages were inserted by the
    // producer. So here we just use a low bound to make sure the internal conversion works.
    assert(row.getAs[java.sql.Timestamp]("timestamp").getTime >= now, s"Unexpected results: $row")
    assert(row.getAs[Int]("timestampType") === 0, s"Unexpected results: $row")

    def checkHeader(row: Row, expected: Seq[(String, Array[Byte])]): Unit = {
      // array<struct<key:string,value:binary>>
      val headers = row.getList[Row](row.fieldIndex("headers")).asScala
      assert(headers.length === expected.length)

      (0 until expected.length).foreach { idx =>
        val key = headers(idx).getAs[String]("key")
        val value = headers(idx).getAs[Array[Byte]]("value")
        assert(key === expected(idx)._1)
        assert(value === expected(idx)._2)
      }
    }

    checkHeader(row, Seq(("a", "b".getBytes(UTF_8)), ("c", "d".getBytes(UTF_8))))
    query.stop()
  }

  private def testFromLatestOffsets(
      topic: String,
      addPartitions: Boolean,
      failOnDataLoss: Boolean,
      options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, Array("-1"))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark
      .readStream
      .format("kafka")
      .option("startingOffsets", "latest")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("failOnDataLoss", failOnDataLoss.toString)
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
        if (addPartitions) setTopicPartitions(topic, 10, query)
        true
      },
      AddKafkaData(Set(topic), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromEarliestOffsets(
      topic: String,
      addPartitions: Boolean,
      failOnDataLoss: Boolean,
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
      .option("failOnDataLoss", failOnDataLoss.toString)
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
        if (addPartitions) setTopicPartitions(topic, 10, query)
        true
      },
      AddKafkaData(Set(topic), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  protected def testNullableKeyValue(trigger: Trigger): Unit = {
    val table = "kafka_null_key_value_source_test"
    withTable(table) {
      val topic = newTopic()
      testUtils.createTopic(topic)
      testUtils.withTransactionalProducer { producer =>
        val df = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", testUtils.brokerAddress)
          .option("kafka.isolation.level", "read_committed")
          .option("startingOffsets", "earliest")
          .option("subscribe", topic)
          .load()
          .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          .as[(String, String)]

        val q = df
          .writeStream
          .format("memory")
          .queryName(table)
          .trigger(trigger)
          .start()
        try {
          var idx = 0
          producer.beginTransaction()
          val expected1 = Seq.tabulate(5) { _ =>
            producer.send(new ProducerRecord[String, String](topic, null, null)).get()
            (null, null)
          }.asInstanceOf[Seq[(String, String)]]

          val expected2 = Seq.tabulate(5) { _ =>
            idx += 1
            producer.send(new ProducerRecord[String, String](topic, idx.toString, null)).get()
            (idx.toString, null)
          }.asInstanceOf[Seq[(String, String)]]

          val expected3 = Seq.tabulate(5) { _ =>
            idx += 1
            producer.send(new ProducerRecord[String, String](topic, null, idx.toString)).get()
            (null, idx.toString)
          }.asInstanceOf[Seq[(String, String)]]

          producer.commitTransaction()
          eventually(timeout(streamingTimeout)) {
            checkAnswer(spark.table(table), (expected1 ++ expected2 ++ expected3).toDF())
          }
        } finally {
          q.stop()
        }
      }
    }
  }
}

object KafkaSourceSuite {
  @volatile var globalTestUtils: KafkaTestUtils = _
  val collectedData = new ConcurrentLinkedQueue[Any]()
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
        .option("kafka.request.timeout.ms", "3000")
        .option("kafka.default.api.timeout.ms", "3000")
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
            val newTopic = newStressTopic
            topics = topics ++ Seq(newTopic)
            AddKafkaData(topics.toSet, d: _*)(message = s"Add topic $newTopic",
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
            AddKafkaData(topics.toSet, d: _*)(message = "Add partition",
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
