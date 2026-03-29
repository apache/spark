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

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.util.Random

import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.util.ResetSystemProperties

/**
 * This is a basic test trait which will set up a Kafka cluster that keeps only several records in
 * a topic and ages out records very quickly. This is a helper trait to test
 * "failonDataLoss=false" case with missing offsets.
 *
 * Note: there is a hard-code 30 seconds delay (kafka.log.LogManager.InitialTaskDelayMs) to clean up
 * records. Hence each class extending this trait needs to wait at least 30 seconds (or even longer
 * when running on a slow Jenkins machine) before records start to be removed. To make sure a test
 * does see missing offsets, you can check the earliest offset in `eventually` and make sure it's
 * not 0 rather than sleeping a hard-code duration.
 */
trait KafkaMissingOffsetsTest extends SharedSparkSession with ResetSystemProperties {

  protected var testUtils: KafkaTestUtils = _

  override def createSparkSession: TestSparkSession = {
    // Set maxRetries to 3 to handle NPE from `poll` when deleting a topic
    new TestSparkSession(new SparkContext("local[2,3]", "test-sql-context", sparkConf))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils {
      override def brokerConfiguration: Properties = {
        val props = super.brokerConfiguration
        // Try to make Kafka clean up messages as fast as possible. However, there is a hard-code
        // 30 seconds delay (kafka.log.LogManager.InitialTaskDelayMs) so this test should run at
        // least 30 seconds.
        props.put("log.cleaner.backoff.ms", "100")
        // The size of RecordBatch V2 increases to support transactional write.
        props.put("log.segment.bytes", "70")
        props.put("log.retention.bytes", "40")
        props.put("log.retention.check.interval.ms", "100")
        props.put("delete.retention.ms", "10")
        props.put("log.flush.scheduler.interval.ms", "10")
        props
      }
    }
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    super.afterAll()
  }
}

class KafkaDontFailOnDataLossSuite extends StreamTest with KafkaMissingOffsetsTest {

  import testImplicits._

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = s"failOnDataLoss-${topicId.getAndIncrement()}"

  /**
   * @param testStreamingQuery whether to test a streaming query or a batch query.
   * @param writeToTable the function to write the specified [[DataFrame]] to the given table.
   */
  private def verifyMissingOffsetsDontCauseDuplicatedRecords(
      testStreamingQuery: Boolean)(writeToTable: (DataFrame, String) => Unit): Unit = {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, (0 until 50).map(_.toString).toArray)

    eventually(timeout(1.minute)) {
      assert(
        testUtils.getEarliestOffsets(Set(topic)).head._2 > 0,
        "Kafka didn't delete records after 1 minute")
    }

    val table = "DontFailOnDataLoss"
    withTable(table) {
      val kafkaOptions = Map(
        "kafka.bootstrap.servers" -> testUtils.brokerAddress,
        "kafka.metadata.max.age.ms" -> "1",
        "subscribe" -> topic,
        "startingOffsets" -> s"""{"$topic":{"0":0}}""",
        "failOnDataLoss" -> "false",
        "kafkaConsumer.pollTimeoutMs" -> "1000")
      val df =
        if (testStreamingQuery) {
          val reader = spark.readStream.format("kafka")
          kafkaOptions.foreach(kv => reader.option(kv._1, kv._2))
          reader.load()
        } else {
          val reader = spark.read.format("kafka")
          kafkaOptions.foreach(kv => reader.option(kv._1, kv._2))
          reader.load()
        }
      writeToTable(df.selectExpr("CAST(value AS STRING)"), table)
      val result = spark.table(table).as[String].collect().toList
      assert(result.distinct.size === result.size, s"$result contains duplicated records")
      // Make sure Kafka did remove some records so that this test is valid.
      assert(result.size > 0 && result.size < 50)
    }
  }

  test("failOnDataLoss=false should not return duplicated records: microbatch v1") {
    withSQLConf(
      SQLConf.DISABLED_V2_STREAMING_MICROBATCH_READERS.key ->
        classOf[KafkaSourceProvider].getCanonicalName) {
      verifyMissingOffsetsDontCauseDuplicatedRecords(testStreamingQuery = true) { (df, table) =>
        val query = df.writeStream.format("memory").queryName(table).start()
        try {
          query.processAllAvailable()
        } finally {
          query.stop()
        }
      }
    }
  }

  test("failOnDataLoss=false should not return duplicated records: microbatch v2") {
    verifyMissingOffsetsDontCauseDuplicatedRecords(testStreamingQuery = true) { (df, table) =>
      val query = df.writeStream.format("memory").queryName(table).start()
      try {
        query.processAllAvailable()
      } finally {
        query.stop()
      }
    }
  }

  test("failOnDataLoss=false should not return duplicated records: continuous processing") {
    verifyMissingOffsetsDontCauseDuplicatedRecords(testStreamingQuery = true) { (df, table) =>
      val query = df.writeStream
        .format("memory")
        .queryName(table)
        .trigger(Trigger.Continuous(100))
        .start()
      try {
        // `processAllAvailable` doesn't work for continuous processing, so just wait until the last
        // record appears in the table.
        eventually(timeout(streamingTimeout)) {
          assert(spark.table(table).as[String].collect().contains("49"))
        }
      } finally {
        query.stop()
      }
    }
  }

  test("failOnDataLoss=false should not return duplicated records: batch v1") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "kafka") {
      verifyMissingOffsetsDontCauseDuplicatedRecords(testStreamingQuery = false) { (df, table) =>
        df.write.saveAsTable(table)
      }
    }
  }

  test("failOnDataLoss=false should not return duplicated records: batch v2") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      verifyMissingOffsetsDontCauseDuplicatedRecords(testStreamingQuery = false) { (df, table) =>
        df.write.saveAsTable(table)
      }
    }
  }
}

class KafkaSourceStressForDontFailOnDataLossSuite extends StreamTest with KafkaMissingOffsetsTest {

  import testImplicits._

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = s"failOnDataLoss-${topicId.getAndIncrement()}"

  protected def startStream(ds: Dataset[Int]) = {
    ds.writeStream.foreach(new ForeachWriter[Int] {

      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Int): Unit = {
        // Slow down the processing speed so that messages may be aged out.
        Thread.sleep(Random.nextInt(500))
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }).start()
  }

  test("stress test for failOnDataLoss=false") {
    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("kafka.request.timeout.ms", "3000")
      .option("kafka.default.api.timeout.ms", "3000")
      .option("subscribePattern", "failOnDataLoss.*")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .option("fetchOffset.retryIntervalMs", "3000")
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val query = startStream(kafka.map(kv => kv._2.toInt))

    val testTimeNs = TimeUnit.SECONDS.toNanos(20)
    val startTimeNs = System.nanoTime()
    // Track the current existing topics
    val topics = mutable.ArrayBuffer[String]()
    // Track topics that have been deleted
    val deletedTopics = mutable.Set[String]()
    while (System.nanoTime() - startTimeNs < testTimeNs) {
      Random.nextInt(10) match {
        case 0 => // Create a new topic
          val topic = newTopic()
          topics += topic
          // As pushing messages into Kafka updates Zookeeper asynchronously, there is a small
          // chance that a topic will be recreated after deletion due to the asynchronous update.
          // Hence, always overwrite to handle this race condition.
          testUtils.createTopic(topic, partitions = 1, overwrite = true)
          logInfo(s"Create topic $topic")
        case 1 if topics.nonEmpty => // Delete an existing topic
          val topic = topics.remove(Random.nextInt(topics.size))
          testUtils.deleteTopic(topic)
          logInfo(s"Delete topic $topic")
          deletedTopics += topic
        case 2 if deletedTopics.nonEmpty => // Recreate a topic that was deleted.
          val topic = deletedTopics.toSeq(Random.nextInt(deletedTopics.size))
          deletedTopics -= topic
          topics += topic
          // As pushing messages into Kafka updates Zookeeper asynchronously, there is a small
          // chance that a topic will be recreated after deletion due to the asynchronous update.
          // Hence, always overwrite to handle this race condition.
          testUtils.createTopic(topic, partitions = 1, overwrite = true)
          logInfo(s"Create topic $topic")
        case 3 =>
          Thread.sleep(100)
        case _ => // Push random messages
          for (topic <- topics) {
            val size = Random.nextInt(10)
            for (_ <- 0 until size) {
              testUtils.sendMessages(topic, Array(Random.nextInt(10).toString))
            }
          }
      }
      // `failOnDataLoss` is `false`, we should not fail the query
      if (query.exception.nonEmpty) {
        throw query.exception.get
      }
    }

    query.stop()
    // `failOnDataLoss` is `false`, we should not fail the query
    if (query.exception.nonEmpty) {
      throw query.exception.get
    }
  }
}
