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
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.time.SpanSugar._
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.sql.test.{SharedSQLContext, TestSparkSession}

trait KafkaContinuousTest extends KafkaSourceTest {
  override val defaultTrigger = Trigger.Continuous(100)
  override val defaultUseV2Sink = true

  // We need more than the default local[2] to be able to schedule all partitions simultaneously.
  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[10]",
      "continuous-stream-test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")))

  override protected def setTopicPartitions(
      topic: String, newCount: Int, query: StreamExecution) = {
    testUtils.addPartitions(topic, newCount)
    eventually(timeout(streamingTimeout)) {
      assert(
        query.lastExecution.logical.collectFirst {
          case DataSourceV2Relation(_, r: KafkaContinuousReader) => r
        }.exists(_.knownPartitions.size == newCount),
        s"query never reconfigured to $newCount partitions")
    }
  }

  test("ensure continuous stream is being used") {
    val query = spark.readStream
      .format("rate")
      .option("numPartitions", "1")
      .option("rowsPerSecond", "1")
      .load()

    testStream(query)(
      Execute(q => assert(q.isInstanceOf[ContinuousExecution]))
    )
  }
}

class KafkaContinuousSourceSuite extends KafkaSourceSuite with KafkaContinuousTest {

  import testImplicits._

  test("kafka sink") {
    withTempDir { dir =>
      val topic = newTopic()
      testUtils.createTopic(topic)
      val query = spark.readStream
        .format("rate")
        .option("numPartitions", "6")
        .option("rowsPerSecond", "10")
        .load()
        .select('value)
        .selectExpr("CAST(value as STRING) value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("topic", topic)
        .option("checkpointLocation", dir.getCanonicalPath)
        .trigger(Trigger.Continuous(100))
        .start()

      eventually(timeout(streamingTimeout)) {
        val results = spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", testUtils.brokerAddress)
          .option("startingOffsets", "earliest")
          .option("endingOffsets", "latest")
          .option("subscribe", topic)
          .load()
          .selectExpr("CAST(value as STRING) value")
          .selectExpr("CAST(value as INT) value")
          .collect()
        assert(Range(0, 20).map(Row(_)).toSet.subsetOf(results.toSet))
      }
      query.stop()
      query.awaitTermination()
    }
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
      Execute { query =>
        testUtils.deleteTopic(topic)
        testUtils.createTopic(topic2, partitions = 5)
        eventually(timeout(streamingTimeout)) {
          assert(
            query.lastExecution.logical.collectFirst {
              case DataSourceV2Relation(_, r: KafkaContinuousReader) => r
            }.exists { r =>
              // Ensure the new topic is present and the old topic is gone.
              r.knownPartitions.exists(_.topic == topic2)
            },
            s"query never reconfigured to new topic $topic2")
        }
      },
      AddKafkaData(Set(topic2), 4, 5, 6),
      CheckAnswer(2, 3, 4, 5, 6, 7)
    )
  }
}

class KafkaContinuousSourceStressSuite
  extends StreamTest with SharedSQLContext {

  import testImplicits._

  private var testUtils: KafkaTestUtils = _

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = s"failOnDataLoss-${topicId.getAndIncrement()}"

  override def createSparkSession(): TestSparkSession = {
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
        props.put("log.segment.bytes", "40")
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
      super.afterAll()
    }
  }

  test("stress test for failOnDataLoss=false") {
    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("subscribePattern", "failOnDataLoss.*")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .option("fetchOffset.retryIntervalMs", "3000")
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val query = kafka.map(kv => kv._2.toInt).writeStream
      .format("memory")
      .queryName("memory")
      .start()

    val testTime = 1.minutes
    val startTime = System.currentTimeMillis()
    // Track the current existing topics
    val topics = mutable.ArrayBuffer[String]()
    // Track topics that have been deleted
    val deletedTopics = mutable.Set[String]()
    while (System.currentTimeMillis() - testTime.toMillis < startTime) {
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
          Thread.sleep(1000)
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
