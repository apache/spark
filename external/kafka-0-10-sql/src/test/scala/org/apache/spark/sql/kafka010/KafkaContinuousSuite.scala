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
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row}
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

class KafkaContinuousSourceSuite extends KafkaSourceSuiteBase with KafkaContinuousTest {

  import testImplicits._
  /* test("subscribing topic by pattern with topic deletions") {
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
  } */
}

class KafkaContinuousSourceStressForDontFailOnDataLossSuite
    extends KafkaSourceStressForDontFailOnDataLossSuite {
  override protected def startStream(ds: Dataset[Int]) = {
    ds.writeStream
      .format("memory")
      .queryName("memory")
      .start()
  }
}
