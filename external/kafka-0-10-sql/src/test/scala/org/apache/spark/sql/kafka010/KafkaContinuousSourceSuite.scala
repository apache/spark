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

import org.apache.kafka.clients.producer.ProducerRecord

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming.continuous.ContinuousTrigger
import org.apache.spark.sql.streaming.Trigger

// Run tests in KafkaSourceSuiteBase in continuous execution mode.
class KafkaContinuousSourceSuite extends KafkaSourceSuiteBase with KafkaContinuousTest {
  import testImplicits._

  test("read Kafka transactional messages: read_committed") {
    val table = "kafka_continuous_source_test"
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
          .map(kv => kv._2.toInt)

        val q = df
          .writeStream
          .format("memory")
          .queryName(table)
          .trigger(ContinuousTrigger(100))
          .start()
        try {
          producer.beginTransaction()
          (1 to 5).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }

          // Should not read any messages before they are committed
          assert(spark.table(table).isEmpty)

          producer.commitTransaction()

          eventually(timeout(streamingTimeout)) {
            // Should read all committed messages
            checkAnswer(spark.table(table), (1 to 5).toDF)
          }

          producer.beginTransaction()
          (6 to 10).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }
          producer.abortTransaction()

          // Should not read aborted messages
          checkAnswer(spark.table(table), (1 to 5).toDF)

          producer.beginTransaction()
          (11 to 15).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }
          producer.commitTransaction()

          eventually(timeout(streamingTimeout)) {
            // Should skip aborted messages and read new committed ones.
            checkAnswer(spark.table(table), ((1 to 5) ++ (11 to 15)).toDF)
          }
        } finally {
          q.stop()
        }
      }
    }
  }

  test("read Kafka transactional messages: read_uncommitted") {
    val table = "kafka_continuous_source_test"
    withTable(table) {
      val topic = newTopic()
      testUtils.createTopic(topic)
      testUtils.withTransactionalProducer { producer =>
        val df = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", testUtils.brokerAddress)
          .option("kafka.isolation.level", "read_uncommitted")
          .option("startingOffsets", "earliest")
          .option("subscribe", topic)
          .load()
          .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          .as[(String, String)]
          .map(kv => kv._2.toInt)

        val q = df
          .writeStream
          .format("memory")
          .queryName(table)
          .trigger(ContinuousTrigger(100))
          .start()
        try {
          producer.beginTransaction()
          (1 to 5).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }

          eventually(timeout(streamingTimeout)) {
            // Should read uncommitted messages
            checkAnswer(spark.table(table), (1 to 5).toDF)
          }

          producer.commitTransaction()

          eventually(timeout(streamingTimeout)) {
            // Should read all committed messages
            checkAnswer(spark.table(table), (1 to 5).toDF)
          }

          producer.beginTransaction()
          (6 to 10).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }
          producer.abortTransaction()

          eventually(timeout(streamingTimeout)) {
            // Should read aborted messages
            checkAnswer(spark.table(table), (1 to 10).toDF)
          }

          producer.beginTransaction()
          (11 to 15).foreach { i =>
            producer.send(new ProducerRecord[String, String](topic, i.toString)).get()
          }

          eventually(timeout(streamingTimeout)) {
            // Should read all messages including committed, aborted and uncommitted messages
            checkAnswer(spark.table(table), (1 to 15).toDF)
          }

          producer.commitTransaction()

          eventually(timeout(streamingTimeout)) {
            // Should read all messages including committed and aborted messages
            checkAnswer(spark.table(table), (1 to 15).toDF)
          }
        } finally {
          q.stop()
        }
      }
    }
  }

  test("SPARK-27494: read kafka record containing null key/values.") {
    testNullableKeyValue(ContinuousTrigger(100))
  }
}

class KafkaContinuousSourceTopicDeletionSuite extends KafkaContinuousTest {
  import testImplicits._

  override val brokerProps = Map("auto.create.topics.enable" -> "false")

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
      Execute { query =>
        testUtils.deleteTopic(topic)
        testUtils.createTopic(topic2, partitions = 5)
        eventually(timeout(streamingTimeout)) {
          assert(
            query.lastExecution.logical.collectFirst {
              case StreamingDataSourceV2Relation(_, _, _, r: KafkaContinuousReader) => r
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

class KafkaContinuousSourceStressForDontFailOnDataLossSuite
    extends KafkaSourceStressForDontFailOnDataLossSuite {
  override protected def startStream(ds: Dataset[Int]) = {
    ds.writeStream
      .format("memory")
      .queryName("memory")
      .trigger(Trigger.Continuous("1 second"))
      .start()
  }
}
