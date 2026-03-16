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

import java.nio.file.Files
import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkContext, ThreadAudit}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.streaming.RealTimeTrigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ResultsCollector, StreamingQuery, StreamRealTimeModeE2ESuiteBase, StreamRealTimeModeSuiteBase}
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class KafkaRealTimeModeE2ESuite extends KafkaSourceTest with StreamRealTimeModeE2ESuiteBase {

  override protected val defaultTrigger: RealTimeTrigger = RealTimeTrigger.apply("5 seconds")

  override protected def createSparkSession =
    new TestSparkSession(
      new SparkContext(
        "local[15]",
        "streaming-key-cuj"
      )
    )

  override def beforeEach(): Unit = {
    super[KafkaSourceTest].beforeEach()
    super[StreamRealTimeModeE2ESuiteBase].beforeEach()
  }

  def getKafkaConsumerProperties: Properties = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", testUtils.brokerAddress)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("compression.type", "snappy")

    props
  }

  test("Union two kafka streams, for each write to sink") {
    var q: StreamingQuery = null
    try {
      val topic1 = newTopic()
      val topic2 = newTopic()
      testUtils.createTopic(topic1, partitions = 2)
      testUtils.createTopic(topic2, partitions = 2)

      val props: Properties = getKafkaConsumerProperties
      val producer1: Producer[String, String] = new KafkaProducer[String, String](props)
      val producer2: Producer[String, String] = new KafkaProducer[String, String](props)

      val readStream1 = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("subscribe", topic1)
        .load()

      val readStream2 = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("subscribe", topic2)
        .load()

      val df = readStream1
        .union(readStream2)
        .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
        .selectExpr("key || ',' || value")
        .toDF()

      q = runStreamingQuery("union-kafka", df)

      waitForTasksToStart(4)

      val expectedResults = new mutable.ListBuffer[String]()
      for (batch <- 0 until 3) {
        (1 to 100).foreach(i => {
          producer1
            .send(
              new ProducerRecord[String, String](
                topic1,
                java.lang.Long.toString(i),
                s"input1-${batch}-${i}"
              )
            )
            .get()
          producer2
            .send(
              new ProducerRecord[String, String](
                topic2,
                java.lang.Long.toString(i),
                s"input2-${batch}-${i}"
              )
            )
            .get()
        })
        producer1.flush()
        producer2.flush()

        expectedResults ++= (1 to 100)
          .flatMap(v => {
            Seq(
              s"${v},input1-${batch}-${v}",
              s"${v},input2-${batch}-${v}"
            )
          })
          .toList

        eventually(timeout(60.seconds)) {
          ResultsCollector
            .get(sinkName)
            .toArray(new Array[String](ResultsCollector.get(sinkName).size()))
            .toList
            .sorted should equal(expectedResults.sorted)
        }
      }
    } finally {
      if (q != null) {
        q.stop()
      }
    }
  }
}


/**
 * Kafka Real-Time Integration test suite.
 * Tests with a distributed spark cluster with
 * separate executors processes deployed.
 */
class KafkaRealTimeIntegrationSuite
  extends KafkaSourceTest
    with StreamRealTimeModeSuiteBase
    with ThreadAudit
    with BeforeAndAfterEach
    with Matchers {

  override protected def createSparkSession =
    new TestSparkSession(
      new SparkContext(
        "local-cluster[3, 5, 1024]", // Ensure we have enough for both stages.
        "microbatch-context",
        sparkConf
          .set("spark.sql.testkey", "true")
          .set("spark.scheduler.mode", "FAIR")
          .set("spark.executor.extraJavaOptions", "-Dio.netty.leakDetection.level=paranoid")
      )
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    // testing to make sure the cluster is usable
    testUtils.createTopic("_test")
    testUtils.sendMessage(new ProducerRecord[String, String]("_test", "", ""))
    testUtils.deleteTopic("_test")
    logInfo("Kafka cluster setup complete....")

    eventually(timeout(10.seconds)) {
      val executors = sparkContext.getExecutorIds()
      assert(executors.size == 3, s"executors: ${executors}}")
    }
  }

  test("e2e stateless") {
    var query: StreamingQuery = null
    try {
      val inputTopic = newTopic()
      testUtils.createTopic(inputTopic, partitions = 5)

      val outputTopic = newTopic()
      testUtils.createTopic(outputTopic, partitions = 5)

      val props: Properties = new Properties()

      props.put("bootstrap.servers", testUtils.brokerAddress)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("compression.type", "snappy")

      val producer: Producer[String, String] = new KafkaProducer[String, String](props)

      query = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("kafka.metadata.max.age.ms", "1")
        .option("startingOffsets", "earliest")
        .option("subscribe", inputTopic)
        .option("kafka.fetch.max.wait.ms", 10)
        .load()
        .withColumn("value", substring(col("value"), 0, 500 * 1000))
        .withColumn("value", base64(col("value")))
        .withColumn(
          "headers",
          array(
            struct(
              lit("source-timestamp") as "key",
              unix_millis(col("timestamp")).cast("STRING").cast("BINARY") as "value"
            )
          )
        )
        .drop(col("timestamp"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("topic", outputTopic)
        .option("checkpointLocation", Files.createTempDirectory("some-prefix").toFile.getName)
        .option("kafka.max.block.ms", "100")
        .trigger(RealTimeTrigger.apply("5 minutes"))
        .outputMode(OutputMode.Update())
        .start()

      waitForTasksToStart(5)

      var expectedResults: ListBuffer[GenericRowWithSchema] = new ListBuffer
      for (i <- 0 until 3) {
        (1 to 100).foreach(i => {
          producer
            .send(
              new ProducerRecord[String, String](
                inputTopic,
                java.lang.Long.toString(i),
                s"payload-${i}"
              )
            )
            .get()
        })

        producer.flush()

        val kafkaSinkData = spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", testUtils.brokerAddress)
          .option("subscribe", outputTopic)
          .option("includeHeaders", "true")
          .option("startingOffsets", "earliest")
          .load()
          .withColumn("value", unbase64(col("value")).cast("STRING"))
          .withColumn("headers-map", map_from_entries(col("headers")))
          .withColumn("source-timestamp", conv(hex(col("headers-map.source-timestamp")), 16, 10))
          .withColumn("sink-timestamp", unix_millis(col("timestamp")))

        // Check the answers
        val newResults = (1 to 100)
          .map(v => {
            new GenericRowWithSchema(
              Array(s"payload-${v}"),
              schema = new StructType().add(StructField("value", StringType))
            )
          })
          .toList

        expectedResults ++= newResults
        expectedResults =
          expectedResults.sorted((x: GenericRowWithSchema, y: GenericRowWithSchema) => {
            x.getString(0).compareTo(y.getString(0))
          })

        eventually(timeout(1.minute)) {
          checkAnswer(kafkaSinkData.select("value"), expectedResults.toSeq)
        }
      }
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }
}
