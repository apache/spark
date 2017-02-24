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

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{BinaryType, DataType}

class KafkaSinkSuite extends StreamTest with SharedSQLContext {
  import testImplicits._

  case class AddMoreData(ms: MemoryStream[String], q: StreamingQuery,
      values: String*) extends ExternalAction {
    override def runAction(): Unit = {
      ms.addData(values)
      q.processAllAvailable()
      Thread.sleep(5000) // wait for data to appear in Kafka
    }
  }

  protected var testUtils: KafkaTestUtils = _

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(withBrokerProps = Map("auto.create.topics.enable" -> "false"))
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  test("write to stream with topic field") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[String]
      val topic = newTopic()
      testUtils.createTopic(topic)

      val writer = input.toDF()
        .selectExpr(s"'$topic' as topic", "value")
        .writeStream
        .format("kafka")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .outputMode(OutputMode.Append)
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .queryName("kafkaStream")
        .start()

      // Create Kafka source that reads from earliest to latest offset
      val reader = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("kafka.metadata.max.age.ms", "1")
        .option("startingOffsets", "earliest")
        .option("subscribe", topic)
        .option("failOnDataLoss", "true")
        .load()
      val kafka = reader
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]
      val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.trim.toInt)

      testStream(mapped, outputMode = OutputMode.Append)(
        StartStream(ProcessingTime(0)),
        AddMoreData(input, writer, "1", "2", "3", "4", "5"),

        CheckAnswer(1, 2, 3, 4, 5),
        AddMoreData(input, writer, "6", "7", "8", "9", "10"),
        CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        StopStream
      )
      writer.stop()
    }
  }

  test("write structured streaming aggregation w/o topic field, with default topic") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[String]
      val topic = newTopic()
      testUtils.createTopic(topic)

      val writer = input.toDF()
        .groupBy("value")
        .count()
        .selectExpr("CAST(value as STRING) key", "CAST(count as STRING) value")
        .writeStream
        .format("kafka")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .outputMode(OutputMode.Update)
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("defaultTopic", topic)
        .queryName("kafkaAggStream")
        .start()

      // Create Kafka source that reads from earliest to latest offset
      val reader = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("kafka.metadata.max.age.ms", "1")
        .option("startingOffsets", "earliest")
        .option("subscribe", topic)
        .option("failOnDataLoss", "true")
        .load()
      val kafka = reader
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .selectExpr("CAST(key AS INT)", "CAST(value AS INT)")
        .as[(Int, Int)]

      testStream(kafka, outputMode = OutputMode.Update)(
        StartStream(ProcessingTime(0)),
        AddMoreData(input, writer, "1", "2", "2", "3", "3", "3"),

        CheckAnswer((1, 1), (2, 2), (3, 3)),
        AddMoreData(input, writer, "1", "2", "3"),
        CheckAnswer((1, 1), (2, 2), (3, 3), (1, 2), (2, 3), (3, 4)),
        StopStream
      )
      writer.stop()
    }
  }

  test("write data with bad schema") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[String]
      val topic = newTopic()
      testUtils.createTopic(topic)

      /* No topic field or default topic */
      var writer: StreamingQuery = null
      var ex = intercept[StreamingQueryException] {
        writer = input.toDF()
          .selectExpr("value as key", "value")
          .writeStream
          .format("kafka")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .option("kafka.bootstrap.servers", testUtils.brokerAddress)
          .queryName("kafkaNoTopicFieldStream")
          .start()
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
      writer.stop()
      assert(ex.getMessage
        .toLowerCase
        .contains("default topic required when no 'topic' attribute is present"))

      /* No value field */
      ex = intercept[StreamingQueryException] {
        writer = input.toDF()
          .selectExpr(s"'$topic' as topic", "value as key")
          .writeStream
          .format("kafka")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .option("kafka.bootstrap.servers", testUtils.brokerAddress)
          .queryName("kafkaNoValueFieldStream")
          .start()
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
      writer.stop()
      assert(ex.getMessage.toLowerCase.contains("required attribute 'value' not found"))
    }
  }

  test("write data with valid schema but wrong types") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[String]
      val topic = newTopic()
      testUtils.createTopic(topic)

      /* value field wrong type */
      var writer: StreamingQuery = null
      var ex = intercept[StreamingQueryException] {
        writer = input.toDF()
          .selectExpr(s"'$topic' as topic", "CAST(value as INT) as value")
          .writeStream
          .format("kafka")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .option("kafka.bootstrap.servers", testUtils.brokerAddress)
          .queryName("kafkaIntValueFieldStream")
          .start()
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
      writer.stop()
      assert(ex.getMessage.toLowerCase.contains(
        "value attribute type must be a string or binarytype"))

      /* key field wrong type */
      ex = intercept[StreamingQueryException] {
        writer = input.toDF()
          .selectExpr(s"'$topic' as topic", "CAST(value as INT) as key", "value")
          .writeStream
          .format("kafka")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .option("kafka.bootstrap.servers", testUtils.brokerAddress)
          .queryName("kafkaIntValueFieldStream")
          .start()
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
      writer.stop()
      assert(ex.getMessage.toLowerCase.contains(
        "key attribute type must be a string or binarytype"))
    }
  }

  test("write to non-existing topic") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[String]
      val topic = newTopic()

      var writer: StreamingQuery = null
      val ex = intercept[StreamingQueryException] {
        writer = input.toDF()
          .writeStream
          .format("kafka")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .option("kafka.bootstrap.servers", testUtils.brokerAddress)
          .option("defaultTopic", topic)
          .queryName("kafkaBadTopicStream")
          .start()
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
      writer.stop()
      assert(ex.getMessage.toLowerCase.contains("job aborted"))
    }
  }

  test("write batch to kafka") {
    val topic = newTopic()
    testUtils.createTopic(topic)
    val df = spark
      .sparkContext
      .parallelize(Seq("1", "2", "3", "4", "5"))
      .map(v => (topic, v))
      .toDF("topic", "value")

    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("defaultTopic", topic)
      .save()
  }

  test("write batch with null topic field value, and no default topic") {
    val df = spark
      .sparkContext
      .parallelize(Seq("1"))
      .map(v => (null.asInstanceOf[String], v))
      .toDF("topic", "value")

    val ex = intercept[SparkException] {
      df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .save()
    }
    assert(ex.getMessage.toLowerCase.contains(
      "null topic present in the data"))
  }

  test("write big data with small producer buffer") {
    val topic = newTopic()
    testUtils.createTopic(topic, 1)
    val options = new java.util.HashMap[String, Object]
    options.put("bootstrap.servers", testUtils.brokerAddress)
    options.put("buffer.memory", "16384") // min buffer size
    val inputSchema = Seq(AttributeReference("value", BinaryType)())
    val data = new Array[Byte](15000) // large value
    val writeTask = new KafkaWriteTask(options, inputSchema, Some(topic))
    writeTask.execute(new Iterator[InternalRow]() {
      var count = 0
      override def hasNext: Boolean = count < 1000

      override def next(): InternalRow = {
        count += 1
        val fieldTypes: Array[DataType] = Array(BinaryType)
        val converter = UnsafeProjection.create(fieldTypes)

        val row = new SpecificInternalRow(fieldTypes)
        row.update(0, data)
        converter.apply(row)
      }
    })
    writeTask.close()
  }
}
