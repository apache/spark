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

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}
import org.apache.spark.sql.test.SharedSQLContext

class KafkaSinkSuite extends StreamTest with SharedSQLContext {
  import testImplicits._

  protected var testUtils: KafkaTestUtils = _

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
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

  test("write to stream") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val topic = newTopic()

      spark.udf.register("intCast", (s: Array[Byte]) => ByteBuffer.wrap(s).getInt())
      spark.udf.register("byteCast", (i: Int) => ByteBuffer.allocate(4).putInt(i).array())

      val writer = input.toDF()
        .selectExpr("byteCast(value) key", "byteCast(value) value")
        .writeStream
        .format("kafka")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .outputMode(OutputMode.Append)
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("defaultTopic", topic)
        .queryName("kafkaStream")
        .start()

      // Create Kafka source that reads from earliest to latest offset
      val reader = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("kafka.metadata.max.age.ms", "1")
        .option("subscribe", topic)
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("intCast(key)", "intCast(value)")
        .as[(Int, Int)]

      input.addData(1, 2, 3)
      writer.processAllAvailable()

      testStream(reader)(
        AddData(input, 1, 2, 3), // Add data when stream is stopped
        CheckAnswer((1, 1), (2, 2), (3, 3)),
        StopStream
      )

    }
  }
}
