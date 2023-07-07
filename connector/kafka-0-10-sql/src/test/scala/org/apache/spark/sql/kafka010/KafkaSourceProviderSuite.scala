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
import java.util.concurrent.ExecutionException

import scala.collection.JavaConverters._

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkEnv, SparkException, SparkFunSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class KafkaSourceProviderSuite extends SparkFunSuite with SharedSparkSession {
  private val expected = "1111"

  protected var testUtils: KafkaTestUtils = _

  override protected def afterEach(): Unit = {
    super.afterEach()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
  }

  override protected def afterAll(): Unit = {
    try {
      if (testUtils != null) {
        testUtils.teardown()
        testUtils = null
      }
    } finally {
      super.afterAll()
    }
  }

  test("batch mode - options should be handled as case-insensitive") {
    verifyFieldsInBatch(KafkaSourceProvider.CONSUMER_POLL_TIMEOUT, expected, batch => {
      assert(expected.toLong === batch.pollTimeoutMs)
    })
  }

  /*
    the goal of this test is to verify the functionality of the aws msk IAM auth
    how this test works:
    if testType contains source/sink, kafka is used as a source/sink option respectively
    if testType contains stream/batch, it is used either in readStream/read or writeStream/write

    In each case, we test that the library paths are discoverable since
    if the library was not to be found, another error message would be thrown.
    Although this broker exists, it does not have IAM capabilities and thus
    it is expected that a timeout error will be thrown.
  */
  Seq("source and stream", "sink and stream",
    "source and batch", "sink and batch").foreach { testType =>
    test(s"test MSK IAM auth on kafka '$testType' side") {
      val options: Map[String, String] = Map(
        "kafka.bootstrap.servers" -> testUtils.brokerAddress,
        "subscribe" -> "msk-123",
        "startingOffsets" -> "earliest",
        "kafka.sasl.mechanism" -> "AWS_MSK_IAM",
        "kafka.sasl.jaas.config" ->
          "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.security.protocol" -> "SASL_SSL",
        "kafka.sasl.client.callback.handler.class" ->
          "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        "retries" -> "0",
        "kafka.request.timeout.ms" -> "3000",
        "kafka.max.block.ms" -> "3000"
      )

      testUtils.createTopic(options("subscribe"))

      var e: Throwable = null
      if (testType.contains("stream")) {
        if (testType.contains("source")) {
          e = intercept[StreamingQueryException] {
            spark.readStream.format("kafka").options(options).load()
              .writeStream.format("console").start().processAllAvailable()
          }
          TestUtils.assertExceptionMsg(e, "Timed out waiting for a node assignment")
        } else {
          e = intercept[StreamingQueryException] {
            spark.readStream.format("rate").option("rowsPerSecond", 10).load()
              .withColumn("value", col("value").cast(StringType)).writeStream
              .format("kafka").options(options).option("checkpointLocation", "temp/testing")
              .option("topic", options("subscribe")).start().processAllAvailable()
          }
          TestUtils.assertExceptionMsg(e, s"TimeoutException: Topic ${options("subscribe")} " +
            s"not present in metadata")
        }
      } else {
        if (testType.contains("source")) {
          e = intercept[ExecutionException] {
            spark.read.format("kafka").options(options).load()
              .write.format("console").save()
          }
          TestUtils.assertExceptionMsg(e, "Timed out waiting for a node assignment")
        } else {
          val schema = new StructType().add("value", "string")
          e = intercept[SparkException] {
            spark.createDataFrame(Seq(Row("test"), Row("test2")).asJava, schema)
              .write.mode("append").format("kafka")
              .options(options).option("checkpointLocation", "temp/testing/1")
              .option("topic", options("subscribe")).save()
          }
          TestUtils.assertExceptionMsg(e, s"TimeoutException: Topic ${options("subscribe")} " +
            s"not present in metadata")
        }
      }

      testUtils.deleteTopic(options("subscribe"))
    }
  }

  test("micro-batch mode - options should be handled as case-insensitive") {
    verifyFieldsInMicroBatchStream(KafkaSourceProvider.CONSUMER_POLL_TIMEOUT, expected, stream => {
      assert(expected.toLong === stream.pollTimeoutMs)
    })
    verifyFieldsInMicroBatchStream(KafkaSourceProvider.MAX_OFFSET_PER_TRIGGER, expected, stream => {
      assert(Some(expected.toLong) === stream.maxOffsetsPerTrigger)
    })
    verifyFieldsInMicroBatchStream(KafkaSourceProvider.MIN_OFFSET_PER_TRIGGER, expected, stream => {
      assert(Some(expected.toLong) === stream.minOffsetPerTrigger)
    })
    verifyFieldsInMicroBatchStream(KafkaSourceProvider.MAX_TRIGGER_DELAY, expected, stream => {
      assert(expected.toLong === stream.maxTriggerDelayMs)
    })
    verifyFieldsInMicroBatchStream(KafkaSourceProvider.FETCH_OFFSET_NUM_RETRY, expected, stream => {
      assert(expected.toInt === stream.kafkaOffsetReader.maxOffsetFetchAttempts)
    })
    verifyFieldsInMicroBatchStream(KafkaSourceProvider.FETCH_OFFSET_RETRY_INTERVAL_MS, expected,
        stream => {
      assert(expected.toLong === stream.kafkaOffsetReader.offsetFetchAttemptIntervalMs)
    })
    SparkEnv.set(null)
  }

  test("continuous mode - options should be handled as case-insensitive") {
    verifyFieldsInContinuousStream(KafkaSourceProvider.CONSUMER_POLL_TIMEOUT, expected, stream => {
      assert(expected.toLong === stream.pollTimeoutMs)
    })
    verifyFieldsInContinuousStream(KafkaSourceProvider.FETCH_OFFSET_NUM_RETRY, expected, stream => {
      assert(expected.toInt === stream.offsetReader.maxOffsetFetchAttempts)
    })
    verifyFieldsInContinuousStream(KafkaSourceProvider.FETCH_OFFSET_RETRY_INTERVAL_MS, expected,
        stream => {
      assert(expected.toLong === stream.offsetReader.offsetFetchAttemptIntervalMs)
    })
  }

  private def verifyFieldsInBatch(
      key: String,
      value: String,
      validate: (KafkaBatch) => Unit): Unit = {
    buildCaseInsensitiveStringMapForUpperAndLowerKey(key -> value).foreach { options =>
      val scan = getKafkaDataSourceScan(options)
      val batch = scan.toBatch().asInstanceOf[KafkaBatch]
      validate(batch)
    }
  }

  private def verifyFieldsInMicroBatchStream(
      key: String,
      value: String,
      validate: (KafkaMicroBatchStream) => Unit): Unit = {
    // KafkaMicroBatchStream reads Spark conf from SparkEnv for default value
    // hence we set mock SparkEnv here before creating KafkaMicroBatchStream
    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.conf).thenReturn(new SparkConf())
    SparkEnv.set(sparkEnv)

    buildCaseInsensitiveStringMapForUpperAndLowerKey(key -> value).foreach { options =>
      val scan = getKafkaDataSourceScan(options)
      val stream = scan.toMicroBatchStream("dummy").asInstanceOf[KafkaMicroBatchStream]
      validate(stream)
    }
  }

  private def verifyFieldsInContinuousStream(
      key: String,
      value: String,
      validate: (KafkaContinuousStream) => Unit): Unit = {
    buildCaseInsensitiveStringMapForUpperAndLowerKey(key -> value).foreach { options =>
      val scan = getKafkaDataSourceScan(options)
      val stream = scan.toContinuousStream("dummy").asInstanceOf[KafkaContinuousStream]
      validate(stream)
    }
  }

  private def buildCaseInsensitiveStringMapForUpperAndLowerKey(
      options: (String, String)*): Seq[CaseInsensitiveStringMap] = {
    Seq(options.map(entry => (entry._1.toUpperCase(Locale.ROOT), entry._2)),
      options.map(entry => (entry._1.toLowerCase(Locale.ROOT), entry._2)))
      .map(buildKafkaSourceCaseInsensitiveStringMap)
  }

  private def buildKafkaSourceCaseInsensitiveStringMap(
      options: (String, String)*): CaseInsensitiveStringMap = {
    val requiredOptions = Map("kafka.bootstrap.servers" -> "dummy", "subscribe" -> "dummy")
    new CaseInsensitiveStringMap((options.toMap ++ requiredOptions).asJava)
  }

  private def getKafkaDataSourceScan(options: CaseInsensitiveStringMap): Scan = {
    val provider = new KafkaSourceProvider()
    provider.getTable(options).newScanBuilder(options).build()
  }
}
