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

import scala.jdk.CollectionConverters._

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class KafkaSourceProviderSuite extends SparkFunSuite {

  private val expected = "1111"

  override protected def afterEach(): Unit = {
    SparkEnv.set(null)
    super.afterEach()
  }

  test("batch mode - options should be handled as case-insensitive") {
    verifyFieldsInBatch(KafkaSourceProvider.CONSUMER_POLL_TIMEOUT, expected, batch => {
      assert(expected.toLong === batch.pollTimeoutMs)
    })
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

  test("SPARK-49442: partition.metadata.cache.ttl.ms validation") {
    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.conf).thenReturn(new SparkConf())
    SparkEnv.set(sparkEnv)

    // -1 (disabled) and positive values are valid; validation fires inside toMicroBatchStream
    Seq("-1", "1", "30000").foreach { v =>
      val options = buildKafkaSourceCaseInsensitiveStringMap(
        KafkaSourceProvider.PARTITION_METADATA_CACHE_TTL_MS -> v)
      getKafkaDataSourceScan(options).toMicroBatchStream("dummy")
    }
    // 0 and other non-positive values (except -1) are invalid
    Seq("0", "-2", "-100").foreach { v =>
      val options = buildKafkaSourceCaseInsensitiveStringMap(
        KafkaSourceProvider.PARTITION_METADATA_CACHE_TTL_MS -> v)
      intercept[IllegalArgumentException] {
        getKafkaDataSourceScan(options).toMicroBatchStream("dummy")
      }
    }
  }

  test("SPARK-59328: disallowed Kafka options are rejected on the source path") {
    // KafkaBatch reads its default poll timeout from SparkEnv, so provide a mock one.
    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.conf).thenReturn(new SparkConf())
    SparkEnv.set(sparkEnv)

    val options = buildKafkaSourceCaseInsensitiveStringMap("kafka.max.poll.records" -> "1")
    // Empty denylist (the default) preserves the previous behavior: the option is accepted.
    getKafkaDataSourceScan(options).toBatch()
    // When the option name is denylisted, building the batch scan is rejected.
    val conf = new SQLConf()
    conf.setConf(StaticSQLConf.KAFKA_DISALLOWED_OPTIONS, Seq("max.poll.records"))
    SQLConf.withExistingConf(conf) {
      checkError(
        exception = intercept[KafkaIllegalArgumentException] {
          getKafkaDataSourceScan(options).toBatch()
        },
        condition = "KAFKA_DISALLOWED_OPTION",
        parameters = Map(
          "option" -> "max.poll.records",
          "config" -> "spark.sql.kafka.disallowedOptions"))
    }
  }

  test("SPARK-59328: disallowed Kafka options are rejected on the sink path") {
    val params = CaseInsensitiveMap(Map(
      "kafka.bootstrap.servers" -> "dummy",
      "kafka.max.poll.records" -> "1"))
    // Empty denylist (the default) preserves the previous behavior: the option is accepted.
    KafkaSourceProvider.kafkaParamsForProducer(params)
    // When the option name is denylisted, building the producer params is rejected.
    val conf = new SQLConf()
    conf.setConf(StaticSQLConf.KAFKA_DISALLOWED_OPTIONS, Seq("max.poll.records"))
    SQLConf.withExistingConf(conf) {
      checkError(
        exception = intercept[KafkaIllegalArgumentException] {
          KafkaSourceProvider.kafkaParamsForProducer(params)
        },
        condition = "KAFKA_DISALLOWED_OPTION",
        parameters = Map(
          "option" -> "max.poll.records",
          "config" -> "spark.sql.kafka.disallowedOptions"))
    }
  }

  test("SPARK-59328: the disallowed-options denylist is an operator boundary") {
    // Static, so an application cannot SET it away at runtime.
    assert(!new SQLConf().isModifiable(StaticSQLConf.KAFKA_DISALLOWED_OPTIONS.key))
    // Entries carrying the "kafka." prefix are rejected, so the denylist cannot silently fail
    // open (a "kafka." prefixed name would never match the stripped option names it is checked
    // against). setConfString is the path that runs the value converter and its checkValue.
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new SQLConf().setConfString(
          StaticSQLConf.KAFKA_DISALLOWED_OPTIONS.key, "kafka.max.poll.records")
      },
      condition = "INVALID_CONF_VALUE.REQUIREMENT",
      parameters = Map(
        "confName" -> "spark.sql.kafka.disallowedOptions",
        "confValue" -> "kafka.max.poll.records",
        "confRequirement" -> "Kafka option names must be listed without the 'kafka.' prefix."))
  }
}
