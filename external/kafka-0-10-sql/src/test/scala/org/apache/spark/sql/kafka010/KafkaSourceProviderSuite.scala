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

import scala.collection.JavaConverters._

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.sql.connector.read.Scan
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
}
