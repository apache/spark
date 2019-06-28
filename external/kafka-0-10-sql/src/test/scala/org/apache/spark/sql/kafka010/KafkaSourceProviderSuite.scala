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

import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.sources.v2.reader.Scan
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class KafkaSourceProviderSuite extends SparkFunSuite with PrivateMethodTester {
  private val pollTimeoutMsMethod = PrivateMethod[Long]('pollTimeoutMs)

  test("SPARK-28142 - continuous mode - options should be handled as case-insensitive") {
    def getPollTimeoutMsFromContinuousStream(options: CaseInsensitiveStringMap): Long = {
      val scan = getKafkaDataSourceScan(options)
      val stream = scan.toContinuousStream("dummy").asInstanceOf[KafkaContinuousStream]
      getValue(stream, pollTimeoutMsMethod)
    }

    // we're trying to read the value of "pollTimeout" to see whether option is handled correctly

    // upper-case
    val expectedValue = 1000
    val mapWithUppercase = buildCaseInsensitiveStringMap(
      KafkaSourceProvider.CONSUMER_POLL_TIMEOUT.toUpperCase(Locale.ROOT) -> expectedValue.toString)
    assert(expectedValue === getPollTimeoutMsFromContinuousStream(mapWithUppercase))

    // lower-case
    val mapWithLowercase = buildCaseInsensitiveStringMap(
      KafkaSourceProvider.CONSUMER_POLL_TIMEOUT.toLowerCase(Locale.ROOT) -> expectedValue.toString)
    assert(expectedValue === getPollTimeoutMsFromContinuousStream(mapWithLowercase))
  }

  private def buildCaseInsensitiveStringMap(
      options: (String, String)*): CaseInsensitiveStringMap = {
    val requiredOptions = Map("kafka.bootstrap.servers" -> "dummy", "subscribe" -> "dummy")
    new CaseInsensitiveStringMap((options.toMap ++ requiredOptions).asJava)
  }

  private def getKafkaDataSourceScan(options: CaseInsensitiveStringMap): Scan = {
    val provider = new KafkaSourceProvider()
    provider.getTable(options).newScanBuilder(options).build()
  }

  private def getValue[T](obj: AnyRef, method: PrivateMethod[T]): T = {
    obj.invokePrivate(method())
  }
}
