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

package org.apache.spark.scheduler

import java.util.{Locale, Properties}

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.{LogEntry, Logging, LogKey, LogKeys, MessageWithContext}
import org.apache.spark.scheduler.StructuredStreamingIdAwareSchedulerLogging.{BATCH_ID_KEY, QUERY_ID_KEY}

class StructuredStreamingIdAwareSchedulerLoggingSuite extends SparkFunSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()
    Logging.enableStructuredLogging()
  }

  override def afterAll(): Unit = {
    Logging.disableStructuredLogging()
    super.afterAll()
  }

  private def assertContextValue(
      context: java.util.Map[String, String],
      key: LogKey,
      expected: String): Unit = {
    assert(context.get(key.name.toLowerCase(Locale.ROOT)) === expected)
  }

  private def assertContextAbsent(
      context: java.util.Map[String, String],
      key: LogKey): Unit = {
    assert(!context.containsKey(key.name.toLowerCase(Locale.ROOT)))
  }

  private val testQueryId = "abc-query-id"
  private val testBatchId = "42"

  private def propsWithBothIds(): Properties = {
    val props = new Properties()
    props.setProperty(QUERY_ID_KEY, testQueryId)
    props.setProperty(BATCH_ID_KEY, testBatchId)
    props
  }

  private def propsWithQueryIdOnly(): Properties = {
    val props = new Properties()
    props.setProperty(QUERY_ID_KEY, testQueryId)
    props
  }

  test("SPARK-56326: constructStreamingLogEntry with String - both queryId and batchId") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(
        propsWithBothIds(), "test message", enabled = true, queryIdLength = -1)

    assertResult(s"[queryId = $testQueryId] [batchId = $testBatchId] test message")(
      result.message)
    assertContextValue(result.context, LogKeys.QUERY_ID, testQueryId)
    assertContextValue(result.context, LogKeys.BATCH_ID, testBatchId)
  }

  test("SPARK-56326: constructStreamingLogEntry with String - only queryId") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(
        propsWithQueryIdOnly(), "test message", enabled = true, queryIdLength = -1)

    assertResult(s"[queryId = $testQueryId] test message")(result.message)
    assertContextValue(result.context, LogKeys.QUERY_ID, testQueryId)
    assertContextAbsent(result.context, LogKeys.BATCH_ID)
  }

  test("SPARK-56326: constructStreamingLogEntry with String - no streaming properties") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(
        new Properties(), "test message", enabled = true, queryIdLength = -1)

    assertResult("test message")(result.message)
    assert(result.context.isEmpty)
  }

  test("SPARK-56326: constructStreamingLogEntry with String - null properties") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(
        null, "test message", enabled = true, queryIdLength = -1)

    assertResult("test message")(result.message)
    assert(result.context.isEmpty)
  }

  test("SPARK-56326: constructStreamingLogEntry with String - empty queryId") {
    val props = new Properties()
    props.setProperty(QUERY_ID_KEY, "")
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(props, "test message", enabled = true, queryIdLength = -1)

    assertResult("test message")(result.message)
    assert(result.context.isEmpty)
  }

  test("SPARK-56326: constructStreamingLogEntry with String - empty batchId") {
    val props = new Properties()
    props.setProperty(QUERY_ID_KEY, testQueryId)
    props.setProperty(BATCH_ID_KEY, "")
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(props, "test message", enabled = true, queryIdLength = -1)

    assertResult(s"[queryId = $testQueryId] test message")(result.message)
    assertContextValue(result.context, LogKeys.QUERY_ID, testQueryId)
    assertContextAbsent(result.context, LogKeys.BATCH_ID)
  }

  test("SPARK-56326: constructStreamingLogEntry with LogEntry - both queryId and batchId") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(propsWithBothIds(),
        log"test message ${MDC(LogKeys.MESSAGE, "Dummy Context")}",
        enabled = true,
        queryIdLength = -1)

    assertResult(s"[queryId = $testQueryId] " +
      s"[batchId = $testBatchId] test message Dummy Context")(
      result.message)
    assertContextValue(result.context, LogKeys.QUERY_ID, testQueryId)
    assertContextValue(result.context, LogKeys.BATCH_ID, testBatchId)
    assertContextValue(result.context, LogKeys.MESSAGE, "Dummy Context")
  }

  test("SPARK-56326: constructStreamingLogEntry with LogEntry - only queryId") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(propsWithQueryIdOnly(),
        log"test message ${MDC(LogKeys.MESSAGE, "Dummy Context")}",
        enabled = true,
        queryIdLength = -1)

    assertResult(s"[queryId = $testQueryId] test message Dummy Context")(result.message)
    assertContextValue(result.context, LogKeys.QUERY_ID, testQueryId)
    assertContextAbsent(result.context, LogKeys.BATCH_ID)
    assertContextValue(result.context, LogKeys.MESSAGE, "Dummy Context")
  }

  test("SPARK-56326: constructStreamingLogEntry with LogEntry - no streaming properties") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(new Properties(),
        log"test message ${MDC(LogKeys.MESSAGE, "Dummy Context")}",
        enabled = true,
        queryIdLength = -1)

    assertResult("test message Dummy Context")(result.message)
    assertContextAbsent(result.context, LogKeys.QUERY_ID)
    assertContextAbsent(result.context, LogKeys.BATCH_ID)
    assertContextValue(result.context, LogKeys.MESSAGE, "Dummy Context")
  }

  test("SPARK-56326: constructStreamingLogEntry with LogEntry - null properties") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(null,
        log"test message ${MDC(LogKeys.MESSAGE, "Dummy Context")}",
        enabled = true,
        queryIdLength = -1)

    assertResult("test message Dummy Context")(result.message)
    assertContextAbsent(result.context, LogKeys.QUERY_ID)
    assertContextAbsent(result.context, LogKeys.BATCH_ID)
    assertContextValue(result.context, LogKeys.MESSAGE, "Dummy Context")
  }


  test("SPARK-56326: constructStreamingLogEntry with LogEntry - empty queryId") {
    val props = new Properties()
    props.setProperty(QUERY_ID_KEY, "")
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(props,
        log"test message ${MDC(LogKeys.MESSAGE, "Dummy Context")}",
        enabled = true,
        queryIdLength = -1)

    assertResult("test message Dummy Context")(result.message)
    assertContextAbsent(result.context, LogKeys.QUERY_ID)
    assertContextAbsent(result.context, LogKeys.BATCH_ID)
    assertContextValue(result.context, LogKeys.MESSAGE, "Dummy Context")
  }

  test("SPARK-56326: constructStreamingLogEntry with LogEntry - empty batchId") {
    val props = new Properties()
    props.setProperty(QUERY_ID_KEY, testQueryId)
    props.setProperty(BATCH_ID_KEY, "")
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(props,
        log"test message ${MDC(LogKeys.MESSAGE, "Dummy Context")}",
        enabled = true,
        queryIdLength = -1)

    assertResult(s"[queryId = $testQueryId] test message Dummy Context")(result.message)
    assertContextValue(result.context, LogKeys.QUERY_ID, testQueryId)
    assertContextAbsent(result.context, LogKeys.BATCH_ID)
    assertContextValue(result.context, LogKeys.MESSAGE, "Dummy Context")
  }

  test("SPARK-56326: constructStreamingLogEntry with LogEntry - defers evaluation") {
    var evaluated = false
    val lazyEntry = new LogEntry({
      evaluated = true
      MessageWithContext("lazy message", java.util.Collections.emptyMap())
    })

    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(
        propsWithBothIds(), lazyEntry, enabled = true, queryIdLength = -1)

    // Work should be deferred
    assert(!evaluated,
      "LogEntry should not be evaluated during constructStreamingLogEntry")

    // Accessing .message triggers evaluation
    result.message
    assert(evaluated, "LogEntry should be evaluated when .message is accessed")
  }

  test("SPARK-56326: constructStreamingLogEntry with LogEntry - defers property access") {
    var propertiesAccessed = false
    val props = new Properties() {
      override def getProperty(key: String): String = {
        propertiesAccessed = true
        super.getProperty(key)
      }
    }
    props.setProperty(QUERY_ID_KEY, testQueryId)
    props.setProperty(BATCH_ID_KEY, testBatchId)

    val entry = log"test message ${MDC(LogKeys.MESSAGE, "Dummy Context")}"
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(
        props, entry, enabled = true, queryIdLength = -1)

    assert(!propertiesAccessed,
      "Properties should not be accessed during constructStreamingLogEntry")

    result.message
    assert(propertiesAccessed,
      "Properties should be accessed when .message is called")
  }

  test("SPARK-56326: constructStreamingLogEntry with String - disabled skips enrichment") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(
        propsWithBothIds(), "test message", enabled = false, queryIdLength = -1)

    assertResult("test message")(result.message)
    assert(result.context.isEmpty)
  }

  test("SPARK-56326: constructStreamingLogEntry with LogEntry - disabled skips enrichment") {
    val entry = log"test message ${MDC(LogKeys.MESSAGE, "Dummy Context")}"
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(
        propsWithBothIds(), entry, enabled = false, queryIdLength = -1)

    assertResult("test message Dummy Context")(result.message)
    assertContextAbsent(result.context, LogKeys.QUERY_ID)
    assertContextAbsent(result.context, LogKeys.BATCH_ID)
    assertContextValue(result.context, LogKeys.MESSAGE, "Dummy Context")
  }

  test("SPARK-56326: constructStreamingLogEntry with String - queryIdLength truncates queryId") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(
        propsWithBothIds(), "test message", enabled = true, queryIdLength = 5)

    val truncatedId = testQueryId.take(5)
    assertResult(s"[queryId = $truncatedId] [batchId = $testBatchId] test message")(
      result.message)
    assertContextValue(result.context, LogKeys.QUERY_ID, truncatedId)
    assertContextValue(result.context, LogKeys.BATCH_ID, testBatchId)
  }

  test("SPARK-56326: constructStreamingLogEntry with LogEntry - queryIdLength truncates queryId") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(
        propsWithBothIds(),
        log"test message ${MDC(LogKeys.MESSAGE, "Dummy Context")}",
        enabled = true,
        queryIdLength = 5)

    val truncatedId = testQueryId.take(5)
    assertResult(s"[queryId = $truncatedId] " +
      s"[batchId = $testBatchId] test message Dummy Context")(
      result.message)
    assertContextValue(result.context, LogKeys.QUERY_ID, truncatedId)
    assertContextValue(result.context, LogKeys.BATCH_ID, testBatchId)
  }

  test("SPARK-56326: constructStreamingLogEntry - " +
    "queryIdLength greater than id length returns full id") {
    val result = StructuredStreamingIdAwareSchedulerLogging
      .constructStreamingLogEntry(
        propsWithBothIds(), "test message", enabled = true, queryIdLength = 1000)

    assertResult(s"[queryId = $testQueryId] [batchId = $testBatchId] test message")(
      result.message)
    assertContextValue(result.context, LogKeys.QUERY_ID, testQueryId)
  }
}
