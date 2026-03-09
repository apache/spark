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
package org.apache.spark.sql.connect.utils

import scala.jdk.CollectionConverters._

import com.google.rpc.ErrorInfo

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.SparkStringUtils

class ErrorUtilsSuite extends SharedSparkSession {

  private def makeThrowable(
      errorClass: String,
      messageParameters: Map[String, String]): Throwable with SparkThrowable = {
    new Exception(s"Test error for $errorClass") with SparkThrowable {
      override def getCondition: String = errorClass
      override def getMessageParameters: java.util.Map[String, String] =
        messageParameters.asJava
    }
  }

  private def buildAndExtractErrorInfo(
      st: Throwable,
      sessionHolderOpt: Option[SessionHolder] = None): ErrorInfo = {
    val status = ErrorUtils.buildStatusFromThrowable(st, sessionHolderOpt)
    status.getDetailsList.asScala
      .find(_.is(classOf[ErrorInfo]))
      .map(_.unpack(classOf[ErrorInfo]))
      .getOrElse(fail("No ErrorInfo found in status details"))
  }

  test("buildStatusFromThrowable includes errorClass and messageParameters when small enough") {
    val throwable = makeThrowable("TEST_ERROR", Map("key" -> "value"))
    val info = buildAndExtractErrorInfo(throwable)
    assert(info.getMetadataMap.get("errorClass") === "TEST_ERROR")
    assert(info.getMetadataMap.containsKey("messageParameters"))
    assert(!info.getMetadataMap.containsKey("errorClassFallback"))
  }

  test("buildStatusFromThrowable uses errorClassFallback when messageParameters exceed limit") {
    // Create messageParameters whose JSON representation exceeds 1024 bytes (default limit)
    val largeParams = Map("key" -> ("x" * 2000))
    val throwable = makeThrowable("TEST_ERROR_LARGE_PARAMS", largeParams)
    val info = buildAndExtractErrorInfo(throwable)
    assert(!info.getMetadataMap.containsKey("errorClass"))
    assert(!info.getMetadataMap.containsKey("messageParameters"))
    assert(info.getMetadataMap.get("errorClassFallback") === "TEST_ERROR_LARGE_PARAMS")
  }

  test("buildStatusFromThrowable does not include errorId when no session holder is provided") {
    val throwable = makeThrowable("TEST_ERROR", Map.empty)
    val info = buildAndExtractErrorInfo(throwable)
    assert(!info.getMetadataMap.containsKey("errorId"))
  }

  test("buildStatusFromThrowable includes errorId and stores throwable when enrichError enabled") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.session.conf.set(Connect.CONNECT_ENRICH_ERROR_ENABLED.key, "true")
    try {
      val throwable = makeThrowable("TEST_ERROR", Map.empty)
      val info = buildAndExtractErrorInfo(throwable, Some(sessionHolder))
      val errorId = info.getMetadataMap.get("errorId")
      assert(errorId !== null)
      assert(sessionHolder.errorIdToError.getIfPresent(errorId) === throwable)
    } finally {
      sessionHolder.session.conf.unset(Connect.CONNECT_ENRICH_ERROR_ENABLED.key)
    }
  }

  test("buildStatusFromThrowable does not include errorId when enrichError is disabled") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.session.conf.set(Connect.CONNECT_ENRICH_ERROR_ENABLED.key, "false")
    try {
      val throwable = makeThrowable("TEST_ERROR", Map.empty)
      val info = buildAndExtractErrorInfo(throwable, Some(sessionHolder))
      assert(!info.getMetadataMap.containsKey("errorId"))
      assert(sessionHolder.errorIdToError.size() === 0)
    } finally {
      sessionHolder.session.conf.unset(Connect.CONNECT_ENRICH_ERROR_ENABLED.key)
    }
  }

  test("buildStatusFromThrowable does not include stackTrace when stackTrace is disabled") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.session.conf.set(SQLConf.PYSPARK_JVM_STACKTRACE_ENABLED.key, "false")
    try {
      val throwable = makeThrowable("TEST_ERROR", Map.empty)
      val info = buildAndExtractErrorInfo(throwable, Some(sessionHolder))
      assert(!info.getMetadataMap.containsKey("stackTrace"))
    } finally {
      sessionHolder.session.conf.unset(SQLConf.PYSPARK_JVM_STACKTRACE_ENABLED.key)
    }
  }

  test("buildStatusFromThrowable includes stackTrace when stackTrace is enabled") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.session.conf.set(SQLConf.PYSPARK_JVM_STACKTRACE_ENABLED.key, "true")
    try {
      val throwable = makeThrowable("TEST_ERROR", Map.empty)
      val info = buildAndExtractErrorInfo(throwable, Some(sessionHolder))
      assert(info.getMetadataMap.containsKey("stackTrace"))
      assert(info.getMetadataMap.get("stackTrace").nonEmpty)
    } finally {
      sessionHolder.session.conf.unset(SQLConf.PYSPARK_JVM_STACKTRACE_ENABLED.key)
    }
  }

  // U+4E2D: a CJK character that encodes to 3 UTF-8 bytes but counts as 1 Java char,
  // producing different results under byte-based vs length-based size checks.
  private val cjk = 0x4e2d.toChar.toString

  test("abbreviateByBytes drops incomplete trailing multi-byte sequence") {
    // budget=7 cuts after 1 byte of the 3rd CJK char (3 bytes each), leaving an incomplete
    // sequence that CodingErrorAction.IGNORE must drop. Without IGNORE the result would
    // contain a replacement character (U+FFFD) and fail the equality check.
    assert(SparkStringUtils.abbreviateByBytes(cjk * 10, "...", 10) === cjk + cjk + "...")
  }

  test("messageParameters with multi-byte chars exceeding byte limit are excluded") {
    // 340 CJK chars: JSON ~1030 bytes (> 1024 limit) but only ~350 chars (< 1024).
    val throwable = makeThrowable("TEST_ERROR", Map("key" -> (cjk * 340)))
    val info = buildAndExtractErrorInfo(throwable)
    assert(!info.getMetadataMap.containsKey("errorClass"))
    assert(!info.getMetadataMap.containsKey("messageParameters"))
    assert(info.getMetadataMap.get("errorClassFallback") === "TEST_ERROR")
  }

  test("messageParameters with multi-byte chars within byte limit are included") {
    // 300 CJK chars: JSON ~910 bytes (<= 1024 limit).
    val throwable = makeThrowable("TEST_ERROR", Map("key" -> (cjk * 300)))
    val info = buildAndExtractErrorInfo(throwable)
    assert(info.getMetadataMap.containsKey("errorClass"))
    assert(info.getMetadataMap.containsKey("messageParameters"))
  }

  test("buildStatusFromThrowable truncates stackTrace to byte size limit") {
    // Exception message with 400 CJK chars (~1200 bytes) ensures truncation falls within
    // the multi-byte content. The result must end with a complete CJK char + "...", proving
    // the cut respected byte boundaries. A length-based cut would retain ~3x more bytes.
    val throwable = new Exception(cjk * 400) with SparkThrowable {
      override def getCondition: String = "TEST_ERROR"
      override def getMessageParameters: java.util.Map[String, String] =
        Map.empty[String, String].asJava
    }
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.session.conf.set(SQLConf.PYSPARK_JVM_STACKTRACE_ENABLED.key, "true")
    try {
      val info = buildAndExtractErrorInfo(throwable, Some(sessionHolder))
      val maxMetadataSize =
        spark.sparkContext.conf.getLong(Connect.CONNECT_GRPC_MAX_METADATA_SIZE.key, 1024L)
      val jvmMaxSize =
        spark.sparkContext.conf.getInt(Connect.CONNECT_JVM_STACK_TRACE_MAX_SIZE.key, 1024)
      val maxSize = Math.min(jvmMaxSize, maxMetadataSize).toInt
      val stackTrace = info.getMetadataMap.get("stackTrace")
      assert(stackTrace != null && stackTrace.nonEmpty)
      assert(SparkStringUtils.sizeInBytes(stackTrace) <= maxSize)
      assert(stackTrace.endsWith(cjk + "..."))
      assert(stackTrace.length < maxSize)
    } finally {
      sessionHolder.session.conf.unset(SQLConf.PYSPARK_JVM_STACKTRACE_ENABLED.key)
    }
  }
}
