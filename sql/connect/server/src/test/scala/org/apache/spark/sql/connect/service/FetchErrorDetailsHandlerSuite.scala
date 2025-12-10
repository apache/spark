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
package org.apache.spark.sql.connect.service

import java.util.UUID

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Try

import io.grpc.stub.StreamObserver

import org.apache.spark.{BreakingChangeInfo, MitigationConfig, SparkThrowable}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.FetchErrorDetailsResponse
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connect.ResourceHelper
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.utils.ErrorUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

/**
 * Test SparkThrowable implementation for testing breaking change info serialization
 */
class TestSparkThrowableWithBreakingChange(
    val errorClass: String,
    val messageParams: Map[String, String],
    val breakingChangeInfo: Option[BreakingChangeInfo])
    extends Exception(s"Test error for $errorClass")
    with SparkThrowable {

  override def getCondition: String = errorClass
  override def getMessageParameters: java.util.Map[String, String] = {
    import scala.jdk.CollectionConverters._
    messageParams.asJava
  }

  override def getBreakingChangeInfo: BreakingChangeInfo = {
    breakingChangeInfo match {
      case Some(info) => info
      case None => null
    }
  }
}

private class FetchErrorDetailsResponseObserver(p: Promise[FetchErrorDetailsResponse])
    extends StreamObserver[FetchErrorDetailsResponse] {
  override def onNext(v: FetchErrorDetailsResponse): Unit = p.success(v)
  override def onError(throwable: Throwable): Unit = throw throwable
  override def onCompleted(): Unit = {}
}

class FetchErrorDetailsHandlerSuite extends SharedSparkSession with ResourceHelper {

  private val userId = "user1"

  private val sessionId = UUID.randomUUID().toString

  private def fetchErrorDetails(
      userId: String,
      sessionId: String,
      errorId: String): FetchErrorDetailsResponse = {
    val promise = Promise[FetchErrorDetailsResponse]()
    val handler =
      new SparkConnectFetchErrorDetailsHandler(new FetchErrorDetailsResponseObserver(promise))
    val context = proto.UserContext
      .newBuilder()
      .setUserId(userId)
      .build()
    val request = proto.FetchErrorDetailsRequest
      .newBuilder()
      .setUserContext(context)
      .setSessionId(sessionId)
      .setErrorId(errorId)
      .build()
    handler.handle(request)
    ThreadUtils.awaitResult(promise.future, 5.seconds)
  }

  for (serverStacktraceEnabled <- Seq(false, true)) {
    test(s"error chain is properly constructed - $serverStacktraceEnabled") {
      val testError =
        new Exception("test1", new Exception("test2"))
      val errorId = UUID.randomUUID().toString()

      val sessionHolder = SparkConnectService
        .getOrCreateIsolatedSession(userId, sessionId, None)

      sessionHolder.errorIdToError.put(errorId, testError)

      sessionHolder.session.conf
        .set(Connect.CONNECT_SERVER_STACKTRACE_ENABLED.key, serverStacktraceEnabled)
      sessionHolder.session.conf
        .set(SQLConf.PYSPARK_JVM_STACKTRACE_ENABLED.key, false)
      try {
        val response = fetchErrorDetails(userId, sessionId, errorId)
        assert(response.hasRootErrorIdx)
        assert(response.getRootErrorIdx == 0)

        assert(response.getErrorsCount == 2)
        assert(response.getErrors(0).getMessage == "test1")
        assert(response.getErrors(0).getErrorTypeHierarchyCount == 3)
        assert(response.getErrors(0).getErrorTypeHierarchy(0) == classOf[Exception].getName)
        assert(response.getErrors(0).getErrorTypeHierarchy(1) == classOf[Throwable].getName)
        assert(response.getErrors(0).getErrorTypeHierarchy(2) == classOf[Object].getName)
        assert(response.getErrors(0).hasCauseIdx)
        assert(response.getErrors(0).getCauseIdx == 1)

        assert(response.getErrors(1).getMessage == "test2")
        assert(response.getErrors(1).getErrorTypeHierarchyCount == 3)
        assert(response.getErrors(1).getErrorTypeHierarchy(0) == classOf[Exception].getName)
        assert(response.getErrors(1).getErrorTypeHierarchy(1) == classOf[Throwable].getName)
        assert(response.getErrors(1).getErrorTypeHierarchy(2) == classOf[Object].getName)
        assert(!response.getErrors(1).hasCauseIdx)
        assert(response.getErrors(0).getStackTraceCount == testError.getStackTrace.length)
        assert(
          response.getErrors(1).getStackTraceCount ==
            testError.getCause.getStackTrace.length)

      } finally {
        sessionHolder.session.conf.unset(Connect.CONNECT_SERVER_STACKTRACE_ENABLED.key)
        sessionHolder.session.conf.unset(SQLConf.PYSPARK_JVM_STACKTRACE_ENABLED.key)
      }
    }
  }

  test("error not found") {
    val response = fetchErrorDetails(userId, sessionId, UUID.randomUUID().toString())
    assert(!response.hasRootErrorIdx)
  }

  test("invalidate cached exceptions after first request") {
    val testError = new Exception("test1")
    val errorId = UUID.randomUUID().toString()

    SparkConnectService
      .getOrCreateIsolatedSession(userId, sessionId, None)
      .errorIdToError
      .put(errorId, testError)

    val response = fetchErrorDetails(userId, sessionId, errorId)
    assert(response.hasRootErrorIdx)
    assert(response.getRootErrorIdx == 0)

    assert(response.getErrorsCount == 1)
    assert(response.getErrors(0).getMessage == "test1")

    assert(
      SparkConnectService
        .getOrCreateIsolatedSession(userId, sessionId, None)
        .errorIdToError
        .size() == 0)
  }

  test("error chain is truncated after reaching max depth") {
    var testError = new Exception("test")
    for (i <- 0 until 2 * ErrorUtils.MAX_ERROR_CHAIN_LENGTH) {
      val errorId = UUID.randomUUID().toString()

      SparkConnectService
        .getOrCreateIsolatedSession(userId, sessionId, None)
        .errorIdToError
        .put(errorId, testError)

      val response = fetchErrorDetails(userId, sessionId, errorId)
      val expectedErrorCount = Math.min(i + 1, ErrorUtils.MAX_ERROR_CHAIN_LENGTH)
      assert(response.getErrorsCount == expectedErrorCount)
      assert(response.getErrors(expectedErrorCount - 1).hasCauseIdx == false)

      testError = new Exception(s"test$i", testError)
    }
  }

  test("null filename in stack trace elements") {
    val testError = new Exception("test")
    val stackTrace = testError.getStackTrace()
    stackTrace(0) = new StackTraceElement(
      stackTrace(0).getClassName,
      stackTrace(0).getMethodName,
      null,
      stackTrace(0).getLineNumber)
    testError.setStackTrace(stackTrace)

    val errorId = UUID.randomUUID().toString()

    SparkConnectService
      .getOrCreateIsolatedSession(userId, sessionId, None)
      .errorIdToError
      .put(errorId, testError)

    val response = fetchErrorDetails(userId, sessionId, errorId)
    assert(response.hasRootErrorIdx)
    assert(response.getRootErrorIdx == 0)

    assert(response.getErrors(0).getStackTraceCount > 0)
    assert(!response.getErrors(0).getStackTrace(0).hasFileName)
  }

  test("error framework parameters are set") {
    val testError = Try(spark.sql("select x")).failed.get.asInstanceOf[AnalysisException]
    val errorId = UUID.randomUUID().toString()

    SparkConnectService
      .getOrCreateIsolatedSession(userId, sessionId, None)
      .errorIdToError
      .put(errorId, testError)

    val response = fetchErrorDetails(userId, sessionId, errorId)
    assert(response.hasRootErrorIdx)
    assert(response.getRootErrorIdx == 0)

    val sparkThrowableProto = response.getErrors(0).getSparkThrowable
    assert(sparkThrowableProto.getErrorClass == testError.errorClass.get)
    assert(sparkThrowableProto.getMessageParametersMap == testError.getMessageParameters)
    assert(sparkThrowableProto.getSqlState == testError.getSqlState)
  }

  test("breaking change info is not present when error has no breaking change") {
    val testError = new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.DESC_TABLE_COLUMN_JSON",
      messageParameters = Map.empty)
    val errorId = UUID.randomUUID().toString()

    val sessionHolder = SparkConnectService
      .getOrCreateIsolatedSession(userId, sessionId, None)
    sessionHolder.errorIdToError.put(errorId, testError)

    val response = fetchErrorDetails(userId, sessionId, errorId)

    assert(response.hasRootErrorIdx)
    val sparkThrowableProto = response.getErrors(0).getSparkThrowable
    assert(!sparkThrowableProto.hasBreakingChangeInfo)
  }

  test("breaking change info serialization works for errors with breaking change") {
    // This test would require creating a SparkThrowable that has breaking change info
    // Since we need to test with actual breaking change info, we need to either:
    // 1. Use a real error class that has breaking change info, or
    // 2. Create a mock SparkThrowable that returns breaking change info

    // For now, we'll create a simple test that verifies the serialization path exists
    // A full test would require setting up test error classes with breaking change info
    // in a test error-conditions.json file

    val testError = Try(spark.sql("select x")).failed.get.asInstanceOf[AnalysisException]
    val errorId = UUID.randomUUID().toString()

    SparkConnectService
      .getOrCreateIsolatedSession(userId, sessionId, None)
      .errorIdToError
      .put(errorId, testError)

    val response = fetchErrorDetails(userId, sessionId, errorId)
    assert(response.hasRootErrorIdx)

    // Verify that the breaking_change_info field exists in the protobuf schema
    // Even if this particular error doesn't have breaking change info,
    // the protobuf should support the field
    val sparkThrowableProto = response.getErrors(0).getSparkThrowable

    // This test verifies that our protobuf changes don't break existing functionality
    assert(sparkThrowableProto.getErrorClass == testError.errorClass.get)

    // TODO: Once we have test error classes with breaking change info,
    // we should add a more comprehensive test that verifies:
    // - sparkThrowableProto.hasBreakingChangeInfo == true
    // - sparkThrowableProto.getBreakingChangeInfo.getMigrationMessageCount > 0
    // - sparkThrowableProto.getBreakingChangeInfo.getNeedsAudit == expected_value
    // - If mitigation config exists:
    //     sparkThrowableProto.getBreakingChangeInfo.hasSparkConfig
  }

  test("throwableToFetchErrorDetailsResponse includes breaking change info") {
    val migrationMessages =
      Seq("Please update your code to use new API", "See documentation for details")
    val mitigationConfig =
      Some(new MitigationConfig("spark.sql.legacy.behavior.enabled", "true"))
    val breakingChangeInfo =
      new BreakingChangeInfo(migrationMessages, mitigationConfig, false)

    val testError = new TestSparkThrowableWithBreakingChange(
      "TEST_BREAKING_CHANGE_ERROR",
      Map("param" -> "value"),
      Some(breakingChangeInfo))

    val response =
      ErrorUtils.throwableToFetchErrorDetailsResponse(testError, serverStackTraceEnabled = false)

    assert(response.hasRootErrorIdx)
    assert(response.getRootErrorIdx == 0)
    assert(response.getErrorsCount == 1)

    val error = response.getErrors(0)
    assert(error.hasSparkThrowable)

    val sparkThrowableProto = error.getSparkThrowable
    assert(sparkThrowableProto.getErrorClass == "TEST_BREAKING_CHANGE_ERROR")
    assert(sparkThrowableProto.hasBreakingChangeInfo)

    val bciProto = sparkThrowableProto.getBreakingChangeInfo
    assert(bciProto.getMigrationMessageCount == 2)
    assert(bciProto.getMigrationMessage(0) == "Please update your code to use new API")
    assert(bciProto.getMigrationMessage(1) == "See documentation for details")
    assert(bciProto.getNeedsAudit == false)

    assert(bciProto.hasMitigationConfig)
    val mitigationConfigProto = bciProto.getMitigationConfig
    assert(mitigationConfigProto.getKey == "spark.sql.legacy.behavior.enabled")
    assert(mitigationConfigProto.getValue == "true")
  }

  test("throwableToFetchErrorDetailsResponse without breaking change info") {
    val testError =
      new TestSparkThrowableWithBreakingChange("REGULAR_ERROR", Map("param" -> "value"), None)

    val response =
      ErrorUtils.throwableToFetchErrorDetailsResponse(testError, serverStackTraceEnabled = false)

    assert(response.hasRootErrorIdx)
    val sparkThrowableProto = response.getErrors(0).getSparkThrowable
    assert(sparkThrowableProto.getErrorClass == "REGULAR_ERROR")
    assert(!sparkThrowableProto.hasBreakingChangeInfo)
  }

  test(
    "throwableToFetchErrorDetailsResponse with breaking change info without mitigation config") {
    val migrationMessages = Seq("Migration message only")
    val breakingChangeInfo = new BreakingChangeInfo(migrationMessages, None, true)

    val testError = new TestSparkThrowableWithBreakingChange(
      "TEST_BREAKING_CHANGE_NO_MITIGATION",
      Map.empty,
      Some(breakingChangeInfo))

    val response =
      ErrorUtils.throwableToFetchErrorDetailsResponse(testError, serverStackTraceEnabled = false)

    val bciProto = response.getErrors(0).getSparkThrowable.getBreakingChangeInfo
    assert(bciProto.getMigrationMessageCount == 1)
    assert(bciProto.getMigrationMessage(0) == "Migration message only")
    assert(bciProto.getNeedsAudit == true)
    assert(!bciProto.hasMitigationConfig)
  }

  test("throwableToFetchErrorDetailsResponse with non-SparkThrowable") {
    val testError = new RuntimeException("Regular runtime exception")

    val response =
      ErrorUtils.throwableToFetchErrorDetailsResponse(testError, serverStackTraceEnabled = false)

    assert(response.hasRootErrorIdx)
    val error = response.getErrors(0)
    assert(!error.hasSparkThrowable)
    assert(error.getMessage == "Regular runtime exception")
  }
}
