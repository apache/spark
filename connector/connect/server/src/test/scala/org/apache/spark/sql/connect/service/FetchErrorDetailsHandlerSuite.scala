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

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.FetchErrorDetailsResponse
import org.apache.spark.sql.connect.ResourceHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

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
    val promise = Promise[FetchErrorDetailsResponse]
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

  test("error chain is properly constructed") {
    val testError =
      new Exception("test1", new Exception("test2"))
    val errorId = UUID.randomUUID().toString()

    SparkConnectService
      .getOrCreateIsolatedSession(userId, sessionId)
      .errorIdToError
      .put(errorId, testError)

    val response = fetchErrorDetails(userId, sessionId, errorId)
    assert(response.hasRootErrorIdx)
    assert(response.getRootErrorIdx == 0)

    assert(response.getErrorsCount == 2)
    assert(response.getErrors(0).getMessage == "test1")
    assert(response.getErrors(0).getErrorTypeHierarchy(0) == classOf[Exception].getName)
    assert(response.getErrors(0).getStackTraceCount == testError.getStackTrace.length)

    assert(response.getErrors(1).getMessage == "test2")
    assert(response.getErrors(1).getErrorTypeHierarchy(0) == classOf[Exception].getName)
    assert(
      response
        .getErrors(1)
        .getStackTraceCount == testError.getCause.getStackTrace.length)
  }

  test("error not found") {
    val response = fetchErrorDetails(userId, sessionId, UUID.randomUUID().toString())
    assert(!response.hasRootErrorIdx)
  }

  test("invalidate cached exceptions after first request") {
    val testError = new Exception("test1")
    val errorId = UUID.randomUUID().toString()

    SparkConnectService
      .getOrCreateIsolatedSession(userId, sessionId)
      .errorIdToError
      .put(errorId, testError)

    val response = fetchErrorDetails(userId, sessionId, errorId)
    assert(response.hasRootErrorIdx)
    assert(response.getRootErrorIdx == 0)

    assert(response.getErrorsCount == 1)
    assert(response.getErrors(0).getMessage == "test1")

    assert(
      SparkConnectService
        .getOrCreateIsolatedSession(userId, sessionId)
        .errorIdToError
        .size() == 0)
  }
}
