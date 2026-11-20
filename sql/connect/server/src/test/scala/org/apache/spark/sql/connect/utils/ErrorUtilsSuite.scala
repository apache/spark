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

import java.util.UUID

import com.google.rpc.{Code, ErrorInfo}
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.protobuf.StatusProto
import io.grpc.stub.StreamObserver

import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.{SessionHolder, SparkConnectService}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ErrorUtilsSuite extends SharedSparkSession {

  for (fatalError <- Seq(new InterruptedException(), new StackOverflowError())) {
    test(s"handleError keeps the UNKNOWN fallback for ${fatalError.getClass.getSimpleName}") {
      val observer = new StreamObserver[Unit] {
        override def onNext(value: Unit): Unit = {
          fail(s"Unexpected response: $value")
        }

        override def onError(t: Throwable): Unit = {
          throw t
        }

        override def onCompleted(): Unit = {
          fail("Unexpected completion")
        }
      }

      val error = intercept[StatusRuntimeException] {
        ErrorUtils.handleError("execute", observer, "user1", UUID.randomUUID().toString)(
          fatalError)
      }

      assert(error.getStatus.getCode == Status.Code.UNKNOWN)
      assert(error.getStatus.getDescription == fatalError.getClass.getName)
      assert(StatusProto.fromThrowable(error).getDetailsCount == 0)
    }
  }

  test("handleError preserves linkage error details when session-state initialization fails") {
    val linkageError = new NoSuchMethodError("incompatible session extension")
    val session = new SparkSession(spark.sparkContext)
    session.sqlConf.setConf(Connect.CONNECT_ENRICH_ERROR_ENABLED, true)
    session.sqlConf.setConf(SQLConf.PYSPARK_JVM_STACKTRACE_ENABLED, true)
    var parserBuilds = 0
    session.extensions.injectParser { (_, _) =>
      parserBuilds += 1
      throw linkageError
    }
    val holder = SessionHolder("user1", UUID.randomUUID().toString, session)
    SparkConnectService.sessionManager.putSessionForTesting(holder)
    var callbackCalled = false
    val observer = new StreamObserver[Unit] {
      override def onNext(value: Unit): Unit = fail("Unexpected response")
      override def onError(t: Throwable): Unit = throw t
      override def onCompleted(): Unit = fail("Unexpected completion")
    }
    try {
      assert(intercept[NoSuchMethodError](session.sessionState) eq linkageError)
      val error = intercept[StatusRuntimeException] {
        ErrorUtils.handleError(
          "execute",
          observer,
          holder.userId,
          holder.sessionId,
          callback = Some(() => callbackCalled = true))(linkageError)
      }
      assert(error.getStatus.getCode == Status.Code.INTERNAL)
      assert(error.getStatus.getDescription == linkageError.getMessage)
      val info = StatusProto.fromThrowable(error).getDetails(0).unpack(classOf[ErrorInfo])
      assert(info.getReason == linkageError.getClass.getName)
      val errorId = info.getMetadataOrThrow("errorId")
      assert(holder.errorIdToError.getIfPresent(errorId) eq linkageError)
      assert(info.getMetadataOrThrow("stackTrace").contains(linkageError.getMessage))
      assert(parserBuilds == 1)
      assert(callbackCalled)
    } finally {
      SparkConnectService.sessionManager.closeSession(holder.key)
    }
  }

  test("buildStatusFromThrowable uses throwable class when non-fatal throwable has no message") {
    val status = ErrorUtils.buildStatusFromThrowable(new RuntimeException(), None)

    assert(status.getCode == Code.INTERNAL_VALUE)
    assert(status.getMessage == classOf[RuntimeException].getName)
  }

  test("buildStatusFromThrowable uses throwable class when non-fatal message is empty") {
    val status = ErrorUtils.buildStatusFromThrowable(new RuntimeException(""), None)

    assert(status.getCode == Code.INTERNAL_VALUE)
    assert(status.getMessage == classOf[RuntimeException].getName)
  }

  test("buildStatusFromThrowable keeps the message when the non-fatal throwable has one") {
    val status = ErrorUtils.buildStatusFromThrowable(new RuntimeException("boom"), None)

    assert(status.getCode == Code.INTERNAL_VALUE)
    assert(status.getMessage == "boom")
  }
}
