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

import java.sql.SQLException
import java.util.UUID

import scala.jdk.CollectionConverters._

import com.google.rpc.{Code, ErrorInfo}
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.stub.StreamObserver
import org.json4s.jackson.JsonMethods

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.test.SharedSparkSession

class ErrorUtilsSuite extends SharedSparkSession {

  test("handleError fallback uses throwable class when fatal throwable has no message") {
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
        new InterruptedException())
    }

    assert(error.getStatus.getCode == Status.Code.UNKNOWN)
    assert(error.getStatus.getDescription == classOf[InterruptedException].getName)
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

  test("JDBC write errors remain reconstructable by Connect clients") {
    val cause = new SQLException("batch rejected", "23000")
    val error = new SparkRuntimeException(
      "FAILED_JDBC.WRITE",
      Map("url" -> "jdbc:redacted", "tableName" -> "t"),
      cause)

    val expectedParameters = Map(
      "url" -> "jdbc:redacted",
      "message" -> "Write rows to the table t.")
    val status = ErrorUtils.buildStatusFromThrowable(error, None)
    val info = status.getDetails(0).unpack(classOf[ErrorInfo])
    assert(info.getMetadataOrDefault("errorClass", "") == "FAILED_JDBC.UNCLASSIFIED")
    assert(info.getMetadataOrDefault("sqlState", "") == "HV000")
    val statusParameters = info.getMetadataOrDefault("messageParameters", "")
    assert(JsonMethods.parse(statusParameters).values == expectedParameters)

    val response = ErrorUtils.throwableToFetchErrorDetailsResponse(error)
    val root = response.getErrors(0)
    val projected = root.getSparkThrowable
    assert(projected.getErrorClass == "FAILED_JDBC.UNCLASSIFIED")
    assert(projected.getSqlState == "HV000")
    assert(projected.getMessageParametersMap.asScala.toMap == expectedParameters)
    assert(root.getCauseIdx == 1)
    assert(response.getErrors(1).getMessage == cause.getMessage)
  }
}
