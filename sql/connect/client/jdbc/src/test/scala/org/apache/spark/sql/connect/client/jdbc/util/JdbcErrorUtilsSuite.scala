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

package org.apache.spark.sql.connect.client.jdbc.util

import java.sql.{SQLException, SQLNonTransientConnectionException, SQLTimeoutException, SQLTransientConnectionException}

import io.grpc.{Status, StatusRuntimeException}

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.connect.test.ConnectFunSuite

/**
 * Tests for [[JdbcErrorUtils.toSQLException]]. A RuntimeException mixing in
 * [[SparkThrowable]] stands in for converted server errors, carrying the sqlState
 * GrpcExceptionConverter would supply. A real [[StatusRuntimeException]] cause
 * models transport errors the way GrpcExceptionConverter preserves them.
 */
class JdbcErrorUtilsSuite extends ConnectFunSuite {

  private def sparkError(
      condition: String,
      sqlState: String,
      msg: String,
      cause: Throwable = null): RuntimeException =
    new RuntimeException(msg, cause) with SparkThrowable {
      override def getCondition: String = condition
      override def getSqlState: String = sqlState
    }

  private def grpcError(status: Status): StatusRuntimeException =
    new StatusRuntimeException(status)

  test("a class-08 SQLSTATE maps to a non-transient connection exception") {
    val e = JdbcErrorUtils.toSQLException(
      sparkError("INVALID_HANDLE.SESSION_CLOSED", "08003", "Session was closed"))
    assert(e.isInstanceOf[SQLNonTransientConnectionException])
    assert(e.getSQLState === "08003")
    assert(e.getMessage === "Session was closed")
  }

  test("the class-08 mapping does not depend on the condition name") {
    val e = JdbcErrorUtils.toSQLException(sparkError("SOME_FUTURE_CONDITION", "08004", "gone"))
    assert(e.isInstanceOf[SQLNonTransientConnectionException])
    assert(e.getSQLState === "08004")
  }

  test("operation-level INVALID_HANDLE subconditions do not map to a connection exception") {
    // the session, and thus the connection, is still healthy: no SQLState class 08
    Seq(
      "INVALID_HANDLE.OPERATION_NOT_FOUND",
      "INVALID_HANDLE.OPERATION_ABANDONED",
      "INVALID_HANDLE.OPERATION_ALREADY_EXISTS",
      "INVALID_HANDLE.FORMAT").foreach { condition =>
      val e = JdbcErrorUtils.toSQLException(sparkError(condition, "HY000", "operation gone"))
      assert(!e.isInstanceOf[SQLNonTransientConnectionException], condition)
      assert(!e.isInstanceOf[SQLTransientConnectionException], condition)
    }
  }

  test("a non-connection Spark error keeps the server-provided SQLState") {
    val e = JdbcErrorUtils.toSQLException(sparkError("DIVIDE_BY_ZERO", "22012", "boom"))
    assert(!e.isInstanceOf[SQLNonTransientConnectionException])
    assert(!e.isInstanceOf[SQLTransientConnectionException])
    assert(e.getSQLState === "22012")
    assert(e.getMessage === "boom")
  }

  test("an UNAVAILABLE StatusRuntimeException maps to a transient connection exception") {
    val e = JdbcErrorUtils.toSQLException(
      grpcError(Status.UNAVAILABLE.withDescription("Channel shutdown invoked")))
    assert(e.isInstanceOf[SQLTransientConnectionException])
    assert(e.getSQLState === "08006")
  }

  test("an UNAVAILABLE cause preserved by GrpcExceptionConverter maps to 08006") {
    // the shape GrpcExceptionConverter produces for transport errors
    val wrapped = sparkError(
      "CONNECT_CLIENT_UNEXPECTED_MISSING_SQL_STATE",
      "XXKCM",
      "io.grpc.StatusRuntimeException: UNAVAILABLE: io exception",
      cause = grpcError(Status.UNAVAILABLE.withDescription("io exception")))
    val e = JdbcErrorUtils.toSQLException(wrapped)
    assert(e.isInstanceOf[SQLTransientConnectionException])
    assert(e.getSQLState === "08006")
  }

  test("a server error merely quoting a gRPC exception is not a connection failure") {
    // the gRPC status comes from exception instances, never from message text, so a
    // server error embedding gRPC text (e.g. from a UDF) is not a transport failure
    val e = JdbcErrorUtils.toSQLException(sparkError(
      "FAILED_EXECUTE_UDF",
      "39000",
      "Job aborted: io.grpc.StatusRuntimeException: UNAVAILABLE: backend down"))
    assert(!e.isInstanceOf[SQLTransientConnectionException])
    assert(!e.isInstanceOf[SQLNonTransientConnectionException])
  }

  test("a DEADLINE_EXCEEDED cause maps to a timeout, not a connection failure") {
    // a slow query fires the RPC deadline on a healthy connection: timeout, not class 08
    val wrapped = sparkError(
      "CONNECT_CLIENT_UNEXPECTED_MISSING_SQL_STATE",
      "XXKCM",
      "io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: deadline exceeded",
      cause = grpcError(Status.DEADLINE_EXCEEDED.withDescription("deadline exceeded")))
    val e = JdbcErrorUtils.toSQLException(wrapped)
    assert(e.isInstanceOf[SQLTimeoutException])
    assert(e.getSQLState === "HYT00")
    assert(!e.isInstanceOf[SQLTransientConnectionException])
  }

  test("a non-retryable gRPC status does not map to a connection exception") {
    val e = JdbcErrorUtils.toSQLException(
      grpcError(Status.INVALID_ARGUMENT.withDescription("bad plan")))
    assert(!e.isInstanceOf[SQLTransientConnectionException])
    assert(!e.isInstanceOf[SQLNonTransientConnectionException])
    assert(!e.isInstanceOf[SQLTimeoutException])
  }

  test("a class-08 error takes precedence over a transport code") {
    // both signals present: the gone session wins
    val e = JdbcErrorUtils.toSQLException(sparkError(
      "INVALID_HANDLE.SESSION_CLOSED",
      "08003",
      "closed",
      cause = grpcError(Status.UNAVAILABLE.withDescription("x"))))
    assert(e.isInstanceOf[SQLNonTransientConnectionException])
    assert(e.getSQLState === "08003")
  }

  test("the connection error is found through the cause chain") {
    val root = sparkError("INVALID_HANDLE.SESSION_CLOSED", "08003", "closed")
    val e = JdbcErrorUtils.toSQLException(new RuntimeException("wrapper", root))
    assert(e.isInstanceOf[SQLNonTransientConnectionException])
    assert(e.getSQLState === "08003")
  }

  test("a non-Spark exception becomes a plain SQLException carrying the cause") {
    val cause = new RuntimeException("raw failure")
    val e = JdbcErrorUtils.toSQLException(cause)
    assert(!e.isInstanceOf[SQLNonTransientConnectionException])
    assert(!e.isInstanceOf[SQLTransientConnectionException])
    assert(e.getSQLState === null)
    assert(e.getMessage === "raw failure")
    assert(e.getCause eq cause)
  }

  test("an existing SQLException passes through unchanged") {
    val original = new SQLException("already mapped")
    assert(JdbcErrorUtils.toSQLException(original) eq original)
  }
}
