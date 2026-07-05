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

import java.sql.{Array => _, _}

import scala.util.control.NonFatal

import io.grpc.{Status, StatusRuntimeException}

import org.apache.spark.SparkThrowable

private[jdbc] object JdbcErrorUtils {

  def stringifyTransactionIsolationLevel(level: Int): String = level match {
    case Connection.TRANSACTION_NONE => "NONE"
    case Connection.TRANSACTION_READ_UNCOMMITTED => "READ_UNCOMMITTED"
    case Connection.TRANSACTION_READ_COMMITTED => "READ_COMMITTED"
    case Connection.TRANSACTION_REPEATABLE_READ => "REPEATABLE_READ"
    case Connection.TRANSACTION_SERIALIZABLE => "SERIALIZABLE"
    case _ =>
      throw new IllegalArgumentException(s"Invalid transaction isolation level: $level")
  }

  def stringifyHoldability(holdability: Int): String = holdability match {
    case ResultSet.HOLD_CURSORS_OVER_COMMIT => "HOLD_CURSORS_OVER_COMMIT"
    case ResultSet.CLOSE_CURSORS_AT_COMMIT => "CLOSE_CURSORS_AT_COMMIT"
    case _ =>
      throw new IllegalArgumentException(s"Invalid holdability: $holdability")
  }

  def stringifyResultSetType(typ: Int): String = typ match {
    case ResultSet.TYPE_FORWARD_ONLY => "FORWARD_ONLY"
    case ResultSet.TYPE_SCROLL_INSENSITIVE => "SCROLL_INSENSITIVE"
    case ResultSet.TYPE_SCROLL_SENSITIVE => "SCROLL_SENSITIVE"
    case _ =>
      throw new IllegalArgumentException(s"Invalid ResultSet type: $typ")
  }

  def stringifyFetchDirection(direction: Int): String = direction match {
    case ResultSet.FETCH_FORWARD => "FETCH_FORWARD"
    case ResultSet.FETCH_REVERSE => "FETCH_REVERSE"
    case ResultSet.FETCH_UNKNOWN => "FETCH_UNKNOWN"
    case _ =>
      throw new IllegalArgumentException(s"Invalid fetch direction: $direction")
  }

  // SQLState class 08 is "connection exception"; HYT00 is the conventional
  // (ODBC-derived) state for an elapsed timeout.
  private val CONNECTION_DOES_NOT_EXIST = "08003"
  private val CONNECTION_FAILURE = "08006"
  private val TIMEOUT_EXPIRED = "HYT00"

  private val SESSION_GONE_CONDITION_PREFIX = "INVALID_HANDLE.SESSION_"

  /**
   * Maps the unchecked exceptions raised by the Spark Connect client (a
   * [[SparkThrowable]] once GrpcExceptionConverter has converted the gRPC error) to
   * the [[SQLException]] a JDBC method is required to throw:
   *
   *  - `INVALID_HANDLE.SESSION_*` means the server-side session is gone (e.g. it
   *    timed out); the connection is unusable and retrying on it is pointless, so it
   *    maps to a [[SQLNonTransientConnectionException]] with SQLState 08003
   *    ("connection does not exist"). The operation-level subconditions
   *    (`OPERATION_*`, `FORMAT`) concern a single operation on a healthy session
   *    and are deliberately not treated as connection errors.
   *  - a gRPC UNAVAILABLE (e.g. a server restart or a network blip) maps to a
   *    [[SQLTransientConnectionException]] with SQLState 08006 ("connection
   *    failure"), since a fresh connection can succeed.
   *  - a gRPC DEADLINE_EXCEEDED means the RPC deadline elapsed, which a slow query
   *    fires on a perfectly healthy connection, so it maps to a
   *    [[SQLTimeoutException]] rather than a connection error.
   *  - any other error keeps the server-provided SQLState when one is available.
   *
   * The gRPC status is read from the [[StatusRuntimeException]] that
   * GrpcExceptionConverter preserves in the cause chain, never from message text,
   * so a server-side error merely quoting a gRPC exception cannot be mistaken for
   * a transport failure. SQLState class 08 is how connection pools and BI tools
   * detect a dead connection and reconnect.
   */
  def toSQLException(t: Throwable): SQLException = t match {
    case e: SQLException => e
    case e =>
      val chain = causeChain(e)
      val sparkThrowableOpt = chain.collectFirst { case st: SparkThrowable => st }
      val condition = sparkThrowableOpt.flatMap(st => Option(st.getCondition))
      val grpcCode = chain.collectFirst { case sre: StatusRuntimeException =>
        sre.getStatus.getCode
      }
      if (condition.exists(_.startsWith(SESSION_GONE_CONDITION_PREFIX))) {
        new SQLNonTransientConnectionException(e.getMessage, CONNECTION_DOES_NOT_EXIST, e)
      } else if (grpcCode.contains(Status.Code.UNAVAILABLE)) {
        new SQLTransientConnectionException(e.getMessage, CONNECTION_FAILURE, e)
      } else if (grpcCode.contains(Status.Code.DEADLINE_EXCEEDED)) {
        new SQLTimeoutException(e.getMessage, TIMEOUT_EXPIRED, e)
      } else {
        new SQLException(e.getMessage, sparkThrowableOpt.map(_.getSqlState).orNull, e)
      }
  }

  /** Runs `body`, rethrowing any non-fatal failure as the mapped [[SQLException]]. */
  def mapToSQLException[T](body: => T): T =
    try body catch { case NonFatal(e) => throw toSQLException(e) }

  // The take(20) caps the walk in case of a cause cycle.
  private def causeChain(t: Throwable): Seq[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(20).toSeq
}
