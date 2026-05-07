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
}
