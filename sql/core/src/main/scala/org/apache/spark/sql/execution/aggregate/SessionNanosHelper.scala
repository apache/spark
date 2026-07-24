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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{AnyTimestampNanoType, StructType, TimestampLTZNanosType, TimestampNTZNanosType}

/**
 * Shared helpers for reading/writing nanosecond-precision session window timestamps
 * in [[MergingSessionsIterator]] and [[UpdatingSessionsIterator]].
 */
private[aggregate] object SessionNanosHelper {

  /** Returns true if the session struct uses nanosecond-precision timestamps. */
  def isSessionNanos(sessionExpression: Expression): Boolean = {
    val sessionType = sessionExpression.dataType.asInstanceOf[StructType]
    sessionType.fields(0).dataType.isInstanceOf[AnyTimestampNanoType]
  }

  /** Reads the session start as epoch-nanos (Long) from a nanosecond session struct. */
  def getSessionStartNanos(sessionStruct: UnsafeRow): Long = {
    val v = sessionStruct.getTimestampNTZNanos(0)
    DateTimeUtils.timestampNanosToEpochNanos(v)
  }

  /** Reads the session end as epoch-nanos (Long) from a nanosecond session struct. */
  def getSessionEndNanos(sessionStruct: UnsafeRow): Long = {
    val v = sessionStruct.getTimestampNTZNanos(1)
    DateTimeUtils.timestampNanosToEpochNanos(v)
  }

  /**
   * Extracts the precision from the session-end field's nanosecond timestamp type.
   * Defaults to 9 if the type is an unexpected [[AnyTimestampNanoType]] subtype.
   */
  def sessionEndPrecision(sessionExpression: Expression): Int = {
    sessionExpression.dataType.asInstanceOf[StructType]
      .fields(1).dataType match {
      case t: TimestampNTZNanosType => t.precision
      case t: TimestampLTZNanosType => t.precision
      case _ => 9
    }
  }

  /** Writes an epoch-nanos value into the session-end field of the struct. */
  def setSessionEnd(sessionStruct: UnsafeRow, sessionEnd: Long,
      sessionExpression: Expression): Unit = {
    val precision = sessionEndPrecision(sessionExpression)
    val v = DateTimeUtils.epochNanosToTimestampNanos(sessionEnd, precision)
    sessionStruct.setTimestampNTZNanos(1, v)
  }
}
