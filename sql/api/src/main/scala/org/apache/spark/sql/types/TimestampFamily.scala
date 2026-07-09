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

package org.apache.spark.sql.types

/**
 * Shared classification helpers for the LTZ/NTZ timestamp family: the microsecond types
 * ([[TimestampType]] / [[TimestampNTZType]]) and their nanosecond-precision counterparts
 * ([[TimestampLTZNanosType]] / [[TimestampNTZNanosType]]). Centralizes the notion of effective
 * fractional-second precision and time-zone family so that up-cast resolution ([[UpCastRule]]),
 * ANSI store assignment, and common-type resolution all agree.
 */
private[sql] object TimestampFamily {

  /**
   * The effective fractional-second precision of a timestamp-family type, or [[None]] for types
   * that are not on the timestamp fractional-precision axis (DATE, TIME, and everything else).
   * The microsecond types [[TimestampType]] / [[TimestampNTZType]] have precision 6; the
   * nanosecond types carry their own precision `p` in [7, 9].
   */
  def fractionalPrecision(dt: DataType): Option[Int] = dt match {
    case TimestampType | TimestampNTZType => Some(6)
    case t: TimestampLTZNanosType => Some(t.precision)
    case t: TimestampNTZNanosType => Some(t.precision)
    case _ => None
  }

  /** Whether `dt` is a local-time-zone (instant) timestamp: micro [[TimestampType]] or nanos. */
  def isLtz(dt: DataType): Boolean =
    dt.isInstanceOf[TimestampType] || dt.isInstanceOf[TimestampLTZNanosType]

  /** Whether `dt` is a no-time-zone (local) timestamp: micro [[TimestampNTZType]] or nanos. */
  def isNtz(dt: DataType): Boolean =
    dt.isInstanceOf[TimestampNTZType] || dt.isInstanceOf[TimestampNTZNanosType]
}
