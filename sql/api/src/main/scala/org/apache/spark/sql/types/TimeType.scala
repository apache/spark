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

import org.apache.spark.annotation.Stable

/**
 * The time type represents a time-of-day value in microsecond precision, which is
 * independent of any specific date. Its valid range is [00:00:00.000000, 23:59:59.999999]
 * representing the time from midnight to one microsecond before the next midnight.
 *
 * The time is stored internally as a Long value representing microseconds since midnight
 * (00:00:00.000000). This provides:
 * - Range: 0 to 86,399,999,999 (24 hours * 3600 seconds * 1,000,000 microseconds - 1)
 * - Precision: Microseconds (6 decimal places for fractional seconds)
 *
 * Please use the singleton `DataTypes.TimeType` to refer the type.
 * @since 4.0.0
 */
@Stable
class TimeType private () extends DatetimeType {

  /**
   * The default size of a value of the TimeType is 8 bytes.
   */
  override def defaultSize: Int = 8

  private[spark] override def asNullable: TimeType = this

  /**
   * For Hive compatibility, TIME type is stored as BIGINT.
   * Override catalogString to return "bigint" instead of "time".
   */
  override def catalogString: String = "bigint"

  /**
   * Ordering for TIME values (microseconds since midnight).
   */
  @transient
  private[sql] lazy val ordering: Ordering[Long] = new Ordering[Long] {
    def compare(x: Long, y: Long): Int = java.lang.Long.compare(x, y)
  }
}

/**
 * The companion case object and its class is separated so the companion object also subclasses
 * the TimeType class. Otherwise, the companion object would be of type "TimeType$" in
 * byte code. Defined with a private constructor so the companion object is the only possible
 * instantiation.
 *
 * @since 4.0.0
 */
@Stable
case object TimeType extends TimeType

