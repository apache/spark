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

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.errors.DataTypeErrors

/**
 * Timestamp without time zone with fractional-second precision in the nanosecond-capable range (7
 * to 9 decimal digits). Represents a local date-time analogous to `TimestampNTZType`, but with
 * sub-microsecond precision: valid range is [0001-01-01T00:00:00.000000000,
 * 9999-12-31T23:59:59.999999999] in the proleptic Gregorian calendar. The value is independent of
 * any time zone. To represent an absolute point in time, use `TimestampLTZNanosType` instead.
 *
 * @param precision
 *   Number of digits of fractional seconds for this SQL type. The valid values are 7, 8, and 9
 *   where 9 means nanosecond precision.
 *
 * @since 4.2.0
 */
@Unstable
case class TimestampNTZNanosType(precision: Int) extends DatetimeType {

  if (precision < TimestampNTZNanosType.MIN_PRECISION ||
    precision > TimestampNTZNanosType.MAX_PRECISION) {
    throw DataTypeErrors.invalidTimestampPrecisionError(precision.toString, "TIMESTAMP_NTZ")
  }

  /**
   * Default size used by Spark for row-size estimation. Values are represented logically as epoch
   * microseconds (Long, 8 bytes) plus nanoseconds within that micro (Short, 2 bytes).
   *
   * In [[org.apache.spark.sql.catalyst.expressions.UnsafeRow]], the physical payload is 16 bytes
   * in the variable-length region (two 8-byte words); see
   * [[org.apache.spark.sql.catalyst.expressions.TimestampNanosRowValues]].
   */
  override def defaultSize: Int = 10

  override def typeName: String = s"timestamp_ntz($precision)"

  private[spark] override def asNullable: TimestampNTZNanosType = this
}

object TimestampNTZNanosType {
  val MIN_PRECISION: Int = 7
  val MAX_PRECISION: Int = 9
  val NANOS_PRECISION: Int = 9
  val DEFAULT_PRECISION: Int = NANOS_PRECISION

  def apply(): TimestampNTZNanosType = new TimestampNTZNanosType(DEFAULT_PRECISION)
}
