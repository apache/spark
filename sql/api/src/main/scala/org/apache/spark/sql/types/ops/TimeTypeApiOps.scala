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

package org.apache.spark.sql.types.ops

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.LocalTimeEncoder
import org.apache.spark.sql.catalyst.util.{FractionTimeFormatter, TimeFormatter}
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Client-side (spark-api) operations for TimeType.
 *
 * This class provides all client-side operations for TIME type including:
 *   - String formatting (FormatTypeOps)
 *   - Row encoding/decoding (EncodeTypeOps)
 *
 * IMPLEMENTATION NOTES:
 *   - Uses FractionTimeFormatter for consistent formatting with ToStringBase
 *   - Uses LocalTimeEncoder for Dataset[T] operations with java.time.LocalTime
 *   - SQL literals use the format: TIME 'HH:mm:ss.ffffff'
 *
 * RELATIONSHIP TO TimeTypeOps: TimeTypeOps (in catalyst package) extends this class to inherit
 * client-side operations while adding server-side operations (physical type, literals, etc.).
 *
 * @param t
 *   The TimeType with precision information
 * @since 4.1.0
 */
class TimeTypeApiOps(val t: TimeType) extends TypeApiOps with FormatTypeOps with EncodeTypeOps {

  override def dataType: DataType = t

  // ==================== FormatTypeOps ====================

  /**
   * Formatter for TIME values.
   *
   * Uses FractionTimeFormatter which:
   *   - Formats times as HH:mm:ss with fractional seconds
   *   - Does not output trailing zeros in the fraction
   *   - Example: "15:00:01.123400" formats as "15:00:01.1234"
   */
  @transient
  private lazy val timeFormatter: TimeFormatter = new FractionTimeFormatter()

  /**
   * Formats a TIME value (nanoseconds since midnight) as a display string.
   *
   * @param v
   *   Long nanoseconds since midnight
   * @return
   *   Formatted string (e.g., "10:30:45.123456")
   */
  override def format(v: Any): String = {
    timeFormatter.format(v.asInstanceOf[Long])
  }

  /**
   * Formats a TIME value as a SQL literal.
   *
   * @param v
   *   Long nanoseconds since midnight
   * @return
   *   SQL literal (e.g., "TIME '10:30:45.123456'")
   */
  override def toSQLValue(v: Any): String = {
    s"TIME '${format(v)}'"
  }

  // ==================== EncodeTypeOps ====================

  /**
   * Returns the encoder for java.time.LocalTime.
   *
   * LocalTimeEncoder handles serialization/deserialization between LocalTime objects and internal
   * Long nanosecond representation.
   */
  override def getEncoder: AgnosticEncoder[_] = LocalTimeEncoder
}
