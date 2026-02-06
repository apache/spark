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

import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Operations for formatting values as strings.
 *
 * PURPOSE:
 * Handles string formatting for display and SQL output. This includes formatting
 * values for CAST to STRING, EXPLAIN output, SHOW commands, and SQL literals.
 *
 * USAGE CONTEXT:
 * Used by:
 * - ToStringBase.scala - CAST(x AS STRING) expressions
 * - EXPLAIN output - displaying literal values
 * - SHOW commands - displaying column values
 * - SQL generation - creating SQL literal representations
 * - HiveResult.scala - formatting results for Thrift/JDBC
 *
 * @see TimeTypeApiOps for reference implementation
 * @since 4.1.0
 */
trait FormatTypeOps extends TypeApiOps {
  /**
   * Formats an internal value as a display string.
   *
   * This method converts the internal Catalyst representation to a human-readable
   * string suitable for display or CAST to STRING operations.
   *
   * @param v The internal value (e.g., Long nanoseconds for TimeType)
   * @return Formatted string (e.g., "10:30:45.123456")
   * @example 37800000000000L -> "10:30:00" (for TIME)
   */
  def format(v: Any): String

  /**
   * Formats an internal value as a UTF8String.
   *
   * Convenience method that wraps format() for use in expressions that
   * need UTF8String output directly.
   *
   * @param v The internal value
   * @return UTF8String representation
   */
  def formatUTF8(v: Any): UTF8String = UTF8String.fromString(format(v))

  /**
   * Formats an internal value as a SQL literal string.
   *
   * This method produces a string that can be used in SQL statements,
   * including the type prefix if appropriate (e.g., "TIME '10:30:00'").
   *
   * @param v The internal value
   * @return SQL literal string (e.g., "TIME '10:30:00'")
   * @example 37800000000000L -> "TIME '10:30:00'" (for TIME)
   */
  def toSQLValue(v: Any): String
}

/**
 * Companion object providing factory methods for FormatTypeOps.
 */
object FormatTypeOps {
  /**
   * Creates a FormatTypeOps instance for the given DataType.
   *
   * @param dt The DataType to get formatting operations for
   * @return FormatTypeOps instance
   * @throws SparkException if the type doesn't support FormatTypeOps
   */
  def apply(dt: DataType): FormatTypeOps = TypeApiOps(dt).asInstanceOf[FormatTypeOps]

  /**
   * Checks if a DataType supports FormatTypeOps operations.
   *
   * @param dt The DataType to check
   * @return true if the type supports FormatTypeOps
   */
  def supports(dt: DataType): Boolean = TypeApiOps.supports(dt)
}
