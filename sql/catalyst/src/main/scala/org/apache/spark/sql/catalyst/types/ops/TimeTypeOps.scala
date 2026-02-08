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

package org.apache.spark.sql.catalyst.types.ops

import java.time.LocalTime

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, MutableLong, MutableValue}
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalLongType}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataType, TimeType}
import org.apache.spark.sql.types.ops.TimeTypeApiOps

/**
 * Server-side (catalyst) operations for TimeType.
 *
 * This class provides all type-specific operations for TIME type including:
 * - Physical type representation (PhyTypeOps)
 * - Literal creation (LiteralTypeOps)
 * - External type conversion (ExternalTypeOps)
 *
 * It also inherits client-side operations from TimeTypeApiOps:
 * - String formatting (FormatTypeOps)
 * - Row encoding (EncodeTypeOps)
 *
 * INTERNAL REPRESENTATION:
 * TIME values are stored as Long representing nanoseconds since midnight.
 * - Range: 0 to 86,399,999,999,999 (00:00:00.000000000 to 23:59:59.999999999)
 * - Physical type: PhysicalLongType
 * - External type: java.time.LocalTime
 *
 * PRECISION:
 * TimeType supports precision from 0 to 6 (fractional seconds digits).
 * The internal representation always uses nanoseconds regardless of precision.
 * Precision only affects parsing and display formatting.
 *
 * EXAMPLE USAGE:
 * {{{
 * val ops = TimeTypeOps(TimeType(6))
 * ops.getPhysicalType // Returns PhysicalLongType
 * ops.getDefaultLiteral // Returns Literal(0L, TimeType(6)) representing "00:00:00"
 * ops.toScala(37800000000000L) // Returns LocalTime.of(10, 30, 0)
 * }}}
 *
 * @param t The TimeType with precision information
 * @since 4.1.0
 */
case class TimeTypeOps(override val t: TimeType)
    extends TimeTypeApiOps(t)
    with TypeOps
    with PhyTypeOps
    with LiteralTypeOps
    with ExternalTypeOps {

  override def dataType: DataType = t

  // ==================== PhyTypeOps ====================

  /**
   * TIME is physically stored as Long (nanoseconds since midnight).
   */
  override def getPhysicalType: PhysicalDataType = PhysicalLongType

  /**
   * Java class for code generation is Long.
   */
  override def getJavaClass: Class[_] = classOf[Long]

  /**
   * MutableLong for SpecificInternalRow operations.
   */
  override def getMutableValue: MutableValue = new MutableLong

  /**
   * Row writer that uses setLong for TIME's Long physical representation.
   */
  override def getRowWriter(ordinal: Int): (InternalRow, Any) => Unit =
    (input, v) => input.setLong(ordinal, v.asInstanceOf[Long])

  // ==================== LiteralTypeOps ====================

  /**
   * Default TIME literal is midnight (00:00:00).
   *
   * Returns Literal(0L, TimeType(precision)) where 0L represents midnight
   * (0 nanoseconds since midnight).
   */
  override def getDefaultLiteral: Literal = Literal.create(0L, t)

  /**
   * Java literal representation for code generation.
   *
   * @param v Long nanoseconds value
   * @return Java literal string (e.g., "37800000000000L")
   */
  override def getJavaLiteral(v: Any): String = s"${v}L"

  // ==================== ExternalTypeOps ====================

  /**
   * Converts LocalTime to internal Long nanoseconds representation.
   *
   * @param scalaValue java.time.LocalTime instance
   * @return Long nanoseconds since midnight
   */
  override def toCatalystImpl(scalaValue: Any): Any = {
    DateTimeUtils.localTimeToNanos(scalaValue.asInstanceOf[LocalTime])
  }

  /**
   * Converts internal Long nanoseconds to LocalTime.
   *
   * @param catalystValue Long nanoseconds since midnight (may be null)
   * @return java.time.LocalTime instance, or null if input was null
   */
  override def toScala(catalystValue: Any): Any = {
    if (catalystValue == null) null
    else DateTimeUtils.nanosToLocalTime(catalystValue.asInstanceOf[Long])
  }

  /**
   * Extracts TIME value from InternalRow and converts to LocalTime.
   *
   * @param row The InternalRow containing the TIME value
   * @param column The column index
   * @return java.time.LocalTime instance
   */
  override def toScalaImpl(row: InternalRow, column: Int): Any = {
    DateTimeUtils.nanosToLocalTime(row.getLong(column))
  }
}
