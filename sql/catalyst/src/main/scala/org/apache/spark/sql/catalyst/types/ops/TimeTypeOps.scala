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
 * This class implements all TypeOps methods for the TIME data type, providing:
 *   - Physical type: PhysicalLongType (nanoseconds since midnight)
 *   - Literals: default is 0L (midnight)
 *   - External conversion: java.time.LocalTime <-> Long nanoseconds
 *
 * It also inherits client-side operations from TimeTypeApiOps:
 *   - String formatting (FractionTimeFormatter)
 *   - Row encoding (LocalTimeEncoder)
 *
 * INTERNAL REPRESENTATION:
 *   - Values stored as Long nanoseconds since midnight
 *   - Range: 0 to 86,399,999,999,999
 *   - External type: java.time.LocalTime
 *   - Precision (0-6) affects display only, not storage
 *
 * @param t The TimeType with precision information
 * @since 4.2.0
 */
case class TimeTypeOps(override val t: TimeType)
    extends TimeTypeApiOps(t)
    with TypeOps {

  override def dataType: DataType = t

  // ==================== Physical Type Representation ====================

  override def getPhysicalType: PhysicalDataType = PhysicalLongType

  override def getJavaClass: Class[_] = classOf[Long]

  override def getMutableValue: MutableValue = new MutableLong

  override def getRowWriter(ordinal: Int): (InternalRow, Any) => Unit =
    (input, v) => input.setLong(ordinal, v.asInstanceOf[Long])

  // ==================== Literal Creation ====================

  override def getDefaultLiteral: Literal = Literal.create(0L, t)

  override def getJavaLiteral(v: Any): String = s"${v}L"

  // ==================== External Type Conversion ====================

  override def toCatalystImpl(scalaValue: Any): Any = {
    DateTimeUtils.localTimeToNanos(scalaValue.asInstanceOf[LocalTime])
  }

  override def toScala(catalystValue: Any): Any = {
    if (catalystValue == null) null
    else DateTimeUtils.nanosToLocalTime(catalystValue.asInstanceOf[Long])
  }

  override def toScalaImpl(row: InternalRow, column: Int): Any = {
    DateTimeUtils.nanosToLocalTime(row.getLong(column))
  }
}
