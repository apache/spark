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
package org.apache.spark.sql

import java.lang.{Boolean => JavaBoolean}
import java.lang.{Byte => JavaByte}
import java.lang.{Character => JavaChar}
import java.lang.{Double => JavaDouble}
import java.lang.{Float => JavaFloat}
import java.lang.{Integer => JavaInteger}
import java.lang.{Long => JavaLong}
import java.lang.{Short => JavaShort}
import java.math.{BigDecimal => JavaBigDecimal}
import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period}

import org.apache.spark.connect.proto
import org.apache.spark.sql.types.{DayTimeIntervalType, DecimalType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.CalendarInterval

// scalastyle:off
object lits {
// scalastyle:on

  def componentTypeToProto(clz: Class[_]): proto.DataType = clz match {
    // primitive types
    case JavaShort.TYPE =>
      proto.DataType
        .newBuilder()
        .setShort(proto.DataType.Short.getDefaultInstance)
        .build()
    case JavaInteger.TYPE =>
      proto.DataType
        .newBuilder()
        .setInteger(proto.DataType.Integer.getDefaultInstance)
        .build()
    case JavaLong.TYPE =>
      proto.DataType
        .newBuilder()
        .setLong(proto.DataType.Long.getDefaultInstance)
        .build()
    case JavaDouble.TYPE =>
      proto.DataType
        .newBuilder()
        .setDouble(proto.DataType.Double.getDefaultInstance)
        .build()
    case JavaByte.TYPE =>
      proto.DataType
        .newBuilder()
        .setByte(proto.DataType.Byte.getDefaultInstance)
        .build()
    case JavaFloat.TYPE =>
      proto.DataType
        .newBuilder()
        .setFloat(proto.DataType.Float.getDefaultInstance)
        .build()
    case JavaBoolean.TYPE =>
      proto.DataType
        .newBuilder()
        .setBoolean(proto.DataType.Boolean.getDefaultInstance)
        .build()
    case JavaChar.TYPE =>
      proto.DataType
        .newBuilder()
        .setString(proto.DataType.String.getDefaultInstance)
        .build()

    // java classes
    case _ if clz == classOf[LocalDate] =>
      proto.DataType
        .newBuilder()
        .setDate(proto.DataType.Date.getDefaultInstance)
        .build()
    case _ if clz == classOf[Date] =>
      proto.DataType
        .newBuilder()
        .setDate(proto.DataType.Date.getDefaultInstance)
        .build()
    case _ if clz == classOf[Instant] =>
      proto.DataType
        .newBuilder()
        .setTimestamp(proto.DataType.Timestamp.getDefaultInstance)
        .build()
    case _ if clz == classOf[Timestamp] =>
      proto.DataType
        .newBuilder()
        .setTimestamp(proto.DataType.Timestamp.getDefaultInstance)
        .build()
    case _ if clz == classOf[LocalDateTime] =>
      proto.DataType
        .newBuilder()
        .setTimestampNtz(proto.DataType.TimestampNTZ.getDefaultInstance)
        .build()
    case _ if clz == classOf[Duration] =>
      proto.DataType
        .newBuilder()
        .setDayTimeInterval(
          proto.DataType.DayTimeInterval
            .newBuilder()
            .setStartField(DayTimeIntervalType().startField)
            .setEndField(DayTimeIntervalType().endField)
            .build())
        .build()
    case _ if clz == classOf[Period] =>
      proto.DataType
        .newBuilder()
        .setYearMonthInterval(
          proto.DataType.YearMonthInterval
            .newBuilder()
            .setStartField(YearMonthIntervalType().startField)
            .setEndField(YearMonthIntervalType().endField)
            .build())
        .build()
    case _ if clz == classOf[JavaBigDecimal] =>
      proto.DataType
        .newBuilder()
        .setDecimal(
          proto.DataType.Decimal.newBuilder()
            .setPrecision(DecimalType.SYSTEM_DEFAULT.precision)
            .setScale(DecimalType.SYSTEM_DEFAULT.scale)
            .build())
        .build()
    case _ if clz == classOf[Array[Byte]] =>
      proto.DataType
        .newBuilder()
        .setBinary(proto.DataType.Binary.getDefaultInstance)
        .build()
    case _ if clz == classOf[Array[Char]] =>
      proto.DataType
        .newBuilder()
        .setString(proto.DataType.String.getDefaultInstance)
        .build()
    case _ if clz == classOf[JavaShort] =>
      proto.DataType
        .newBuilder()
        .setShort(proto.DataType.Short.getDefaultInstance)
        .build()
    case _ if clz == classOf[JavaInteger] =>
      proto.DataType
        .newBuilder()
        .setInteger(proto.DataType.Integer.getDefaultInstance)
        .build()
    case _ if clz == classOf[JavaLong] =>
      proto.DataType
        .newBuilder()
        .setLong(proto.DataType.Long.getDefaultInstance)
        .build()
    case _ if clz == classOf[JavaDouble] =>
      proto.DataType
        .newBuilder()
        .setDouble(proto.DataType.Double.getDefaultInstance)
        .build()
    case _ if clz == classOf[JavaByte] =>
      proto.DataType
        .newBuilder()
        .setByte(proto.DataType.Byte.getDefaultInstance)
        .build()
    case _ if clz == classOf[JavaFloat] =>
      proto.DataType
        .newBuilder()
        .setFloat(proto.DataType.Float.getDefaultInstance)
        .build()
    case _ if clz == classOf[JavaBoolean] =>
      proto.DataType
        .newBuilder()
        .setBoolean(proto.DataType.Boolean.getDefaultInstance)
        .build()

    // other scala classes
    case _ if clz == classOf[String] =>
      proto.DataType
        .newBuilder()
        .setString(proto.DataType.String.getDefaultInstance)
        .build()
    case _ if clz == classOf[BigInt] =>
      proto.DataType
        .newBuilder()
        .setDecimal(
          proto.DataType.Decimal.newBuilder()
            .setPrecision(DecimalType.SYSTEM_DEFAULT.precision)
            .setScale(DecimalType.SYSTEM_DEFAULT.scale)
            .build())
        .build()
    case _ if clz == classOf[BigDecimal] =>
      proto.DataType
        .newBuilder()
        .setDecimal(
          proto.DataType.Decimal.newBuilder()
            .setPrecision(DecimalType.SYSTEM_DEFAULT.precision)
            .setScale(DecimalType.SYSTEM_DEFAULT.scale)
            .build())
        .build()
    case _ if clz == classOf[CalendarInterval] =>
      proto.DataType
        .newBuilder()
        .setCalendarInterval(proto.DataType.CalendarInterval.getDefaultInstance)
        .build()
    case _ if clz.isArray =>
      proto.DataType
        .newBuilder()
        .setArray(
          proto.DataType.Array
            .newBuilder()
            .setElementType(componentTypeToProto(clz.getComponentType))
            .setContainsNull(true)
            .build())
        .build()
    case _ => throw new UnsupportedOperationException(s"Unsupported component type $clz in arrays.")
  }
}
