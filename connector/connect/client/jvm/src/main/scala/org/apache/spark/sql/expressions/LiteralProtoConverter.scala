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
package org.apache.spark.sql.expressions

import java.lang.{Boolean => JavaBoolean, Byte => JavaByte, Character => JavaChar, Double => JavaDouble, Float => JavaFloat, Integer => JavaInteger, Long => JavaLong, Short => JavaShort}
import java.math.{BigDecimal => JavaBigDecimal}
import java.sql.{Date, Timestamp}
import java.time._

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.connect.client.unsupported
import org.apache.spark.sql.types.{DayTimeIntervalType, Decimal, DecimalType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.CalendarInterval

object LiteralProtoConverter {

  private lazy val nullType =
    proto.DataType.newBuilder().setNull(proto.DataType.NULL.getDefaultInstance).build()

  /**
   * Transforms literal value to the `proto.Expression.Literal.Builder`.
   *
   * @return proto.Expression.Literal.Builder
   */
  def toLiteralProtoBuilder(literal: Any): proto.Expression.Literal.Builder = {
    val builder = proto.Expression.Literal.newBuilder()

    def decimalBuilder(precision: Int, scale: Int, value: String) = {
      builder.getDecimalBuilder.setPrecision(precision).setScale(scale).setValue(value)
    }

    def calendarIntervalBuilder(months: Int, days: Int, microseconds: Long) = {
      builder.getCalendarIntervalBuilder.setMonths(months).setDays(days)
        .setMicroseconds(microseconds)
    }

    def arrayBuilder(array: Array[_]) = {
      val ab = builder.getArrayBuilder
        .setElementType(componentTypeToProto(array.getClass.getComponentType))
      array.foreach(x => ab.addElement(toLiteralProtoBuilder(x)))
      ab
    }

    literal match {
      case v: Boolean => builder.setBoolean(v)
      case v: Byte => builder.setByte(v)
      case v: Short => builder.setShort(v)
      case v: Int => builder.setInteger(v)
      case v: Long => builder.setLong(v)
      case v: Float => builder.setFloat(v)
      case v: Double => builder.setDouble(v)
      case v: BigDecimal =>
        builder.setDecimal(decimalBuilder(v.precision, v.scale, v.toString))
      case v: JavaBigDecimal =>
        builder.setDecimal(decimalBuilder(v.precision, v.scale, v.toString))
      case v: String => builder.setString(v)
      case v: Char => builder.setString(v.toString)
      case v: Array[Char] => builder.setString(String.valueOf(v))
      case v: Array[Byte] => builder.setBinary(ByteString.copyFrom(v))
      case v: collection.mutable.WrappedArray[_] => toLiteralProtoBuilder(v.array)
      case v: LocalDate => builder.setDate(v.toEpochDay.toInt)
      case v: Decimal =>
        builder.setDecimal(decimalBuilder(Math.max(v.precision, v.scale), v.scale, v.toString))
      case v: Instant => builder.setTimestamp(DateTimeUtils.instantToMicros(v))
      case v: Timestamp => builder.setTimestamp(DateTimeUtils.fromJavaTimestamp(v))
      case v: LocalDateTime => builder.setTimestampNtz(DateTimeUtils.localDateTimeToMicros(v))
      case v: Date => builder.setDate(DateTimeUtils.fromJavaDate(v))
      case v: Duration => builder.setDayTimeInterval(IntervalUtils.durationToMicros(v))
      case v: Period => builder.setYearMonthInterval(IntervalUtils.periodToMonths(v))
      case v: Array[_] => builder.setArray(arrayBuilder(v))
      case v: CalendarInterval =>
        builder.setCalendarInterval(calendarIntervalBuilder(v.months, v.days, v.microseconds))
      case null => builder.setNull(nullType)
      case _ => unsupported(s"literal $literal not supported (yet).")
    }
  }

  private def componentTypeToProto(clz: Class[_]): proto.DataType = clz match {
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
