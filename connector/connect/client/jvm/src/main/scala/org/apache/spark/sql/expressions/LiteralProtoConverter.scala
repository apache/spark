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

import java.lang.{Boolean => JBoolean, Byte => JByte, Character => JChar, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}
import java.time._

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.connect.client.unsupported
import org.apache.spark.sql.connect.common.DataTypeProtoConverter._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

object LiteralProtoConverter {

  private lazy val nullType =
    proto.DataType.newBuilder().setNull(proto.DataType.NULL.getDefaultInstance).build()

  /**
   * Transforms literal value to the `proto.Expression.Literal.Builder`.
   *
   * @return
   *   proto.Expression.Literal.Builder
   */
  @scala.annotation.tailrec
  def toLiteralProtoBuilder(literal: Any): proto.Expression.Literal.Builder = {
    val builder = proto.Expression.Literal.newBuilder()

    def decimalBuilder(precision: Int, scale: Int, value: String) = {
      builder.getDecimalBuilder.setPrecision(precision).setScale(scale).setValue(value)
    }

    def calendarIntervalBuilder(months: Int, days: Int, microseconds: Long) = {
      builder.getCalendarIntervalBuilder
        .setMonths(months)
        .setDays(days)
        .setMicroseconds(microseconds)
    }

    def arrayBuilder(array: Array[_]) = {
      val ab = builder.getArrayBuilder
        .setElementType(componentTypeToProto(array.getClass.getComponentType))
      array.foreach(x => ab.addElement(toLiteralProto(x)))
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
      case v: JBigDecimal =>
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

  /**
   * Transforms literal value to the `proto.Expression.Literal`.
   *
   * @return
   *   proto.Expression.Literal
   */
  def toLiteralProto(literal: Any): proto.Expression.Literal =
    toLiteralProtoBuilder(literal).build()

  private def componentTypeToProto(clz: Class[_]): proto.DataType = clz match {
    // primitive types
    case JShort.TYPE => toConnectProtoType(ShortType)
    case JInteger.TYPE => toConnectProtoType(IntegerType)
    case JLong.TYPE => toConnectProtoType(LongType)
    case JDouble.TYPE => toConnectProtoType(DoubleType)
    case JByte.TYPE => toConnectProtoType(ByteType)
    case JFloat.TYPE => toConnectProtoType(FloatType)
    case JBoolean.TYPE => toConnectProtoType(BooleanType)
    case JChar.TYPE => toConnectProtoType(StringType)

    // java classes
    case _ if clz == classOf[LocalDate] || clz == classOf[Date] =>
      toConnectProtoType(DateType)
    case _ if clz == classOf[Instant] || clz == classOf[Timestamp] =>
      toConnectProtoType(TimestampType)
    case _ if clz == classOf[LocalDateTime] => toConnectProtoType(TimestampNTZType)
    case _ if clz == classOf[Duration] => toConnectProtoType(DayTimeIntervalType.DEFAULT)
    case _ if clz == classOf[Period] => toConnectProtoType(YearMonthIntervalType.DEFAULT)
    case _ if clz == classOf[JBigDecimal] => toConnectProtoType(DecimalType.SYSTEM_DEFAULT)
    case _ if clz == classOf[Array[Byte]] => toConnectProtoType(BinaryType)
    case _ if clz == classOf[Array[Char]] => toConnectProtoType(StringType)
    case _ if clz == classOf[JShort] => toConnectProtoType(ShortType)
    case _ if clz == classOf[JInteger] => toConnectProtoType(IntegerType)
    case _ if clz == classOf[JLong] => toConnectProtoType(LongType)
    case _ if clz == classOf[JDouble] => toConnectProtoType(DoubleType)
    case _ if clz == classOf[JByte] => toConnectProtoType(ByteType)
    case _ if clz == classOf[JFloat] => toConnectProtoType(FloatType)
    case _ if clz == classOf[JBoolean] => toConnectProtoType(BooleanType)

    // other scala classes
    case _ if clz == classOf[String] => toConnectProtoType(StringType)
    case _ if clz == classOf[BigInt] || clz == classOf[BigDecimal] =>
      toConnectProtoType(DecimalType.SYSTEM_DEFAULT)
    case _ if clz == classOf[CalendarInterval] => toConnectProtoType(CalendarIntervalType)
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
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported component type $clz in arrays.")
  }
}
