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

package org.apache.spark.sql.connect.planner

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object LiteralExpressionProtoConverter {

  /**
   * Transforms the protocol buffers literals into the appropriate Catalyst literal expression.
   *
   * @return
   *   Expression
   */
  def toCatalystExpression(lit: proto.Expression.Literal): expressions.Literal = {
    lit.getLiteralTypeCase match {
      case proto.Expression.Literal.LiteralTypeCase.NULL =>
        expressions.Literal(null, DataTypeProtoConverter.toCatalystType(lit.getNull))

      case proto.Expression.Literal.LiteralTypeCase.BINARY =>
        expressions.Literal(lit.getBinary.toByteArray, BinaryType)

      case proto.Expression.Literal.LiteralTypeCase.BOOLEAN =>
        expressions.Literal(lit.getBoolean, BooleanType)

      case proto.Expression.Literal.LiteralTypeCase.BYTE =>
        expressions.Literal(lit.getByte.toByte, ByteType)

      case proto.Expression.Literal.LiteralTypeCase.SHORT =>
        expressions.Literal(lit.getShort.toShort, ShortType)

      case proto.Expression.Literal.LiteralTypeCase.INTEGER =>
        expressions.Literal(lit.getInteger, IntegerType)

      case proto.Expression.Literal.LiteralTypeCase.LONG =>
        expressions.Literal(lit.getLong, LongType)

      case proto.Expression.Literal.LiteralTypeCase.FLOAT =>
        expressions.Literal(lit.getFloat, FloatType)

      case proto.Expression.Literal.LiteralTypeCase.DOUBLE =>
        expressions.Literal(lit.getDouble, DoubleType)

      case proto.Expression.Literal.LiteralTypeCase.DECIMAL =>
        val decimal = Decimal.apply(lit.getDecimal.getValue)
        var precision = decimal.precision
        if (lit.getDecimal.hasPrecision) {
          precision = math.max(precision, lit.getDecimal.getPrecision)
        }
        var scale = decimal.scale
        if (lit.getDecimal.hasScale) {
          scale = math.max(scale, lit.getDecimal.getScale)
        }
        expressions.Literal(decimal, DecimalType(math.max(precision, scale), scale))

      case proto.Expression.Literal.LiteralTypeCase.STRING =>
        expressions.Literal(UTF8String.fromString(lit.getString), StringType)

      case proto.Expression.Literal.LiteralTypeCase.DATE =>
        expressions.Literal(lit.getDate, DateType)

      case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP =>
        expressions.Literal(lit.getTimestamp, TimestampType)

      case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP_NTZ =>
        expressions.Literal(lit.getTimestampNtz, TimestampNTZType)

      case proto.Expression.Literal.LiteralTypeCase.CALENDAR_INTERVAL =>
        val interval = new CalendarInterval(
          lit.getCalendarInterval.getMonths,
          lit.getCalendarInterval.getDays,
          lit.getCalendarInterval.getMicroseconds)
        expressions.Literal(interval, CalendarIntervalType)

      case proto.Expression.Literal.LiteralTypeCase.YEAR_MONTH_INTERVAL =>
        expressions.Literal(lit.getYearMonthInterval, YearMonthIntervalType())

      case proto.Expression.Literal.LiteralTypeCase.DAY_TIME_INTERVAL =>
        expressions.Literal(lit.getDayTimeInterval, DayTimeIntervalType())

      case proto.Expression.Literal.LiteralTypeCase.ARRAY =>
        expressions.Literal.create(
          toArrayData(lit.getArray),
          ArrayType(DataTypeProtoConverter.toCatalystType(lit.getArray.getElementType)))

      case _ =>
        throw InvalidPlanInput(
          s"Unsupported Literal Type: ${lit.getLiteralTypeCase.getNumber}" +
            s"(${lit.getLiteralTypeCase.name})")
    }
  }

  def toCatalystValue(lit: proto.Expression.Literal): Any = {
    lit.getLiteralTypeCase match {
      case proto.Expression.Literal.LiteralTypeCase.STRING => lit.getString

      case _ => toCatalystExpression(lit).value
    }
  }

  private def toArrayData(array: proto.Expression.Literal.Array): Any = {
    def makeArrayData[T](converter: proto.Expression.Literal => T)(implicit
        tag: ClassTag[T]): Array[T] = {
      val builder = mutable.ArrayBuilder.make[T]
      val elementList = array.getElementsList
      builder.sizeHint(elementList.size())
      val iter = elementList.iterator()
      while (iter.hasNext) {
        builder += converter(iter.next())
      }
      builder.result()
    }

    val elementType = array.getElementType
    if (elementType.hasShort) {
      makeArrayData(v => v.getShort.toShort)
    } else if (elementType.hasInteger) {
      makeArrayData(v => v.getInteger)
    } else if (elementType.hasLong) {
      makeArrayData(v => v.getLong)
    } else if (elementType.hasDouble) {
      makeArrayData(v => v.getDouble)
    } else if (elementType.hasByte) {
      makeArrayData(v => v.getByte.toByte)
    } else if (elementType.hasFloat) {
      makeArrayData(v => v.getFloat)
    } else if (elementType.hasBoolean) {
      makeArrayData(v => v.getBoolean)
    } else if (elementType.hasString) {
      makeArrayData(v => v.getString)
    } else if (elementType.hasBinary) {
      makeArrayData(v => v.getBinary.toByteArray)
    } else if (elementType.hasDate) {
      makeArrayData(v => DateTimeUtils.toJavaDate(v.getDate))
    } else if (elementType.hasTimestamp) {
      makeArrayData(v => DateTimeUtils.toJavaTimestamp(v.getTimestamp))
    } else if (elementType.hasTimestampNtz) {
      makeArrayData(v => DateTimeUtils.microsToLocalDateTime(v.getTimestampNtz))
    } else if (elementType.hasDayTimeInterval) {
      makeArrayData(v => IntervalUtils.microsToDuration(v.getDayTimeInterval))
    } else if (elementType.hasYearMonthInterval) {
      makeArrayData(v => IntervalUtils.monthsToPeriod(v.getYearMonthInterval))
    } else if (elementType.hasDecimal) {
      makeArrayData(v => Decimal(v.getDecimal.getValue))
    } else if (elementType.hasCalendarInterval) {
      makeArrayData(v => {
        val interval = v.getCalendarInterval
        new CalendarInterval(interval.getMonths, interval.getDays, interval.getMicroseconds)
      })
    } else if (elementType.hasArray) {
      makeArrayData(v => toArrayData(v.getArray))
    } else {
      throw InvalidPlanInput(s"Unsupported Literal Type: $elementType)")
    }
  }
}
