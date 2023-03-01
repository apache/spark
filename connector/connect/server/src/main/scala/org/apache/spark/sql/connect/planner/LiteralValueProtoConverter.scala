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

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object LiteralValueProtoConverter {

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
          toArrayType(lit.getArray.getElementType))
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

  def toConnectProtoValue(value: Any): proto.Expression.Literal = {
    value match {
      case null =>
        proto.Expression.Literal
          .newBuilder()
          .setNull(DataTypeProtoConverter.toConnectProtoType(NullType))
          .build()
      case b: Boolean => proto.Expression.Literal.newBuilder().setBoolean(b).build()
      case b: Byte => proto.Expression.Literal.newBuilder().setByte(b).build()
      case s: Short => proto.Expression.Literal.newBuilder().setShort(s).build()
      case i: Int => proto.Expression.Literal.newBuilder().setInteger(i).build()
      case l: Long => proto.Expression.Literal.newBuilder().setLong(l).build()
      case f: Float => proto.Expression.Literal.newBuilder().setFloat(f).build()
      case d: Double => proto.Expression.Literal.newBuilder().setDouble(d).build()
      case s: String => proto.Expression.Literal.newBuilder().setString(s).build()
      case o => throw new Exception(s"Unsupported value type: $o")
    }
  }

  private def toArrayData(array: proto.Expression.Literal.Array): Any = {
    def makeArrayData[T](
        initFunc: Int => Array[T],
        converter: proto.Expression.Literal => T): Array[T] = {
      val elementList = array.getElementList
      val data = initFunc(elementList.size())
      var idx = 0
      val iter = elementList.iterator()
      while (iter.hasNext) {
        data(idx) = converter(iter.next())
        idx += 1
      }
      data
    }

    val elementType = array.getElementType
    if (elementType.hasShort) {
      makeArrayData(size => new Array[Short](size), v => v.getShort.toShort)
    } else if (elementType.hasInteger) {
      makeArrayData(size => new Array[Int](size), v => v.getInteger)
    } else if (elementType.hasLong) {
      makeArrayData(size => new Array[Long](size), v => v.getLong)
    } else if (elementType.hasDouble) {
      makeArrayData(size => new Array[Double](size), v => v.getDouble)
    } else if (elementType.hasByte) {
      makeArrayData(size => new Array[Byte](size), v => v.getByte.toByte)
    } else if (elementType.hasFloat) {
      makeArrayData(size => new Array[Float](size), v => v.getFloat)
    } else if (elementType.hasBoolean) {
      makeArrayData(size => new Array[Boolean](size), v => v.getBoolean)
    } else if (elementType.hasString) {
      makeArrayData(size => new Array[String](size), v => v.getString)
    } else if (elementType.hasBinary) {
      makeArrayData(size => new Array[Array[Byte]](size), v => v.getBinary.toByteArray)
    } else if (elementType.hasDate) {
      makeArrayData(
        size => new Array[java.sql.Date](size),
        v => DateTimeUtils.toJavaDate(v.getDate))
    } else if (elementType.hasTimestamp) {
      makeArrayData(
        size => new Array[java.sql.Timestamp](size),
        v => DateTimeUtils.toJavaTimestamp(v.getTimestamp))
    } else if (elementType.hasTimestampNtz) {
      makeArrayData(
        size => new Array[java.time.LocalDateTime](size),
        v => DateTimeUtils.microsToLocalDateTime(v.getTimestampNtz))
    } else if (elementType.hasDayTimeInterval) {
      makeArrayData(
        size => new Array[java.time.Duration](size),
        v => IntervalUtils.microsToDuration(v.getDayTimeInterval))
    } else if (elementType.hasYearMonthInterval) {
      makeArrayData(
        size => new Array[java.time.Period](size),
        v => IntervalUtils.monthsToPeriod(v.getYearMonthInterval))
    } else if (elementType.hasDecimal) {
      makeArrayData(size => new Array[Decimal](size), v => Decimal(v.getDecimal.getValue))
    } else if (elementType.hasCalendarInterval) {
      makeArrayData(
        size => new Array[CalendarInterval](size),
        v => {
          val interval = v.getCalendarInterval
          new CalendarInterval(interval.getMonths, interval.getDays, interval.getMicroseconds)
        })
    } else if (elementType.hasArray) {
      makeArrayData(size => new Array[Any](size), v => toArrayData(v.getArray))
    } else {
      throw InvalidPlanInput(s"Unsupported Literal Type: $elementType)")
    }
  }

  private def toArrayType(elementType: proto.DataType): ArrayType = {
    if (elementType.hasShort) {
      ArrayType(ShortType)
    } else if (elementType.hasInteger) {
      ArrayType(IntegerType)
    } else if (elementType.hasLong) {
      ArrayType(LongType)
    } else if (elementType.hasDouble) {
      ArrayType(DoubleType)
    } else if (elementType.hasByte) {
      ArrayType(ByteType)
    } else if (elementType.hasFloat) {
      ArrayType(FloatType)
    } else if (elementType.hasBoolean) {
      ArrayType(BooleanType)
    } else if (elementType.hasString) {
      ArrayType(StringType)
    } else if (elementType.hasBinary) {
      ArrayType(BinaryType)
    } else if (elementType.hasDate) {
      ArrayType(DateType)
    } else if (elementType.hasTimestamp) {
      ArrayType(TimestampType)
    } else if (elementType.hasTimestampNtz) {
      ArrayType(TimestampNTZType)
    } else if (elementType.hasDayTimeInterval) {
      val interval = elementType.getDayTimeInterval
      ArrayType(DayTimeIntervalType(interval.getStartField.toByte, interval.getEndField.toByte))
    } else if (elementType.hasYearMonthInterval) {
      val interval = elementType.getYearMonthInterval
      ArrayType(YearMonthIntervalType(interval.getStartField.toByte, interval.getEndField.toByte))
    } else if (elementType.hasDecimal) {
      val decimal = elementType.getDecimal
      ArrayType(DecimalType(decimal.getPrecision, decimal.getScale))
    } else if (elementType.hasCalendarInterval) {
      ArrayType(CalendarIntervalType)
    } else if (elementType.hasArray) {
      ArrayType(toArrayType(elementType.getArray.getElementType))
    } else {
      throw InvalidPlanInput(s"Unsupported Literal Type: $elementType)")
    }
  }
}
