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
package org.apache.spark.sql.connect.client.arrow

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period, ZoneOffset}

import org.apache.arrow.vector.{BigIntVector, BitVector, DateDayVector, DecimalVector, DurationVector, FieldVector, Float4Vector, Float8Vector, IntervalYearVector, IntVector, NullVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, VarBinaryVector, VarCharVector}
import org.apache.arrow.vector.util.Text

import org.apache.spark.sql.catalyst.util.{DateFormatter, SparkIntervalUtils, SparkStringUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_SECOND
import org.apache.spark.sql.catalyst.util.IntervalStringStyles.ANSI_STYLE
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils._
import org.apache.spark.sql.types.{DataType, DayTimeIntervalType, Decimal, UpCastRule, YearMonthIntervalType}
import org.apache.spark.sql.util.ArrowUtils

/**
 * Base class for reading leaf values from an arrow vector. This reader has read methods for all
 * leaf data types supported by the encoder framework. A subclass should always implement one of
 * the read methods. If upcasting is allowed for the given vector, then all allowed read methods
 * must be implemented.
 */
private[arrow] abstract class ArrowVectorReader {
  def isNull(i: Int): Boolean
  def getBoolean(i: Int): Boolean = unsupported()
  def getByte(i: Int): Byte = unsupported()
  def getShort(i: Int): Short = unsupported()
  def getInt(i: Int): Int = unsupported()
  def getLong(i: Int): Long = unsupported()
  def getFloat(i: Int): Float = unsupported()
  def getDouble(i: Int): Double = unsupported()
  def getString(i: Int): String = unsupported()
  def getBytes(i: Int): Array[Byte] = unsupported()
  def getJavaDecimal(i: Int): JBigDecimal = unsupported()
  def getJavaBigInt(i: Int): java.math.BigInteger = getJavaDecimal(i).toBigInteger
  def getScalaDecimal(i: Int): BigDecimal = BigDecimal(getJavaDecimal(i))
  def getScalaBigInt(i: Int): BigInt = BigInt(getJavaBigInt(i))
  def getDecimal(i: Int): Decimal = Decimal(getJavaDecimal(i))
  def getPeriod(i: Int): java.time.Period = unsupported()
  def getDuration(i: Int): java.time.Duration = unsupported()
  def getDate(i: Int): java.sql.Date = unsupported()
  def getTimestamp(i: Int): java.sql.Timestamp = unsupported()
  def getInstant(i: Int): java.time.Instant = unsupported()
  def getLocalDate(i: Int): java.time.LocalDate = unsupported()
  def getLocalDateTime(i: Int): java.time.LocalDateTime = unsupported()
  private def unsupported(): Nothing = throw new UnsupportedOperationException()
}

object ArrowVectorReader {
  def apply(
      targetDataType: DataType,
      vector: FieldVector,
      timeZoneId: String): ArrowVectorReader = {
    val vectorDataType = ArrowUtils.fromArrowType(vector.getField.getType)
    if (!UpCastRule.canUpCast(vectorDataType, targetDataType)) {
      throw new RuntimeException(
        s"Reading '$targetDataType' values from a ${vector.getClass} instance is not supported.")
    }
    vector match {
      case v: BitVector => new BitVectorReader(v)
      case v: TinyIntVector => new TinyIntVectorReader(v)
      case v: SmallIntVector => new SmallIntVectorReader(v)
      case v: IntVector => new IntVectorReader(v)
      case v: BigIntVector => new BigIntVectorReader(v)
      case v: Float4Vector => new Float4VectorReader(v)
      case v: Float8Vector => new Float8VectorReader(v)
      case v: DecimalVector => new DecimalVectorReader(v)
      case v: VarCharVector => new VarCharVectorReader(v)
      case v: VarBinaryVector => new VarBinaryVectorReader(v)
      case v: DurationVector => new DurationVectorReader(v)
      case v: IntervalYearVector => new IntervalYearVectorReader(v)
      case v: DateDayVector => new DateDayVectorReader(v, timeZoneId)
      case v: TimeStampMicroTZVector => new TimeStampMicroTZVectorReader(v)
      case v: TimeStampMicroVector => new TimeStampMicroVectorReader(v, timeZoneId)
      case _: NullVector => NullVectorReader
      case _ => throw new RuntimeException("Unsupported Vector Type: " + vector.getClass)
    }
  }
}

private[arrow] object NullVectorReader extends ArrowVectorReader {
  override def isNull(i: Int): Boolean = true
}

private[arrow] abstract class TypedArrowVectorReader[E <: FieldVector](val vector: E)
    extends ArrowVectorReader {
  override def isNull(i: Int): Boolean = vector.isNull(i)
}

private[arrow] class BitVectorReader(v: BitVector) extends TypedArrowVectorReader[BitVector](v) {
  override def getBoolean(i: Int): Boolean = vector.get(i) > 0
  override def getString(i: Int): String = String.valueOf(getBoolean(i))
}

private[arrow] class TinyIntVectorReader(v: TinyIntVector)
    extends TypedArrowVectorReader[TinyIntVector](v) {
  override def getByte(i: Int): Byte = vector.get(i)
  override def getShort(i: Int): Short = getByte(i)
  override def getInt(i: Int): Int = getByte(i)
  override def getLong(i: Int): Long = getByte(i)
  override def getFloat(i: Int): Float = getByte(i)
  override def getDouble(i: Int): Double = getByte(i)
  override def getString(i: Int): String = String.valueOf(getByte(i))
  override def getJavaDecimal(i: Int): JBigDecimal = JBigDecimal.valueOf(getByte(i))
}

private[arrow] class SmallIntVectorReader(v: SmallIntVector)
    extends TypedArrowVectorReader[SmallIntVector](v) {
  override def getShort(i: Int): Short = vector.get(i)
  override def getInt(i: Int): Int = getShort(i)
  override def getLong(i: Int): Long = getShort(i)
  override def getFloat(i: Int): Float = getShort(i)
  override def getDouble(i: Int): Double = getShort(i)
  override def getString(i: Int): String = String.valueOf(getShort(i))
  override def getJavaDecimal(i: Int): JBigDecimal = JBigDecimal.valueOf(getShort(i))
}

private[arrow] class IntVectorReader(v: IntVector) extends TypedArrowVectorReader[IntVector](v) {
  override def getInt(i: Int): Int = vector.get(i)
  override def getLong(i: Int): Long = getInt(i)
  override def getFloat(i: Int): Float = getInt(i).toFloat
  override def getDouble(i: Int): Double = getInt(i)
  override def getString(i: Int): String = String.valueOf(getInt(i))
  override def getJavaDecimal(i: Int): JBigDecimal = JBigDecimal.valueOf(getInt(i))
}

private[arrow] class BigIntVectorReader(v: BigIntVector)
    extends TypedArrowVectorReader[BigIntVector](v) {
  override def getLong(i: Int): Long = vector.get(i)
  override def getFloat(i: Int): Float = getLong(i).toFloat
  override def getDouble(i: Int): Double = getLong(i).toDouble
  override def getString(i: Int): String = String.valueOf(getLong(i))
  override def getJavaDecimal(i: Int): JBigDecimal = JBigDecimal.valueOf(getLong(i))
  override def getTimestamp(i: Int): Timestamp = toJavaTimestamp(getLong(i) * MICROS_PER_SECOND)
  override def getInstant(i: Int): Instant = microsToInstant(getLong(i))
}

private[arrow] class Float4VectorReader(v: Float4Vector)
    extends TypedArrowVectorReader[Float4Vector](v) {
  override def getFloat(i: Int): Float = vector.get(i)
  override def getDouble(i: Int): Double = getFloat(i)
  override def getString(i: Int): String = String.valueOf(getFloat(i))
  override def getJavaDecimal(i: Int): JBigDecimal = JBigDecimal.valueOf(getFloat(i))
}

private[arrow] class Float8VectorReader(v: Float8Vector)
    extends TypedArrowVectorReader[Float8Vector](v) {
  override def getDouble(i: Int): Double = vector.get(i)
  override def getString(i: Int): String = String.valueOf(getDouble(i))
  override def getJavaDecimal(i: Int): JBigDecimal = JBigDecimal.valueOf(getDouble(i))
}

private[arrow] class DecimalVectorReader(v: DecimalVector)
    extends TypedArrowVectorReader[DecimalVector](v) {
  override def getByte(i: Int): Byte = getJavaDecimal(i).byteValueExact()
  override def getShort(i: Int): Short = getJavaDecimal(i).shortValueExact()
  override def getInt(i: Int): Int = getJavaDecimal(i).intValueExact()
  override def getLong(i: Int): Long = getJavaDecimal(i).longValueExact()
  override def getFloat(i: Int): Float = getJavaDecimal(i).floatValue()
  override def getDouble(i: Int): Double = getJavaDecimal(i).doubleValue()
  override def getJavaDecimal(i: Int): JBigDecimal = vector.getObject(i)
  override def getString(i: Int): String = getJavaDecimal(i).toPlainString
}

private[arrow] class VarCharVectorReader(v: VarCharVector)
    extends TypedArrowVectorReader[VarCharVector](v) {
  // This is currently a bit heavy on allocations:
  // - byte array created in VarCharVector.get
  // - CharBuffer created CharSetEncoder
  // - char array in String
  // By using direct buffers and reusing the char buffer
  // we could get rid of the first two allocations.
  override def getString(i: Int): String = Text.decode(vector.get(i))
}

private[arrow] class VarBinaryVectorReader(v: VarBinaryVector)
    extends TypedArrowVectorReader[VarBinaryVector](v) {
  override def getBytes(i: Int): Array[Byte] = vector.get(i)
  override def getString(i: Int): String = SparkStringUtils.getHexString(getBytes(i))
}

private[arrow] class DurationVectorReader(v: DurationVector)
    extends TypedArrowVectorReader[DurationVector](v) {
  override def getDuration(i: Int): Duration = vector.getObject(i)
  override def getString(i: Int): String = {
    SparkIntervalUtils.toDayTimeIntervalString(
      SparkIntervalUtils.durationToMicros(getDuration(i)),
      ANSI_STYLE,
      DayTimeIntervalType.DEFAULT.startField,
      DayTimeIntervalType.DEFAULT.endField)
  }
}

private[arrow] class IntervalYearVectorReader(v: IntervalYearVector)
    extends TypedArrowVectorReader[IntervalYearVector](v) {
  override def getPeriod(i: Int): Period = vector.getObject(i).normalized()
  override def getString(i: Int): String = {
    SparkIntervalUtils.toYearMonthIntervalString(
      vector.get(i),
      ANSI_STYLE,
      YearMonthIntervalType.DEFAULT.startField,
      YearMonthIntervalType.DEFAULT.endField)
  }
}

private[arrow] class DateDayVectorReader(v: DateDayVector, timeZoneId: String)
    extends TypedArrowVectorReader[DateDayVector](v) {
  private val zone = getZoneId(timeZoneId)
  private lazy val formatter = DateFormatter()
  private def days(i: Int): Int = vector.get(i)
  private def micros(i: Int): Long = daysToMicros(days(i), zone)
  override def getDate(i: Int): Date = toJavaDate(days(i))
  override def getLocalDate(i: Int): LocalDate = daysToLocalDate(days(i))
  override def getTimestamp(i: Int): Timestamp = toJavaTimestamp(micros(i))
  override def getInstant(i: Int): Instant = microsToInstant(micros(i))
  override def getLocalDateTime(i: Int): LocalDateTime = microsToLocalDateTime(micros(i))
  override def getString(i: Int): String = formatter.format(getLocalDate(i))
}

private[arrow] class TimeStampMicroTZVectorReader(v: TimeStampMicroTZVector)
    extends TypedArrowVectorReader[TimeStampMicroTZVector](v) {
  private val zone = getZoneId(v.getTimeZone)
  private lazy val formatter = TimestampFormatter.getFractionFormatter(zone)
  private def utcMicros(i: Int): Long = convertTz(vector.get(i), zone, ZoneOffset.UTC)
  override def getLong(i: Int): Long = Math.floorDiv(vector.get(i), MICROS_PER_SECOND)
  override def getTimestamp(i: Int): Timestamp = toJavaTimestamp(vector.get(i))
  override def getInstant(i: Int): Instant = microsToInstant(vector.get(i))
  override def getLocalDateTime(i: Int): LocalDateTime = microsToLocalDateTime(utcMicros(i))
  override def getString(i: Int): String = formatter.format(vector.get(i))
}

private[arrow] class TimeStampMicroVectorReader(v: TimeStampMicroVector, timeZoneId: String)
    extends TypedArrowVectorReader[TimeStampMicroVector](v) {
  private val zone = getZoneId(timeZoneId)
  private lazy val formatter = TimestampFormatter.getFractionFormatter(ZoneOffset.UTC)
  private def tzMicros(i: Int): Long = convertTz(utcMicros(i), ZoneOffset.UTC, zone)
  private def utcMicros(i: Int): Long = vector.get(i)
  override def getTimestamp(i: Int): Timestamp = toJavaTimestamp(tzMicros(i))
  override def getInstant(i: Int): Instant = microsToInstant(tzMicros(i))
  override def getLocalDateTime(i: Int): LocalDateTime = microsToLocalDateTime(utcMicros(i))
  override def getString(i: Int): String = formatter.format(utcMicros(i))
}
