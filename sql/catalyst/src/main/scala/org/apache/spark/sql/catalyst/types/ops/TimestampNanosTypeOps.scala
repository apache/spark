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

import java.time.{Instant, LocalDateTime, ZoneOffset}

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, MutableTimestampNanos, MutableValue}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalTimestampLTZNanosType, PhysicalTimestampNTZNanosType}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{ObjectType, TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.sql.types.ops.{TimestampLTZNanosTypeApiOps, TimestampNTZNanosTypeApiOps}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Server-side (catalyst) operations shared by the nanosecond timestamp types
 * (TimestampNTZNanosType and TimestampLTZNanosType).
 *
 * Internal values are [[TimestampNanosVal]] (epoch micros + nanos within the micro), stored in
 * [[org.apache.spark.sql.catalyst.expressions.UnsafeRow]] via a 16-byte variable-length payload;
 * see [[org.apache.spark.sql.catalyst.expressions.TimestampNanosRowValues]].
 *
 * The Types Framework is the sole integration path for the nanosecond timestamp types: physical
 * representation, literals, row accessors, codegen class selection, external java.time conversion,
 * and serializer/deserializer expression building all flow through these Ops. When the framework is
 * disabled the types are unsupported (the legacy fallbacks no longer special-case them). External
 * java.time conversion uses the helpers added by SPARK-57033: TimestampNTZNanosType maps to
 * java.time.LocalDateTime and TimestampLTZNanosType to java.time.Instant, with sub-micro digits
 * truncated to the column precision. Concrete subclasses supply the NTZ/LTZ-specific physical type,
 * row accessors, conversions, and serializer/deserializer expressions.
 *
 * @since 4.3.0
 */
trait TimestampNanosTypeOps extends TypeOps {

  // ==================== Physical Type Representation ====================

  override def getJavaClass: Class[_] = classOf[TimestampNanosVal]

  override def getMutableValue: MutableValue = new MutableTimestampNanos

  // ==================== Literal Creation ====================

  override def getDefaultLiteral: Literal = Literal.create(TimestampNanosVal.ZERO, dataType)

  override def getJavaLiteral(v: Any): String = {
    val tn = v.asInstanceOf[TimestampNanosVal]
    "org.apache.spark.unsafe.types.TimestampNanosVal.fromParts(" +
      s"${tn.epochMicros}L, (short) ${tn.nanosWithinMicro})"
  }

  // ==================== External Type Conversion ====================

  // Raises the same error as the legacy CatalystTypeConverters (SPARK-57033) when an external
  // value of an unexpected type is supplied for a nanosecond timestamp column.
  protected def invalidExternalValue(other: Any): Nothing =
    throw new SparkIllegalArgumentException(
      errorClass = "INVALID_EXTERNAL_VALUE",
      messageParameters = Map(
        "other" -> other.toString,
        "otherClass" -> other.getClass.getCanonicalName,
        "dataType" -> dataType.sql))
}

/**
 * Server-side operations for [[TimestampNTZNanosType]].
 *
 * @param t
 *   The TimestampNTZNanosType with precision information
 * @since 4.3.0
 */
case class TimestampNTZNanosTypeOps(override val t: TimestampNTZNanosType)
  extends TimestampNTZNanosTypeApiOps(t) with TimestampNanosTypeOps {

  override def getPhysicalType: PhysicalDataType = PhysicalTimestampNTZNanosType

  override def getRowWriter(ordinal: Int): (InternalRow, Any) => Unit =
    (input, v) => input.setTimestampNTZNanos(ordinal, v.asInstanceOf[TimestampNanosVal])

  override def toCatalystImpl(scalaValue: Any): Any = scalaValue match {
    case l: LocalDateTime => DateTimeUtils.localDateTimeToTimestampNanos(l, t.precision)
    case other => invalidExternalValue(other)
  }

  override def toScala(catalystValue: Any): Any =
    if (catalystValue == null) null
    else DateTimeUtils.timestampNanosToLocalDateTime(catalystValue.asInstanceOf[TimestampNanosVal])

  override def toScalaImpl(row: InternalRow, column: Int): Any =
    DateTimeUtils.timestampNanosToLocalDateTime(row.getTimestampNTZNanos(column))

  override def createSerializer(input: Expression): Option[Expression] =
    Some(StaticInvoke(
      DateTimeUtils.getClass,
      t,
      "localDateTimeToTimestampNanos",
      input :: Literal(t.precision) :: Nil,
      returnNullable = false))

  override def createDeserializer(path: Expression): Option[Expression] =
    Some(StaticInvoke(
      DateTimeUtils.getClass,
      ObjectType(classOf[LocalDateTime]),
      "timestampNanosToLocalDateTime",
      path :: Nil,
      returnNullable = false))
}

/**
 * Server-side operations for [[TimestampLTZNanosType]].
 *
 * @param t
 *   The TimestampLTZNanosType with precision information
 * @since 4.3.0
 */
// Server-side TypeOps is used only for physical type, literals, row accessors, and serde - never
// for rendering (cast-to-string flows through TypeApiOps.apply). So the rendering zone is unused
// here; pass UTC rather than reading the session config on every construction.
case class TimestampLTZNanosTypeOps(override val t: TimestampLTZNanosType)
  extends TimestampLTZNanosTypeApiOps(t, ZoneOffset.UTC) with TimestampNanosTypeOps {

  override def getPhysicalType: PhysicalDataType = PhysicalTimestampLTZNanosType

  override def getRowWriter(ordinal: Int): (InternalRow, Any) => Unit =
    (input, v) => input.setTimestampLTZNanos(ordinal, v.asInstanceOf[TimestampNanosVal])

  override def toCatalystImpl(scalaValue: Any): Any = scalaValue match {
    case i: Instant => DateTimeUtils.instantToTimestampNanos(i, t.precision)
    case other => invalidExternalValue(other)
  }

  override def toScala(catalystValue: Any): Any =
    if (catalystValue == null) null
    else DateTimeUtils.timestampNanosToInstant(catalystValue.asInstanceOf[TimestampNanosVal])

  override def toScalaImpl(row: InternalRow, column: Int): Any =
    DateTimeUtils.timestampNanosToInstant(row.getTimestampLTZNanos(column))

  override def createSerializer(input: Expression): Option[Expression] =
    Some(StaticInvoke(
      DateTimeUtils.getClass,
      t,
      "instantToTimestampNanos",
      input :: Literal(t.precision) :: Nil,
      returnNullable = false))

  override def createDeserializer(path: Expression): Option[Expression] =
    Some(StaticInvoke(
      DateTimeUtils.getClass,
      ObjectType(classOf[Instant]),
      "timestampNanosToInstant",
      path :: Nil,
      returnNullable = false))
}
