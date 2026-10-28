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

package org.apache.spark.sql.connect.client.arrow.types.ops

import java.time.{Instant, LocalDateTime}

import org.apache.arrow.vector.{FieldVector, TimeStampNanoTZVector, TimeStampNanoVector}

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{InstantNanosEncoder, LocalDateTimeNanosEncoder}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MICROS
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils
import org.apache.spark.sql.connect.client.arrow.{ArrowDeserializers, ArrowSerializer, ArrowVectorReader, TypedArrowVectorReader}
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.common.types.ops.ConnectTypeOps
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Combined Connect operations shared by the nanosecond-capable timestamp types
 * ([[TimestampNTZNanosType]] and [[TimestampLTZNanosType]], precision 7..9).
 *
 * Implements both the proto DataType/Literal side and the Arrow side of [[ConnectTypeOps]].
 * The physical value is [[org.apache.spark.unsafe.types.TimestampNanosVal]] (epoch micros +
 * nanos within the micro), which the proto carries as `epoch_micros` + `nanos_within_micro`
 * rather than a single int64 of nanoseconds, because nanoseconds-since-epoch cannot span the
 * supported 0001..9999 year range.
 * The two concrete subclasses differ only in their proto message arm, external java.time value
 * ([[LocalDateTime]] for NTZ, [[Instant]] for LTZ) and the conversion helpers used.
 *
 * On the Arrow side the value is stored as a single int64 of epoch-nanoseconds
 * (TimeStampNanoVector for NTZ, TimeStampNanoTZVector for LTZ); the precision is recovered from
 * the Arrow field metadata (see ArrowUtils).
 *
 * Lives under the arrow.types.ops sub-package to co-locate with [[TimeTypeConnectOps]], the
 * reference implementation this mirrors.
 *
 * @since 4.3.0
 */
private[connect] abstract class TimestampNanosTypeConnectOps extends ConnectTypeOps {

  /**
   * Rebuilds the physical value from the two proto components. `nanosWithinMicro` is an int32 on
   * the wire, so its range is checked here before narrowing to `Short`: without the check
   * `.toShort` would truncate an out-of-range value modulo 2^16 (e.g. 65536 -> 0) and slip it
   * past the `[0, 999]` guard in `fromParts`, yielding a silently wrong value instead of a clear
   * error.
   */
  protected def toTimestampNanosVal(
      epochMicros: Long,
      nanosWithinMicro: Int): TimestampNanosVal = {
    if (nanosWithinMicro < 0 || nanosWithinMicro > TimestampNanosVal.MAX_NANOS_WITHIN_MICRO) {
      throw InvalidPlanInput(
        s"nanos_within_micro must be in [0, ${TimestampNanosVal.MAX_NANOS_WITHIN_MICRO}], got: " +
          nanosWithinMicro)
    }
    TimestampNanosVal.fromParts(epochMicros, nanosWithinMicro.toShort)
  }

  /** Converts a physical value to the epoch-nanoseconds stored in an Arrow vector. */
  protected def toEpochNanos(v: TimestampNanosVal): Long =
    Math.addExact(Math.multiplyExact(v.epochMicros, NANOS_PER_MICROS), v.nanosWithinMicro.toLong)
}

/**
 * Connect operations for [[TimestampNTZNanosType]]. The external java.time value is
 * [[LocalDateTime]] (interpreted at UTC), matching the server-side TypeOps and RowEncoder.
 *
 * @param t
 *   The TimestampNTZNanosType with precision information
 * @since 4.3.0
 */
private[connect] class TimestampNTZNanosTypeConnectOps(val t: TimestampNTZNanosType)
    extends TimestampNanosTypeConnectOps {

  override def dataType: DataType = t

  override def encoder: AgnosticEncoder[_] = LocalDateTimeNanosEncoder(t.precision)

  // ==================== Proto Conversions ====================

  override def toCatalystTypeFromProto(t: proto.DataType): DataType = {
    val nanos = t.getTimestampNtzNanos
    if (nanos.hasPrecision) TimestampNTZNanosType(nanos.getPrecision) else TimestampNTZNanosType()
  }

  override def toConnectProtoType: proto.DataType = {
    proto.DataType
      .newBuilder()
      .setTimestampNtzNanos(
        proto.DataType.TimestampNTZNanos.newBuilder().setPrecision(t.precision).build())
      .build()
  }

  override def toLiteralProto(
      value: Any,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder =
    setLiteral(value, TimestampNTZNanosType.DEFAULT_PRECISION, builder)

  override def toLiteralProtoWithType(
      value: Any,
      dt: DataType,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder =
    setLiteral(value, dt.asInstanceOf[TimestampNTZNanosType].precision, builder)

  private def setLiteral(
      value: Any,
      precision: Int,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    val v = SparkDateTimeUtils
      .localDateTimeToTimestampNanos(value.asInstanceOf[LocalDateTime], precision)
    builder.setTimestampNtzNanos(
      builder.getTimestampNtzNanosBuilder
        .setEpochMicros(v.epochMicros)
        .setNanosWithinMicro(v.nanosWithinMicro.toInt)
        .setPrecision(precision))
  }

  override def getScalaConverter: proto.Expression.Literal => Any = { v =>
    val nanos = v.getTimestampNtzNanos
    SparkDateTimeUtils.timestampNanosToLocalDateTime(
      toTimestampNanosVal(nanos.getEpochMicros, nanos.getNanosWithinMicro))
  }

  override def getProtoDataTypeFromLiteral(literal: proto.Expression.Literal): proto.DataType = {
    val typeBuilder = proto.DataType.TimestampNTZNanos.newBuilder()
    if (literal.getTimestampNtzNanos.hasPrecision) {
      typeBuilder.setPrecision(literal.getTimestampNtzNanos.getPrecision)
    }
    proto.DataType.newBuilder().setTimestampNtzNanos(typeBuilder.build()).build()
  }

  // ==================== Arrow Serialization ====================

  override def createArrowSerializer(vector: AnyRef): ArrowSerializer.Serializer = {
    val v = vector.asInstanceOf[TimeStampNanoVector]
    new ArrowSerializer.FieldSerializer[LocalDateTime, TimeStampNanoVector](v) {
      override def set(index: Int, value: LocalDateTime): Unit = {
        v.setSafe(
          index,
          toEpochNanos(SparkDateTimeUtils.localDateTimeToTimestampNanos(value, t.precision)))
      }
    }
  }

  override def createArrowDeserializer(
      enc: AgnosticEncoder[_],
      data: AnyRef,
      timeZoneId: String): ArrowDeserializers.Deserializer[Any] = {
    val v = data.asInstanceOf[FieldVector]
    new ArrowDeserializers.LeafFieldDeserializer[LocalDateTime](enc, v, timeZoneId) {
      override def value(i: Int): LocalDateTime = reader.getLocalDateTime(i)
    }
  }

  override def createArrowVectorReader(vector: FieldVector): ArrowVectorReader = {
    new TimestampNTZNanosVectorReader(vector.asInstanceOf[TimeStampNanoVector])
  }
}

/**
 * Connect operations for [[TimestampLTZNanosType]]. The external java.time value is [[Instant]],
 * matching the server-side TypeOps and RowEncoder.
 *
 * @param t
 *   The TimestampLTZNanosType with precision information
 * @since 4.3.0
 */
private[connect] class TimestampLTZNanosTypeConnectOps(val t: TimestampLTZNanosType)
    extends TimestampNanosTypeConnectOps {

  override def dataType: DataType = t

  override def encoder: AgnosticEncoder[_] = InstantNanosEncoder(t.precision)

  // ==================== Proto Conversions ====================

  override def toCatalystTypeFromProto(t: proto.DataType): DataType = {
    val nanos = t.getTimestampLtzNanos
    if (nanos.hasPrecision) TimestampLTZNanosType(nanos.getPrecision) else TimestampLTZNanosType()
  }

  override def toConnectProtoType: proto.DataType = {
    proto.DataType
      .newBuilder()
      .setTimestampLtzNanos(
        proto.DataType.TimestampLTZNanos.newBuilder().setPrecision(t.precision).build())
      .build()
  }

  override def toLiteralProto(
      value: Any,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder =
    setLiteral(value, TimestampLTZNanosType.DEFAULT_PRECISION, builder)

  override def toLiteralProtoWithType(
      value: Any,
      dt: DataType,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder =
    setLiteral(value, dt.asInstanceOf[TimestampLTZNanosType].precision, builder)

  private def setLiteral(
      value: Any,
      precision: Int,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    val v = SparkDateTimeUtils.instantToTimestampNanos(value.asInstanceOf[Instant], precision)
    builder.setTimestampLtzNanos(
      builder.getTimestampLtzNanosBuilder
        .setEpochMicros(v.epochMicros)
        .setNanosWithinMicro(v.nanosWithinMicro.toInt)
        .setPrecision(precision))
  }

  override def getScalaConverter: proto.Expression.Literal => Any = { v =>
    val nanos = v.getTimestampLtzNanos
    SparkDateTimeUtils.timestampNanosToInstant(
      toTimestampNanosVal(nanos.getEpochMicros, nanos.getNanosWithinMicro))
  }

  override def getProtoDataTypeFromLiteral(literal: proto.Expression.Literal): proto.DataType = {
    val typeBuilder = proto.DataType.TimestampLTZNanos.newBuilder()
    if (literal.getTimestampLtzNanos.hasPrecision) {
      typeBuilder.setPrecision(literal.getTimestampLtzNanos.getPrecision)
    }
    proto.DataType.newBuilder().setTimestampLtzNanos(typeBuilder.build()).build()
  }

  // ==================== Arrow Serialization ====================

  override def createArrowSerializer(vector: AnyRef): ArrowSerializer.Serializer = {
    val v = vector.asInstanceOf[TimeStampNanoTZVector]
    new ArrowSerializer.FieldSerializer[Instant, TimeStampNanoTZVector](v) {
      override def set(index: Int, value: Instant): Unit = {
        v.setSafe(
          index,
          toEpochNanos(SparkDateTimeUtils.instantToTimestampNanos(value, t.precision)))
      }
    }
  }

  override def createArrowDeserializer(
      enc: AgnosticEncoder[_],
      data: AnyRef,
      timeZoneId: String): ArrowDeserializers.Deserializer[Any] = {
    val v = data.asInstanceOf[FieldVector]
    new ArrowDeserializers.LeafFieldDeserializer[Instant](enc, v, timeZoneId) {
      override def value(i: Int): Instant = reader.getInstant(i)
    }
  }

  override def createArrowVectorReader(vector: FieldVector): ArrowVectorReader = {
    new TimestampLTZNanosVectorReader(vector.asInstanceOf[TimeStampNanoTZVector])
  }
}

/** Reads epoch-nanoseconds from a TimeStampNanoVector as [[LocalDateTime]]. */
private[connect] class TimestampNTZNanosVectorReader(v: TimeStampNanoVector)
    extends TypedArrowVectorReader[TimeStampNanoVector](v) {
  override def getLocalDateTime(i: Int): LocalDateTime =
    SparkDateTimeUtils.timestampNanosToLocalDateTime(
      TimestampNanosTypeConnectOps.fromEpochNanos(vector.get(i)))
}

/** Reads epoch-nanoseconds from a TimeStampNanoTZVector as [[Instant]]. */
private[connect] class TimestampLTZNanosVectorReader(v: TimeStampNanoTZVector)
    extends TypedArrowVectorReader[TimeStampNanoTZVector](v) {
  override def getInstant(i: Int): Instant =
    SparkDateTimeUtils.timestampNanosToInstant(
      TimestampNanosTypeConnectOps.fromEpochNanos(vector.get(i)))
}

private[connect] object TimestampNanosTypeConnectOps {

  /** Splits epoch-nanoseconds read from an Arrow vector into the physical value. */
  def fromEpochNanos(epochNanos: Long): TimestampNanosVal = {
    val epochMicros = Math.floorDiv(epochNanos, NANOS_PER_MICROS)
    val nanosWithinMicro = Math.floorMod(epochNanos, NANOS_PER_MICROS).toShort
    TimestampNanosVal.fromParts(epochMicros, nanosWithinMicro)
  }
}
