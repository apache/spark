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
import org.apache.spark.sql.connect.common.types.ops.ConnectTypeOps
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Combined Connect operations for TimestampNTZNanosType.
 *
 * Implements ConnectTypeOps for TimestampNTZNanosType, providing both proto DataType/Literal
 * conversions and Arrow serialization/deserialization.
 *
 * The Arrow physical type is TimeStampNanoVector (no timezone), which stores epoch-nanoseconds as
 * a single int64. The precision is recovered from the Arrow field metadata (see ArrowUtils).
 *
 * @param t
 *   The TimestampNTZNanosType with precision information
 * @since 4.3.0
 */
private[connect] class TimestampNTZNanosConnectOps(val t: TimestampNTZNanosType)
    extends ConnectTypeOps {

  override def dataType: DataType = t

  override def encoder: AgnosticEncoder[_] = LocalDateTimeNanosEncoder(t.precision)

  // ==================== Proto Conversions ====================

  override def toCatalystTypeFromProto(dt: proto.DataType): DataType = {
    val ntz = dt.getTimestampNtzNanos
    if (ntz.hasPrecision) TimestampNTZNanosType(ntz.getPrecision)
    else TimestampNTZNanosType(TimestampNTZNanosType.MAX_PRECISION)
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
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    val v = value.asInstanceOf[LocalDateTime]
    val tsv =
      SparkDateTimeUtils.localDateTimeToTimestampNanos(v, TimestampNTZNanosType.MAX_PRECISION)
    builder.setTimestampNtzNanos(
      builder.getTimestampNtzNanosBuilder
        .setEpochMicros(tsv.epochMicros)
        .setNanosWithinMicro(tsv.nanosWithinMicro)
        .setPrecision(TimestampNTZNanosType.MAX_PRECISION))
  }

  override def toLiteralProtoWithType(
      value: Any,
      dt: DataType,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    val v = value.asInstanceOf[LocalDateTime]
    val precision = dt.asInstanceOf[TimestampNTZNanosType].precision
    val tsv = SparkDateTimeUtils.localDateTimeToTimestampNanos(v, precision)
    builder.setTimestampNtzNanos(
      builder.getTimestampNtzNanosBuilder
        .setEpochMicros(tsv.epochMicros)
        .setNanosWithinMicro(tsv.nanosWithinMicro)
        .setPrecision(precision))
  }

  override def getScalaConverter: proto.Expression.Literal => Any = { v =>
    val ntz = v.getTimestampNtzNanos
    val tsv = TimestampNanosVal.fromParts(ntz.getEpochMicros, ntz.getNanosWithinMicro.toShort)
    SparkDateTimeUtils.timestampNanosToLocalDateTime(tsv)
  }

  override def getProtoDataTypeFromLiteral(literal: proto.Expression.Literal): proto.DataType = {
    val ntzBuilder = proto.DataType.TimestampNTZNanos.newBuilder()
    if (literal.getTimestampNtzNanos.hasPrecision) {
      ntzBuilder.setPrecision(literal.getTimestampNtzNanos.getPrecision)
    }
    proto.DataType.newBuilder().setTimestampNtzNanos(ntzBuilder.build()).build()
  }

  // ==================== Arrow Serialization ====================

  override def createArrowSerializer(vector: AnyRef): ArrowSerializer.Serializer = {
    val v = vector.asInstanceOf[TimeStampNanoVector]
    new ArrowSerializer.FieldSerializer[LocalDateTime, TimeStampNanoVector](v) {
      override def set(index: Int, value: LocalDateTime): Unit = {
        val tsv = SparkDateTimeUtils.localDateTimeToTimestampNanos(value, t.precision)
        val epochNanos = Math.addExact(
          Math.multiplyExact(tsv.epochMicros, NANOS_PER_MICROS),
          tsv.nanosWithinMicro.toLong)
        v.setSafe(index, epochNanos)
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
    new TimestampNTZNanosVectorReader(vector.asInstanceOf[TimeStampNanoVector], t.precision)
  }
}

/**
 * Combined Connect operations for TimestampLTZNanosType.
 *
 * The Arrow physical type is TimeStampNanoTZVector (with timezone), which stores
 * epoch-nanoseconds as a single int64.
 *
 * @param t
 *   The TimestampLTZNanosType with precision information
 * @since 4.3.0
 */
private[connect] class TimestampLTZNanosConnectOps(val t: TimestampLTZNanosType)
    extends ConnectTypeOps {

  override def dataType: DataType = t

  override def encoder: AgnosticEncoder[_] = InstantNanosEncoder(t.precision)

  // ==================== Proto Conversions ====================

  override def toCatalystTypeFromProto(dt: proto.DataType): DataType = {
    val ltz = dt.getTimestampLtzNanos
    if (ltz.hasPrecision) TimestampLTZNanosType(ltz.getPrecision)
    else TimestampLTZNanosType(TimestampLTZNanosType.MAX_PRECISION)
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
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    val v = value.asInstanceOf[Instant]
    val tsv = SparkDateTimeUtils.instantToTimestampNanos(v, TimestampLTZNanosType.MAX_PRECISION)
    builder.setTimestampLtzNanos(
      builder.getTimestampLtzNanosBuilder
        .setEpochMicros(tsv.epochMicros)
        .setNanosWithinMicro(tsv.nanosWithinMicro)
        .setPrecision(TimestampLTZNanosType.MAX_PRECISION))
  }

  override def toLiteralProtoWithType(
      value: Any,
      dt: DataType,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    val v = value.asInstanceOf[Instant]
    val precision = dt.asInstanceOf[TimestampLTZNanosType].precision
    val tsv = SparkDateTimeUtils.instantToTimestampNanos(v, precision)
    builder.setTimestampLtzNanos(
      builder.getTimestampLtzNanosBuilder
        .setEpochMicros(tsv.epochMicros)
        .setNanosWithinMicro(tsv.nanosWithinMicro)
        .setPrecision(precision))
  }

  override def getScalaConverter: proto.Expression.Literal => Any = { v =>
    val ltz = v.getTimestampLtzNanos
    val tsv = TimestampNanosVal.fromParts(ltz.getEpochMicros, ltz.getNanosWithinMicro.toShort)
    SparkDateTimeUtils.timestampNanosToInstant(tsv)
  }

  override def getProtoDataTypeFromLiteral(literal: proto.Expression.Literal): proto.DataType = {
    val ltzBuilder = proto.DataType.TimestampLTZNanos.newBuilder()
    if (literal.getTimestampLtzNanos.hasPrecision) {
      ltzBuilder.setPrecision(literal.getTimestampLtzNanos.getPrecision)
    }
    proto.DataType.newBuilder().setTimestampLtzNanos(ltzBuilder.build()).build()
  }

  // ==================== Arrow Serialization ====================

  override def createArrowSerializer(vector: AnyRef): ArrowSerializer.Serializer = {
    val v = vector.asInstanceOf[TimeStampNanoTZVector]
    new ArrowSerializer.FieldSerializer[Instant, TimeStampNanoTZVector](v) {
      override def set(index: Int, value: Instant): Unit = {
        val tsv = SparkDateTimeUtils.instantToTimestampNanos(value, t.precision)
        val epochNanos = Math.addExact(
          Math.multiplyExact(tsv.epochMicros, NANOS_PER_MICROS),
          tsv.nanosWithinMicro.toLong)
        v.setSafe(index, epochNanos)
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
    new TimestampLTZNanosVectorReader(vector.asInstanceOf[TimeStampNanoTZVector], t.precision)
  }
}

private[connect] class TimestampNTZNanosVectorReader(v: TimeStampNanoVector, precision: Int)
    extends TypedArrowVectorReader[TimeStampNanoVector](v) {
  private def epochNanosToVal(epochNanos: Long): TimestampNanosVal = {
    val epochMicros = Math.floorDiv(epochNanos, NANOS_PER_MICROS)
    val nanosWithinMicro = Math.floorMod(epochNanos, NANOS_PER_MICROS).toInt
    TimestampNanosVal.fromParts(epochMicros, nanosWithinMicro.toShort)
  }
  override def getLocalDateTime(i: Int): LocalDateTime =
    SparkDateTimeUtils.timestampNanosToLocalDateTime(epochNanosToVal(vector.get(i)))
}

private[connect] class TimestampLTZNanosVectorReader(v: TimeStampNanoTZVector, precision: Int)
    extends TypedArrowVectorReader[TimeStampNanoTZVector](v) {
  private def epochNanosToVal(epochNanos: Long): TimestampNanosVal = {
    val epochMicros = Math.floorDiv(epochNanos, NANOS_PER_MICROS)
    val nanosWithinMicro = Math.floorMod(epochNanos, NANOS_PER_MICROS).toInt
    TimestampNanosVal.fromParts(epochMicros, nanosWithinMicro.toShort)
  }
  override def getInstant(i: Int): Instant =
    SparkDateTimeUtils.timestampNanosToInstant(epochNanosToVal(vector.get(i)))
}
