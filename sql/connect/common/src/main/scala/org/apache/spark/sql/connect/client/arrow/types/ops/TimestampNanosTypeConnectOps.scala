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

import org.apache.arrow.vector.FieldVector

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{InstantNanosEncoder, LocalDateTimeNanosEncoder}
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils
import org.apache.spark.sql.connect.client.arrow.{ArrowDeserializers, ArrowSerializer, ArrowVectorReader}
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.common.types.ops.ConnectTypeOps
import org.apache.spark.sql.errors.DataTypeErrors
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

private[connect] abstract class TimestampNanosTypeConnectOps extends ConnectTypeOps {

  protected def checkTimestampNanosTypesEnabled(): Unit = {
    DataTypeErrors.checkTimestampNanosTypesEnabled()
  }

  protected def timestampNanosVal(epochMicros: Long, nanosWithinMicro: Int): TimestampNanosVal = {
    if (nanosWithinMicro < 0 ||
      nanosWithinMicro > TimestampNanosVal.MAX_NANOS_WITHIN_MICRO) {
      throw InvalidPlanInput(
        s"Invalid nanosWithinMicro for timestamp nanos literal: $nanosWithinMicro")
    }
    TimestampNanosVal.fromParts(epochMicros, nanosWithinMicro.toShort)
  }

  protected def unsupportedArrow: Nothing = {
    throw new UnsupportedOperationException(
      s"Arrow conversion for ${dataType.sql} is not supported by Spark Connect yet.")
  }

  override def createArrowSerializer(vector: AnyRef): ArrowSerializer.Serializer =
    unsupportedArrow

  override def createArrowDeserializer(
      enc: AgnosticEncoder[_],
      data: AnyRef,
      timeZoneId: String): ArrowDeserializers.Deserializer[Any] = unsupportedArrow

  override def createArrowVectorReader(vector: FieldVector): ArrowVectorReader = unsupportedArrow
}

/**
 * Spark Connect proto conversions for TimestampNTZNanosType.
 *
 * @param t
 *   The TimestampNTZNanosType with precision information
 * @since 4.3.0
 */
private[connect] class TimestampNTZNanosTypeConnectOps(val t: TimestampNTZNanosType)
    extends TimestampNanosTypeConnectOps {

  override def dataType: DataType = t

  override def encoder: AgnosticEncoder[_] = {
    checkTimestampNanosTypesEnabled()
    LocalDateTimeNanosEncoder(t.precision)
  }

  override def toCatalystTypeFromProto(t: proto.DataType): DataType = {
    val timestampNanos = t.getTimestampNtzNanos
    val dataType =
      if (timestampNanos.hasPrecision) {
        TimestampNTZNanosType(timestampNanos.getPrecision)
      } else {
        TimestampNTZNanosType()
      }
    checkTimestampNanosTypesEnabled()
    dataType
  }

  override def toConnectProtoType: proto.DataType = {
    checkTimestampNanosTypesEnabled()
    proto.DataType
      .newBuilder()
      .setTimestampNtzNanos(
        proto.DataType.TimestampNTZNanos.newBuilder().setPrecision(t.precision).build())
      .build()
  }

  override def toLiteralProto(
      value: Any,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    toLiteralProtoWithType(value, t, builder)
  }

  override def toLiteralProtoWithType(
      value: Any,
      dt: DataType,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    checkTimestampNanosTypesEnabled()
    val timestampType = dt.asInstanceOf[TimestampNTZNanosType]
    val v = value match {
      case localDateTime: LocalDateTime =>
        SparkDateTimeUtils.localDateTimeToTimestampNanos(localDateTime, timestampType.precision)
      case timestampNanos: TimestampNanosVal =>
        SparkDateTimeUtils.truncateTimestampNanosToPrecision(
          timestampNanos,
          timestampType.precision)
      case other =>
        throw new IllegalArgumentException(s"literal $other not supported for ${dt.sql}.")
    }
    builder.setTimestampNtzNanos(
      builder.getTimestampNtzNanosBuilder
        .setEpochMicros(v.epochMicros)
        .setNanosWithinMicro(v.nanosWithinMicro)
        .setPrecision(timestampType.precision))
  }

  override def getScalaConverter: proto.Expression.Literal => Any = { literal =>
    checkTimestampNanosTypesEnabled()
    val timestampNanos = literal.getTimestampNtzNanos
    val v = timestampNanosVal(timestampNanos.getEpochMicros, timestampNanos.getNanosWithinMicro)
    SparkDateTimeUtils.timestampNanosToLocalDateTime(v)
  }

  override def getProtoDataTypeFromLiteral(literal: proto.Expression.Literal): proto.DataType = {
    val timestampNanosBuilder = proto.DataType.TimestampNTZNanos.newBuilder()
    if (literal.getTimestampNtzNanos.hasPrecision) {
      timestampNanosBuilder.setPrecision(literal.getTimestampNtzNanos.getPrecision)
    }
    val dataType = proto.DataType.newBuilder().setTimestampNtzNanos(timestampNanosBuilder).build()
    toCatalystTypeFromProto(dataType)
    dataType
  }
}

/**
 * Spark Connect proto conversions for TimestampLTZNanosType.
 *
 * @param t
 *   The TimestampLTZNanosType with precision information
 * @since 4.3.0
 */
private[connect] class TimestampLTZNanosTypeConnectOps(val t: TimestampLTZNanosType)
    extends TimestampNanosTypeConnectOps {

  override def dataType: DataType = t

  override def encoder: AgnosticEncoder[_] = {
    checkTimestampNanosTypesEnabled()
    InstantNanosEncoder(t.precision)
  }

  override def toCatalystTypeFromProto(t: proto.DataType): DataType = {
    val timestampNanos = t.getTimestampLtzNanos
    val dataType =
      if (timestampNanos.hasPrecision) {
        TimestampLTZNanosType(timestampNanos.getPrecision)
      } else {
        TimestampLTZNanosType()
      }
    checkTimestampNanosTypesEnabled()
    dataType
  }

  override def toConnectProtoType: proto.DataType = {
    checkTimestampNanosTypesEnabled()
    proto.DataType
      .newBuilder()
      .setTimestampLtzNanos(
        proto.DataType.TimestampLTZNanos.newBuilder().setPrecision(t.precision).build())
      .build()
  }

  override def toLiteralProto(
      value: Any,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    toLiteralProtoWithType(value, t, builder)
  }

  override def toLiteralProtoWithType(
      value: Any,
      dt: DataType,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    checkTimestampNanosTypesEnabled()
    val timestampType = dt.asInstanceOf[TimestampLTZNanosType]
    val v = value match {
      case instant: Instant =>
        SparkDateTimeUtils.instantToTimestampNanos(instant, timestampType.precision)
      case timestampNanos: TimestampNanosVal =>
        SparkDateTimeUtils.truncateTimestampNanosToPrecision(
          timestampNanos,
          timestampType.precision)
      case other =>
        throw new IllegalArgumentException(s"literal $other not supported for ${dt.sql}.")
    }
    builder.setTimestampLtzNanos(
      builder.getTimestampLtzNanosBuilder
        .setEpochMicros(v.epochMicros)
        .setNanosWithinMicro(v.nanosWithinMicro)
        .setPrecision(timestampType.precision))
  }

  override def getScalaConverter: proto.Expression.Literal => Any = { literal =>
    checkTimestampNanosTypesEnabled()
    val timestampNanos = literal.getTimestampLtzNanos
    val v = timestampNanosVal(timestampNanos.getEpochMicros, timestampNanos.getNanosWithinMicro)
    SparkDateTimeUtils.timestampNanosToInstant(v)
  }

  override def getProtoDataTypeFromLiteral(literal: proto.Expression.Literal): proto.DataType = {
    val timestampNanosBuilder = proto.DataType.TimestampLTZNanos.newBuilder()
    if (literal.getTimestampLtzNanos.hasPrecision) {
      timestampNanosBuilder.setPrecision(literal.getTimestampLtzNanos.getPrecision)
    }
    val dataType = proto.DataType.newBuilder().setTimestampLtzNanos(timestampNanosBuilder).build()
    toCatalystTypeFromProto(dataType)
    dataType
  }
}
