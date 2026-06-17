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

import java.time.LocalTime

import org.apache.arrow.vector.{FieldVector, TimeNanoVector}

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.LocalTimeEncoder
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils
import org.apache.spark.sql.connect.client.arrow.{ArrowDeserializers, ArrowSerializer, ArrowVectorReader, TimeVectorReader}
import org.apache.spark.sql.connect.common.types.ops.ConnectTypeOps
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Combined Connect operations for TimeType.
 *
 * Implements ConnectTypeOps for TimeType, providing both proto DataType/Literal conversions and
 * Arrow serialization/deserialization.
 *
 * Lives under the arrow.types.ops sub-package to co-locate with Arrow infrastructure while
 * keeping ops implementations separate from core arrow classes.
 *
 * @param t
 *   The TimeType with precision information
 * @since 4.2.0
 */
private[connect] class TimeTypeConnectOps(val t: TimeType) extends ConnectTypeOps {

  override def dataType: DataType = t

  override def encoder: AgnosticEncoder[_] = LocalTimeEncoder

  // ==================== Proto Conversions ====================

  override def toCatalystTypeFromProto(t: proto.DataType): DataType = {
    val time = t.getTime
    if (time.hasPrecision) TimeType(time.getPrecision) else TimeType()
  }

  override def toConnectProtoType: proto.DataType = {
    proto.DataType
      .newBuilder()
      .setTime(proto.DataType.Time.newBuilder().setPrecision(t.precision).build())
      .build()
  }

  override def toLiteralProto(
      value: Any,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    val v = value.asInstanceOf[LocalTime]
    builder.setTime(
      builder.getTimeBuilder
        .setNano(SparkDateTimeUtils.localTimeToNanos(v))
        .setPrecision(TimeType.DEFAULT_PRECISION))
  }

  override def toLiteralProtoWithType(
      value: Any,
      dt: DataType,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder = {
    val v = value.asInstanceOf[LocalTime]
    val timeType = dt.asInstanceOf[TimeType]
    builder.setTime(
      builder.getTimeBuilder
        .setNano(SparkDateTimeUtils.localTimeToNanos(v))
        .setPrecision(timeType.precision))
  }

  override def getScalaConverter: proto.Expression.Literal => Any = { v =>
    SparkDateTimeUtils.nanosToLocalTime(v.getTime.getNano)
  }

  override def getProtoDataTypeFromLiteral(literal: proto.Expression.Literal): proto.DataType = {
    val timeBuilder = proto.DataType.Time.newBuilder()
    if (literal.getTime.hasPrecision) {
      timeBuilder.setPrecision(literal.getTime.getPrecision)
    }
    proto.DataType.newBuilder().setTime(timeBuilder.build()).build()
  }

  // ==================== Arrow Serialization ====================

  override def createArrowSerializer(vector: AnyRef): ArrowSerializer.Serializer = {
    val v = vector.asInstanceOf[TimeNanoVector]
    new ArrowSerializer.FieldSerializer[LocalTime, TimeNanoVector](v) {
      override def set(index: Int, value: LocalTime): Unit =
        v.setSafe(index, SparkDateTimeUtils.localTimeToNanos(value))
    }
  }

  override def createArrowDeserializer(
      enc: AgnosticEncoder[_],
      data: AnyRef,
      timeZoneId: String): ArrowDeserializers.Deserializer[Any] = {
    val v = data.asInstanceOf[FieldVector]
    new ArrowDeserializers.LeafFieldDeserializer[LocalTime](enc, v, timeZoneId) {
      override def value(i: Int): LocalTime = reader.getLocalTime(i)
    }
  }

  override def createArrowVectorReader(vector: FieldVector): ArrowVectorReader = {
    new TimeVectorReader(vector.asInstanceOf[TimeNanoVector])
  }
}
