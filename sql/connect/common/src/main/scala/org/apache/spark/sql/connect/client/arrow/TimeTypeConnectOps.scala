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

import java.time.LocalTime

import org.apache.arrow.vector.{FieldVector, TimeNanoVector}

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.LocalTimeEncoder
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils
import org.apache.spark.sql.connect.common.types.ops.{ConnectArrowTypeOps, ProtoTypeOps}
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Combined Connect operations for TimeType.
 *
 * Implements both ProtoTypeOps (proto DataType/Literal conversions) and ConnectArrowTypeOps
 * (Arrow serialization/deserialization) in a single class.
 *
 * Lives in the arrow package to access arrow-private types (ArrowVectorReader, TimeVectorReader,
 * ArrowSerializer.Serializer, ArrowDeserializers.LeafFieldDeserializer).
 *
 * @param t
 *   The TimeType with precision information
 * @since 4.2.0
 */
private[connect] class TimeTypeConnectOps(val t: TimeType)
    extends ProtoTypeOps with ConnectArrowTypeOps {

  override def dataType: DataType = t

  override def encoder: AgnosticEncoder[_] = LocalTimeEncoder

  // ==================== ProtoTypeOps ====================

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

  override def getScalaConverter: proto.Expression.Literal => Any = {
    v => SparkDateTimeUtils.nanosToLocalTime(v.getTime.getNano)
  }

  override def buildProtoDataType(
      literal: proto.Expression.Literal,
      builder: proto.DataType.Builder): Unit = {
    val timeBuilder = proto.DataType.Time.newBuilder()
    if (literal.getTime.hasPrecision) {
      timeBuilder.setPrecision(literal.getTime.getPrecision)
    }
    builder.setTime(timeBuilder.build())
  }

  // ==================== ConnectArrowTypeOps ====================

  override def createArrowSerializer(vector: Any): Any = {
    val v = vector.asInstanceOf[TimeNanoVector]
    new ArrowSerializer.Serializer {
      override def write(index: Int, value: Any): Unit = {
        if (value != null) {
          v.setSafe(index, SparkDateTimeUtils.localTimeToNanos(value.asInstanceOf[LocalTime]))
        } else {
          v.setNull(index)
        }
      }
    }
  }

  override def createArrowDeserializer(
      enc: AgnosticEncoder[_],
      vector: Any,
      timeZoneId: String): Any = {
    val v = vector.asInstanceOf[FieldVector]
    new ArrowDeserializers.LeafFieldDeserializer[LocalTime](enc, v, timeZoneId) {
      override def value(i: Int): LocalTime = reader.getLocalTime(i)
    }
  }

  override def createArrowVectorReader(vector: Any): Any = {
    new TimeVectorReader(vector.asInstanceOf[TimeNanoVector])
  }
}
