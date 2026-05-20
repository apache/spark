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

package org.apache.spark.sql.connect.common.types.ops

import org.apache.arrow.vector.FieldVector

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.LocalTimeEncoder
import org.apache.spark.sql.connect.client.arrow.{ArrowDeserializers, ArrowSerializer, ArrowVectorReader}
import org.apache.spark.sql.connect.client.arrow.types.ops.TimeTypeConnectOps
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Optional type operations for Spark Connect infrastructure.
 *
 * Consolidates both proto conversions (DataTypeProtoConverter, LiteralValueProtoConverter) and
 * Arrow serialization/deserialization (ArrowSerializer, ArrowDeserializer, ArrowVectorReader) for
 * framework-managed types.
 *
 * @since 4.2.0
 */
trait ConnectTypeOps extends Serializable {

  def dataType: DataType

  def encoder: AgnosticEncoder[_]

  // ==================== Proto Conversions ====================

  /** Converts this DataType to its Connect proto representation. */
  def toConnectProtoType: proto.DataType

  /** Converts a value to a proto literal builder (generic, no DataType context). */
  def toLiteralProto(
      value: Any,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder

  /** Converts a value to a proto literal builder (with DataType context). */
  def toLiteralProtoWithType(
      value: Any,
      dt: DataType,
      builder: proto.Expression.Literal.Builder): proto.Expression.Literal.Builder

  /**
   * Returns a converter from proto literal to Scala value. The returned converter assumes
   * non-null input - null handling is done by the caller (LiteralValueProtoConverter wraps with
   * `if (v.hasNull) null`).
   */
  def getScalaConverter: proto.Expression.Literal => Any

  /** Returns a proto DataType inferred from a proto literal (for type inference). */
  def getProtoDataTypeFromLiteral(literal: proto.Expression.Literal): proto.DataType

  /** Converts a proto DataType to a Spark DataType (reverse of toConnectProtoType). */
  def toCatalystTypeFromProto(t: proto.DataType): DataType

  // ==================== Arrow Serialization ====================

  /** Creates an Arrow serializer for writing values to a vector. */
  def createArrowSerializer(vector: AnyRef): ArrowSerializer.Serializer

  /** Creates an Arrow deserializer for reading values from a vector. */
  def createArrowDeserializer(
      enc: AgnosticEncoder[_],
      data: AnyRef,
      timeZoneId: String): ArrowDeserializers.Deserializer[Any]

  /** Creates an ArrowVectorReader for this type's vector. */
  def createArrowVectorReader(vector: FieldVector): ArrowVectorReader
}

/**
 * Factory object for ConnectTypeOps lookup.
 *
 * Provides separate factory methods for proto (server-side, feature-flag-gated) and Arrow
 * (client-side, no flag) dispatch.
 */
object ConnectTypeOps {

  // ==================== Proto Dispatch (server-side, flag-gated) ====================

  /** DataType-keyed dispatch for proto conversions. Checks feature flag. */
  def apply(dt: DataType): Option[ConnectTypeOps] = {
    if (!SqlApiConf.get.typesFrameworkEnabled) return None
    dt match {
      case tt: TimeType => Some(new TimeTypeConnectOps(tt))
      // Add new framework types here
      case _ => None
    }
  }

  /** Reverse lookup by value class for the generic literal builder. Checks feature flag. */
  def toLiteralProtoForValue(
      value: Any,
      builder: proto.Expression.Literal.Builder): Option[proto.Expression.Literal.Builder] = {
    if (!SqlApiConf.get.typesFrameworkEnabled) return None
    value match {
      case v: java.time.LocalTime =>
        Some(new TimeTypeConnectOps(TimeType()).toLiteralProto(v, builder))
      // Add new framework value types here
      case _ => None
    }
  }

  /**
   * Shared KindCase -> ConnectTypeOps lookup. All reverse lookups by proto enum case dispatch
   * through this single registration point. Checks feature flag.
   */
  private def opsForKindCase(kindCase: proto.DataType.KindCase): Option[ConnectTypeOps] = {
    if (!SqlApiConf.get.typesFrameworkEnabled) return None
    kindCase match {
      case proto.DataType.KindCase.TIME => Some(new TimeTypeConnectOps(TimeType()))
      // Add new framework proto kinds here - single registration for all KindCase lookups
      case _ => None
    }
  }

  /** Reverse lookup: converts a proto DataType to a Spark DataType. Checks feature flag. */
  def toCatalystType(t: proto.DataType): Option[DataType] =
    opsForKindCase(t.getKindCase).map(_.toCatalystTypeFromProto(t))

  /** Reverse lookup: returns a Scala converter for a proto literal KindCase. */
  def getScalaConverterForKind(
      kindCase: proto.DataType.KindCase): Option[proto.Expression.Literal => Any] =
    opsForKindCase(kindCase).map(_.getScalaConverter)

  /** Reverse lookup: returns the proto DataType inferred from a proto literal. */
  def getProtoDataTypeFromLiteral(literal: proto.Expression.Literal): Option[proto.DataType] =
    opsForKindCase(literalCaseToKindCase(literal.getLiteralTypeCase))
      .map(_.getProtoDataTypeFromLiteral(literal))

  /** Maps LiteralTypeCase to KindCase (1:1 for framework types). */
  private def literalCaseToKindCase(
      litCase: proto.Expression.Literal.LiteralTypeCase): proto.DataType.KindCase =
    litCase match {
      case proto.Expression.Literal.LiteralTypeCase.TIME => proto.DataType.KindCase.TIME
      // Add new framework literal-to-kind mappings here
      case _ => proto.DataType.KindCase.KIND_NOT_SET
    }

  // ==================== Arrow Dispatch (client-side, NO flag check) ====================

  /** Encoder-keyed dispatch for Arrow serialization. No feature flag check. */
  def forEncoder(enc: AgnosticEncoder[_]): Option[ConnectTypeOps] =
    enc match {
      case LocalTimeEncoder => Some(new TimeTypeConnectOps(TimeType()))
      // Add new framework encoders here
      case _ => None
    }

  /** DataType-keyed dispatch for ArrowVectorReader. No feature flag check. */
  def forDataType(dt: DataType): Option[ConnectTypeOps] =
    dt match {
      case tt: TimeType => Some(new TimeTypeConnectOps(tt))
      // Add new framework types here
      case _ => None
    }
}
