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

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.arrow.types.ops.TimeTypeConnectOps
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Optional type operations for Spark Connect protobuf conversions.
 *
 * Handles bidirectional DataType <-> proto and Literal <-> proto conversions for
 * framework-managed types in DataTypeProtoConverter and LiteralValueProtoConverter.
 *
 * @since 4.2.0
 */
trait ProtoTypeOps extends Serializable {

  def dataType: DataType

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
}

/**
 * Factory object for ProtoTypeOps lookup.
 */
object ProtoTypeOps {

  def apply(dt: DataType): Option[ProtoTypeOps] = {
    if (!SqlApiConf.get.typesFrameworkEnabled) return None
    dt match {
      case tt: TimeType => Some(new TimeTypeConnectOps(tt))
      // Add new framework types here
      case _ => None
    }
  }

  /** Reverse lookup by value class for the generic literal builder. */
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
   * Shared KindCase -> ProtoTypeOps lookup. All reverse lookups by proto enum case
   * dispatch through this single registration point.
   */
  private def opsForKindCase(
      kindCase: proto.DataType.KindCase): Option[ProtoTypeOps] = {
    if (!SqlApiConf.get.typesFrameworkEnabled) return None
    kindCase match {
      case proto.DataType.KindCase.TIME => Some(new TimeTypeConnectOps(TimeType()))
      // Add new framework proto kinds here - single registration for all KindCase lookups
      case _ => None
    }
  }

  /** Reverse lookup: converts a proto DataType to a Spark DataType. */
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
}
