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
import org.apache.spark.sql.connect.client.arrow.TimeTypeConnectOps
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Optional type operations for Spark Connect protobuf conversions.
 *
 * Handles bidirectional DataType <-> proto and Literal <-> proto conversions
 * for framework-managed types in DataTypeProtoConverter and LiteralValueProtoConverter.
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

  /** Returns a converter from proto literal to Scala value. */
  def getScalaConverter: proto.Expression.Literal => Any

  /** Builds a proto DataType from a proto literal (for type inference). */
  def buildProtoDataType(
      literal: proto.Expression.Literal,
      builder: proto.DataType.Builder): Unit
}

/**
 * Factory object for ProtoTypeOps lookup.
 */
object ProtoTypeOps {

  def apply(dt: DataType): Option[ProtoTypeOps] = {
    if (!SqlApiConf.get.typesFrameworkEnabled) return None
    dt match {
      case tt: TimeType => Some(new TimeTypeConnectOps(tt))
      case _ => None
    }
  }

  /**
   * Reverse lookup: converts a proto DataType to a Spark DataType, if it belongs
   * to a framework-managed type.
   */
  def toCatalystType(t: proto.DataType): Option[DataType] = {
    if (!SqlApiConf.get.typesFrameworkEnabled) return None
    t.getKindCase match {
      case proto.DataType.KindCase.TIME =>
        val time = t.getTime
        if (time.hasPrecision) Some(TimeType(time.getPrecision))
        else Some(TimeType())
      case _ => None
    }
  }

  /**
   * Reverse lookup: returns a Scala converter for a proto literal KindCase.
   */
  def getScalaConverterForKind(
      kindCase: proto.DataType.KindCase): Option[proto.Expression.Literal => Any] = {
    if (!SqlApiConf.get.typesFrameworkEnabled) return None
    kindCase match {
      case proto.DataType.KindCase.TIME =>
        Some(new TimeTypeConnectOps(TimeType()).getScalaConverter)
      case _ => None
    }
  }

  /**
   * Reverse lookup: builds a proto DataType from a proto literal's type case.
   */
  def buildProtoDataTypeForLiteral(
      literal: proto.Expression.Literal,
      builder: proto.DataType.Builder): Boolean = {
    if (!SqlApiConf.get.typesFrameworkEnabled) return false
    literal.getLiteralTypeCase match {
      case proto.Expression.Literal.LiteralTypeCase.TIME =>
        new TimeTypeConnectOps(TimeType()).buildProtoDataType(literal, builder)
        true
      case _ => false
    }
  }
}
