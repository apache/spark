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
package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.expressions.Expression

private[sql] abstract class DataTypeExpression(val dataType: DataType) {
  /**
   * Enables matching against DataType for expressions:
   * {{{
   *   case Cast(child @ BinaryType(), StringType) =>
   *     ...
   * }}}
   */
  private[sql] def unapply(e: Expression): Boolean = e.dataType == dataType
}

private[sql] case object BooleanTypeExpression extends DataTypeExpression(BooleanType)
private[sql] case object StringTypeExpression {
  /**
   * Enables matching against StringType for expressions:
   * {{{
   *   case Cast(child @ StringType(collationId), NumericType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = {
    e.dataType.isInstanceOf[StringType]
  }
}
private[sql] case object TimestampTypeExpression extends DataTypeExpression(TimestampType)
private[sql] case object DateTypeExpression extends DataTypeExpression(DateType)
private[sql] case object ByteTypeExpression extends DataTypeExpression(ByteType)
private[sql] case object ShortTypeExpression extends DataTypeExpression(ShortType)
private[sql] case object IntegerTypeExpression extends DataTypeExpression(IntegerType)
private[sql] case object LongTypeExpression extends DataTypeExpression(LongType)
private[sql] case object DoubleTypeExpression extends DataTypeExpression(DoubleType)
private[sql] case object FloatTypeExpression extends DataTypeExpression(FloatType)

private[sql] object NumericTypeExpression {
  /**
   * Enables matching against NumericType for expressions:
   * {{{
   *   case Cast(child @ NumericType(), StringType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = {
    e.dataType.isInstanceOf[NumericType]
  }
}

private[sql] object IntegralTypeExpression {
  /**
   * Enables matching against IntegralType for expressions:
   * {{{
   *   case Cast(child @ IntegralType(), StringType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = {
    e.dataType.isInstanceOf[IntegralType]
  }
}

private[sql] object AnyTimestampTypeExpression {
  def unapply(e: Expression): Boolean =
    e.dataType.isInstanceOf[TimestampType] || e.dataType.isInstanceOf[TimestampNTZType]
}

private[sql] object DecimalExpression {
  def unapply(e: Expression): Option[(Int, Int)] = e.dataType match {
    case t: DecimalType => Some((t.precision, t.scale))
    case _ => None
  }
}
