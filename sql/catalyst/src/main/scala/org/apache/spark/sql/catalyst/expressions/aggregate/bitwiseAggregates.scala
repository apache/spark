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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BitwiseAnd, BitwiseOr, BitwiseXor, ExpectsInputTypes, Expression, ExpressionDescription, If, IsNull, Literal}
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegralType}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the bitwise AND of all non-null input values, or null if none.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (3), (5) AS tab(col);
       1
  """,
  since = "3.0.0")
case class BitAndAgg(child: Expression) extends DeclarativeAggregate with ExpectsInputTypes {

  override def nodeName: String = "bit_and"

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType)

  private lazy val bitAnd = AttributeReference("bit_and", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = bitAnd :: Nil

  override lazy val initialValues: Seq[Literal] = Literal.create(null, dataType) :: Nil

  override lazy val updateExpressions: Seq[Expression] =
    If(IsNull(bitAnd),
      child,
      If(IsNull(child), bitAnd, BitwiseAnd(bitAnd, child))) :: Nil

  override lazy val mergeExpressions: Seq[Expression] =
    If(IsNull(bitAnd.left),
      bitAnd.right,
      If(IsNull(bitAnd.right), bitAnd.left, BitwiseAnd(bitAnd.left, bitAnd.right))) :: Nil

  override lazy val evaluateExpression: AttributeReference = bitAnd
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the bitwise OR of all non-null input values, or null if none.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (3), (5) AS tab(col);
       7
  """,
  since = "3.0.0")
case class BitOrAgg(child: Expression) extends DeclarativeAggregate with ExpectsInputTypes {

  override def nodeName: String = "bit_or"

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType)

  private lazy val bitOr = AttributeReference("bit_or", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = bitOr :: Nil

  override lazy val initialValues: Seq[Literal] = Literal.create(null, dataType) :: Nil

  override lazy val updateExpressions: Seq[Expression] =
    If(IsNull(bitOr),
      child,
      If(IsNull(child), bitOr, BitwiseOr(bitOr, child))) :: Nil

  override lazy val mergeExpressions: Seq[Expression] =
    If(IsNull(bitOr.left),
      bitOr.right,
      If(IsNull(bitOr.right), bitOr.left, BitwiseOr(bitOr.left, bitOr.right))) :: Nil

  override lazy val evaluateExpression: AttributeReference = bitOr
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the bitwise XOR of all non-null input values, or null if none.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (3), (5) AS tab(col);
       6
  """,
  since = "3.0.0")
case class BitXorAgg(child: Expression) extends DeclarativeAggregate with ExpectsInputTypes {

  override def nodeName: String = "bit_xor"

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType)

  private lazy val bitXOr = AttributeReference("bit_xor", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = bitXOr :: Nil

  override lazy val initialValues: Seq[Literal] = Literal.create(null, dataType) :: Nil

  override lazy val updateExpressions: Seq[Expression] =
    If(IsNull(bitXOr),
      child,
      If(IsNull(child), bitXOr, BitwiseXor(bitXOr, child))) :: Nil

  override lazy val mergeExpressions: Seq[Expression] =
    If(IsNull(bitXOr.left),
      bitXOr.right,
      If(IsNull(bitXOr.right), bitXOr.left, BitwiseXor(bitXOr.left, bitXOr.right))) :: Nil

  override lazy val evaluateExpression: AttributeReference = bitXOr
}
