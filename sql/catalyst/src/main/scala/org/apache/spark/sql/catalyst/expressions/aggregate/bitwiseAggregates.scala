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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BinaryArithmetic, BitwiseAnd, BitwiseOr, BitwiseXor, ExpectsInputTypes, Expression, ExpressionDescription, If, IsNull, Literal}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegralType}

abstract class BitAggregate extends DeclarativeAggregate with ExpectsInputTypes
  with UnaryLike[Expression] {

  val child: Expression

  def bitOperator(left: Expression, right: Expression): BinaryArithmetic

  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType)

  private lazy val bitAgg = AttributeReference(nodeName, child.dataType)()

  override lazy val initialValues: Seq[Literal] = Literal.create(null, dataType) :: Nil

  override lazy val aggBufferAttributes: Seq[AttributeReference] = bitAgg :: Nil

  override lazy val evaluateExpression: AttributeReference = bitAgg

  override lazy val updateExpressions: Seq[Expression] =
    If(IsNull(bitAgg),
      child,
      If(IsNull(child), bitAgg, bitOperator(bitAgg, child))) :: Nil

  override lazy val mergeExpressions: Seq[Expression] =
    If(IsNull(bitAgg.left),
      bitAgg.right,
      If(IsNull(bitAgg.right), bitAgg.left, bitOperator(bitAgg.left, bitAgg.right))) :: Nil
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the bitwise AND of all non-null input values, or null if none.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (3), (5) AS tab(col);
       1
  """,
  group = "agg_funcs",
  since = "3.0.0")
case class BitAndAgg(child: Expression) extends BitAggregate {

  override def nodeName: String = "bit_and"

  override def bitOperator(left: Expression, right: Expression): BinaryArithmetic = {
    BitwiseAnd(left, right)
  }

  override protected def withNewChildInternal(newChild: Expression): BitAndAgg =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the bitwise OR of all non-null input values, or null if none.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (3), (5) AS tab(col);
       7
  """,
  group = "agg_funcs",
  since = "3.0.0")
case class BitOrAgg(child: Expression) extends BitAggregate {

  override def nodeName: String = "bit_or"

  override def bitOperator(left: Expression, right: Expression): BinaryArithmetic = {
    BitwiseOr(left, right)
  }

  override protected def withNewChildInternal(newChild: Expression): BitOrAgg =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the bitwise XOR of all non-null input values, or null if none.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (3), (5) AS tab(col);
       6
  """,
  group = "agg_funcs",
  since = "3.0.0")
case class BitXorAgg(child: Expression) extends BitAggregate {

  override def nodeName: String = "bit_xor"

  override def bitOperator(left: Expression, right: Expression): BinaryArithmetic = {
    BitwiseXor(left, right)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
