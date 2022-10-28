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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Abs, AttributeReference, Exp, Expression, If, ImplicitCastInputTypes, IsNull, Literal, Log}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType, DoubleType, IntegralType, LongType, NumericType}


/** Multiply numerical values within an aggregation group */
case class Product(child: Expression)
    extends DeclarativeAggregate with ImplicitCastInputTypes with UnaryLike[Expression] {

  override def nullable: Boolean = true

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)

  private lazy val product = AttributeReference("product", dataType)()

  private lazy val one = Literal(1.0, dataType)

  override lazy val aggBufferAttributes = product :: Nil

  override lazy val initialValues: Seq[Expression] =
    Seq(Literal(null, dataType))

  override lazy val updateExpressions: Seq[Expression] = {
    // Treat the result as null until we have seen at least one child value,
    // whereupon the previous product is promoted to being unity.

    val protoResult = coalesce(product, one) * child

    if (child.nullable) {
      Seq(coalesce(protoResult, product))
    } else {
      Seq(protoResult)
    }
  }

  override lazy val mergeExpressions: Seq[Expression] =
    Seq(coalesce(coalesce(product.left, one) * product.right, product.left))

  override lazy val evaluateExpression: Expression = product

  override protected def withNewChildInternal(newChild: Expression): Product =
    copy(child = newChild)
}

/**
 * Product in Pandas' fashion. This expression is dedicated only for Pandas API on Spark.
 * It has three main differences from `Product`:
 * 1, it compute the product of `Fractional` inputs in a more numerical-stable way;
 * 2, it compute the product of `Integral` inputs with LongType variables internally;
 * 3, it accepts NULLs when `ignoreNA` is False;
 */
case class PandasProduct(
    child: Expression,
    ignoreNA: Boolean)
    extends DeclarativeAggregate with ImplicitCastInputTypes with UnaryLike[Expression] {

  override def nullable: Boolean = !ignoreNA

  override def dataType: DataType = child.dataType match {
    case _: IntegralType => LongType
    case _ => DoubleType
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  private lazy val product =
    AttributeReference("product", LongType, nullable = false)()
  private lazy val logSum =
    AttributeReference("logSum", DoubleType, nullable = false)()
  private lazy val positive =
    AttributeReference("positive", BooleanType, nullable = false)()
  private lazy val containsZero =
    AttributeReference("containsZero", BooleanType, nullable = false)()
  private lazy val containsNull =
    AttributeReference("containsNull", BooleanType, nullable = false)()

  override lazy val aggBufferAttributes = child.dataType match {
    case _: IntegralType =>
      Seq(product, containsNull)
    case _ =>
      Seq(logSum, positive, containsZero, containsNull)
  }

  override lazy val initialValues: Seq[Expression] = child.dataType match {
    case _: IntegralType =>
      Seq(Literal(1L), Literal(false))
    case _ =>
      Seq(Literal(0.0), Literal(true), Literal(false), Literal(false))
  }

  override lazy val updateExpressions: Seq[Expression] = child.dataType match {
    case _: IntegralType =>
      Seq(
        If(IsNull(child), product, product * child),
        containsNull || IsNull(child)
      )
    case _ =>
      val newLogSum = logSum + Log(Abs(child))
      val newPositive = If(child < Literal(0.0), !positive, positive)
      val newContainsZero = containsZero || child <=> Literal(0.0)
      val newContainsNull = containsNull || IsNull(child)
      if (ignoreNA) {
        Seq(
          If(IsNull(child) || newContainsZero, logSum, newLogSum),
          newPositive,
          newContainsZero,
          newContainsNull
        )
      } else {
        Seq(
          If(newContainsNull || newContainsZero, logSum, newLogSum),
          newPositive,
          newContainsZero,
          newContainsNull
        )
      }
  }

  override lazy val mergeExpressions: Seq[Expression] = child.dataType match {
    case _: IntegralType =>
      Seq(
        product.left * product.right,
        containsNull.left || containsNull.right
      )
    case _ =>
      Seq(
        logSum.left + logSum.right,
        positive.left === positive.right,
        containsZero.left || containsZero.right,
        containsNull.left || containsNull.right
      )
  }

  override lazy val evaluateExpression: Expression = child.dataType match {
    case _: IntegralType =>
      if (ignoreNA) {
        product
      } else {
        If(containsNull, Literal(null, LongType), product)
      }
    case _ =>
      val product = If(positive, Exp(logSum), -Exp(logSum))
      if (ignoreNA) {
        If(containsZero, Literal(0.0), product)
      } else {
        If(containsNull, Literal(null, DoubleType),
          If(containsZero, Literal(0.0), product))
      }
  }

  override def prettyName: String = "pandas_product"
  override protected def withNewChildInternal(newChild: Expression): PandasProduct =
    copy(child = newChild)
}
