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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

case class DecimalSum(child: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    case _: IntegralType => LongType
    case _ => DoubleType
  }

  private lazy val sumDataType = resultType

  private lazy val sum = AttributeReference("sum", sumDataType)()
  private lazy val overflow = AttributeReference("overflow", BooleanType, false)()

  private lazy val zero = Literal.default(resultType)

  override lazy val aggBufferAttributes = sum :: overflow :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* sum = */ Literal.create(null, sumDataType),
    /* overflow = */ Literal.create(false, BooleanType)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    if (child.nullable) {
      resultType match {
        case d: DecimalType =>
          Seq(
            If(overflow, Literal.create(null, sumDataType), coalesce(
              CheckOverflow(coalesce(sum, zero) + child.cast(sumDataType), d, true), sum)),
              overflow ||
                coalesce(HasOverflow(coalesce(sum, zero) + child.cast(sumDataType), d), false)
          )
          case _ => Seq(coalesce(coalesce(sum, zero) + child.cast(sumDataType), sum), false)
      }
    } else {
      resultType match {
        case d: DecimalType =>
          Seq(
            If(overflow, Literal.create(null, sumDataType),
              CheckOverflow(coalesce(sum, zero) + child.cast(sumDataType), d, true)),
              overflow ||
              coalesce(HasOverflow(coalesce(sum, zero) + child.cast(sumDataType), d), false)
          )
        case _ => Seq(coalesce(sum, zero) + child.cast(sumDataType), false)
      }
    }
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    resultType match {
      case d: DecimalType =>
        Seq(
          If(coalesce(overflow.left, false) || coalesce(overflow.right, false),
            Literal.create(null, d),
            coalesce(CheckOverflow(coalesce(sum.left, zero) + sum.right, d, true), sum.left)),
            If(coalesce(overflow.left, false) || coalesce(overflow.right, false),
            true, HasOverflow(coalesce(sum.left, zero) + sum.right, d))
        )
      case _ =>
        Seq(
          coalesce(coalesce(sum.left, zero) + sum.right, sum.left),
          If(coalesce(overflow.left, false) || coalesce(overflow.right, false), true, false)
        )
    }
  }

  override lazy val evaluateExpression: Expression = resultType match {
    case d: DecimalType =>
      If(EqualTo(overflow, true),
        If(!SQLConf.get.ansiEnabled,
          Literal.create(null, sumDataType),
          OverflowException(resultType, "Arithmetic Operation overflow")),
        sum)
    case _ => sum
  }
}
