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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the sum calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (5), (10), (15) AS tab(col);
       30
      > SELECT _FUNC_(col) FROM VALUES (NULL), (10), (15) AS tab(col);
       25
      > SELECT _FUNC_(col) FROM VALUES (NULL), (NULL) AS tab(col);
       NULL
  """,
  group = "agg_funcs",
  since = "1.0.0")
case class Sum(child: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sum")

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    case _: IntegralType => LongType
    case _ => DoubleType
  }

  private lazy val sumDataType = resultType

  private lazy val sum = AttributeReference("sum", sumDataType)()
  private lazy val overflow = AttributeReference("overflow", BooleanType)()

  private lazy val zero = Literal.default(sumDataType)

  override lazy val aggBufferAttributes = sum :: overflow :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* sum = */ Literal.create(null, sumDataType),
    /* overflow = */ Literal.create(false, BooleanType)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    if (child.nullable) {
      if (!SQLConf.get.ansiEnabled) {
        Seq(
          /* sum = */
          resultType match {
            case d: DecimalType => coalesce(coalesce(sum, zero) + child.cast(sumDataType), sum)
            case _ => coalesce(coalesce(sum, zero) + child.cast(sumDataType), sum)
          },
          /* overflow = */
          resultType match {
            case d: DecimalType =>
              If (overflow, true, HasOverflow(coalesce(sum, zero) + child.cast(sumDataType), d))
            case _ => If(overflow, true, false)
          })
      } else {
        Seq(
          /* sum = */
          resultType match {
            case d: DecimalType => coalesce(
              CheckOverflow(
                coalesce(sum, zero) + child.cast(sumDataType), d, !SQLConf.get.ansiEnabled), sum)
            case _ => coalesce(coalesce(sum, zero) + child.cast(sumDataType), sum)
          },
          /* overflow = */
          false
        )
      }
    } else {
      if (!SQLConf.get.ansiEnabled) {
        Seq(
          /* sum = */
          resultType match {
            case d: DecimalType => coalesce(sum, zero) + child.cast(sumDataType)
            case _ => coalesce(sum, zero) + child.cast(sumDataType)
          },
          /* overflow = */
          resultType match {
            case d: DecimalType =>
              If(overflow, true, HasOverflow(coalesce(sum, zero) + child.cast(sumDataType), d))
            case _ => If(overflow, true, false)
          })
      } else {
        Seq(
          /* sum = */
          resultType match {
            case d: DecimalType => coalesce(
              CheckOverflow(
                coalesce(sum, zero) + child.cast(sumDataType), d, !SQLConf.get.ansiEnabled), sum)
            case _ => coalesce(sum, zero) + child.cast(sumDataType)
          },
          /* overflow = */
          false
        )
      }
    }
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* sum = */
      resultType match {
        case d: DecimalType =>
          if (!SQLConf.get.ansiEnabled) {
            coalesce(coalesce(sum.left, zero) + sum.right, sum.left)
          } else {
            coalesce(CheckOverflow(
              coalesce(sum.left, zero) + sum.right, d, !SQLConf.get.ansiEnabled), sum.left)
          }
        case _ => coalesce(coalesce(sum.left, zero) + sum.right, sum.left)
      },
      /* overflow = */
      resultType match {
        case d: DecimalType =>
          if (!SQLConf.get.ansiEnabled) {
            If(overflow.left || overflow.right,
              true, HasOverflow(coalesce(sum.left, zero) + sum.right, d))
          } else {
           If(overflow.left || overflow.right, true, false)
          }
      }
    )
  }

  override lazy val evaluateExpression: Expression = resultType match {
    case d: DecimalType => If(overflow && !SQLConf.get.ansiEnabled,
      Literal.create(null, sumDataType) , sum)
    case _ => sum
  }

}
