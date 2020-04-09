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

  private lazy val overflow = AttributeReference("overflow", BooleanType, false)()

  private lazy val zero = Literal.default(sumDataType)

  override lazy val aggBufferAttributes = sum :: overflow :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* sum = */ Literal.create(null, sumDataType),
    /* overflow = */ Literal.create(false, BooleanType)
  )

  /**
   * For decimal types, update will do the following:
   * We have a overflow flag and when it is true, it indicates overflow has happened
   * 1. Start initial state with overflow = false, sum = null
   * 2. Set sum to null if the value overflows else sum contains the intermediate sum
   * 3. If overflow flag is true, keep sum as null
   * 4. If overflow happened, then set overflow flag to true
   */
  override lazy val updateExpressions: Seq[Expression] = {
    if (child.nullable) {
      resultType match {
        case d: DecimalType =>
          Seq(
            If(overflow, sum, coalesce(
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
            If(overflow, sum,
              CheckOverflow(coalesce(sum, zero) + child.cast(sumDataType), d, true)),
            overflow ||
              coalesce(HasOverflow(coalesce(sum, zero) + child.cast(sumDataType), d), false)
          )
        case _ => Seq(coalesce(sum, zero) + child.cast(sumDataType), false)
      }
    }
  }

  /**
   *
   * Decimal handling:
   * If any of the left or right portion of the agg buffers has the overflow flag to true,
   * then sum is set to null else sum is added for both sum.left and sum.right
   * and if the value overflows it is set to null.
   * If we have already seen overflow , then set overflow to true, else check if the addition
   * overflowed and update the overflow buffer.
   */
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
          false
        )
    }
  }

  /**
   * Decimal handling:
   * If overflow buffer is true, and if ansiEnabled is true then throw exception, else return null
   * If overflow did not happen, then return the sum value
   */
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
