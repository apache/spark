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
import org.apache.spark.sql.types.{ AbstractDataType, DataType, DoubleType, NumericType }


@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the product calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (2), (3), (5) AS tab(col);
       30
      > SELECT _FUNC_(col) FROM VALUES (NULL), (5), (7) AS tab(col);
       35
      > SELECT _FUNC_(col) FROM VALUES (NULL), (NULL) AS tab(col);
       NULL
  """,
  group = "agg_funcs",
  since = "3.2.0")
case class Product(child: Expression, scale: Double = 1.0)
    extends DeclarativeAggregate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function product")

  private val resultType = DoubleType

  private lazy val product = AttributeReference("product", resultType)()

  private lazy val one = Literal(1.0, resultType)

  override lazy val aggBufferAttributes = product :: Nil

  override lazy val initialValues: Seq[Expression] =
    Seq(Literal(null, resultType))

  override lazy val updateExpressions: Seq[Expression] = {
    // Treat the result as null until we have seen at least one child value,
    // whereupon the previous product is promoted to being unity.
    // Each child value is implicitly multiplied by a scaling factor
    // (1.0 by default) so that the client has some control over overflow
    // when multiplying together many child values, without needing
    // to explicitly rescale a Column beforehand.

    val castChild = child.cast(resultType)

    val protoResult =
      coalesce(product, one) * (scale match {
                                        case 1.0 => castChild
                                        case -1.0 => -castChild
                                        case _ => castChild * scale })

    if (child.nullable) {
      Seq(coalesce(protoResult, product))
    } else {
      Seq(protoResult)
    }
  }

  override lazy val mergeExpressions: Seq[Expression] =
    Seq(coalesce(coalesce(product.left, one) * product.right,
                 product.left))

  override lazy val evaluateExpression: Expression = product
}
