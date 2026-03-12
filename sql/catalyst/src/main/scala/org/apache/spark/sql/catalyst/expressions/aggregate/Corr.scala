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
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * Base class for computing Pearson correlation between two expressions.
 * When applied on empty data (i.e., count is zero), it returns NULL.
 *
 * Definition of Pearson correlation can be found at
 * http://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
 */
abstract class PearsonCorrelation(x: Expression, y: Expression, nullOnDivideByZero: Boolean)
  extends DeclarativeAggregate with ImplicitCastInputTypes with BinaryLike[Expression] {

  override def left: Expression = x
  override def right: Expression = y
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

  protected val n = AttributeReference("n", DoubleType, nullable = false)()
  protected val xAvg = AttributeReference("xAvg", DoubleType, nullable = false)()
  protected val yAvg = AttributeReference("yAvg", DoubleType, nullable = false)()
  protected val ck = AttributeReference("ck", DoubleType, nullable = false)()
  protected val xMk = AttributeReference("xMk", DoubleType, nullable = false)()
  protected val yMk = AttributeReference("yMk", DoubleType, nullable = false)()

  protected def divideByZeroEvalResult: Expression = {
    if (nullOnDivideByZero) Literal.create(null, DoubleType) else Double.NaN
  }

  override def stringArgs: Iterator[Any] =
    super.stringArgs.filter(_.isInstanceOf[Expression])

  override val aggBufferAttributes: Seq[AttributeReference] = Seq(n, xAvg, yAvg, ck, xMk, yMk)

  override val initialValues: Seq[Expression] = Array.fill(6)(Literal(0.0)).toImmutableArraySeq

  override lazy val updateExpressions: Seq[Expression] = updateExpressionsDef

  override val mergeExpressions: Seq[Expression] = {
    val n1 = n.left
    val n2 = n.right
    val newN = n1 + n2
    val dx = xAvg.right - xAvg.left
    val dxN = If(newN === 0.0, 0.0, dx / newN)
    val dy = yAvg.right - yAvg.left
    val dyN = If(newN === 0.0, 0.0, dy / newN)
    val newXAvg = xAvg.left + dxN * n2
    val newYAvg = yAvg.left + dyN * n2
    val newCk = ck.left + ck.right + dx * dyN * n1 * n2
    val newXMk = xMk.left + xMk.right + dx * dxN * n1 * n2
    val newYMk = yMk.left + yMk.right + dy * dyN * n1 * n2

    Seq(newN, newXAvg, newYAvg, newCk, newXMk, newYMk)
  }

  protected def updateExpressionsDef: Seq[Expression] = {
    val newN = n + 1.0
    val dx = x - xAvg
    val dxN = dx / newN
    val dy = y - yAvg
    val dyN = dy / newN
    val newXAvg = xAvg + dxN
    val newYAvg = yAvg + dyN
    val newCk = ck + dx * (y - newYAvg)
    val newXMk = xMk + dx * (x - newXAvg)
    val newYMk = yMk + dy * (y - newYAvg)

    val isNull = x.isNull || y.isNull
    Seq(
      If(isNull, n, newN),
      If(isNull, xAvg, newXAvg),
      If(isNull, yAvg, newYAvg),
      If(isNull, ck, newCk),
      If(isNull, xMk, newXMk),
      If(isNull, yMk, newYMk)
    )
  }
}


// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns Pearson coefficient of correlation between a set of number pairs.",
  examples = """
    Examples:
      > SELECT _FUNC_(c1, c2) FROM VALUES (3, 2), (3, 3), (6, 4) as tab(c1, c2);
       0.8660254037844387
  """,
  group = "agg_funcs",
  since = "1.6.0")
// scalastyle:on line.size.limit
case class Corr(
    x: Expression,
    y: Expression,
    nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate)
  extends PearsonCorrelation(x, y, nullOnDivideByZero) {

  def this(x: Expression, y: Expression) =
    this(x, y, !SQLConf.get.legacyStatisticalAggregate)

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType),
      If(n === 1.0, divideByZeroEvalResult, ck / sqrt(xMk * yMk)))
  }

  override def prettyName: String = "corr"

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Corr =
    copy(x = newLeft, y = newRight)
}
