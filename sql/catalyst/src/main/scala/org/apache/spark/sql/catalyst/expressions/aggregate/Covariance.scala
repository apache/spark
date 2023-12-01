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

/**
 * Compute the covariance between two expressions.
 * When applied on empty data (i.e., count is zero), it returns NULL.
 */
abstract class Covariance(val left: Expression, val right: Expression, nullOnDivideByZero: Boolean)
  extends DeclarativeAggregate with ImplicitCastInputTypes with BinaryLike[Expression] {

  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

  protected[sql] val n = AttributeReference("n", DoubleType, nullable = false)()
  protected[sql] val xAvg = AttributeReference("xAvg", DoubleType, nullable = false)()
  protected[sql] val yAvg = AttributeReference("yAvg", DoubleType, nullable = false)()
  protected[sql] val ck = AttributeReference("ck", DoubleType, nullable = false)()

  protected def divideByZeroEvalResult: Expression = {
    if (nullOnDivideByZero) Literal.create(null, DoubleType) else Double.NaN
  }

  override def stringArgs: Iterator[Any] =
    super.stringArgs.filter(_.isInstanceOf[Expression])

  override val aggBufferAttributes: Seq[AttributeReference] = Seq(n, xAvg, yAvg, ck)

  override val initialValues: Seq[Expression] = Array.fill(4)(Literal(0.0))

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

    Seq(newN, newXAvg, newYAvg, newCk)
  }

  protected def updateExpressionsDef: Seq[Expression] = {
    val newN = n + 1.0
    val dx = left - xAvg
    val dy = right - yAvg
    val dyN = dy / newN
    val newXAvg = xAvg + dx / newN
    val newYAvg = yAvg + dyN
    val newCk = ck + dx * (right - newYAvg)

    val isNull = left.isNull || right.isNull
    Seq(
      If(isNull, n, newN),
      If(isNull, xAvg, newXAvg),
      If(isNull, yAvg, newYAvg),
      If(isNull, ck, newCk)
    )
  }
}

@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns the population covariance of a set of number pairs.",
  examples = """
    Examples:
      > SELECT _FUNC_(c1, c2) FROM VALUES (1,1), (2,2), (3,3) AS tab(c1, c2);
       0.6666666666666666
  """,
  group = "agg_funcs",
  since = "2.0.0")
case class CovPopulation(
    override val left: Expression,
    override val right: Expression,
    nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate)
  extends Covariance(left, right, nullOnDivideByZero) {

  def this(left: Expression, right: Expression) =
    this(left, right, !SQLConf.get.legacyStatisticalAggregate)

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType), ck / n)
  }
  override def prettyName: String = "covar_pop"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): CovPopulation =
    copy(left = newLeft, right = newRight)
}


@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns the sample covariance of a set of number pairs.",
  examples = """
    Examples:
      > SELECT _FUNC_(c1, c2) FROM VALUES (1,1), (2,2), (3,3) AS tab(c1, c2);
       1.0
  """,
  group = "agg_funcs",
  since = "2.0.0")
case class CovSample(
    override val left: Expression,
    override val right: Expression,
    nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate)
  extends Covariance(left, right, nullOnDivideByZero) {

  def this(left: Expression, right: Expression) =
    this(left, right, !SQLConf.get.legacyStatisticalAggregate)

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType),
      If(n === 1.0, divideByZeroEvalResult, ck / (n - 1.0)))
  }
  override def prettyName: String = "covar_samp"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): CovSample = copy(left = newLeft, right = newRight)
}

/**
 * Covariance in Pandas' fashion. This expression is dedicated only for Pandas API on Spark.
 * Refer to numpy.cov.
 */
case class PandasCovar(
    override val left: Expression,
    override val right: Expression,
    ddof: Int)
  extends Covariance(left, right, true) {

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType),
      If(n === ddof.toDouble, divideByZeroEvalResult, ck / (n - ddof.toDouble)))
  }
  override def prettyName: String = "pandas_covar"

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): PandasCovar =
    copy(left = newLeft, right = newRight)
}
