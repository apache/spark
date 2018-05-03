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
import org.apache.spark.sql.types.{AbstractDataType, DoubleType}

/**
 * Base trait for all regression functions.
 */
trait RegrLike extends AggregateFunction with ImplicitCastInputTypes {
  def y: Expression
  def x: Expression

  override def children: Seq[Expression] = Seq(y, x)
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

  protected def updateIfNotNull(exprs: Seq[Expression]): Seq[Expression] = {
    assert(aggBufferAttributes.length == exprs.length)
    val nullableChildren = children.filter(_.nullable)
    if (nullableChildren.isEmpty) {
      exprs
    } else {
      exprs.zip(aggBufferAttributes).map { case (e, a) =>
        If(nullableChildren.map(IsNull).reduce(Or), a, e)
      }
    }
  }
}


@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the number of non-null pairs.",
  since = "2.4.0")
case class RegrCount(y: Expression, x: Expression)
  extends CountLike with RegrLike {

  override lazy val updateExpressions: Seq[Expression] = updateIfNotNull(Seq(count + 1L))

  override def prettyName: String = "regr_count"
}


@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns SUM(x*x)-SUM(x)*SUM(x)/N. Any pair with a NULL is ignored.",
  since = "2.4.0")
case class RegrSXX(y: Expression, x: Expression)
  extends CentralMomentAgg(x) with RegrLike {

  override protected def momentOrder = 2

  override lazy val updateExpressions: Seq[Expression] = updateIfNotNull(updateExpressionsDef)

  override val evaluateExpression: Expression = {
    If(n === Literal(0.0), Literal.create(null, DoubleType), m2)
  }

  override def prettyName: String = "regr_sxx"
}


@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns SUM(y*y)-SUM(y)*SUM(y)/N. Any pair with a NULL is ignored.",
  since = "2.4.0")
case class RegrSYY(y: Expression, x: Expression)
  extends CentralMomentAgg(y) with RegrLike {

  override protected def momentOrder = 2

  override lazy val updateExpressions: Seq[Expression] = updateIfNotNull(updateExpressionsDef)

  override val evaluateExpression: Expression = {
    If(n === Literal(0.0), Literal.create(null, DoubleType), m2)
  }

  override def prettyName: String = "regr_syy"
}


@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the average of x. Any pair with a NULL is ignored.",
  since = "2.4.0")
case class RegrAvgX(y: Expression, x: Expression)
  extends AverageLike(x) with RegrLike {

  override lazy val updateExpressions: Seq[Expression] = updateIfNotNull(updateExpressionsDef)

  override def prettyName: String = "regr_avgx"
}


@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the average of y. Any pair with a NULL is ignored.",
  since = "2.4.0")
case class RegrAvgY(y: Expression, x: Expression)
  extends AverageLike(y) with RegrLike {

  override lazy val updateExpressions: Seq[Expression] = updateIfNotNull(updateExpressionsDef)

  override def prettyName: String = "regr_avgy"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the covariance of y and x multiplied for the number of items in the dataset. Any pair with a NULL is ignored.",
  since = "2.4.0")
// scalastyle:on line.size.limit
case class RegrSXY(y: Expression, x: Expression)
  extends Covariance(y, x) with RegrLike {

  override lazy val updateExpressions: Seq[Expression] = updateIfNotNull(updateExpressionsDef)

  override val evaluateExpression: Expression = {
    If(n === Literal(0.0), Literal.create(null, DoubleType), ck)
  }

  override def prettyName: String = "regr_sxy"
}


// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the slope of the linear regression line. Any pair with a NULL is ignored.",
  since = "2.4.0")
// scalastyle:on line.size.limit
case class RegrSlope(y: Expression, x: Expression)
  extends PearsonCorrelation(y, x) with RegrLike {

  override lazy val updateExpressions: Seq[Expression] = updateIfNotNull(updateExpressionsDef)

  override val evaluateExpression: Expression = {
    If(n < Literal(2.0) || yMk === Literal(0.0), Literal.create(null, DoubleType), ck / yMk)
  }

  override def prettyName: String = "regr_slope"
}


// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the coefficient of determination (also called R-squared or goodness of fit) for the regression line. Any pair with a NULL is ignored.",
  since = "2.4.0")
// scalastyle:on line.size.limit
case class RegrR2(y: Expression, x: Expression)
  extends PearsonCorrelation(y, x) with RegrLike {

  override lazy val updateExpressions: Seq[Expression] = updateIfNotNull(updateExpressionsDef)

  override val evaluateExpression: Expression = {
    If(n < Literal(2.0) || yMk === Literal(0.0), Literal.create(null, DoubleType),
      If(xMk === Literal(0.0), Literal(1.0), ck * ck / yMk / xMk))
  }

  override def prettyName: String = "regr_r2"
}


// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the y-intercept of the linear regression line. Any pair with a NULL is ignored.",
  since = "2.4.0")
// scalastyle:on line.size.limit
case class RegrIntercept(y: Expression, x: Expression)
  extends PearsonCorrelation(y, x) with RegrLike {

  override lazy val updateExpressions: Seq[Expression] = updateIfNotNull(updateExpressionsDef)

  override val evaluateExpression: Expression = {
    If(n === Literal(0.0) || yMk === Literal(0.0), Literal.create(null, DoubleType),
      xAvg - (ck / yMk) * yAvg)
  }

  override def prettyName: String = "regr_intercept"
}
