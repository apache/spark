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
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, ExpressionDescription, If, ImplicitCastInputTypes, IsNotNull, IsNull, Literal, Or, RuntimeReplaceableAggregate}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType, NumericType}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the number of non-null number pairs in a group, where `y` is the dependent variable and `x` is the independent variable.",
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       4
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       3
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       2
  """,
  group = "agg_funcs",
  since = "3.3.0")
// scalastyle:on line.size.limit
case class RegrCount(left: Expression, right: Expression)
  extends AggregateFunction
  with RuntimeReplaceableAggregate
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {
  override lazy val replacement: Expression = Count(Seq(left, right))
  override def nodeName: String = "regr_count"
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, NumericType)
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrCount =
    this.copy(left = newLeft, right = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the average of the independent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.",
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       2.75
      > SELECT _FUNC_(y, x) FROM VALUES (1, null) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (null, 1) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       3.0
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       3.0
  """,
  group = "agg_funcs",
  since = "3.3.0")
// scalastyle:on line.size.limit
case class RegrAvgX(
    left: Expression,
    right: Expression)
  extends AggregateFunction
  with RuntimeReplaceableAggregate
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {
  override lazy val replacement: Expression =
    Average(If(And(IsNotNull(left), IsNotNull(right)), right, Literal.create(null, right.dataType)))
  override def nodeName: String = "regr_avgx"
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, NumericType)
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrAvgX =
    this.copy(left = newLeft, right = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the average of the dependent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.",
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       1.75
      > SELECT _FUNC_(y, x) FROM VALUES (1, null) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (null, 1) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       1.6666666666666667
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       1.5
  """,
  group = "agg_funcs",
  since = "3.3.0")
// scalastyle:on line.size.limit
case class RegrAvgY(
    left: Expression,
    right: Expression)
  extends AggregateFunction
  with RuntimeReplaceableAggregate
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {
  override lazy val replacement: Expression =
    Average(If(And(IsNotNull(left), IsNotNull(right)), left, Literal.create(null, left.dataType)))
  override def nodeName: String = "regr_avgy"
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, NumericType)
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrAvgY =
    this.copy(left = newLeft, right = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the coefficient of determination for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.",
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       0.2727272727272727
      > SELECT _FUNC_(y, x) FROM VALUES (1, null) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (null, 1) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       0.7500000000000001
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       1.0
  """,
  group = "agg_funcs",
  since = "3.3.0")
// scalastyle:on line.size.limit
case class RegrR2(y: Expression, x: Expression) extends PearsonCorrelation(y, x, true) {
  override def prettyName: String = "regr_r2"
  override val evaluateExpression: Expression = {
    val corr = ck / sqrt(xMk * yMk)
    If(xMk === 0.0, Literal.create(null, DoubleType),
      If(yMk === 0.0, Literal.create(1.0, DoubleType), corr * corr))
  }
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrR2 =
    this.copy(y = newLeft, x = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.",
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       2.75
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       2.0
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       2.0
  """,
  group = "agg_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
case class RegrSXX(
    left: Expression,
    right: Expression)
  extends AggregateFunction
    with RuntimeReplaceableAggregate
    with ImplicitCastInputTypes
    with BinaryLike[Expression] {
  override lazy val replacement: Expression =
    RegrReplacement(If(Or(IsNull(left), IsNull(right)), Literal.create(null, DoubleType), right))
  override def nodeName: String = "regr_sxx"
  override def inputTypes: Seq[DoubleType] = Seq(DoubleType, DoubleType)
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrSXX =
    this.copy(left = newLeft, right = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.",
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       0.75
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       1.0
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       1.0
  """,
  group = "agg_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
case class RegrSXY(y: Expression, x: Expression) extends Covariance(y, x, true) {
  override def prettyName: String = "regr_sxy"
  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType), ck)
  }
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrSXY =
    this.copy(y = newLeft, x = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.",
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       0.75
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       0.6666666666666666
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       0.5
  """,
  group = "agg_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
case class RegrSYY(
    left: Expression,
    right: Expression)
  extends AggregateFunction
    with RuntimeReplaceableAggregate
    with ImplicitCastInputTypes
    with BinaryLike[Expression] {
  override lazy val replacement: Expression =
    RegrReplacement(If(Or(IsNull(left), IsNull(right)), Literal.create(null, DoubleType), left))
  override def nodeName: String = "regr_syy"
  override def inputTypes: Seq[DoubleType] = Seq(DoubleType, DoubleType)
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrSYY =
    this.copy(left = newLeft, right = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the slope of the linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.",
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1,1), (2,2), (3,3) AS tab(y, x);
       1.0
      > SELECT _FUNC_(y, x) FROM VALUES (1, null) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (null, 1) AS tab(y, x);
       NULL
  """,
  group = "agg_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
case class RegrSlope(left: Expression, right: Expression) extends DeclarativeAggregate
  with ImplicitCastInputTypes with BinaryLike[Expression] {

  private val covarPop = new CovPopulation(right, left)

  private val varPop = new VariancePop(right)

  override def nullable: Boolean = true

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[DoubleType] = Seq(DoubleType, DoubleType)

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    covarPop.aggBufferAttributes ++ varPop.aggBufferAttributes

  override lazy val initialValues: Seq[Expression] = covarPop.initialValues ++ varPop.initialValues

  override lazy val updateExpressions: Seq[Expression] =
    covarPop.updateExpressions ++ varPop.updateExpressions

  override lazy val mergeExpressions: Seq[Expression] =
    covarPop.mergeExpressions ++ varPop.mergeExpressions

  override lazy val evaluateExpression: Expression = {
    If(varPop.m2 === 0.0, Literal.create(null, DoubleType), covarPop.ck / varPop.m2)
  }

  override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    covarPop.inputAggBufferAttributes ++ varPop.inputAggBufferAttributes

  override def prettyName: String = "regr_slope"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrSlope =
    copy(left = newLeft, right = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the intercept of the univariate linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.",
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1,1), (2,2), (3,3) AS tab(y, x);
       0.0
      > SELECT _FUNC_(y, x) FROM VALUES (1, null) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (null, 1) AS tab(y, x);
       NULL
  """,
  group = "agg_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
case class RegrIntercept(left: Expression, right: Expression) extends DeclarativeAggregate
  with ImplicitCastInputTypes with BinaryLike[Expression] {

  private val covarPop = new CovPopulation(right, left)

  private val varPop = new VariancePop(right)

  override def nullable: Boolean = true

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[DoubleType] = Seq(DoubleType, DoubleType)

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    covarPop.aggBufferAttributes ++ varPop.aggBufferAttributes

  override lazy val initialValues: Seq[Expression] = covarPop.initialValues ++ varPop.initialValues

  override lazy val updateExpressions: Seq[Expression] =
    covarPop.updateExpressions ++ varPop.updateExpressions

  override lazy val mergeExpressions: Seq[Expression] =
    covarPop.mergeExpressions ++ varPop.mergeExpressions

  override lazy val evaluateExpression: Expression = {
    If(varPop.m2 === 0.0, Literal.create(null, DoubleType),
      covarPop.yAvg - covarPop.ck / varPop.m2 * covarPop.xAvg)
  }

  override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    covarPop.inputAggBufferAttributes ++ varPop.inputAggBufferAttributes

  override def prettyName: String = "regr_intercept"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrIntercept =
    copy(left = newLeft, right = newRight)
}
