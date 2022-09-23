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

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * A central moment is the expected value of a specified power of the deviation of a random
 * variable from the mean. Central moments are often used to characterize the properties of about
 * the shape of a distribution.
 *
 * This class implements online, one-pass algorithms for computing the central moments of a set of
 * points.
 *
 * Behavior:
 *  - null values are ignored
 *  - returns `Double.NaN` when the column contains `Double.NaN` values
 *
 * References:
 *  - Xiangrui Meng.  "Simpler Online Updates for Arbitrary-Order Central Moments."
 *      2015. http://arxiv.org/abs/1510.04923
 *
 * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance">
 * Algorithms for calculating variance (Wikipedia)</a>
 *
 * @param child to compute central moments of.
 */
abstract class CentralMomentAgg(child: Expression, nullOnDivideByZero: Boolean)
  extends DeclarativeAggregate with ImplicitCastInputTypes with UnaryLike[Expression] {

  /**
   * The central moment order to be computed.
   */
  protected def momentOrder: Int

  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)

  protected val n = AttributeReference("n", DoubleType, nullable = false)()
  protected val avg = AttributeReference("avg", DoubleType, nullable = false)()
  protected[sql] val m2 = AttributeReference("m2", DoubleType, nullable = false)()
  protected val m3 = AttributeReference("m3", DoubleType, nullable = false)()
  protected val m4 = AttributeReference("m4", DoubleType, nullable = false)()

  protected def divideByZeroEvalResult: Expression = {
    if (nullOnDivideByZero) Literal.create(null, DoubleType) else Double.NaN
  }

  override def stringArgs: Iterator[Any] =
    super.stringArgs.filter(_.isInstanceOf[Expression])

  private def trimHigherOrder[T](expressions: Seq[T]) = expressions.take(momentOrder + 1)

  override val aggBufferAttributes = trimHigherOrder(Seq(n, avg, m2, m3, m4))

  override val initialValues: Seq[Expression] = Array.fill(momentOrder + 1)(Literal(0.0))

  override lazy val updateExpressions: Seq[Expression] = updateExpressionsDef

  override val mergeExpressions: Seq[Expression] = {

    val n1 = n.left
    val n2 = n.right
    val newN = n1 + n2
    val delta = avg.right - avg.left
    val deltaN = If(newN === 0.0, 0.0, delta / newN)
    val newAvg = avg.left + deltaN * n2

    // higher order moments computed according to:
    // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
    val newM2 = m2.left + m2.right + delta * deltaN * n1 * n2
    // `m3.right` is not available if momentOrder < 3
    val newM3 = if (momentOrder >= 3) {
      m3.left + m3.right + deltaN * deltaN * delta * n1 * n2 * (n1 - n2) +
        Literal(3.0) * deltaN * (n1 * m2.right - n2 * m2.left)
    } else {
      Literal(0.0)
    }
    // `m4.right` is not available if momentOrder < 4
    val newM4 = if (momentOrder >= 4) {
      m4.left + m4.right +
        deltaN * deltaN * deltaN * delta * n1 * n2 * (n1 * n1 - n1 * n2 + n2 * n2) +
        Literal(6.0) * deltaN * deltaN * (n1 * n1 * m2.right + n2 * n2 * m2.left) +
        Literal(4.0) * deltaN * (n1 * m3.right - n2 * m3.left)
    } else {
      Literal(0.0)
    }

    trimHigherOrder(Seq(newN, newAvg, newM2, newM3, newM4))
  }

  protected def updateExpressionsDef: Seq[Expression] = {
    val newN = n + 1.0
    val delta = child - avg
    val deltaN = delta / newN
    val newAvg = avg + deltaN
    val newM2 = m2 + delta * (delta - deltaN)

    val delta2 = delta * delta
    val deltaN2 = deltaN * deltaN
    val newM3 = if (momentOrder >= 3) {
      m3 - Literal(3.0) * deltaN * newM2 + delta * (delta2 - deltaN2)
    } else {
      Literal(0.0)
    }
    val newM4 = if (momentOrder >= 4) {
      m4 - Literal(4.0) * deltaN * newM3 - Literal(6.0) * deltaN2 * newM2 +
        delta * (delta * delta2 - deltaN * deltaN2)
    } else {
      Literal(0.0)
    }

    trimHigherOrder(Seq(
      If(child.isNull, n, newN),
      If(child.isNull, avg, newAvg),
      If(child.isNull, m2, newM2),
      If(child.isNull, m3, newM3),
      If(child.isNull, m4, newM4)
    ))
  }
}

// Compute the population standard deviation of a column
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the population standard deviation calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (3) AS tab(col);
       0.816496580927726
  """,
  group = "agg_funcs",
  since = "1.6.0")
// scalastyle:on line.size.limit
case class StddevPop(
    child: Expression,
    nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate)
  extends CentralMomentAgg(child, nullOnDivideByZero) {

  def this(child: Expression) = this(child, !SQLConf.get.legacyStatisticalAggregate)

  override protected def momentOrder = 2

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType), sqrt(m2 / n))
  }

  override def prettyName: String = "stddev_pop"

  override protected def withNewChildInternal(newChild: Expression): StddevPop =
    copy(child = newChild)
}

// Compute the sample standard deviation of a column
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the sample standard deviation calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (3) AS tab(col);
       1.0
  """,
  group = "agg_funcs",
  since = "1.6.0")
// scalastyle:on line.size.limit
case class StddevSamp(
    child: Expression,
    nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate)
  extends CentralMomentAgg(child, nullOnDivideByZero) {

  def this(child: Expression) = this(child, !SQLConf.get.legacyStatisticalAggregate)

  override protected def momentOrder = 2

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType),
      If(n === 1.0, divideByZeroEvalResult, sqrt(m2 / (n - 1.0))))
  }

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("stddev_samp")

  override protected def withNewChildInternal(newChild: Expression): StddevSamp =
    copy(child = newChild)
}

// Compute the population variance of a column
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the population variance calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (3) AS tab(col);
       0.6666666666666666
  """,
  group = "agg_funcs",
  since = "1.6.0")
case class VariancePop(
    child: Expression,
    nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate)
  extends CentralMomentAgg(child, nullOnDivideByZero) {

  def this(child: Expression) = this(child, !SQLConf.get.legacyStatisticalAggregate)

  override protected def momentOrder = 2

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType), m2 / n)
  }

  override def prettyName: String = "var_pop"

  override protected def withNewChildInternal(newChild: Expression): VariancePop =
    copy(child = newChild)
}

// Compute the sample variance of a column
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the sample variance calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (3) AS tab(col);
       1.0
  """,
  group = "agg_funcs",
  since = "1.6.0")
case class VarianceSamp(
    child: Expression,
    nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate)
  extends CentralMomentAgg(child, nullOnDivideByZero) {

  def this(child: Expression) = this(child, !SQLConf.get.legacyStatisticalAggregate)

  override protected def momentOrder = 2

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType),
      If(n === 1.0, divideByZeroEvalResult, m2 / (n - 1.0)))
  }

  override def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("var_samp")

  override protected def withNewChildInternal(newChild: Expression): VarianceSamp =
    copy(child = newChild)
}

case class RegrReplacement(child: Expression)
  extends CentralMomentAgg(child, !SQLConf.get.legacyStatisticalAggregate) {

  override protected def momentOrder = 2

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType), m2)
  }

  override protected def withNewChildInternal(newChild: Expression): RegrReplacement =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the skewness value calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (-10), (-20), (100), (1000) AS tab(col);
       1.1135657469022011
      > SELECT _FUNC_(col) FROM VALUES (-1000), (-100), (10), (20) AS tab(col);
       -1.1135657469022011
  """,
  group = "agg_funcs",
  since = "1.6.0")
case class Skewness(
    child: Expression,
    nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate)
  extends CentralMomentAgg(child, nullOnDivideByZero) {

  def this(child: Expression) = this(child, !SQLConf.get.legacyStatisticalAggregate)

  override def prettyName: String = "skewness"

  override protected def momentOrder = 3

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType),
      If(m2 === 0.0, divideByZeroEvalResult, sqrt(n) * m3 / sqrt(m2 * m2 * m2)))
  }

  override protected def withNewChildInternal(newChild: Expression): Skewness =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the kurtosis value calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (-10), (-20), (100), (1000) AS tab(col);
       -0.7014368047529627
      > SELECT _FUNC_(col) FROM VALUES (1), (10), (100), (10), (1) as tab(col);
       0.19432323191699075
  """,
  group = "agg_funcs",
  since = "1.6.0")
case class Kurtosis(
    child: Expression,
    nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate)
  extends CentralMomentAgg(child, nullOnDivideByZero) {

  def this(child: Expression) = this(child, !SQLConf.get.legacyStatisticalAggregate)

  override protected def momentOrder = 4

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType),
      If(m2 === 0.0, divideByZeroEvalResult, n * m4 / (m2 * m2) - 3.0))
  }

  override def prettyName: String = "kurtosis"

  override protected def withNewChildInternal(newChild: Expression): Kurtosis =
    copy(child = newChild)
}

/**
 * Standard deviation in Pandas' fashion.
 * This expression is dedicated only for Pandas API on Spark.
 * Refer to pandas.core.nanops.nanstd.
 */
case class PandasStddev(
    child: Expression,
    ddof: Int)
  extends CentralMomentAgg(child, true) {

  override protected def momentOrder = 2

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType),
      If(n === ddof, divideByZeroEvalResult, sqrt(m2 / (n - ddof))))
  }

  override def prettyName: String = "pandas_stddev"

  override protected def withNewChildInternal(newChild: Expression): PandasStddev =
    copy(child = newChild)
}

/**
 * Skewness in Pandas' fashion. This expression is dedicated only for Pandas API on Spark.
 * Refer to pandas.core.nanops.nanskew.
 */
case class PandasSkewness(child: Expression)
  extends CentralMomentAgg(child, true) {

  override def prettyName: String = "pandas_skewness"

  override protected def momentOrder = 3

  override val evaluateExpression: Expression = {
    // floating point error
    //
    // Pandas #18044 in _libs/windows.pyx calc_skew follow this behavior
    // to fix the fperr to treat m2 <1e-14 as zero
    //
    // see https://github.com/pandas-dev/pandas/issues/18044 for details
    val _m2 = If(abs(m2) < 1e-14, Literal(0.0), m2)
    val _m3 = If(abs(m3) < 1e-14, Literal(0.0), m3)

    If(n < 3, Literal.create(null, DoubleType),
      If(_m2 === 0.0, Literal(0.0), sqrt(n - 1) * (n / (n - 2)) * _m3 / sqrt(_m2 * _m2 * _m2)))
  }

  override protected def withNewChildInternal(newChild: Expression): PandasSkewness =
    copy(child = newChild)
}

/**
 * Kurtosis in Pandas' fashion. This expression is dedicated only for Pandas API on Spark.
 * Refer to pandas.core.nanops.nankurt.
 */
case class PandasKurtosis(child: Expression)
  extends CentralMomentAgg(child, true) {

  override protected def momentOrder = 4

  override val evaluateExpression: Expression = {
    val adj = ((n - 1) / (n - 2)) * ((n - 1) / (n - 3)) * 3
    val numerator = n * (n + 1) * (n - 1) * m4
    val denominator = (n - 2) * (n - 3) * m2 * m2

    // floating point error
    //
    // Pandas #18044 in _libs/windows.pyx calc_kurt follow this behavior
    // to fix the fperr to treat denom <1e-14 as zero
    //
    // see https://github.com/pandas-dev/pandas/issues/18044 for details
    val _numerator = If(abs(numerator) < 1e-14, Literal(0.0), numerator)
    val _denominator = If(abs(denominator) < 1e-14, Literal(0.0), denominator)

    If(n < 4, Literal.create(null, DoubleType),
      If(_denominator === 0.0, Literal(0.0), _numerator / _denominator - adj))
  }

  override def prettyName: String = "pandas_kurtosis"

  override protected def withNewChildInternal(newChild: Expression): PandasKurtosis =
    copy(child = newChild)
}
