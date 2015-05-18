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

package org.apache.spark.sql

import scala.language.implicitConversions
import scala.reflect.runtime.universe.{TypeTag, typeTag}

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction, Star}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.mathfuncs._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * :: Experimental ::
 * Functions available for [[DataFrame]].
 *
 * @groupname udf_funcs UDF functions
 * @groupname agg_funcs Aggregate functions
 * @groupname sort_funcs Sorting functions
 * @groupname normal_funcs Non-aggregate functions
 * @groupname math_funcs Math functions
 * @groupname Ungrouped Support functions for DataFrames.
 * @since 1.3.0
 */
@Experimental
// scalastyle:off
object functions {
// scalastyle:on

  private[this] implicit def toColumn(expr: Expression): Column = Column(expr)

  /**
   * Returns a [[Column]] based on the given column name.
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def col(colName: String): Column = Column(colName)

  /**
   * Returns a [[Column]] based on the given column name. Alias of [[col]].
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def column(colName: String): Column = Column(colName)

  /**
   * Creates a [[Column]] of literal value.
   *
   * The passed in object is returned directly if it is already a [[Column]].
   * If the object is a Scala Symbol, it is converted into a [[Column]] also.
   * Otherwise, a new [[Column]] is created to represent the literal value.
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def lit(literal: Any): Column = {
    literal match {
      case c: Column => return c
      case s: Symbol => return new ColumnName(literal.asInstanceOf[Symbol].name)
      case _ =>  // continue
    }

    val literalExpr = Literal(literal)
    Column(literalExpr)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Sort functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a sort expression based on ascending order of the column.
   * {{{
   *   // Sort by dept in ascending order, and then age in descending order.
   *   df.sort(asc("dept"), desc("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 1.3.0
   */
  def asc(columnName: String): Column = Column(columnName).asc

  /**
   * Returns a sort expression based on the descending order of the column.
   * {{{
   *   // Sort by dept in ascending order, and then age in descending order.
   *   df.sort(asc("dept"), desc("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 1.3.0
   */
  def desc(columnName: String): Column = Column(columnName).desc

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Aggregate functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Aggregate function: returns the sum of all values in the expression.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def sum(e: Column): Column = Sum(e.expr)

  /**
   * Aggregate function: returns the sum of all values in the given column.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def sum(columnName: String): Column = sum(Column(columnName))

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def sumDistinct(e: Column): Column = SumDistinct(e.expr)

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def sumDistinct(columnName: String): Column = sumDistinct(Column(columnName))

  /**
   * Aggregate function: returns the number of items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def count(e: Column): Column = e.expr match {
    // Turn count(*) into count(1)
    case s: Star => Count(Literal(1))
    case _ => Count(e.expr)
  }

  /**
   * Aggregate function: returns the number of items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def count(columnName: String): Column = count(Column(columnName))

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def countDistinct(expr: Column, exprs: Column*): Column =
    CountDistinct((expr +: exprs).map(_.expr))

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def countDistinct(columnName: String, columnNames: String*): Column =
    countDistinct(Column(columnName), columnNames.map(Column.apply) :_*)

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def approxCountDistinct(e: Column): Column = ApproxCountDistinct(e.expr)

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def approxCountDistinct(columnName: String): Column = approxCountDistinct(column(columnName))

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def approxCountDistinct(e: Column, rsd: Double): Column = ApproxCountDistinct(e.expr, rsd)

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def approxCountDistinct(columnName: String, rsd: Double): Column = {
    approxCountDistinct(Column(columnName), rsd)
  }

  /**
   * Aggregate function: returns the average of the values in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def avg(e: Column): Column = Average(e.expr)

  /**
   * Aggregate function: returns the average of the values in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def avg(columnName: String): Column = avg(Column(columnName))

  /**
   * Aggregate function: returns the first value in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def first(e: Column): Column = First(e.expr)

  /**
   * Aggregate function: returns the first value of a column in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def first(columnName: String): Column = first(Column(columnName))

  /**
   * Aggregate function: returns the last value in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def last(e: Column): Column = Last(e.expr)

  /**
   * Aggregate function: returns the last value of the column in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def last(columnName: String): Column = last(Column(columnName))

  /**
   * Aggregate function: returns the average of the values in a group.
   * Alias for avg.
   *
   * @group agg_funcs
   * @since 1.4.0
   */
  def mean(e: Column): Column = avg(e)

  /**
   * Aggregate function: returns the average of the values in a group.
   * Alias for avg.
   *
   * @group agg_funcs
   * @since 1.4.0
   */
  def mean(columnName: String): Column = avg(columnName)

  /**
   * Aggregate function: returns the minimum value of the expression in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def min(e: Column): Column = Min(e.expr)

  /**
   * Aggregate function: returns the minimum value of the column in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def min(columnName: String): Column = min(Column(columnName))

  /**
   * Aggregate function: returns the maximum value of the expression in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def max(e: Column): Column = Max(e.expr)

  /**
   * Aggregate function: returns the maximum value of the column in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def max(columnName: String): Column = max(Column(columnName))

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Non-aggregate functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Computes the absolute value.
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def abs(e: Column): Column = Abs(e.expr)

  /**
   * Creates a new array column. The input columns must all have the same data type.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def array(cols: Column*): Column = CreateArray(cols.map(_.expr))

  /**
   * Creates a new array column. The input columns must all have the same data type.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def array(colName: String, colNames: String*): Column = {
    array((colName +: colNames).map(col) : _*)
  }

  /**
   * Returns the first column that is not null.
   * {{{
   *   df.select(coalesce(df("a"), df("b")))
   * }}}
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def coalesce(e: Column*): Column = Coalesce(e.map(_.expr))

  /**
   * Creates a new row for each element in the given array or map column.
   */
   def explode(e: Column): Column = Explode(e.expr)

  /**
   * Converts a string exprsesion to lower case.
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def lower(e: Column): Column = Lower(e.expr)

  /**
   * A column expression that generates monotonically increasing 64-bit integers.
   *
   * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
   * The current implementation puts the partition ID in the upper 31 bits, and the record number
   * within each partition in the lower 33 bits. The assumption is that the data frame has
   * less than 1 billion partitions, and each partition has less than 8 billion records.
   *
   * As an example, consider a [[DataFrame]] with two partitions, each with 3 records.
   * This expression would return the following IDs:
   * 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def monotonicallyIncreasingId(): Column = execution.expressions.MonotonicallyIncreasingID()

  /**
   * Unary minus, i.e. negate the expression.
   * {{{
   *   // Select the amount column and negates all values.
   *   // Scala:
   *   df.select( -df("amount") )
   *
   *   // Java:
   *   df.select( negate(df.col("amount")) );
   * }}}
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def negate(e: Column): Column = -e

  /**
   * Inversion of boolean expression, i.e. NOT.
   * {{{
   *   // Scala: select rows that are not active (isActive === false)
   *   df.filter( !df("isActive") )
   *
   *   // Java:
   *   df.filter( not(df.col("isActive")) );
   * }}}
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def not(e: Column): Column = !e

  /**
   * Evaluates a list of conditions and returns one of multiple possible result expressions.
   * If otherwise is not defined at the end, null is returned for unmatched conditions.
   *
   * {{{
   *   // Example: encoding gender string column into integer.
   *
   *   // Scala:
   *   people.select(when(people("gender") === "male", 0)
   *     .when(people("gender") === "female", 1)
   *     .otherwise(2))
   *
   *   // Java:
   *   people.select(when(col("gender").equalTo("male"), 0)
   *     .when(col("gender").equalTo("female"), 1)
   *     .otherwise(2))
   * }}}
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def when(condition: Column, value: Any): Column = {
    CaseWhen(Seq(condition.expr, lit(value).expr))
  }

  /**
   * Generate a random column with i.i.d. samples from U[0.0, 1.0].
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def rand(seed: Long): Column = Rand(seed)

  /**
   * Generate a random column with i.i.d. samples from U[0.0, 1.0].
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def rand(): Column = rand(Utils.random.nextLong)

  /**
   * Generate a column with i.i.d. samples from the standard normal distribution.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def randn(seed: Long): Column = Randn(seed)

  /**
   * Generate a column with i.i.d. samples from the standard normal distribution.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def randn(): Column = randn(Utils.random.nextLong)

  /**
   * Partition ID of the Spark task.
   *
   * Note that this is indeterministic because it depends on data partitioning and task scheduling.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def sparkPartitionId(): Column = execution.expressions.SparkPartitionID

  /**
   * Computes the square root of the specified float value.
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def sqrt(e: Column): Column = Sqrt(e.expr)

  /**
   * Creates a new struct column. The input column must be a column in a [[DataFrame]], or
   * a derived column expression that is named (i.e. aliased).
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def struct(cols: Column*): Column = {
    require(cols.forall(_.expr.isInstanceOf[NamedExpression]),
      s"struct input columns must all be named or aliased ($cols)")
    CreateStruct(cols.map(_.expr.asInstanceOf[NamedExpression]))
  }

  /**
   * Creates a new struct column that composes multiple input columns.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def struct(colName: String, colNames: String*): Column = {
    struct((colName +: colNames).map(col) : _*)
  }

  /**
   * Converts a string expression to upper case.
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def upper(e: Column): Column = Upper(e.expr)

  /**
   * Computes bitwise NOT.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def bitwiseNOT(e: Column): Column = BitwiseNot(e.expr)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Math Functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Computes the cosine inverse of the given value; the returned angle is in the range
   * 0.0 through pi.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def acos(e: Column): Column = Acos(e.expr)

  /**
   * Computes the cosine inverse of the given column; the returned angle is in the range
   * 0.0 through pi.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def acos(columnName: String): Column = acos(Column(columnName))

  /**
   * Computes the sine inverse of the given value; the returned angle is in the range
   * -pi/2 through pi/2.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def asin(e: Column): Column = Asin(e.expr)

  /**
   * Computes the sine inverse of the given column; the returned angle is in the range
   * -pi/2 through pi/2.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def asin(columnName: String): Column = asin(Column(columnName))

  /**
   * Computes the tangent inverse of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan(e: Column): Column = Atan(e.expr)

  /**
   * Computes the tangent inverse of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan(columnName: String): Column = atan(Column(columnName))

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(l: Column, r: Column): Column = Atan2(l.expr, r.expr)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(l: Column, rightName: String): Column = atan2(l, Column(rightName))

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(leftName: String, r: Column): Column = atan2(Column(leftName), r)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(leftName: String, rightName: String): Column =
    atan2(Column(leftName), Column(rightName))

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(l: Column, r: Double): Column = atan2(l, lit(r).expr)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(leftName: String, r: Double): Column = atan2(Column(leftName), r)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(l: Double, r: Column): Column = atan2(lit(l).expr, r)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(l: Double, rightName: String): Column = atan2(l, Column(rightName))

  /**
   * Computes the cube-root of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cbrt(e: Column): Column = Cbrt(e.expr)

  /**
   * Computes the cube-root of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cbrt(columnName: String): Column = cbrt(Column(columnName))

  /**
   * Computes the ceiling of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def ceil(e: Column): Column = Ceil(e.expr)

  /**
   * Computes the ceiling of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def ceil(columnName: String): Column = ceil(Column(columnName))

  /**
   * Computes the cosine of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cos(e: Column): Column = Cos(e.expr)

  /**
   * Computes the cosine of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cos(columnName: String): Column = cos(Column(columnName))

  /**
   * Computes the hyperbolic cosine of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cosh(e: Column): Column = Cosh(e.expr)

  /**
   * Computes the hyperbolic cosine of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cosh(columnName: String): Column = cosh(Column(columnName))

  /**
   * Computes the exponential of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def exp(e: Column): Column = Exp(e.expr)

  /**
   * Computes the exponential of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def exp(columnName: String): Column = exp(Column(columnName))

  /**
   * Computes the exponential of the given value minus one.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def expm1(e: Column): Column = Expm1(e.expr)

  /**
   * Computes the exponential of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def expm1(columnName: String): Column = expm1(Column(columnName))

  /**
   * Computes the floor of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def floor(e: Column): Column = Floor(e.expr)

  /**
   * Computes the floor of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def floor(columnName: String): Column = floor(Column(columnName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(l: Column, r: Column): Column = Hypot(l.expr, r.expr)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(l: Column, rightName: String): Column = hypot(l, Column(rightName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(leftName: String, r: Column): Column = hypot(Column(leftName), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(leftName: String, rightName: String): Column =
    hypot(Column(leftName), Column(rightName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(l: Column, r: Double): Column = hypot(l, lit(r).expr)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(leftName: String, r: Double): Column = hypot(Column(leftName), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(l: Double, r: Column): Column = hypot(lit(l).expr, r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(l: Double, rightName: String): Column = hypot(l, Column(rightName))

  /**
   * Computes the natural logarithm of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log(e: Column): Column = Log(e.expr)

  /**
   * Computes the natural logarithm of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log(columnName: String): Column = log(Column(columnName))

  /**
   * Computes the logarithm of the given value in Base 10.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log10(e: Column): Column = Log10(e.expr)

  /**
   * Computes the logarithm of the given value in Base 10.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log10(columnName: String): Column = log10(Column(columnName))

  /**
   * Computes the natural logarithm of the given value plus one.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log1p(e: Column): Column = Log1p(e.expr)

  /**
   * Computes the natural logarithm of the given column plus one.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log1p(columnName: String): Column = log1p(Column(columnName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(l: Column, r: Column): Column = Pow(l.expr, r.expr)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(l: Column, rightName: String): Column = pow(l, Column(rightName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(leftName: String, r: Column): Column = pow(Column(leftName), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(leftName: String, rightName: String): Column = pow(Column(leftName), Column(rightName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(l: Column, r: Double): Column = pow(l, lit(r).expr)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(leftName: String, r: Double): Column = pow(Column(leftName), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(l: Double, r: Column): Column = pow(lit(l).expr, r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(l: Double, rightName: String): Column = pow(l, Column(rightName))

  /**
   * Returns the double value that is closest in value to the argument and
   * is equal to a mathematical integer.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def rint(e: Column): Column = Rint(e.expr)

  /**
   * Returns the double value that is closest in value to the argument and
   * is equal to a mathematical integer.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def rint(columnName: String): Column = rint(Column(columnName))

  /**
   * Computes the signum of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def signum(e: Column): Column = Signum(e.expr)

  /**
   * Computes the signum of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def signum(columnName: String): Column = signum(Column(columnName))

  /**
   * Computes the sine of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sin(e: Column): Column = Sin(e.expr)

  /**
   * Computes the sine of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sin(columnName: String): Column = sin(Column(columnName))

  /**
   * Computes the hyperbolic sine of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sinh(e: Column): Column = Sinh(e.expr)

  /**
   * Computes the hyperbolic sine of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sinh(columnName: String): Column = sinh(Column(columnName))

  /**
   * Computes the tangent of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tan(e: Column): Column = Tan(e.expr)

  /**
   * Computes the tangent of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tan(columnName: String): Column = tan(Column(columnName))

  /**
   * Computes the hyperbolic tangent of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tanh(e: Column): Column = Tanh(e.expr)

  /**
   * Computes the hyperbolic tangent of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tanh(columnName: String): Column = tanh(Column(columnName))

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def toDegrees(e: Column): Column = ToDegrees(e.expr)

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def toDegrees(columnName: String): Column = toDegrees(Column(columnName))

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def toRadians(e: Column): Column = ToRadians(e.expr)

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def toRadians(columnName: String): Column = toRadians(Column(columnName))
    

  //////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////

  // scalastyle:off

  /* Use the following code to generate:
  (0 to 10).map { x =>
    val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
    val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
    println(s"""
    /**
     * Defines a user-defined function of ${x} arguments as user-defined function (UDF).
     * The data types are automatically inferred based on the function's signature.
     *
     * @group udf_funcs
     * @since 1.3.0
     */
    def udf[$typeTags](f: Function$x[$types]): UserDefinedFunction = {
      UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
    }""")
  }

  (0 to 10).map { x =>
    val args = (1 to x).map(i => s"arg$i: Column").mkString(", ")
    val fTypes = Seq.fill(x + 1)("_").mkString(", ")
    val argsInUdf = (1 to x).map(i => s"arg$i.expr").mkString(", ")
    println(s"""
    /**
     * Call a Scala function of ${x} arguments as user-defined function (UDF). This requires
     * you to specify the return data type.
     *
     * @group udf_funcs
     * @since 1.3.0
     */
    def callUDF(f: Function$x[$fTypes], returnType: DataType${if (args.length > 0) ", " + args else ""}): Column = {
      ScalaUdf(f, returnType, Seq($argsInUdf))
    }""")
  }
  }
  */
  /**
   * Defines a user-defined function of 0 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag](f: Function0[RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 1 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag](f: Function1[A1, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 2 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag](f: Function2[A1, A2, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 3 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](f: Function3[A1, A2, A3, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 4 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](f: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 5 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](f: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 6 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag](f: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 7 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag](f: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 8 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag](f: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 9 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag](f: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 10 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag](f: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Call a Scala function of 0 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def callUDF(f: Function0[_], returnType: DataType): Column = {
    ScalaUdf(f, returnType, Seq())
  }

  /**
   * Call a Scala function of 1 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def callUDF(f: Function1[_, _], returnType: DataType, arg1: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr))
  }

  /**
   * Call a Scala function of 2 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def callUDF(f: Function2[_, _, _], returnType: DataType, arg1: Column, arg2: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr))
  }

  /**
   * Call a Scala function of 3 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def callUDF(f: Function3[_, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr))
  }

  /**
   * Call a Scala function of 4 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def callUDF(f: Function4[_, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr))
  }

  /**
   * Call a Scala function of 5 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def callUDF(f: Function5[_, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr))
  }

  /**
   * Call a Scala function of 6 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def callUDF(f: Function6[_, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr))
  }

  /**
   * Call a Scala function of 7 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def callUDF(f: Function7[_, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr))
  }

  /**
   * Call a Scala function of 8 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def callUDF(f: Function8[_, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr))
  }

  /**
   * Call a Scala function of 9 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def callUDF(f: Function9[_, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr))
  }

  /**
   * Call a Scala function of 10 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def callUDF(f: Function10[_, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr))
  }

  // scalastyle:on

  /**
   * Call an user-defined function.
   * Example:
   * {{{
   *  import org.apache.spark.sql._
   *
   *  val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
   *  val sqlContext = df.sqlContext
   *  sqlContext.udf.register("simpleUdf", (v: Int) => v * v)
   *  df.select($"id", callUdf("simpleUdf", $"value"))
   * }}}
   *
   * @group udf_funcs
   * @since 1.4.0
   */
  def callUdf(udfName: String, cols: Column*): Column = {
     UnresolvedFunction(udfName, cols.map(_.expr))
  }

}
