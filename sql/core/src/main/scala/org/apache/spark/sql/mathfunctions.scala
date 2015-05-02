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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.mathfuncs._
import org.apache.spark.sql.functions.lit

/**
 * :: Experimental ::
 * Mathematical Functions available for [[DataFrame]].
 */
@Experimental
// scalastyle:off
object mathfunctions {
// scalastyle:on

  private[this] implicit def toColumn(expr: Expression): Column = Column(expr)

  /**
   * Computes the cosine inverse of the given value; the returned angle is in the range
   * 0.0 through pi.
   */
  def acos(e: Column): Column = Acos(e.expr)

  /**
   * Computes the cosine inverse of the given column; the returned angle is in the range
   * 0.0 through pi.
   */
  def acos(columnName: String): Column = acos(Column(columnName))

  /**
   * Computes the sine inverse of the given value; the returned angle is in the range
   * -pi/2 through pi/2.
   */
  def asin(e: Column): Column = Asin(e.expr)

  /**
   * Computes the sine inverse of the given column; the returned angle is in the range
   * -pi/2 through pi/2.
   */
  def asin(columnName: String): Column = asin(Column(columnName))

  /**
   * Computes the tangent inverse of the given value.
   */
  def atan(e: Column): Column = Atan(e.expr)

  /**
   * Computes the tangent inverse of the given column.
   */
  def atan(columnName: String): Column = atan(Column(columnName))

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   */
  def atan2(l: Column, r: Column): Column = Atan2(l.expr, r.expr)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   */
  def atan2(l: Column, rightName: String): Column = atan2(l, Column(rightName))

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   */
  def atan2(leftName: String, r: Column): Column = atan2(Column(leftName), r)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   */
  def atan2(leftName: String, rightName: String): Column =
    atan2(Column(leftName), Column(rightName))

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   */
  def atan2(l: Column, r: Double): Column = atan2(l, lit(r).expr)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).=
   */
  def atan2(leftName: String, r: Double): Column = atan2(Column(leftName), r)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   */
  def atan2(l: Double, r: Column): Column = atan2(lit(l).expr, r)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   */
  def atan2(l: Double, rightName: String): Column = atan2(l, Column(rightName))

  /**
   * Computes the cube-root of the given value.
   */
  def cbrt(e: Column): Column = Cbrt(e.expr)

  /**
   * Computes the cube-root of the given column.
   */
  def cbrt(columnName: String): Column = cbrt(Column(columnName))

  /**
   * Computes the ceiling of the given value.
   */
  def ceil(e: Column): Column = Ceil(e.expr)

  /**
   * Computes the ceiling of the given column.
   */
  def ceil(columnName: String): Column = ceil(Column(columnName))

  /**
   * Computes the cosine of the given value.
   */
  def cos(e: Column): Column = Cos(e.expr)

  /**
   * Computes the cosine of the given column.
   */
  def cos(columnName: String): Column = cos(Column(columnName))

  /**
   * Computes the hyperbolic cosine of the given value.
   */
  def cosh(e: Column): Column = Cosh(e.expr)

  /**
   * Computes the hyperbolic cosine of the given column.
   */
  def cosh(columnName: String): Column = cosh(Column(columnName))

  /**
   * Computes the exponential of the given value.
   */
  def exp(e: Column): Column = Exp(e.expr)

  /**
   * Computes the exponential of the given column.
   */
  def exp(columnName: String): Column = exp(Column(columnName))

  /**
   * Computes the exponential of the given value minus one.
   */
  def expm1(e: Column): Column = Expm1(e.expr)

  /**
   * Computes the exponential of the given column.
   */
  def expm1(columnName: String): Column = expm1(Column(columnName))

  /**
   * Computes the floor of the given value.
   */
  def floor(e: Column): Column = Floor(e.expr)

  /**
   * Computes the floor of the given column.
   */
  def floor(columnName: String): Column = floor(Column(columnName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   */
  def hypot(l: Column, r: Column): Column = Hypot(l.expr, r.expr)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   */
  def hypot(l: Column, rightName: String): Column = hypot(l, Column(rightName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   */
  def hypot(leftName: String, r: Column): Column = hypot(Column(leftName), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   */
  def hypot(leftName: String, rightName: String): Column =
    hypot(Column(leftName), Column(rightName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   */
  def hypot(l: Column, r: Double): Column = hypot(l, lit(r).expr)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   */
  def hypot(leftName: String, r: Double): Column = hypot(Column(leftName), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   */
  def hypot(l: Double, r: Column): Column = hypot(lit(l).expr, r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   */
  def hypot(l: Double, rightName: String): Column = hypot(l, Column(rightName))

  /**
   * Computes the natural logarithm of the given value.
   */
  def log(e: Column): Column = Log(e.expr)

  /**
   * Computes the natural logarithm of the given column.
   */
  def log(columnName: String): Column = log(Column(columnName))

  /**
   * Computes the logarithm of the given value in Base 10.
   */
  def log10(e: Column): Column = Log10(e.expr)

  /**
   * Computes the logarithm of the given value in Base 10.
   */
  def log10(columnName: String): Column = log10(Column(columnName))

  /**
   * Computes the natural logarithm of the given value plus one.
   */
  def log1p(e: Column): Column = Log1p(e.expr)

  /**
   * Computes the natural logarithm of the given column plus one.
   */
  def log1p(columnName: String): Column = log1p(Column(columnName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   */
  def pow(l: Column, r: Column): Column = Pow(l.expr, r.expr)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   */
  def pow(l: Column, rightName: String): Column = pow(l, Column(rightName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   */
  def pow(leftName: String, r: Column): Column = pow(Column(leftName), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   */
  def pow(leftName: String, rightName: String): Column = pow(Column(leftName), Column(rightName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   */
  def pow(l: Column, r: Double): Column = pow(l, lit(r).expr)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   */
  def pow(leftName: String, r: Double): Column = pow(Column(leftName), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   */
  def pow(l: Double, r: Column): Column = pow(lit(l).expr, r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   */
  def pow(l: Double, rightName: String): Column = pow(l, Column(rightName))

  /**
   * Returns the double value that is closest in value to the argument and
   * is equal to a mathematical integer.
   */
  def rint(e: Column): Column = Rint(e.expr)

  /**
   * Returns the double value that is closest in value to the argument and
   * is equal to a mathematical integer.
   */
  def rint(columnName: String): Column = rint(Column(columnName))

  /**
   * Computes the signum of the given value.
   */
  def signum(e: Column): Column = Signum(e.expr)

  /**
   * Computes the signum of the given column.
   */
  def signum(columnName: String): Column = signum(Column(columnName))

  /**
   * Computes the sine of the given value.
   */
  def sin(e: Column): Column = Sin(e.expr)

  /**
   * Computes the sine of the given column.
   */
  def sin(columnName: String): Column = sin(Column(columnName))

  /**
   * Computes the hyperbolic sine of the given value.
   */
  def sinh(e: Column): Column = Sinh(e.expr)

  /**
   * Computes the hyperbolic sine of the given column.
   */
  def sinh(columnName: String): Column = sinh(Column(columnName))
  
  /**
   * Computes the tangent of the given value.
   */
  def tan(e: Column): Column = Tan(e.expr)

  /**
   * Computes the tangent of the given column.
   */
  def tan(columnName: String): Column = tan(Column(columnName))
  
  /**
   * Computes the hyperbolic tangent of the given value.
   */
  def tanh(e: Column): Column = Tanh(e.expr)

  /**
   * Computes the hyperbolic tangent of the given column.
   */
  def tanh(columnName: String): Column = tanh(Column(columnName))

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   */
  def toDeg(e: Column): Column = ToDegrees(e.expr)

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   */
  def toDeg(columnName: String): Column = toDeg(Column(columnName))

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   */
  def toRad(e: Column): Column = ToRadians(e.expr)

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   */
  def toRad(columnName: String): Column = toRad(Column(columnName))
}
