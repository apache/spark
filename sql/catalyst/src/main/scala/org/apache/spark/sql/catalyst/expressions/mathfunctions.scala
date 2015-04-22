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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.types._

abstract class MathematicalExpression(name: String) extends UnaryExpression with Serializable { 
  self: Product =>
  type EvaluatedType = Any

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = true
  override def toString: String = s"$name($child)"

  lazy val numeric = child.dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other => sys.error(s"Type $other does not support numeric operations")
  }
}

abstract class MathematicalExpressionForDouble(f: Double => Double, name: String)
  extends MathematicalExpression(name) { self: Product =>
  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val result = f(numeric.toDouble(evalE)) 
      if (result.isNaN) null
      else result
    }
  }
}

abstract class MathematicalExpressionForInt(f: Int => Int, name: String)
  extends MathematicalExpression(name) { self: Product =>
  override def dataType: DataType = IntegerType

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      f(numeric.toInt(evalE))
    }
  }
}

abstract class MathematicalExpressionForFloat(f: Float => Float, name: String)
  extends MathematicalExpression(name) { self: Product =>

  override def dataType: DataType = FloatType

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val result = f(numeric.toFloat(evalE))
      if (result.isNaN) null
      else result
    }
  }
}

abstract class MathematicalExpressionForLong(f: Long => Long, name: String)
  extends MathematicalExpression(name) { self: Product =>

  override def dataType: DataType = LongType

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      f(numeric.toLong(evalE))
    }
  }
}

case class Sin(child: Expression) extends MathematicalExpressionForDouble(math.sin, "SIN")

case class Asin(child: Expression) extends MathematicalExpressionForDouble(math.asin, "ASIN")

case class Sinh(child: Expression) extends MathematicalExpressionForDouble(math.sinh, "SINH")

case class Cos(child: Expression) extends MathematicalExpressionForDouble(math.cos, "COS")

case class Acos(child: Expression) extends MathematicalExpressionForDouble(math.acos, "ACOS")

case class Cosh(child: Expression) extends MathematicalExpressionForDouble(math.cosh, "COSH")

case class Tan(child: Expression) extends MathematicalExpressionForDouble(math.tan, "TAN")

case class Atan(child: Expression) extends MathematicalExpressionForDouble(math.atan, "ATAN")

case class Tanh(child: Expression) extends MathematicalExpressionForDouble(math.tanh, "TANH")

case class Ceil(child: Expression) extends MathematicalExpressionForDouble(math.ceil, "CEIL")

case class Floor(child: Expression) extends MathematicalExpressionForDouble(math.floor, "FLOOR")

case class Rint(child: Expression) extends MathematicalExpressionForDouble(math.rint, "ROUND")

case class Cbrt(child: Expression) extends MathematicalExpressionForDouble(math.cbrt, "CBRT")

case class Signum(child: Expression) extends MathematicalExpressionForDouble(math.signum, "SIGNUM")

case class ISignum(child: Expression) extends MathematicalExpressionForInt(math.signum, "ISIGNUM")

case class FSignum(child: Expression) extends MathematicalExpressionForFloat(math.signum, "FSIGNUM")

case class LSignum(child: Expression) extends MathematicalExpressionForLong(math.signum, "LSIGNUM")

case class ToDegrees(child: Expression) 
  extends MathematicalExpressionForDouble(math.toDegrees, "DEGREES")

case class ToRadians(child: Expression) 
  extends MathematicalExpressionForDouble(math.toRadians, "RADIANS")

case class Log(child: Expression) extends MathematicalExpressionForDouble(math.log, "LOG")

case class Log10(child: Expression) extends MathematicalExpressionForDouble(math.log10, "LOG10")

case class Log1p(child: Expression) extends MathematicalExpressionForDouble(math.log1p, "LOG1P")

case class Exp(child: Expression) extends MathematicalExpressionForDouble(math.exp, "EXP")

case class Expm1(child: Expression) extends MathematicalExpressionForDouble(math.expm1, "EXPM1")

abstract class BinaryMathExpression(f: (Double, Double) => Double, name: String) 
  extends BinaryFunctionExpression with Serializable { self: Product =>
  type EvaluatedType = Any

  def nullable: Boolean = left.nullable || right.nullable
  override def toString: String = s"$name($left, $right)"

  override lazy val resolved =
    left.resolved && right.resolved &&
      left.dataType == right.dataType &&
      !DecimalType.isFixed(left.dataType)

  def dataType: DataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }

  lazy val numeric = dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other => sys.error(s"Type $other does not support numeric operations")
  }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if (evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        val result = f(numeric.toDouble(evalE1), numeric.toDouble(evalE2)) 
        if (result.isNaN) null
        else result
      }
    }
  }
}

case class Pow(left: Expression, right: Expression) extends BinaryMathExpression(math.pow, "POWER")

case class Hypot(
    left: Expression,
    right: Expression) extends BinaryMathExpression(math.hypot, "HYPOT")

case class Atan2(
    left: Expression,
    right: Expression) extends BinaryMathExpression(math.atan2, "ATAN2")
