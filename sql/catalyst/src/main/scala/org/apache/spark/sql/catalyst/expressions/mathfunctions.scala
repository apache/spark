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

trait MathematicalExpression extends UnaryExpression with Serializable { self: Product =>
  type EvaluatedType = Any

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = true

  lazy val numeric = child.dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other => sys.error(s"Type $other does not support numeric operations")
  }
}

abstract class MathematicalExpressionForDouble(f: Double => Double)
  extends MathematicalExpression { self: Product =>

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      f(numeric.toDouble(evalE))
    }
  }
}

abstract class MathematicalExpressionForInt(f: Int => Int)
  extends MathematicalExpression { self: Product =>

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

abstract class MathematicalExpressionForFloat(f: Float => Float)
  extends MathematicalExpression { self: Product =>

  override def dataType: DataType = FloatType

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      f(numeric.toFloat(evalE))
    }
  }
}

abstract class MathematicalExpressionForLong(f: Long => Long)
  extends MathematicalExpression { self: Product =>

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

case class Sin(child: Expression) extends MathematicalExpressionForDouble(math.sin) {
  override def toString: String = s"SIN($child)"
}

case class Asin(child: Expression) extends MathematicalExpressionForDouble(math.asin) {
  override def toString: String = s"ASIN($child)"
}

case class Sinh(child: Expression) extends MathematicalExpressionForDouble(math.sinh) {
  override def toString: String = s"SINH($child)"
}

case class Cos(child: Expression) extends MathematicalExpressionForDouble(math.cos) {
  override def toString: String = s"COS($child)"
}

case class Acos(child: Expression) extends MathematicalExpressionForDouble(math.acos) {
  override def toString: String = s"ACOS($child)"
}

case class Cosh(child: Expression) extends MathematicalExpressionForDouble(math.cosh) {
  override def toString: String = s"COSH($child)"
}

case class Tan(child: Expression) extends MathematicalExpressionForDouble(math.tan) {
  override def toString: String = s"TAN($child)"
}

case class Atan(child: Expression) extends MathematicalExpressionForDouble(math.atan) {
  override def toString: String = s"ATAN($child)"
}

case class Tanh(child: Expression) extends MathematicalExpressionForDouble(math.tanh) {
  override def toString: String = s"TANH($child)"
}

case class Ceil(child: Expression) extends MathematicalExpressionForDouble(math.ceil) {
  override def toString: String = s"CEIL($child)"
}

case class Floor(child: Expression) extends MathematicalExpressionForDouble(math.floor) {
  override def toString: String = s"FLOOR($child)"
}

case class Rint(child: Expression) extends MathematicalExpressionForDouble(math.rint) {
  override def toString: String = s"RINT($child)"
}

case class Cbrt(child: Expression) extends MathematicalExpressionForDouble(math.cbrt) {
  override def toString: String = s"CBRT($child)"
}

case class Signum(child: Expression) extends MathematicalExpressionForDouble(math.signum) {
  override def toString: String = s"SIGNUM($child)"
}

case class ISignum(child: Expression) extends MathematicalExpressionForInt(math.signum) {
  override def toString: String = s"ISIGNUM($child)"
}

case class FSignum(child: Expression) extends MathematicalExpressionForFloat(math.signum) {
  override def toString: String = s"FSIGNUM($child)"
}

case class LSignum(child: Expression) extends MathematicalExpressionForLong(math.signum) {
  override def toString: String = s"LSIGNUM($child)"
}

case class ToDegrees(child: Expression) extends MathematicalExpressionForDouble(math.toDegrees) {
  override def toString: String = s"TODEG($child)"
}

case class ToRadians(child: Expression) extends MathematicalExpressionForDouble(math.toRadians) {
  override def toString: String = s"TORAD($child)"
}

case class Log(child: Expression) extends MathematicalExpressionForDouble(math.log) {
  override def toString: String = s"LOG($child)"

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val value = numeric.toDouble(evalE)
      if (value < 0) null
      else math.log(value)
    }
  }
}

case class Log10(child: Expression) extends MathematicalExpressionForDouble(math.log10) {
  override def toString: String = s"LOG10($child)"

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val value = numeric.toDouble(evalE)
      if (value < 0) null
      else math.log10(value)
    }
  }
}

case class Log1p(child: Expression) extends MathematicalExpressionForDouble(math.log1p) {
  override def toString: String = s"LOG1P($child)"

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val value = numeric.toDouble(evalE)
      if (value < -1) null
      else math.log1p(value)
    }
  }
}

case class Exp(child: Expression) extends MathematicalExpressionForDouble(math.exp) {
  override def toString: String = s"EXP($child)"
}

case class Expm1(child: Expression) extends MathematicalExpressionForDouble(math.expm1) {
  override def toString: String = s"EXPM1($child)"
}

abstract class BinaryMathExpression(f: (Double, Double) => Double) 
  extends BinaryFunctionExpression with Serializable { self: Product =>
  type EvaluatedType = Any

  def nullable: Boolean = left.nullable || right.nullable

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
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        f(numeric.toDouble(evalE1), numeric.toDouble(evalE2))
      }
    }
  }
}

case class Pow(left: Expression, right: Expression) extends BinaryMathExpression(math.pow) {
  override def toString: String = s"POW($left, $right)"
}

case class Hypot(left: Expression, right: Expression) extends BinaryMathExpression(math.hypot) {
  override def toString: String = s"HYPOT($left, $right)"
}

case class Atan2(left: Expression, right: Expression) extends BinaryMathExpression(math.atan2) {
  override def toString: String = s"ATAN2($left, $right)"
}
