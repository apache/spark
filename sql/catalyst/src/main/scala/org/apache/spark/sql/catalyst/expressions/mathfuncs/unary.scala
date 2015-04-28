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

package org.apache.spark.sql.catalyst.expressions.mathfuncs

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, Row, UnaryExpression}
import org.apache.spark.sql.types._

/**
 * A unary expression specifically for math functions. Math Functions expect a specific type of
 * input format, therefore these functions extend `ExpectsInputTypes`.
 * @param name The short name of the function
 */
abstract class MathematicalExpression(name: String)
  extends UnaryExpression with Serializable with ExpectsInputTypes {
  self: Product =>
  type EvaluatedType = Any

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = true
  override def toString: String = s"$name($child)"
}

/**
 * A unary expression specifically for math functions that take a `Double` as input and return
 * a `Double`.
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class MathematicalExpressionForDouble(f: Double => Double, name: String)
  extends MathematicalExpression(name) { self: Product =>
  
  override def expectedChildTypes: Seq[DataType] = Seq(DoubleType)

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val result = f(evalE.asInstanceOf[Double])
      if (result.isNaN) null else result
    }
  }
}

/**
 * A unary expression specifically for math functions that take an `Int` as input and return
 * an `Int`.
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class MathematicalExpressionForInt(f: Int => Int, name: String)
  extends MathematicalExpression(name) { self: Product =>

  override def dataType: DataType = IntegerType
  override def expectedChildTypes: Seq[DataType] = Seq(IntegerType)

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) null else f(evalE.asInstanceOf[Int])
  }
}

/**
 * A unary expression specifically for math functions that take a `Float` as input and return
 * a `Float`.
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class MathematicalExpressionForFloat(f: Float => Float, name: String)
  extends MathematicalExpression(name) { self: Product =>

  override def dataType: DataType = FloatType
  override def expectedChildTypes: Seq[DataType] = Seq(FloatType)

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val result = f(evalE.asInstanceOf[Float])
      if (result.isNaN) null else result
    }
  }
}

/**
 * A unary expression specifically for math functions that take a `Long` as input and return
 * a `Long`.
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class MathematicalExpressionForLong(f: Long => Long, name: String)
  extends MathematicalExpression(name) { self: Product =>

  override def dataType: DataType = LongType
  override def expectedChildTypes: Seq[DataType] = Seq(LongType)

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) null else f(evalE.asInstanceOf[Long])
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
