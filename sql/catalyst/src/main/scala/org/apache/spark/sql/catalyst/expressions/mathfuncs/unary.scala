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
abstract class MathematicalExpression(f: Double => Double, name: String)
  extends UnaryExpression with Serializable with ExpectsInputTypes {
  self: Product =>
  type EvaluatedType = Any

  override def expectedChildTypes: Seq[DataType] = Seq(DoubleType)
  override def dataType: DataType = DoubleType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = true
  override def toString: String = s"$name($child)"

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

case class Acos(child: Expression) extends MathematicalExpression(math.acos, "ACOS")

case class Asin(child: Expression) extends MathematicalExpression(math.asin, "ASIN")

case class Atan(child: Expression) extends MathematicalExpression(math.atan, "ATAN")

case class Cbrt(child: Expression) extends MathematicalExpression(math.cbrt, "CBRT")

case class Ceil(child: Expression) extends MathematicalExpression(math.ceil, "CEIL")

case class Cos(child: Expression) extends MathematicalExpression(math.cos, "COS")

case class Cosh(child: Expression) extends MathematicalExpression(math.cosh, "COSH")

case class Exp(child: Expression) extends MathematicalExpression(math.exp, "EXP")

case class Expm1(child: Expression) extends MathematicalExpression(math.expm1, "EXPM1")

case class Floor(child: Expression) extends MathematicalExpression(math.floor, "FLOOR")

case class Log(child: Expression) extends MathematicalExpression(math.log, "LOG")

case class Log10(child: Expression) extends MathematicalExpression(math.log10, "LOG10")

case class Log1p(child: Expression) extends MathematicalExpression(math.log1p, "LOG1P")

case class Rint(child: Expression) extends MathematicalExpression(math.rint, "ROUND")

case class Signum(child: Expression) extends MathematicalExpression(math.signum, "SIGNUM")

case class Sin(child: Expression) extends MathematicalExpression(math.sin, "SIN")

case class Sinh(child: Expression) extends MathematicalExpression(math.sinh, "SINH")

case class Tan(child: Expression) extends MathematicalExpression(math.tan, "TAN")

case class Tanh(child: Expression) extends MathematicalExpression(math.tanh, "TANH")

case class ToDegrees(child: Expression) 
  extends MathematicalExpression(math.toDegrees, "DEGREES")

case class ToRadians(child: Expression) 
  extends MathematicalExpression(math.toRadians, "RADIANS")
