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

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, BinaryExpression, Expression, Row}
import org.apache.spark.sql.types._

/**
 * A binary expression specifically for math functions that take two `Double`s as input and returns
 * a `Double`.
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class BinaryMathExpression(f: (Double, Double) => Double, name: String) 
  extends BinaryExpression with Serializable with ExpectsInputTypes { self: Product =>
  type EvaluatedType = Any
  override def symbol: String = null
  override def expectedChildTypes: Seq[DataType] = Seq(DoubleType, DoubleType)

  override def nullable: Boolean = left.nullable || right.nullable
  override def toString: String = s"$name($left, $right)"

  override lazy val resolved =
    left.resolved && right.resolved &&
      left.dataType == right.dataType &&
      !DecimalType.isFixed(left.dataType)

  override def dataType: DataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
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
        val result = f(evalE1.asInstanceOf[Double], evalE2.asInstanceOf[Double])
        if (result.isNaN) null else result
      }
    }
  }
}

case class Atan2(
    left: Expression,
    right: Expression) extends BinaryMathExpression(math.atan2, "ATAN2") {
  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if (evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        // With codegen, the values returned by -0.0 and 0.0 are different. Handled with +0.0
        val result = math.atan2(evalE1.asInstanceOf[Double] + 0.0,
          evalE2.asInstanceOf[Double] + 0.0)
        if (result.isNaN) null else result
      }
    }
  }
}

case class Hypot(
    left: Expression,
    right: Expression) extends BinaryMathExpression(math.hypot, "HYPOT")

case class Pow(left: Expression, right: Expression) extends BinaryMathExpression(math.pow, "POWER")
