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
import org.apache.spark.sql.catalyst.types._

case class UnaryMinus(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  def dataType = child.dataType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"-$child"

  override def eval(input: Row): Any = {
    n1(child, input, _.negate(_))
  }
}

case class Sqrt(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any
  
  def dataType = DoubleType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"SQRT($child)"

  override def eval(input: Row): Any = {
    n1(child, input, ((na,a) => math.sqrt(na.toDouble(a))))
  }
}

abstract class BinaryArithmetic extends BinaryExpression {
  self: Product =>

  type EvaluatedType = Any

  def nullable = left.nullable || right.nullable

  override lazy val resolved =
    left.resolved && right.resolved && left.dataType == right.dataType

  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }
}

case class Add(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "+"

  override def eval(input: Row): Any = n2(input, left, right, _.plus(_, _))
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "-"

  override def eval(input: Row): Any = n2(input, left, right, _.minus(_, _))
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "*"

  override def eval(input: Row): Any = n2(input, left, right, _.times(_, _))
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "/"

  override def eval(input: Row): Any = dataType match {
    case _: FractionalType => f2(input, left, right, _.div(_, _))
    case _: IntegralType => i2(input, left , right, _.quot(_, _))
  }

}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "%"

  override def eval(input: Row): Any = i2(input, left, right, _.rem(_, _))
}

case class MaxOf(left: Expression, right: Expression) extends Expression {
  type EvaluatedType = Any

  override def foldable = left.foldable && right.foldable

  override def nullable = left.nullable && right.nullable

  override def children = left :: right :: Nil

  override def dataType = left.dataType

  override def eval(input: Row): Any = {
    val leftEval = left.eval(input)
    val rightEval = right.eval(input)
    if (leftEval == null) {
      rightEval
    } else if (rightEval == null) {
      leftEval
    } else {
      val numeric = left.dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
      if (numeric.compare(leftEval, rightEval) < 0) {
        rightEval
      } else {
        leftEval
      }
    }
  }

  override def toString = s"MaxOf($left, $right)"
}
