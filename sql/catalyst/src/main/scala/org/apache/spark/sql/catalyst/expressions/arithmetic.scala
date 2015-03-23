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

case class UnaryMinus(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  override lazy val resolved = child.resolved &&
    (child.dataType.isInstanceOf[NumericType] || child.dataType.isInstanceOf[NullType])

  def dataType = child.dataType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"-$child"

  val numeric =
    if (resolved) {
      dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case n: NullType => UnresolvedNumeric
      }
    } else {
      UnresolvedNumeric
    }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      numeric.negate(evalE)
    }
  }
}

case class Sqrt(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  override lazy val resolved = child.resolved &&
    (child.dataType.isInstanceOf[NumericType] || child.dataType.isInstanceOf[NullType])

  def dataType = DoubleType
  override def foldable = child.foldable
  def nullable = true
  override def toString = s"SQRT($child)"

  val numeric =
    if (resolved) {
      child.dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case n: NullType => UnresolvedNumeric
      }
    } else {
      UnresolvedNumeric
    }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val value = numeric.toDouble(evalE)
      if (value < 0) null
      else math.sqrt(value)
    }
  }
}

abstract class BinaryArithmetic extends BinaryExpression {
  self: Product =>

  type EvaluatedType = Any

  def nullable = left.nullable || right.nullable

  override lazy val resolved =
    left.resolved && right.resolved &&
    left.dataType == right.dataType &&
    (left.dataType.isInstanceOf[NumericType] || left.dataType.isInstanceOf[NullType]) &&
    !DecimalType.isFixed(left.dataType)

  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to incompatible types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
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
        evalInternal(evalE1, evalE2)
      }
    }
  }

  def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any =
    sys.error(s"BinaryExpressions must either override eval or evalInternal")
}

case class Add(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "+"

  val numeric =
    if (resolved) {
      dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case n: NullType => UnresolvedNumeric
      }
    } else {
      UnresolvedNumeric
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
        numeric.plus(evalE1, evalE2)
      }
    }
  }
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "-"

  val numeric =
    if (resolved) {
      dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case n: NullType => UnresolvedNumeric
      }
    } else {
      UnresolvedNumeric
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
        numeric.minus(evalE1, evalE2)
      }
    }
  }
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "*"

  val numeric =
    if (resolved) {
      dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case n: NullType => UnresolvedNumeric
      }
    } else {
      UnresolvedNumeric
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
        numeric.times(evalE1, evalE2)
      }
    }
  }
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "/"

  override def nullable = true

  val div: (Any, Any) => Any =
    if (resolved) {
      dataType match {
        case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
        case it: IntegralType => it.integral.asInstanceOf[Integral[Any]].quot
        case NullType => UnresolvedIntegral.quot
      }
    } else {
      UnresolvedIntegral.quot
    }
  
  override def eval(input: Row): Any = {
    val evalE2 = right.eval(input)
    if (evalE2 == null || evalE2 == 0) {
      null
    } else {
      val evalE1 = left.eval(input)
      if (evalE1 == null) {
        null
      } else {
        div(evalE1, evalE2)
      }
    }
  }
}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "%"

  override def nullable = true

  val integral =
    if (resolved) {
      dataType match {
        case i: IntegralType => i.integral.asInstanceOf[Integral[Any]]
        case i: FractionalType => i.asIntegral.asInstanceOf[Integral[Any]]
        case NullType => UnresolvedIntegral
      }
    } else {
      UnresolvedIntegral
    }

  override def eval(input: Row): Any = {
    val evalE2 = right.eval(input)
    if (evalE2 == null || evalE2 == 0) {
      null
    } else {
      val evalE1 = left.eval(input)
      if (evalE1 == null) {
        null
      } else {
        integral.rem(evalE1, evalE2)
      }
    }
  }
}

/**
 * A function that calculates bitwise and(&) of two numbers.
 */
case class BitwiseAnd(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "&"

  override lazy val resolved =
    left.resolved && right.resolved &&
    left.dataType == right.dataType &&
    (left.dataType.isInstanceOf[IntegralType] || left.dataType.isInstanceOf[NullType]) &&
    !DecimalType.isFixed(left.dataType)

  val and: (Any, Any) => Any =
    if (resolved) {
      dataType match {
        case ByteType =>
          ((evalE1: Byte, evalE2: Byte) => (evalE1 & evalE2).toByte)
            .asInstanceOf[(Any, Any) => Any]
        case ShortType =>
          ((evalE1: Short, evalE2: Short) => (evalE1 & evalE2).toShort)
            .asInstanceOf[(Any, Any) => Any]
        case IntegerType =>
          ((evalE1: Int, evalE2: Int) => evalE1 & evalE2)
            .asInstanceOf[(Any, Any) => Any]
        case LongType =>
          ((evalE1: Long, evalE2: Long) => evalE1 & evalE2)
            .asInstanceOf[(Any, Any) => Any]
        case NullType => UnresolvedIntegral.bitwiseAnd
      }
    } else {
      UnresolvedIntegral.bitwiseAnd
    }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = and(evalE1, evalE2)
}

/**
 * A function that calculates bitwise or(|) of two numbers.
 */
case class BitwiseOr(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "|"

  override lazy val resolved =
    left.resolved && right.resolved &&
    left.dataType == right.dataType &&
    (left.dataType.isInstanceOf[IntegralType] || left.dataType.isInstanceOf[NullType]) &&
    !DecimalType.isFixed(left.dataType)

  val or: (Any, Any) => Any =
    if (resolved) {
      dataType match {
        case ByteType =>
          ((evalE1: Byte, evalE2: Byte) => (evalE1 | evalE2).toByte)
            .asInstanceOf[(Any, Any) => Any]
        case ShortType =>
          ((evalE1: Short, evalE2: Short) => (evalE1 | evalE2).toShort)
            .asInstanceOf[(Any, Any) => Any]
        case IntegerType =>
          ((evalE1: Int, evalE2: Int) => evalE1 | evalE2)
            .asInstanceOf[(Any, Any) => Any]
        case LongType =>
          ((evalE1: Long, evalE2: Long) => evalE1 | evalE2)
            .asInstanceOf[(Any, Any) => Any]
        case NullType => UnresolvedIntegral.bitwiseOr
      }
    } else {
      UnresolvedIntegral.bitwiseOr
    }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = or(evalE1, evalE2)
}

/**
 * A function that calculates bitwise xor(^) of two numbers.
 */
case class BitwiseXor(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "^"

  override lazy val resolved =
    left.resolved && right.resolved &&
    left.dataType == right.dataType &&
    (left.dataType.isInstanceOf[IntegralType] || left.dataType.isInstanceOf[NullType]) &&
    !DecimalType.isFixed(left.dataType)

  val xor: (Any, Any) => Any =
    if (resolved) {
      dataType match {
        case ByteType =>
          ((evalE1: Byte, evalE2: Byte) => (evalE1 ^ evalE2).toByte)
            .asInstanceOf[(Any, Any) => Any]
        case ShortType =>
          ((evalE1: Short, evalE2: Short) => (evalE1 ^ evalE2).toShort)
            .asInstanceOf[(Any, Any) => Any]
        case IntegerType =>
          ((evalE1: Int, evalE2: Int) => evalE1 ^ evalE2)
            .asInstanceOf[(Any, Any) => Any]
        case LongType =>
          ((evalE1: Long, evalE2: Long) => evalE1 ^ evalE2)
            .asInstanceOf[(Any, Any) => Any]
        case NullType => UnresolvedIntegral.bitwiseXor
      }
    } else {
      UnresolvedIntegral.bitwiseXor
    }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = xor(evalE1, evalE2)
}

/**
 * A function that calculates bitwise not(~) of a number.
 */
case class BitwiseNot(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  override lazy val resolved =
    child.resolved &&
    (child.dataType.isInstanceOf[IntegralType] || child.dataType.isInstanceOf[NullType])

  def dataType = child.dataType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"~$child"

  val not: (Any) => Any =
    if (resolved) {
      dataType match {
        case ByteType =>
          ((evalE: Byte) => (~evalE).toByte).asInstanceOf[(Any) => Any]
        case ShortType =>
          ((evalE: Short) => (~evalE).toShort).asInstanceOf[(Any) => Any]
        case IntegerType =>
          ((evalE: Int) => ~evalE).asInstanceOf[(Any) => Any]
        case LongType =>
          ((evalE: Long) => ~evalE).asInstanceOf[(Any) => Any]
        case NullType => UnresolvedIntegral.bitwiseNot
      }
    } else {
      UnresolvedIntegral.bitwiseNot
    }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      not(evalE)
    }
  }
}

case class MaxOf(left: Expression, right: Expression) extends Expression {
  type EvaluatedType = Any

  override def foldable = left.foldable && right.foldable

  override def nullable = left.nullable && right.nullable

  override def children = left :: right :: Nil

  override lazy val resolved =
    left.resolved && right.resolved &&
    left.dataType == right.dataType &&
    (left.dataType.isInstanceOf[NativeType] || left.dataType.isInstanceOf[NullType])

  override def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }

  val ordering =
    if (resolved) {
      dataType match {
        case n: NativeType => n.ordering.asInstanceOf[Ordering[Any]]
        case n: NullType => UnresolvedOrdering
      }
    } else {
      UnresolvedOrdering
    }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    val evalE2 = right.eval(input)
    if (evalE1 == null) {
      evalE2
    } else if (evalE2 == null) {
      evalE1
    } else {
      if (ordering.compare(evalE1, evalE2) < 0) {
        evalE2
      } else {
        evalE1
      }
    }
  }

  override def toString = s"MaxOf($left, $right)"
}

/**
 * A function that get the absolute value of the numeric value.
 */
case class Abs(child: Expression) extends UnaryExpression  {
  type EvaluatedType = Any

  override lazy val resolved = child.resolved &&
    (child.dataType.isInstanceOf[NumericType] || child.dataType.isInstanceOf[NullType])

  def dataType = child.dataType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"Abs($child)"

  val numeric =
    if (resolved) {
      dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case n: NullType => UnresolvedNumeric
      }
    } else {
      UnresolvedNumeric
    }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      numeric.abs(evalE)
    }
  }
}
