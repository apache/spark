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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{Code, GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

abstract class UnaryArithmetic extends UnaryExpression {
  self: Product =>

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = child.dataType

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      evalInternal(evalE)
    }
  }

  protected def evalInternal(evalE: Any): Any =
    sys.error(s"UnaryArithmetics must override either eval or evalInternal")
}

case class UnaryMinus(child: Expression) extends UnaryArithmetic {
  override def toString: String = s"-$child"

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "operator -")

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def evalInternal(evalE: Any) = numeric.negate(evalE)
}

case class Sqrt(child: Expression) extends UnaryArithmetic {
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  override def toString: String = s"SQRT($child)"

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sqrt")

  private lazy val numeric = TypeUtils.getNumeric(child.dataType)

  protected override def evalInternal(evalE: Any) = {
    val value = numeric.toDouble(evalE)
    if (value < 0) null
    else math.sqrt(value)
  }
}

/**
 * A function that get the absolute value of the numeric value.
 */
case class Abs(child: Expression) extends UnaryArithmetic {
  override def toString: String = s"Abs($child)"

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function abs")

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def evalInternal(evalE: Any) = numeric.abs(evalE)
}

abstract class BinaryArithmetic extends BinaryExpression {
  self: Product =>

  def decimalMethod: String = ""

  override def dataType: DataType = left.dataType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (left.dataType != right.dataType) {
      TypeCheckResult.TypeCheckFailure(
        s"differing types in ${this.getClass.getSimpleName} " +
        s"(${left.dataType} and ${right.dataType}).")
    } else {
      checkTypesInternal(dataType)
    }
  }

  protected def checkTypesInternal(t: DataType): TypeCheckResult

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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    if (left.dataType.isInstanceOf[DecimalType]) {
      evaluate(ctx, ev, { case (eval1, eval2) => s"$eval1.$decimalMethod($eval2)" } )
    } else {
      evaluate(ctx, ev, { case (eval1, eval2) => s"$eval1 $symbol $eval2" } )
    }
  }

  protected def evalInternal(evalE1: Any, evalE2: Any): Any =
    sys.error(s"BinaryArithmetics must override either eval or evalInternal")
}

case class Add(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "+"
  override def decimalMethod: String = "$plus"

  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !DecimalType.isFixed(dataType)

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "operator " + symbol)

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def evalInternal(evalE1: Any, evalE2: Any) = numeric.plus(evalE1, evalE2)
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "-"
  override def decimalMethod: String = "$minus"

  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !DecimalType.isFixed(dataType)

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "operator " + symbol)

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def evalInternal(evalE1: Any, evalE2: Any) = numeric.minus(evalE1, evalE2)
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "*"
  override def decimalMethod: String = "$times"

  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !DecimalType.isFixed(dataType)

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "operator " + symbol)

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def evalInternal(evalE1: Any, evalE2: Any) = numeric.times(evalE1, evalE2)
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "/"
  override def decimalMethod: String = "$divide"

  override def nullable: Boolean = true

  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !DecimalType.isFixed(dataType)

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "operator " + symbol)

  private lazy val div: (Any, Any) => Any = dataType match {
    case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
    case it: IntegralType => it.integral.asInstanceOf[Integral[Any]].quot
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val test = if (left.dataType.isInstanceOf[DecimalType]) {
      s"${eval2.primitiveTerm}.isZero()"
    } else {
      s"${eval2.primitiveTerm} == 0"
    }
    val method = if (left.dataType.isInstanceOf[DecimalType]) {
      s".$decimalMethod"
    } else {
      s"$symbol"
    }
    eval1.code + eval2.code +
      s"""
      boolean ${ev.nullTerm} = false;
      ${ctx.primitiveType(left.dataType)} ${ev.primitiveTerm} =
        ${ctx.defaultValue(left.dataType)};
      if (${eval1.nullTerm} || ${eval2.nullTerm} || $test) {
        ${ev.nullTerm} = true;
      } else {
        ${ev.primitiveTerm} = ${eval1.primitiveTerm}$method(${eval2.primitiveTerm});
      }
      """
  }
}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "%"
  override def decimalMethod: String = "reminder"

  override def nullable: Boolean = true

  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !DecimalType.isFixed(dataType)

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "operator " + symbol)

  private lazy val integral = dataType match {
    case i: IntegralType => i.integral.asInstanceOf[Integral[Any]]
    case i: FractionalType => i.asIntegral.asInstanceOf[Integral[Any]]
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val test = if (left.dataType.isInstanceOf[DecimalType]) {
      s"${eval2.primitiveTerm}.isZero()"
    } else {
      s"${eval2.primitiveTerm} == 0"
    }
    val method = if (left.dataType.isInstanceOf[DecimalType]) {
      s".$decimalMethod"
    } else {
      s"$symbol"
    }
    eval1.code + eval2.code +
      s"""
      boolean ${ev.nullTerm} = false;
      ${ctx.primitiveType(left.dataType)} ${ev.primitiveTerm} =
        ${ctx.defaultValue(left.dataType)};
      if (${eval1.nullTerm} || ${eval2.nullTerm} || $test) {
        ${ev.nullTerm} = true;
      } else {
        ${ev.primitiveTerm} = ${eval1.primitiveTerm}$method(${eval2.primitiveTerm});
      }
      """
  }
}

/**
 * A function that calculates bitwise and(&) of two numbers.
 */
case class BitwiseAnd(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "&"

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForBitwiseExpr(t, "operator " + symbol)

  private lazy val and: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 & evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 & evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
  }

  protected override def evalInternal(evalE1: Any, evalE2: Any) = and(evalE1, evalE2)
}

/**
 * A function that calculates bitwise or(|) of two numbers.
 */
case class BitwiseOr(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "|"

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForBitwiseExpr(t, "operator " + symbol)

  private lazy val or: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 | evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 | evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
  }

  protected override def evalInternal(evalE1: Any, evalE2: Any) = or(evalE1, evalE2)
}

/**
 * A function that calculates bitwise xor(^) of two numbers.
 */
case class BitwiseXor(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "^"

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForBitwiseExpr(t, "operator " + symbol)

  private lazy val xor: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 ^ evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 ^ evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
  }

  protected override def evalInternal(evalE1: Any, evalE2: Any): Any = xor(evalE1, evalE2)
}

/**
 * A function that calculates bitwise not(~) of a number.
 */
case class BitwiseNot(child: Expression) extends UnaryArithmetic {
  override def toString: String = s"~$child"

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForBitwiseExpr(child.dataType, "operator ~")

  private lazy val not: (Any) => Any = dataType match {
    case ByteType =>
      ((evalE: Byte) => (~evalE).toByte).asInstanceOf[(Any) => Any]
    case ShortType =>
      ((evalE: Short) => (~evalE).toShort).asInstanceOf[(Any) => Any]
    case IntegerType =>
      ((evalE: Int) => ~evalE).asInstanceOf[(Any) => Any]
    case LongType =>
      ((evalE: Long) => ~evalE).asInstanceOf[(Any) => Any]
  }

  protected override def evalInternal(evalE: Any) = not(evalE)
}

case class MaxOf(left: Expression, right: Expression) extends BinaryArithmetic {
  override def nullable: Boolean = left.nullable && right.nullable

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForOrderingExpr(t, "function maxOf")

  private lazy val ordering = TypeUtils.getOrdering(dataType)

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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    if (ctx.isNativeType(left.dataType)) {
      val eval1 = left.gen(ctx)
      val eval2 = right.gen(ctx)
      eval1.code + eval2.code + s"""
        boolean ${ev.nullTerm} = false;
        ${ctx.primitiveType(left.dataType)} ${ev.primitiveTerm} =
          ${ctx.defaultValue(left.dataType)};

        if (${eval1.nullTerm}) {
          ${ev.nullTerm} = ${eval2.nullTerm};
          ${ev.primitiveTerm} = ${eval2.primitiveTerm};
        } else if (${eval2.nullTerm}) {
          ${ev.nullTerm} = ${eval1.nullTerm};
          ${ev.primitiveTerm} = ${eval1.primitiveTerm};
        } else {
          if (${eval1.primitiveTerm} > ${eval2.primitiveTerm}) {
            ${ev.primitiveTerm} = ${eval1.primitiveTerm};
          } else {
            ${ev.primitiveTerm} = ${eval2.primitiveTerm};
          }
        }
      """
    } else {
      super.genCode(ctx, ev)
    }
  }
  override def toString: String = s"MaxOf($left, $right)"
}

case class MinOf(left: Expression, right: Expression) extends BinaryArithmetic {
  override def nullable: Boolean = left.nullable && right.nullable

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForOrderingExpr(t, "function minOf")

  private lazy val ordering = TypeUtils.getOrdering(dataType)

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    val evalE2 = right.eval(input)
    if (evalE1 == null) {
      evalE2
    } else if (evalE2 == null) {
      evalE1
    } else {
      if (ordering.compare(evalE1, evalE2) < 0) {
        evalE1
      } else {
        evalE2
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    if (ctx.isNativeType(left.dataType)) {

      val eval1 = left.gen(ctx)
      val eval2 = right.gen(ctx)

      eval1.code + eval2.code + s"""
        boolean ${ev.nullTerm} = false;
        ${ctx.primitiveType(left.dataType)} ${ev.primitiveTerm} =
          ${ctx.defaultValue(left.dataType)};

        if (${eval1.nullTerm}) {
          ${ev.nullTerm} = ${eval2.nullTerm};
          ${ev.primitiveTerm} = ${eval2.primitiveTerm};
        } else if (${eval2.nullTerm}) {
          ${ev.nullTerm} = ${eval1.nullTerm};
          ${ev.primitiveTerm} = ${eval1.primitiveTerm};
        } else {
          if (${eval1.primitiveTerm} < ${eval2.primitiveTerm}) {
            ${ev.primitiveTerm} = ${eval1.primitiveTerm};
          } else {
            ${ev.primitiveTerm} = ${eval2.primitiveTerm};
          }
        }
      """
    } else {
      super.genCode(ctx, ev)
    }
  }

  override def toString: String = s"MinOf($left, $right)"
}
