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

import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

abstract class UnaryArithmetic extends UnaryExpression {
  self: Product =>

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = child.dataType

  override def eval(input: catalyst.InternalRow): Any = {
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType => defineCodeGen(ctx, ev, c => s"c.unary_$$minus()")
    case dt: NumericType => defineCodeGen(ctx, ev, c => s"-($c)")
  }

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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    eval.code + s"""
      boolean ${ev.isNull} = ${eval.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        if (${eval.primitive} < 0.0) {
          ${ev.isNull} = true;
        } else {
          ${ev.primitive} = java.lang.Math.sqrt(${eval.primitive});
        }
      }
    """
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

  /** Name of the function for this expression on a [[Decimal]] type. */
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

  override def eval(input: catalyst.InternalRow): Any = {
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$decimalMethod($eval2)")
    // byte and short are casted into int when add, minus, times or divide
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev, (eval1, eval2) =>
        s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }

  protected def evalInternal(evalE1: Any, evalE2: Any): Any =
    sys.error(s"BinaryArithmetics must override either eval or evalInternal")
}

private[sql] object BinaryArithmetic {
  def unapply(e: BinaryArithmetic): Option[(Expression, Expression)] = Some((e.left, e.right))
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

  override def eval(input: catalyst.InternalRow): Any = {
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

  /**
   * Special case handling due to division by 0 => null.
   */
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val test = if (left.dataType.isInstanceOf[DecimalType]) {
      s"${eval2.primitive}.isZero()"
    } else {
      s"${eval2.primitive} == 0"
    }
    val method = if (left.dataType.isInstanceOf[DecimalType]) {
      s".$decimalMethod"
    } else {
      s"$symbol"
    }
    eval1.code + eval2.code +
      s"""
      boolean ${ev.isNull} = false;
      ${ctx.javaType(left.dataType)} ${ev.primitive} = ${ctx.defaultValue(left.dataType)};
      if (${eval1.isNull} || ${eval2.isNull} || $test) {
        ${ev.isNull} = true;
      } else {
        ${ev.primitive} = ${eval1.primitive}$method(${eval2.primitive});
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

  override def eval(input: catalyst.InternalRow): Any = {
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

  /**
   * Special case handling for x % 0 ==> null.
   */
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val test = if (left.dataType.isInstanceOf[DecimalType]) {
      s"${eval2.primitive}.isZero()"
    } else {
      s"${eval2.primitive} == 0"
    }
    val method = if (left.dataType.isInstanceOf[DecimalType]) {
      s".$decimalMethod"
    } else {
      s"$symbol"
    }
    eval1.code + eval2.code +
      s"""
      boolean ${ev.isNull} = false;
      ${ctx.javaType(left.dataType)} ${ev.primitive} = ${ctx.defaultValue(left.dataType)};
      if (${eval1.isNull} || ${eval2.isNull} || $test) {
        ${ev.isNull} = true;
      } else {
        ${ev.primitive} = ${eval1.primitive}$method(${eval2.primitive});
      }
      """
  }
}

case class MaxOf(left: Expression, right: Expression) extends BinaryArithmetic {
  override def nullable: Boolean = left.nullable && right.nullable

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForOrderingExpr(t, "function maxOf")

  private lazy val ordering = TypeUtils.getOrdering(dataType)

  override def eval(input: catalyst.InternalRow): Any = {
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (ctx.isNativeType(left.dataType)) {
      val eval1 = left.gen(ctx)
      val eval2 = right.gen(ctx)
      eval1.code + eval2.code + s"""
        boolean ${ev.isNull} = false;
        ${ctx.javaType(left.dataType)} ${ev.primitive} =
          ${ctx.defaultValue(left.dataType)};

        if (${eval1.isNull}) {
          ${ev.isNull} = ${eval2.isNull};
          ${ev.primitive} = ${eval2.primitive};
        } else if (${eval2.isNull}) {
          ${ev.isNull} = ${eval1.isNull};
          ${ev.primitive} = ${eval1.primitive};
        } else {
          if (${eval1.primitive} > ${eval2.primitive}) {
            ${ev.primitive} = ${eval1.primitive};
          } else {
            ${ev.primitive} = ${eval2.primitive};
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

  override def eval(input: catalyst.InternalRow): Any = {
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (ctx.isNativeType(left.dataType)) {

      val eval1 = left.gen(ctx)
      val eval2 = right.gen(ctx)

      eval1.code + eval2.code + s"""
        boolean ${ev.isNull} = false;
        ${ctx.javaType(left.dataType)} ${ev.primitive} =
          ${ctx.defaultValue(left.dataType)};

        if (${eval1.isNull}) {
          ${ev.isNull} = ${eval2.isNull};
          ${ev.primitive} = ${eval2.primitive};
        } else if (${eval2.isNull}) {
          ${ev.isNull} = ${eval1.isNull};
          ${ev.primitive} = ${eval1.primitive};
        } else {
          if (${eval1.primitive} < ${eval2.primitive}) {
            ${ev.primitive} = ${eval1.primitive};
          } else {
            ${ev.primitive} = ${eval2.primitive};
          }
        }
      """
    } else {
      super.genCode(ctx, ev)
    }
  }

  override def toString: String = s"MinOf($left, $right)"
}
