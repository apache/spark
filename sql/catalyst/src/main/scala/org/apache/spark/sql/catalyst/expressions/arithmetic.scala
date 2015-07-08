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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

abstract class UnaryArithmetic extends UnaryExpression {
  self: Product =>

  override def dataType: DataType = child.dataType
}

case class UnaryMinus(child: Expression) extends UnaryArithmetic {
  override def toString: String = s"-$child"

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "operator -")

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType => defineCodeGen(ctx, ev, c => s"$c.unary_$$minus()")
    case dt: NumericType => defineCodeGen(ctx, ev, c => s"(${ctx.javaType(dt)})(-($c))")
  }

  protected override def nullSafeEval(input: Any): Any = numeric.negate(input)
}

case class UnaryPositive(child: Expression) extends UnaryArithmetic {
  override def prettyName: String = "positive"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
    defineCodeGen(ctx, ev, c => c)

  protected override def nullSafeEval(input: Any): Any = input
}

/**
 * A function that get the absolute value of the numeric value.
 */
case class Abs(child: Expression) extends UnaryArithmetic {
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function abs")

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input: Any): Any = numeric.abs(input)
}

abstract class BinaryArithmetic extends BinaryOperator {
  self: Product =>

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

  /** Name of the function for this expression on a [[Decimal]] type. */
  def decimalMethod: String =
    sys.error("BinaryArithmetics must override either decimalMethod or genCode")

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$decimalMethod($eval2)")
    // byte and short are casted into int when add, minus, times or divide
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev,
        (eval1, eval2) => s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }
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

  protected override def nullSafeEval(input1: Any, input2: Any): Any = numeric.plus(input1, input2)
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "-"
  override def decimalMethod: String = "$minus"

  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !DecimalType.isFixed(dataType)

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "operator " + symbol)

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = numeric.minus(input1, input2)
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "*"
  override def decimalMethod: String = "$times"

  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !DecimalType.isFixed(dataType)

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "operator " + symbol)

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = numeric.times(input1, input2)
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "/"
  override def decimalMethod: String = "$div"

  override def nullable: Boolean = true

  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !DecimalType.isFixed(dataType)

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "operator " + symbol)

  private lazy val div: (Any, Any) => Any = dataType match {
    case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
    case it: IntegralType => it.integral.asInstanceOf[Integral[Any]].quot
  }

  override def eval(input: InternalRow): Any = {
    val input2 = right.eval(input)
    if (input2 == null || input2 == 0) {
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        div(input1, input2)
      }
    }
  }

  /**
   * Special case handling due to division by 0 => null.
   */
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val isZero = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval2.primitive}.isZero()"
    } else {
      s"${eval2.primitive} == 0"
    }
    val javaType = ctx.javaType(dataType)
    val divide = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval1.primitive}.$decimalMethod(${eval2.primitive})"
    } else {
      s"($javaType)(${eval1.primitive} $symbol ${eval2.primitive})"
    }
    s"""
      ${eval2.code}
      boolean ${ev.isNull} = false;
      $javaType ${ev.primitive} = ${ctx.defaultValue(javaType)};
      if (${eval2.isNull} || $isZero) {
        ${ev.isNull} = true;
      } else {
        ${eval1.code}
        if (${eval1.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.primitive} = $divide;
        }
      }
    """
  }
}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "%"
  override def decimalMethod: String = "remainder"

  override def nullable: Boolean = true

  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !DecimalType.isFixed(dataType)

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "operator " + symbol)

  private lazy val integral = dataType match {
    case i: IntegralType => i.integral.asInstanceOf[Integral[Any]]
    case i: FractionalType => i.asIntegral.asInstanceOf[Integral[Any]]
  }

  override def eval(input: InternalRow): Any = {
    val input2 = right.eval(input)
    if (input2 == null || input2 == 0) {
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        integral.rem(input1, input2)
      }
    }
  }

  /**
   * Special case handling for x % 0 ==> null.
   */
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val isZero = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval2.primitive}.isZero()"
    } else {
      s"${eval2.primitive} == 0"
    }
    val javaType = ctx.javaType(dataType)
    val remainder = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval1.primitive}.$decimalMethod(${eval2.primitive})"
    } else {
      s"($javaType)(${eval1.primitive} $symbol ${eval2.primitive})"
    }
    s"""
      ${eval2.code}
      boolean ${ev.isNull} = false;
      $javaType ${ev.primitive} = ${ctx.defaultValue(javaType)};
      if (${eval2.isNull} || $isZero) {
        ${ev.isNull} = true;
      } else {
        ${eval1.code}
        if (${eval1.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.primitive} = $remainder;
        }
      }
    """
  }
}

case class MaxOf(left: Expression, right: Expression) extends BinaryArithmetic {
  override def nullable: Boolean = left.nullable && right.nullable

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForOrderingExpr(t, "function maxOf")

  private lazy val ordering = TypeUtils.getOrdering(dataType)

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    val input2 = right.eval(input)
    if (input1 == null) {
      input2
    } else if (input2 == null) {
      input1
    } else {
      if (ordering.compare(input1, input2) < 0) {
        input2
      } else {
        input1
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val compCode = ctx.genComp(dataType, eval1.primitive, eval2.primitive)

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
        if ($compCode > 0) {
          ${ev.primitive} = ${eval1.primitive};
        } else {
          ${ev.primitive} = ${eval2.primitive};
        }
      }
    """
  }

  override def symbol: String = "max"
  override def prettyName: String = symbol
}

case class MinOf(left: Expression, right: Expression) extends BinaryArithmetic {
  override def nullable: Boolean = left.nullable && right.nullable

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForOrderingExpr(t, "function minOf")

  private lazy val ordering = TypeUtils.getOrdering(dataType)

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    val input2 = right.eval(input)
    if (input1 == null) {
      input2
    } else if (input2 == null) {
      input1
    } else {
      if (ordering.compare(input1, input2) < 0) {
        input1
      } else {
        input2
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val compCode = ctx.genComp(dataType, eval1.primitive, eval2.primitive)

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
        if ($compCode < 0) {
          ${ev.primitive} = ${eval1.primitive};
        } else {
          ${ev.primitive} = ${eval2.primitive};
        }
      }
    """
  }

  override def symbol: String = "min"
  override def prettyName: String = symbol
}
