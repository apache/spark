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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

@ExpressionDescription(
  usage = "_FUNC_(a) - Returns -a.")
case class UnaryMinus(child: Expression) extends UnaryExpression
    with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def toString: String = s"-$child"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case dt: DecimalType => defineCodeGen(ctx, ev, c => s"$c.unary_$$minus()")
    case dt: NumericType => nullSafeCodeGen(ctx, ev, eval => {
      val originValue = ctx.freshName("origin")
      // codegen would fail to compile if we just write (-($c))
      // for example, we could not write --9223372036854775808L in code
      s"""
        ${ctx.javaType(dt)} $originValue = (${ctx.javaType(dt)})($eval);
        ${ev.value} = (${ctx.javaType(dt)})(-($originValue));
      """})
    case dt: CalendarIntervalType => defineCodeGen(ctx, ev, c => s"$c.negate()")
  }

  protected override def nullSafeEval(input: Any): Any = {
    if (dataType.isInstanceOf[CalendarIntervalType]) {
      input.asInstanceOf[CalendarInterval].negate()
    } else {
      numeric.negate(input)
    }
  }

  override def sql: String = s"(- ${child.sql})"
}

@ExpressionDescription(
  usage = "_FUNC_(a) - Returns a.")
case class UnaryPositive(child: Expression)
    extends UnaryExpression with ExpectsInputTypes with NullIntolerant {
  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => c)

  protected override def nullSafeEval(input: Any): Any = input

  override def sql: String = s"(+ ${child.sql})"
}

/**
 * A function that get the absolute value of the numeric value.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the absolute value of the numeric value.",
  extended = "> SELECT _FUNC_('-1');\n 1")
case class Abs(child: Expression)
    extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def dataType: DataType = child.dataType

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, c => s"$c.abs()")
    case dt: NumericType =>
      defineCodeGen(ctx, ev, c => s"(${ctx.javaType(dt)})(java.lang.Math.abs($c))")
  }

  protected override def nullSafeEval(input: Any): Any = numeric.abs(input)
}

abstract class BinaryArithmetic extends BinaryOperator {

  override def dataType: DataType = left.dataType

  override lazy val resolved = childrenResolved && checkInputDataTypes().isSuccess

  /** Name of the function for this expression on a [[Decimal]] type. */
  def decimalMethod: String =
    sys.error("BinaryArithmetics must override either decimalMethod or genCode")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
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

object BinaryArithmetic {
  def unapply(e: BinaryArithmetic): Option[(Expression, Expression)] = Some((e.left, e.right))
}

@ExpressionDescription(
  usage = "a _FUNC_ b - Returns a+b.")
case class Add(left: Expression, right: Expression) extends BinaryArithmetic with NullIntolerant {

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    if (dataType.isInstanceOf[CalendarIntervalType]) {
      input1.asInstanceOf[CalendarInterval].add(input2.asInstanceOf[CalendarInterval])
    } else {
      numeric.plus(input1, input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$$plus($eval2)")
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev,
        (eval1, eval2) => s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case CalendarIntervalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.add($eval2)")
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }
}

@ExpressionDescription(
  usage = "a _FUNC_ b - Returns a-b.")
case class Subtract(left: Expression, right: Expression)
    extends BinaryArithmetic with NullIntolerant {

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    if (dataType.isInstanceOf[CalendarIntervalType]) {
      input1.asInstanceOf[CalendarInterval].subtract(input2.asInstanceOf[CalendarInterval])
    } else {
      numeric.minus(input1, input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$$minus($eval2)")
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev,
        (eval1, eval2) => s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case CalendarIntervalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.subtract($eval2)")
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }
}

@ExpressionDescription(
  usage = "a _FUNC_ b - Multiplies a by b.")
case class Multiply(left: Expression, right: Expression)
    extends BinaryArithmetic with NullIntolerant {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"
  override def decimalMethod: String = "$times"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = numeric.times(input1, input2)
}

@ExpressionDescription(
  usage = "a _FUNC_ b - Divides a by b.",
  extended = "> SELECT 3 _FUNC_ 2;\n 1.5")
case class Divide(left: Expression, right: Expression)
    extends BinaryArithmetic with NullIntolerant {

  override def inputType: AbstractDataType = TypeCollection(DoubleType, DecimalType)

  override def symbol: String = "/"
  override def decimalMethod: String = "$div"
  override def nullable: Boolean = true

  private lazy val div: (Any, Any) => Any = dataType match {
    case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
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
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val isZero = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval2.value}.isZero()"
    } else {
      s"${eval2.value} == 0"
    }
    val javaType = ctx.javaType(dataType)
    val divide = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval1.value}.$decimalMethod(${eval2.value})"
    } else {
      s"($javaType)(${eval1.value} $symbol ${eval2.value})"
    }
    if (!left.nullable && !right.nullable) {
      ev.copy(code = s"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${ctx.defaultValue(javaType)};
        if ($isZero) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          ${ev.value} = $divide;
        }""")
    } else {
      ev.copy(code = s"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${ctx.defaultValue(javaType)};
        if (${eval2.isNull} || $isZero) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          if (${eval1.isNull}) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = $divide;
          }
        }""")
    }
  }
}

@ExpressionDescription(
  usage = "a _FUNC_ b - Returns the remainder when dividing a by b.")
case class Remainder(left: Expression, right: Expression)
    extends BinaryArithmetic with NullIntolerant {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "%"
  override def decimalMethod: String = "remainder"
  override def nullable: Boolean = true

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
        input1 match {
          case d: Double => d % input2.asInstanceOf[java.lang.Double]
          case f: Float => f % input2.asInstanceOf[java.lang.Float]
          case _ => integral.rem(input1, input2)
        }
      }
    }
  }

  /**
   * Special case handling for x % 0 ==> null.
   */
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val isZero = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval2.value}.isZero()"
    } else {
      s"${eval2.value} == 0"
    }
    val javaType = ctx.javaType(dataType)
    val remainder = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval1.value}.$decimalMethod(${eval2.value})"
    } else {
      s"($javaType)(${eval1.value} $symbol ${eval2.value})"
    }
    if (!left.nullable && !right.nullable) {
      ev.copy(code = s"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${ctx.defaultValue(javaType)};
        if ($isZero) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          ${ev.value} = $remainder;
        }""")
    } else {
      ev.copy(code = s"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${ctx.defaultValue(javaType)};
        if (${eval2.isNull} || $isZero) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          if (${eval1.isNull}) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = $remainder;
          }
        }""")
    }
  }
}

case class MaxOf(left: Expression, right: Expression)
  extends BinaryArithmetic with NonSQLExpression {

  // TODO: Remove MaxOf and MinOf, and replace its usage with Greatest and Least.

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def nullable: Boolean = left.nullable && right.nullable

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val compCode = ctx.genComp(dataType, eval1.value, eval2.value)

    ev.copy(code = eval1.code + eval2.code + s"""
      boolean ${ev.isNull} = false;
      ${ctx.javaType(left.dataType)} ${ev.value} =
        ${ctx.defaultValue(left.dataType)};

      if (${eval1.isNull}) {
        ${ev.isNull} = ${eval2.isNull};
        ${ev.value} = ${eval2.value};
      } else if (${eval2.isNull}) {
        ${ev.isNull} = ${eval1.isNull};
        ${ev.value} = ${eval1.value};
      } else {
        if ($compCode > 0) {
          ${ev.value} = ${eval1.value};
        } else {
          ${ev.value} = ${eval2.value};
        }
      }""")
  }

  override def symbol: String = "max"
}

case class MinOf(left: Expression, right: Expression)
  extends BinaryArithmetic with NonSQLExpression {

  // TODO: Remove MaxOf and MinOf, and replace its usage with Greatest and Least.

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def nullable: Boolean = left.nullable && right.nullable

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val compCode = ctx.genComp(dataType, eval1.value, eval2.value)

    ev.copy(code = eval1.code + eval2.code + s"""
      boolean ${ev.isNull} = false;
      ${ctx.javaType(left.dataType)} ${ev.value} =
        ${ctx.defaultValue(left.dataType)};

      if (${eval1.isNull}) {
        ${ev.isNull} = ${eval2.isNull};
        ${ev.value} = ${eval2.value};
      } else if (${eval2.isNull}) {
        ${ev.isNull} = ${eval1.isNull};
        ${ev.value} = ${eval1.value};
      } else {
        if ($compCode < 0) {
          ${ev.value} = ${eval1.value};
        } else {
          ${ev.value} = ${eval2.value};
        }
      }""")
  }

  override def symbol: String = "min"
}

@ExpressionDescription(
  usage = "_FUNC_(a, b) - Returns the positive modulo",
  extended = "> SELECT _FUNC_(10,3);\n 1")
case class Pmod(left: Expression, right: Expression) extends BinaryArithmetic with NullIntolerant {

  override def toString: String = s"pmod($left, $right)"

  override def symbol: String = "pmod"

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "pmod")

  override def inputType: AbstractDataType = NumericType

  protected override def nullSafeEval(left: Any, right: Any) =
    dataType match {
      case IntegerType => pmod(left.asInstanceOf[Int], right.asInstanceOf[Int])
      case LongType => pmod(left.asInstanceOf[Long], right.asInstanceOf[Long])
      case ShortType => pmod(left.asInstanceOf[Short], right.asInstanceOf[Short])
      case ByteType => pmod(left.asInstanceOf[Byte], right.asInstanceOf[Byte])
      case FloatType => pmod(left.asInstanceOf[Float], right.asInstanceOf[Float])
      case DoubleType => pmod(left.asInstanceOf[Double], right.asInstanceOf[Double])
      case _: DecimalType => pmod(left.asInstanceOf[Decimal], right.asInstanceOf[Decimal])
    }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      val remainder = ctx.freshName("remainder")
      dataType match {
        case dt: DecimalType =>
          val decimalAdd = "$plus"
          s"""
            ${ctx.javaType(dataType)} $remainder = $eval1.remainder($eval2);
            if ($remainder.compare(new org.apache.spark.sql.types.Decimal().set(0)) < 0) {
              ${ev.value} = ($remainder.$decimalAdd($eval2)).remainder($eval2);
            } else {
              ${ev.value} = $remainder;
            }
          """
        // byte and short are casted into int when add, minus, times or divide
        case ByteType | ShortType =>
          s"""
            ${ctx.javaType(dataType)} $remainder = (${ctx.javaType(dataType)})($eval1 % $eval2);
            if ($remainder < 0) {
              ${ev.value} = (${ctx.javaType(dataType)})(($remainder + $eval2) % $eval2);
            } else {
              ${ev.value} = $remainder;
            }
          """
        case _ =>
          s"""
            ${ctx.javaType(dataType)} $remainder = $eval1 % $eval2;
            if ($remainder < 0) {
              ${ev.value} = ($remainder + $eval2) % $eval2;
            } else {
              ${ev.value} = $remainder;
            }
          """
      }
    })
  }

  private def pmod(a: Int, n: Int): Int = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Long, n: Long): Long = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Byte, n: Byte): Byte = {
    val r = a % n
    if (r < 0) {((r + n) % n).toByte} else r.toByte
  }

  private def pmod(a: Double, n: Double): Double = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Short, n: Short): Short = {
    val r = a % n
    if (r < 0) {((r + n) % n).toShort} else r.toShort
  }

  private def pmod(a: Float, n: Float): Float = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Decimal, n: Decimal): Decimal = {
    val r = a % n
    if (r.compare(Decimal.ZERO) < 0) {(r + n) % n} else r
  }

  override def sql: String = s"$prettyName(${left.sql}, ${right.sql})"
}
