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

import java.lang.{Long => JLong}

import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

import scala.math.BigDecimal.RoundingMode
import scala.math.BigDecimal.RoundingMode._

/**
 * A leaf expression specifically for math constants. Math constants expect no input.
 * @param c The math constant.
 * @param name The short name of the function
 */
abstract class LeafMathExpression(c: Double, name: String)
  extends LeafExpression with Serializable {
  self: Product =>

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def toString: String = s"$name()"

  override def eval(input: InternalRow): Any = c

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    s"""
      boolean ${ev.isNull} = false;
      ${ctx.javaType(dataType)} ${ev.primitive} = java.lang.Math.$name;
    """
  }
}

/**
 * A unary expression specifically for math functions. Math Functions expect a specific type of
 * input format, therefore these functions extend `ExpectsInputTypes`.
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class UnaryMathExpression(f: Double => Double, name: String)
  extends UnaryExpression with Serializable with ExpectsInputTypes {
  self: Product =>

  override def expectedChildTypes: Seq[DataType] = Seq(DoubleType)
  override def dataType: DataType = DoubleType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = true
  override def toString: String = s"$name($child)"

  override def eval(input: InternalRow): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val result = f(evalE.asInstanceOf[Double])
      if (result.isNaN) null else result
    }
  }

  // name of function in java.lang.Math
  def funcName: String = name.toLowerCase

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    eval.code + s"""
      boolean ${ev.isNull} = ${eval.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.primitive} = java.lang.Math.${funcName}(${eval.primitive});
        if (Double.valueOf(${ev.primitive}).isNaN()) {
          ${ev.isNull} = true;
        }
      }
    """
  }
}

/**
 * A binary expression specifically for math functions that take two `Double`s as input and returns
 * a `Double`.
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class BinaryMathExpression(f: (Double, Double) => Double, name: String)
  extends BinaryExpression with Serializable with ExpectsInputTypes { self: Product =>

  override def expectedChildTypes: Seq[DataType] = Seq(DoubleType, DoubleType)

  override def toString: String = s"$name($left, $right)"

  override def dataType: DataType = DoubleType

  override def eval(input: InternalRow): Any = {
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.${name.toLowerCase}($c1, $c2)")
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Leaf math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

case class EulerNumber() extends LeafMathExpression(math.E, "E")

case class Pi() extends LeafMathExpression(math.Pi, "PI")

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Unary math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

case class Acos(child: Expression) extends UnaryMathExpression(math.acos, "ACOS")

case class Asin(child: Expression) extends UnaryMathExpression(math.asin, "ASIN")

case class Atan(child: Expression) extends UnaryMathExpression(math.atan, "ATAN")

case class Cbrt(child: Expression) extends UnaryMathExpression(math.cbrt, "CBRT")

case class Ceil(child: Expression) extends UnaryMathExpression(math.ceil, "CEIL")

case class Cos(child: Expression) extends UnaryMathExpression(math.cos, "COS")

case class Cosh(child: Expression) extends UnaryMathExpression(math.cosh, "COSH")

case class Exp(child: Expression) extends UnaryMathExpression(math.exp, "EXP")

case class Expm1(child: Expression) extends UnaryMathExpression(math.expm1, "EXPM1")

case class Floor(child: Expression) extends UnaryMathExpression(math.floor, "FLOOR")

case class Log(child: Expression) extends UnaryMathExpression(math.log, "LOG")

case class Log2(child: Expression)
  extends UnaryMathExpression((x: Double) => math.log(x) / math.log(2), "LOG2") {
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    eval.code + s"""
      boolean ${ev.isNull} = ${eval.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.primitive} = java.lang.Math.log(${eval.primitive}) / java.lang.Math.log(2);
        if (Double.valueOf(${ev.primitive}).isNaN()) {
          ${ev.isNull} = true;
        }
      }
    """
  }
}

case class Log10(child: Expression) extends UnaryMathExpression(math.log10, "LOG10")

case class Log1p(child: Expression) extends UnaryMathExpression(math.log1p, "LOG1P")

case class Rint(child: Expression) extends UnaryMathExpression(math.rint, "ROUND") {
  override def funcName: String = "rint"
}

case class Signum(child: Expression) extends UnaryMathExpression(math.signum, "SIGNUM")

case class Sin(child: Expression) extends UnaryMathExpression(math.sin, "SIN")

case class Sinh(child: Expression) extends UnaryMathExpression(math.sinh, "SINH")

case class Sqrt(child: Expression) extends UnaryMathExpression(math.sqrt, "SQRT")

case class Tan(child: Expression) extends UnaryMathExpression(math.tan, "TAN")

case class Tanh(child: Expression) extends UnaryMathExpression(math.tanh, "TANH")

case class ToDegrees(child: Expression) extends UnaryMathExpression(math.toDegrees, "DEGREES") {
  override def funcName: String = "toDegrees"
}

case class ToRadians(child: Expression) extends UnaryMathExpression(math.toRadians, "RADIANS") {
  override def funcName: String = "toRadians"
}

case class Bin(child: Expression)
  extends UnaryExpression with Serializable with ExpectsInputTypes {

  val name: String = "BIN"

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = true
  override def toString: String = s"$name($child)"

  override def expectedChildTypes: Seq[DataType] = Seq(LongType)
  override def dataType: DataType = StringType

  def funcName: String = name.toLowerCase

  override def eval(input: catalyst.InternalRow): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      UTF8String.fromString(JLong.toBinaryString(evalE.asInstanceOf[Long]))
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c) =>
      s"${ctx.stringType}.fromString(java.lang.Long.toBinaryString($c))")
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Binary math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////


case class Atan2(left: Expression, right: Expression)
  extends BinaryMathExpression(math.atan2, "ATAN2") {

  override def eval(input: InternalRow): Any = {
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.atan2($c1 + 0.0, $c2 + 0.0)") + s"""
      if (Double.valueOf(${ev.primitive}).isNaN()) {
        ${ev.isNull} = true;
      }
      """
  }
}

case class Hypot(left: Expression, right: Expression)
  extends BinaryMathExpression(math.hypot, "HYPOT")

case class Pow(left: Expression, right: Expression)
  extends BinaryMathExpression(math.pow, "POWER") {
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.pow($c1, $c2)") + s"""
      if (Double.valueOf(${ev.primitive}).isNaN()) {
        ${ev.isNull} = true;
      }
      """
  }
}

case class Logarithm(left: Expression, right: Expression)
  extends BinaryMathExpression((c1, c2) => math.log(c2) / math.log(c1), "LOG") {

  /**
   * Natural log, i.e. using e as the base.
   */
  def this(child: Expression) = {
    this(EulerNumber(), child)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val logCode = if (left.isInstanceOf[EulerNumber]) {
      defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.log($c2)")
    } else {
      defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.log($c2) / java.lang.Math.log($c1)")
    }
    logCode + s"""
      if (Double.valueOf(${ev.primitive}).isNaN()) {
        ${ev.isNull} = true;
        """
  }
}

case class Round(left: Expression, right: Expression)
  extends Expression with trees.BinaryNode[Expression] with Serializable {

  def this(left: Expression) = {
    this(left, Literal(0))
  }

  override def nullable: Boolean = left.nullable || right.nullable

  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !DecimalType.isFixed(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if ((left.dataType.isInstanceOf[NumericType] || left.dataType.isInstanceOf[NullType])
      && (right.dataType.isInstanceOf[IntegerType] || right.dataType.isInstanceOf[NullType])) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"round accepts numeric types as the value and integer type as the scale")
    }
  }

  override def toString: String = s"round($left, $right)"

  override def dataType: DataType = left.dataType

  override def eval(input: InternalRow): Any = {
    val value = left.eval(input)
    val scale = right.eval(input)
    if (value == null || scale == null) {
      null
    } else {
      dataType match {
        case _: DecimalType => {
          val result = value.asInstanceOf[Decimal]
          result.set(result.toBigDecimal, result.precision, scale.asInstanceOf[Integer])
          result
        }
        case FloatType => {
          BigDecimal.valueOf(value.asInstanceOf[Float].toDouble)
            .setScale(scale.asInstanceOf[Integer], RoundingMode.HALF_UP).floatValue()
        }
        case DoubleType => {
          BigDecimal.valueOf(value.asInstanceOf[Double])
            .setScale(scale.asInstanceOf[Integer], RoundingMode.HALF_UP).doubleValue()
        }
        case LongType => {
          BigDecimal.valueOf(value.asInstanceOf[Long])
            .setScale(scale.asInstanceOf[Integer], RoundingMode.HALF_UP).longValue()
        }
        case IntegerType => {
            BigDecimal.valueOf(value.asInstanceOf[Integer].toInt)
              .setScale(scale.asInstanceOf[Integer], RoundingMode.HALF_UP).intValue()
          }
        case ShortType => {
          BigDecimal.valueOf(value.asInstanceOf[Short])
            .setScale(scale.asInstanceOf[Integer], RoundingMode.HALF_UP).shortValue()
        }
        case ByteType => {
          BigDecimal.valueOf(value.asInstanceOf[Byte])
            .setScale(scale.asInstanceOf[Integer], RoundingMode.HALF_UP).byteValue()
        }
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType => defineCodeGen(ctx, ev, (c1, c2) =>
      s"$c1.set($c1.toBigDecimal(), $c1.precision(), $c2)")
    case FloatType => defineCodeGen(ctx, ev, (c1, c2) =>
      s"java.math.BigDecimal.valueOf((double)$c1)" +
        s".setScale(Integer.valueOf($c2), java.math.RoundingMode.HALF_UP).floatValue()")
    case DoubleType => defineCodeGen(ctx, ev, (c1, c2) =>
      s"java.math.BigDecimal.valueOf((double)$c1)" +
      s".setScale(Integer.valueOf($c2), java.math.RoundingMode.HALF_UP).doubleValue()")
    case LongType => defineCodeGen(ctx, ev, (c1, c2) =>
      s"java.math.BigDecimal.valueOf($c1)" +
        s".setScale(Integer.valueOf($c2), java.math.RoundingMode.HALF_UP).longValue()")
    case IntegerType => defineCodeGen(ctx, ev, (c1, c2) =>
      s"java.math.BigDecimal.valueOf((long)$c1)" +
        s".setScale(Integer.valueOf($c2), java.math.RoundingMode.HALF_UP).intValue()")
    case ShortType => defineCodeGen(ctx, ev, (c1, c2) =>
      s"java.math.BigDecimal.valueOf((long)$c1)" +
        s".setScale(Integer.valueOf($c2), java.math.RoundingMode.HALF_UP).shortValue()")
    case ByteType => defineCodeGen(ctx, ev, (c1, c2) =>
      s"java.math.BigDecimal.valueOf((long)$c1)" +
        s".setScale(Integer.valueOf($c2), java.math.RoundingMode.HALF_UP).byteValue()")
  }

  protected def defineCodeGen(
                               ctx: CodeGenContext,
                               ev: GeneratedExpressionCode,
                               f: (String, String) => String): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val resultCode = f(eval1.primitive, eval2.primitive)

    s"""
      ${eval1.code}
      boolean ${ev.isNull} = ${eval1.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${eval2.code}
        if (!${eval2.isNull}) {
          ${ev.primitive} = $resultCode;
        } else {
          ${ev.isNull} = true;
        }
      }
    """
  }
}
