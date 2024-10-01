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

import java.{lang => jl}
import java.util.HexFormat.fromHexDigit
import java.util.Locale

import org.apache.spark.QueryContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, FunctionRegistry, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{MathUtils, NumberConverter, TypeUtils}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCaseAccentSensitivity
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A leaf expression specifically for math constants. Math constants expect no input.
 *
 * There is no code generation because they should get constant folded by the optimizer.
 *
 * @param c The math constant.
 * @param name The short name of the function
 */
abstract class LeafMathExpression(c: Double, name: String)
  extends LeafExpression with CodegenFallback with Serializable {

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def toString: String = s"$name()"
  override def prettyName: String = name

  override def eval(input: InternalRow): Any = c
}

/**
 * A unary expression specifically for math functions. Math Functions expect a specific type of
 * input format, therefore these functions extend `ExpectsInputTypes`.
 *
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class UnaryMathExpression(val f: Double => Double, name: String)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant with Serializable {

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  override def toString: String = s"$prettyName($child)"
  override def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse(name)

  protected override def nullSafeEval(input: Any): Any = {
    f(input.asInstanceOf[Double])
  }

  // name of function in java.lang.Math
  def funcName: String = name.toLowerCase(Locale.ROOT)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"java.lang.Math.${funcName}($c)")
  }
}

abstract class UnaryLogExpression(f: Double => Double, name: String)
    extends UnaryMathExpression(f, name) {

  override def nullable: Boolean = true

  // values less than or equal to yAsymptote eval to null in Hive, instead of NaN or -Infinity
  protected val yAsymptote: Double = 0.0

  protected override def nullSafeEval(input: Any): Any = {
    val d = input.asInstanceOf[Double]
    if (d <= yAsymptote) null else f(d)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c =>
      s"""
        if ($c <= $yAsymptote) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = java.lang.StrictMath.${funcName}($c);
        }
      """
    )
  }
}

/**
 * A binary expression specifically for math functions that take two `Double`s as input and returns
 * a `Double`.
 *
 * @param f The math function.
 * @param name The short name of the function
 */
abstract class BinaryMathExpression(f: (Double, Double) => Double, name: String)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant with Serializable {

  override def inputTypes: Seq[DataType] = Seq(DoubleType, DoubleType)

  override def toString: String = s"$prettyName($left, $right)"

  override def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse(name)

  override def dataType: DataType = DoubleType

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    f(input1.asInstanceOf[Double], input2.asInstanceOf[Double])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) =>
      s"java.lang.Math.${name.toLowerCase(Locale.ROOT)}($c1, $c2)")
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Leaf math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Euler's number. Note that there is no code generation because this is only
 * evaluated by the optimizer during constant folding.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns Euler's number, e.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       2.718281828459045
  """,
  since = "1.5.0",
  group = "math_funcs")
case class EulerNumber() extends LeafMathExpression(math.E, "E")

/**
 * Pi. Note that there is no code generation because this is only
 * evaluated by the optimizer during constant folding.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns pi.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       3.141592653589793
  """,
  since = "1.5.0",
  group = "math_funcs")
case class Pi() extends LeafMathExpression(math.Pi, "PI")

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Unary math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the inverse cosine (a.k.a. arc cosine) of `expr`, as if computed by
      `java.lang.Math._FUNC_`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       0.0
      > SELECT _FUNC_(2);
       NaN
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Acos(child: Expression) extends UnaryMathExpression(math.acos, "ACOS") {
  override protected def withNewChildInternal(newChild: Expression): Acos = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the inverse sine (a.k.a. arc sine) the arc sin of `expr`,
      as if computed by `java.lang.Math._FUNC_`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0.0
      > SELECT _FUNC_(2);
       NaN
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Asin(child: Expression) extends UnaryMathExpression(math.asin, "ASIN") {
  override protected def withNewChildInternal(newChild: Expression): Asin = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the inverse tangent (a.k.a. arc tangent) of `expr`, as if computed by
      `java.lang.Math._FUNC_`
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Atan(child: Expression) extends UnaryMathExpression(math.atan, "ATAN") {
  override protected def withNewChildInternal(newChild: Expression): Atan = copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the cube root of `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_(27.0);
       3.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Cbrt(child: Expression) extends UnaryMathExpression(math.cbrt, "CBRT") {
  override protected def withNewChildInternal(newChild: Expression): Cbrt = copy(child = newChild)
}

case class Ceil(child: Expression) extends UnaryMathExpression(math.ceil, "CEIL") {
  override def dataType: DataType = child.dataType match {
    case dt @ DecimalType.Fixed(_, 0) => dt
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision - scale + 1, 0)
    case _ => LongType
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DecimalType, LongType))

  protected override def nullSafeEval(input: Any): Any = child.dataType match {
    case LongType => input.asInstanceOf[Long]
    case DoubleType => f(input.asInstanceOf[Double]).toLong
    case DecimalType.Fixed(_, _) => input.asInstanceOf[Decimal].ceil
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.dataType match {
      case DecimalType.Fixed(_, 0) => defineCodeGen(ctx, ev, c => s"$c")
      case DecimalType.Fixed(_, _) =>
        defineCodeGen(ctx, ev, c => s"$c.ceil()")
      case LongType => defineCodeGen(ctx, ev, c => s"$c")
      case _ => defineCodeGen(ctx, ev, c => s"(long)(java.lang.Math.${funcName}($c))")
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Ceil = copy(child = newChild)
}

trait CeilFloorExpressionBuilderBase extends ExpressionBuilder {
  protected def buildWithOneParam(param: Expression): Expression
  protected def buildWithTwoParams(param1: Expression, param2: Expression): Expression

  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 1) {
      buildWithOneParam(expressions.head)
    } else if (numArgs == 2) {
      val scale = expressions(1)
      if (!(scale.foldable && scale.dataType == IntegerType)) {
        throw QueryCompilationErrors.nonFoldableArgumentError(funcName, "scale", IntegerType)
      }
      if (scale.eval() == null) {
        throw QueryCompilationErrors.nonFoldableArgumentError(funcName, "scale", IntegerType)
      }
      buildWithTwoParams(expressions(0), scale)
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(2), numArgs)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr[, scale]) - Returns the smallest number after rounding up that is not smaller than `expr`. An optional `scale` parameter can be specified to control the rounding behavior.",
  examples = """
    Examples:
      > SELECT _FUNC_(-0.1);
       0
      > SELECT _FUNC_(5);
       5
      > SELECT _FUNC_(3.1411, 3);
       3.142
      > SELECT _FUNC_(3.1411, -3);
       1000
  """,
  since = "3.3.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
object CeilExpressionBuilder extends CeilFloorExpressionBuilderBase {
  override protected def buildWithOneParam(param: Expression): Expression = Ceil(param)

  override protected def buildWithTwoParams(param1: Expression, param2: Expression): Expression =
    RoundCeil(param1, param2)
}

case class RoundCeil(child: Expression, scale: Expression)
  extends RoundBase(child, scale, BigDecimal.RoundingMode.CEILING, "ROUND_CEILING") {

  override def inputTypes: Seq[AbstractDataType] = Seq(DecimalType, IntegerType)

  // No need to add an expression, ansiEnabled is set to false in the base. If ansiEnabled is
  // switched to true, this hint should be updated.
  override def tryHint(): String = ""

  override def nodeName: String = "ceil"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RoundCeil =
    copy(child = newLeft, scale = newRight)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the cosine of `expr`, as if computed by
      `java.lang.Math._FUNC_`.
  """,
  arguments = """
    Arguments:
      * expr - angle in radians
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       1.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Cos(child: Expression) extends UnaryMathExpression(math.cos, "COS") {
  override protected def withNewChildInternal(newChild: Expression): Cos = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the secant of `expr`, as if computed by `1/java.lang.Math.cos`.
  """,
  arguments = """
    Arguments:
      * expr - angle in radians
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       1.0
  """,
  since = "3.3.0",
  group = "math_funcs")
case class Sec(child: Expression)
  extends UnaryMathExpression((x: Double) => 1 / math.cos(x), "SEC") {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"${ev.value} = 1 / java.lang.Math.cos($c);")
  }
  override protected def withNewChildInternal(newChild: Expression): Sec = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
      _FUNC_(expr) - Returns the hyperbolic cosine of `expr`, as if computed by
        `java.lang.Math._FUNC_`.
  """,
  arguments = """
    Arguments:
      * expr - hyperbolic angle
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       1.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Cosh(child: Expression) extends UnaryMathExpression(math.cosh, "COSH") {
  override protected def withNewChildInternal(newChild: Expression): Cosh = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns inverse hyperbolic cosine of `expr`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       0.0
      > SELECT _FUNC_(0);
       NaN
  """,
  since = "3.0.0",
  group = "math_funcs")
case class Acosh(child: Expression)
  extends UnaryMathExpression((x: Double) => StrictMath.log(x + math.sqrt(x * x - 1.0)), "ACOSH") {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev,
      c => s"java.lang.StrictMath.log($c + java.lang.Math.sqrt($c * $c - 1.0))")
  }
  override protected def withNewChildInternal(newChild: Expression): Acosh = copy(child = newChild)
}

/**
 * Try to convert a num from one base to another
 *
 * @param numExpr the number to be converted
 * @param fromBaseExpr from which base
 * @param toBaseExpr to which base
 */
@ExpressionDescription(
  usage = "_FUNC_(num, from_base, to_base) - Convert `num` from `from_base` to `to_base`.",
  examples = """
    Examples:
      > SELECT _FUNC_('100', 2, 10);
       4
      > SELECT _FUNC_(-10, 16, -10);
       -16
  """,
  since = "4.0.0",
  group = "math_funcs")
case class TryConv(
    numExpr: Expression,
    fromBaseExpr: Expression,
    toBaseExpr: Expression,
    replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {

  def this(
      numExpr: Expression,
      fromBaseExpr: Expression,
      toBaseExpr: Expression) =
    this(numExpr, fromBaseExpr, toBaseExpr,
      Conv(numExpr, fromBaseExpr, toBaseExpr, failOnError = false))

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(replacement = newChild)
  }

  override def parameters: Seq[Expression] = Seq(numExpr, fromBaseExpr, toBaseExpr)

  override def prettyName: String = "try_conv"
}

/**
 * Convert a num from one base to another
 *
 * @param numExpr the number to be converted
 * @param fromBaseExpr from which base
 * @param toBaseExpr to which base
 */
@ExpressionDescription(
  usage = "_FUNC_(num, from_base, to_base) - Convert `num` from `from_base` to `to_base`.",
  examples = """
    Examples:
      > SELECT _FUNC_('100', 2, 10);
       4
      > SELECT _FUNC_(-10, 16, -10);
       -16
  """,
  since = "1.5.0",
  group = "math_funcs")
case class Conv(
    numExpr: Expression,
    fromBaseExpr: Expression,
    toBaseExpr: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends TernaryExpression
    with ImplicitCastInputTypes
    with NullIntolerant
    with SupportQueryContext {

  def this(numExpr: Expression, fromBaseExpr: Expression, toBaseExpr: Expression) =
    this(numExpr, fromBaseExpr, toBaseExpr, failOnError = SQLConf.get.ansiEnabled)

  override def first: Expression = numExpr
  override def second: Expression = fromBaseExpr
  override def third: Expression = toBaseExpr
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCaseAccentSensitivity, IntegerType, IntegerType)
  override def dataType: DataType = first.dataType
  override def nullable: Boolean = true

  override def nullSafeEval(num: Any, fromBase: Any, toBase: Any): Any = {
    NumberConverter.convert(
      num.asInstanceOf[UTF8String].trim().getBytes,
      fromBase.asInstanceOf[Int],
      toBase.asInstanceOf[Int],
      failOnError,
      getContextOrNull())
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val numconv = NumberConverter.getClass.getName.stripSuffix("$")
    val context = getContextOrNullCode(ctx, failOnError)
    nullSafeCodeGen(ctx, ev, (num, from, to) =>
      s"""
       ${ev.value} = $numconv.convert($num.trim().getBytes(), $from, $to, $failOnError, $context);
       if (${ev.value} == null) {
         ${ev.isNull} = true;
       }
       """
    )
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
    copy(numExpr = newFirst, fromBaseExpr = newSecond, toBaseExpr = newThird)

  override def initQueryContext(): Option[QueryContext] = if (failOnError) {
    Some(origin.context)
  } else {
    None
  }
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns e to the power of `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       1.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Exp(child: Expression) extends UnaryMathExpression(StrictMath.exp, "EXP") {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"java.lang.StrictMath.exp($c)")
  }
  override protected def withNewChildInternal(newChild: Expression): Exp = copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns exp(`expr`) - 1.",
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Expm1(child: Expression) extends UnaryMathExpression(StrictMath.expm1, "EXPM1") {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"java.lang.StrictMath.expm1($c)")
  }
  override protected def withNewChildInternal(newChild: Expression): Expm1 = copy(child = newChild)
}

case class Floor(child: Expression) extends UnaryMathExpression(math.floor, "FLOOR") {
  override def dataType: DataType = child.dataType match {
    case dt @ DecimalType.Fixed(_, 0) => dt
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision - scale + 1, 0)
    case _ => LongType
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DecimalType, LongType))

  protected override def nullSafeEval(input: Any): Any = child.dataType match {
    case LongType => input.asInstanceOf[Long]
    case DoubleType => f(input.asInstanceOf[Double]).toLong
    case DecimalType.Fixed(_, _) => input.asInstanceOf[Decimal].floor
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.dataType match {
      case DecimalType.Fixed(_, 0) => defineCodeGen(ctx, ev, c => s"$c")
      case DecimalType.Fixed(_, _) =>
        defineCodeGen(ctx, ev, c => s"$c.floor()")
      case LongType => defineCodeGen(ctx, ev, c => s"$c")
      case _ => defineCodeGen(ctx, ev, c => s"(long)(java.lang.Math.${funcName}($c))")
    }
 }
 override protected def withNewChildInternal(newChild: Expression): Floor =
  copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = " _FUNC_(expr[, scale]) - Returns the largest number after rounding down that is not greater than `expr`. An optional `scale` parameter can be specified to control the rounding behavior.",
  examples = """
    Examples:
      > SELECT _FUNC_(-0.1);
       -1
      > SELECT _FUNC_(5);
       5
      > SELECT _FUNC_(3.1411, 3);
       3.141
      > SELECT _FUNC_(3.1411, -3);
       0
  """,
  since = "3.3.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
object FloorExpressionBuilder extends CeilFloorExpressionBuilderBase {
  override protected def buildWithOneParam(param: Expression): Expression = Floor(param)

  override protected def buildWithTwoParams(param1: Expression, param2: Expression): Expression =
    RoundFloor(param1, param2)
}

case class RoundFloor(child: Expression, scale: Expression)
  extends RoundBase(child, scale, BigDecimal.RoundingMode.FLOOR, "ROUND_FLOOR") {

  override def inputTypes: Seq[AbstractDataType] = Seq(DecimalType, IntegerType)

  // No need to add an expression, ansiEnabled is set to false in the base. If ansiEnabled is
  // switched to true, this hint should be updated.
  override def tryHint(): String = ""

  override def nodeName: String = "floor"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RoundFloor =
    copy(child = newLeft, scale = newRight)
}

object Factorial {

  def factorial(n: Int): Long = {
    if (n < factorials.length) factorials(n) else Long.MaxValue
  }

  private val factorials: Array[Long] = Array[Long](
    1,
    1,
    2,
    6,
    24,
    120,
    720,
    5040,
    40320,
    362880,
    3628800,
    39916800,
    479001600,
    6227020800L,
    87178291200L,
    1307674368000L,
    20922789888000L,
    355687428096000L,
    6402373705728000L,
    121645100408832000L,
    2432902008176640000L
  )
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the factorial of `expr`. `expr` is [0..20]. Otherwise, null.",
  examples = """
    Examples:
      > SELECT _FUNC_(5);
       120
  """,
  since = "1.5.0",
  group = "math_funcs")
case class Factorial(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(IntegerType)

  override def dataType: DataType = LongType

  // If the value not in the range of [0, 20], it still will be null, so set it to be true here.
  override def nullable: Boolean = true

  protected override def nullSafeEval(input: Any): Any = {
    val value = input.asInstanceOf[jl.Integer]
    if (value > 20 || value < 0) {
      null
    } else {
      Factorial.factorial(value)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
        if ($eval > 20 || $eval < 0) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} =
            org.apache.spark.sql.catalyst.expressions.Factorial.factorial($eval);
        }
      """
    })
  }

  override protected def withNewChildInternal(newChild: Expression): Factorial =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the natural logarithm (base e) of `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       0.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Log(child: Expression) extends UnaryLogExpression(StrictMath.log, "LOG") {
  override def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("ln")
  override protected def withNewChildInternal(newChild: Expression): Log = copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the logarithm of `expr` with base 2.",
  examples = """
    Examples:
      > SELECT _FUNC_(2);
       1.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Log2(child: Expression)
  extends UnaryLogExpression((x: Double) => StrictMath.log(x) / StrictMath.log(2), "LOG2") {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c =>
      s"""
        if ($c <= $yAsymptote) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = java.lang.StrictMath.log($c) / java.lang.StrictMath.log(2);
        }
      """
    )
  }
  override protected def withNewChildInternal(newChild: Expression): Log2 = copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the logarithm of `expr` with base 10.",
  examples = """
    Examples:
      > SELECT _FUNC_(10);
       1.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Log10(child: Expression) extends UnaryLogExpression(StrictMath.log10, "LOG10") {
  override protected def withNewChildInternal(newChild: Expression): Log10 = copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns log(1 + `expr`).",
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Log1p(child: Expression) extends UnaryLogExpression(StrictMath.log1p, "LOG1P") {
  protected override val yAsymptote: Double = -1.0
  override protected def withNewChildInternal(newChild: Expression): Log1p = copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the double value that is closest in value to the argument and is equal to a mathematical integer.",
  examples = """
    Examples:
      > SELECT _FUNC_(12.3456);
       12.0
  """,
  since = "1.4.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class Rint(child: Expression) extends UnaryMathExpression(math.rint, "ROUND") {
  override def funcName: String = "rint"
  override def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("rint")
  override protected def withNewChildInternal(newChild: Expression): Rint = copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns -1.0, 0.0 or 1.0 as `expr` is negative, 0 or positive.",
  examples = """
    Examples:
      > SELECT _FUNC_(40);
       1.0
      > SELECT _FUNC_(INTERVAL -'100' YEAR);
       -1.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Signum(child: Expression) extends UnaryMathExpression(math.signum, "SIGNUM") {
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, YearMonthIntervalType, DayTimeIntervalType))
  protected override def nullSafeEval(input: Any): Any = {
    f(input.asInstanceOf[Number].doubleValue())
  }
  override protected def withNewChildInternal(newChild: Expression): Signum = copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the sine of `expr`, as if computed by `java.lang.Math._FUNC_`.",
  arguments = """
    Arguments:
      * expr - angle in radians
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Sin(child: Expression) extends UnaryMathExpression(math.sin, "SIN") {
  override protected def withNewChildInternal(newChild: Expression): Sin = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the cosecant of `expr`, as if computed by `1/java.lang.Math.sin`.
  """,
  arguments = """
    Arguments:
      * expr - angle in radians
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       1.1883951057781212
  """,
  since = "3.3.0",
  group = "math_funcs")
case class Csc(child: Expression)
  extends UnaryMathExpression((x: Double) => 1 / math.sin(x), "CSC") {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"${ev.value} = 1 / java.lang.Math.sin($c);")
  }
  override protected def withNewChildInternal(newChild: Expression): Csc = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns hyperbolic sine of `expr`, as if computed by `java.lang.Math._FUNC_`.
  """,
  arguments = """
    Arguments:
      * expr - hyperbolic angle
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Sinh(child: Expression) extends UnaryMathExpression(math.sinh, "SINH") {
  override protected def withNewChildInternal(newChild: Expression): Sinh = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns inverse hyperbolic sine of `expr`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0.0
  """,
  since = "3.0.0",
  group = "math_funcs")
case class Asinh(child: Expression)
  extends UnaryMathExpression((x: Double) => x match {
    case Double.NegativeInfinity => Double.NegativeInfinity
    case _ => StrictMath.log(x + math.sqrt(x * x + 1.0)) }, "ASINH") {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c =>
      s"$c == Double.NEGATIVE_INFINITY ? Double.NEGATIVE_INFINITY : " +
      s"java.lang.StrictMath.log($c + java.lang.Math.sqrt($c * $c + 1.0))")
  }
  override protected def withNewChildInternal(newChild: Expression): Asinh = copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the square root of `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_(4);
       2.0
  """,
  since = "1.1.1",
  group = "math_funcs")
case class Sqrt(child: Expression) extends UnaryMathExpression(math.sqrt, "SQRT") {
  override protected def withNewChildInternal(newChild: Expression): Sqrt = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the tangent of `expr`, as if computed by `java.lang.Math._FUNC_`.
  """,
  arguments = """
    Arguments:
      * expr - angle in radians
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Tan(child: Expression) extends UnaryMathExpression(math.tan, "TAN") {
  override protected def withNewChildInternal(newChild: Expression): Tan = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the cotangent of `expr`, as if computed by `1/java.lang.Math.tan`.
  """,
  arguments = """
    Arguments:
      * expr - angle in radians
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       0.6420926159343306
  """,
  since = "2.3.0",
  group = "math_funcs")
case class Cot(child: Expression)
  extends UnaryMathExpression((x: Double) => 1 / math.tan(x), "COT") {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"${ev.value} = 1 / java.lang.Math.tan($c);")
  }
  override protected def withNewChildInternal(newChild: Expression): Cot = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the hyperbolic tangent of `expr`, as if computed by
      `java.lang.Math._FUNC_`.
  """,
  arguments = """
    Arguments:
      * expr - hyperbolic angle
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Tanh(child: Expression) extends UnaryMathExpression(math.tanh, "TANH") {
  override protected def withNewChildInternal(newChild: Expression): Tanh = copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns inverse hyperbolic tangent of `expr`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0.0
      > SELECT _FUNC_(2);
       NaN
  """,
  since = "3.0.0",
  group = "math_funcs")
case class Atanh(child: Expression)
  // SPARK-28519: more accurate express for 1/2 * ln((1 + x) / (1 - x))
  extends UnaryMathExpression((x: Double) =>
    0.5 * (StrictMath.log1p(x) - StrictMath.log1p(-x)), "ATANH") {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev,
      c => s"0.5 * (java.lang.StrictMath.log1p($c) - java.lang.StrictMath.log1p(- $c))")
  }
  override protected def withNewChildInternal(newChild: Expression): Atanh = copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Converts radians to degrees.",
  arguments = """
    Arguments:
      * expr - angle in radians
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(3.141592653589793);
       180.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class ToDegrees(child: Expression) extends UnaryMathExpression(math.toDegrees, "DEGREES") {
  override def funcName: String = "toDegrees"
  override protected def withNewChildInternal(newChild: Expression): ToDegrees =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Converts degrees to radians.",
  arguments = """
    Arguments:
      * expr - angle in degrees
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(180);
       3.141592653589793
  """,
  since = "1.4.0",
  group = "math_funcs")
case class ToRadians(child: Expression) extends UnaryMathExpression(math.toRadians, "RADIANS") {
  override def funcName: String = "toRadians"
  override protected def withNewChildInternal(newChild: Expression): ToRadians =
    copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the string representation of the long value `expr` represented in binary.",
  examples = """
    Examples:
      > SELECT _FUNC_(13);
       1101
      > SELECT _FUNC_(-13);
       1111111111111111111111111111111111111111111111111111111111110011
      > SELECT _FUNC_(13.3);
       1101
  """,
  since = "1.5.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class Bin(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant with Serializable {

  override def inputTypes: Seq[DataType] = Seq(LongType)
  override def dataType: DataType = SQLConf.get.defaultStringType

  protected override def nullSafeEval(input: Any): Any =
    UTF8String.toBinaryString(input.asInstanceOf[Long])

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"UTF8String.toBinaryString($c)")
  }

  override protected def withNewChildInternal(newChild: Expression): Bin = copy(child = newChild)
}

object Hex {
  private final val hexDigits =
    Array[Byte]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

  // lookup table to translate '0' -> 0 ... 'F'/'f' -> 15
  val unhexDigits = {
    val array = Array.fill[Byte](128)(-1)
    (0 to 9).foreach(i => array('0' + i) = i.toByte)
    (0 to 5).foreach(i => array('A' + i) = (i + 10).toByte)
    (0 to 5).foreach(i => array('a' + i) = (i + 10).toByte)
    array
  }

  def hex(bytes: Array[Byte]): UTF8String = {
    val length = bytes.length
    if (length == 0) {
      return UTF8String.EMPTY_UTF8
    }
    val targetLength = length * 2L
    if (targetLength > Int.MaxValue) {
      throw QueryExecutionErrors.tooManyArrayElementsError(targetLength, Int.MaxValue)
    }
    val value = new Array[Byte](targetLength.toInt)
    var i = 0
    while (i < length) {
      value(i * 2) = hexDigits((bytes(i) & 0xF0) >> 4)
      value(i * 2 + 1) = hexDigits(bytes(i) & 0x0F)
      i += 1
    }
    UTF8String.fromBytes(value)
  }

  def hex(num: Long): UTF8String = {
    val zeros = jl.Long.numberOfLeadingZeros(num)
    if (zeros == jl.Long.SIZE) return UTF8String.ZERO_UTF8
    val len = (jl.Long.SIZE - zeros + 3) / 4
    var numBuf = num
    val value = new Array[Byte](len)
    var i = len - 1
    while (i >= 0) {
      value(i) = hexDigits((numBuf & 0xF).toInt)
      numBuf >>>= 4
      i -= 1
    }
    UTF8String.fromBytes(value)
  }

  def unhex(bytes: Array[Byte]): Array[Byte] = {
    val length = bytes.length
    if (length == 0) {
      return Array.emptyByteArray
    }
    if ((length & 0x1) != 0) {
      // while length of bytes is odd, loop from the end to beginning w/o the head
      val result = new Array[Byte](length / 2  + 1)
      var i = result.length - 1
      while (i > 0) {
        result(i) = ((fromHexDigit(bytes(i * 2 - 1)) << 4) | fromHexDigit(bytes(i * 2))).toByte
        i -= 1
      }
      // add it 'tailing' head
      result(0) = fromHexDigit(bytes(0)).toByte
      result
    } else {
      val result = new Array[Byte](length / 2)
      var i = 0
      while (i < result.length) {
        result(i) = ((fromHexDigit(bytes(2 * i)) << 4) | fromHexDigit(bytes(2 * i + 1))).toByte
        i += 1
      }
      result
    }
  }

  def unhex(str: String): Array[Byte] = unhex(str.getBytes())
}

/**
 * If the argument is an INT or binary, hex returns the number as a STRING in hexadecimal format.
 * Otherwise if the number is a STRING, it converts each character into its hex representation
 * and returns the resulting STRING. Negative numbers would be treated as two's complement.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Converts `expr` to hexadecimal.",
  examples = """
    Examples:
      > SELECT _FUNC_(17);
       11
      > SELECT _FUNC_('Spark SQL');
       537061726B2053514C
  """,
  since = "1.5.0",
  group = "math_funcs")
case class Hex(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(LongType, BinaryType, StringTypeWithCaseAccentSensitivity))

  override def dataType: DataType = child.dataType match {
    case st: StringType => st
    case _ => SQLConf.get.defaultStringType
  }

  protected override def nullSafeEval(num: Any): Any = child.dataType match {
    case LongType => Hex.hex(num.asInstanceOf[Long])
    case BinaryType => Hex.hex(num.asInstanceOf[Array[Byte]])
    case _: StringType => Hex.hex(num.asInstanceOf[UTF8String].getBytes)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (c) => {
      val hex = Hex.getClass.getName.stripSuffix("$")
      s"${ev.value} = " + (child.dataType match {
        case _: StringType => s"""$hex.hex($c.getBytes());"""
        case _ => s"""$hex.hex($c);"""
      })
    })
  }

  override protected def withNewChildInternal(newChild: Expression): Hex = copy(child = newChild)
}

/**
 * Performs the inverse operation of HEX.
 * Resulting characters are returned as a byte array.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Converts hexadecimal `expr` to binary.",
  examples = """
    Examples:
      > SELECT decode(_FUNC_('537061726B2053514C'), 'UTF-8');
       Spark SQL
  """,
  since = "1.5.0",
  group = "math_funcs")
case class Unhex(child: Expression, failOnError: Boolean = false)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(expr: Expression) = this(expr, false)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeWithCaseAccentSensitivity)

  override def nullable: Boolean = true
  override def dataType: DataType = BinaryType

  protected override def nullSafeEval(num: Any): Any = {
    try {
      Hex.unhex(num.asInstanceOf[UTF8String].getBytes)
    } catch {
      case _: IllegalArgumentException if !failOnError => null
      case _: IllegalArgumentException =>
        throw QueryExecutionErrors.invalidInputInConversionError(
          BinaryType,
          num.asInstanceOf[UTF8String],
          UTF8String.fromString("HEX"),
          "try_to_binary")
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(ctx, ev, input => {
      s"""
        ${ev.value} = (byte[]) $expr.nullSafeEval($input);
        ${ev.isNull} = ${ev.value} == null;
      """
    })
  }

  override protected def withNewChildInternal(newChild: Expression): Unhex =
    copy(child = newChild, failOnError)
}


////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Binary math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

@ExpressionDescription(
  usage = """
    _FUNC_(exprY, exprX) - Returns the angle in radians between the positive x-axis of a plane
      and the point given by the coordinates (`exprX`, `exprY`), as if computed by
      `java.lang.Math._FUNC_`.
  """,
  arguments = """
    Arguments:
      * exprY - coordinate on y-axis
      * exprX - coordinate on x-axis
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0, 0);
       0.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Atan2(left: Expression, right: Expression)
  extends BinaryMathExpression(math.atan2, "ATAN2") {

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    // With codegen, the values returned by -0.0 and 0.0 are different. Handled with +0.0
    math.atan2(input1.asInstanceOf[Double] + 0.0, input2.asInstanceOf[Double] + 0.0)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.Math.atan2($c1 + 0.0, $c2 + 0.0)")
  }

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Raises `expr1` to the power of `expr2`.",
  examples = """
    Examples:
      > SELECT _FUNC_(2, 3);
       8.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Pow(left: Expression, right: Expression)
  extends BinaryMathExpression(StrictMath.pow, "POWER") {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) => s"java.lang.StrictMath.pow($c1, $c2)")
  }
  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

sealed trait BitShiftOperation
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def symbol: String
  def shiftInt: (Int, Int) => Int
  def shiftLong: (Long, Int) => Long

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType), IntegerType)

  override def dataType: DataType = left.dataType

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (left, right) => s"$left $symbol $right")
  }

  override protected def nullSafeEval(input1: Any, input2: Any): Any = input1 match {
    case l: jl.Long => shiftLong(l, input2.asInstanceOf[Int])
    case i: jl.Integer => shiftInt(i, input2.asInstanceOf[Int])
  }

  override def toString: String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS) match {
      case Some(alias) if alias == symbol => s"($left $symbol $right)"
      case _ => super.toString
    }
  }

  override def sql: String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS) match {
      case Some(alias) if alias == symbol => s"(${left.sql} $symbol ${right.sql})"
      case _ => super.sql
    }
  }
}

/**
 * Bitwise left shift.
 *
 * @param left the base number to shift.
 * @param right number of bits to left shift.
 */
@ExpressionDescription(
  usage = "base _FUNC_ exp - Bitwise left shift.",
  examples = """
    Examples:
      > SELECT shiftleft(2, 1);
       4
      > SELECT 2 << 1;
       4
  """,
  note = """
      `<<` operator is added in Spark 4.0.0 as an alias for `shiftleft`.
    """,
  since = "1.5.0",
  group = "bitwise_funcs")
case class ShiftLeft(left: Expression, right: Expression) extends BitShiftOperation {
  override def symbol: String = "<<"
  override def shiftInt: (Int, Int) => Int = (x: Int, y: Int) => x << y
  override def shiftLong: (Long, Int) => Long = (x: Long, y: Int) => x << y
  val shift: (Number, Int) => Any = (x: Number, y: Int) => x.longValue() << y
  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): ShiftLeft = copy(left = newLeft, right = newRight)
}

/**
 * Bitwise (signed) right shift.
 *
 * @param left the base number to shift.
 * @param right number of bits to right shift.
 */
@ExpressionDescription(
  usage = "base _FUNC_ expr - Bitwise (signed) right shift.",
  examples = """
    Examples:
      > SELECT shiftright(4, 1);
       2
      > SELECT 4 >> 1;
       2
  """,
  note = """
      `>>` operator is added in Spark 4.0.0 as an alias for `shiftright`.
    """,
  since = "1.5.0",
  group = "bitwise_funcs")
case class ShiftRight(left: Expression, right: Expression) extends BitShiftOperation {
  override def symbol: String = ">>"
  override def shiftInt: (Int, Int) => Int = (x: Int, y: Int) => x >> y
  override def shiftLong: (Long, Int) => Long = (x: Long, y: Int) => x >> y
  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): ShiftRight = copy(left = newLeft, right = newRight)
}

/**
 * Bitwise unsigned right shift, for integer and long data type.
 *
 * @param left the base number.
 * @param right the number of bits to right shift.
 */
@ExpressionDescription(
  usage = "base _FUNC_ expr - Bitwise unsigned right shift.",
  examples = """
    Examples:
      > SELECT shiftrightunsigned(4, 1);
       2
      > SELECT 4 >>> 1;
       2
  """,
  note = """
    `>>>` operator is added in Spark 4.0.0 as an alias for `shiftrightunsigned`.
  """,
  since = "1.5.0",
  group = "bitwise_funcs")
case class ShiftRightUnsigned(left: Expression, right: Expression) extends BitShiftOperation {
  override def symbol: String = ">>>"
  override def shiftInt: (Int, Int) => Int = (x: Int, y: Int) => x >>> y
  override def shiftLong: (Long, Int) => Long = (x: Long, y: Int) => x >>> y
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): ShiftRightUnsigned =
    copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns sqrt(`expr1`**2 + `expr2`**2).",
  examples = """
    Examples:
      > SELECT _FUNC_(3, 4);
       5.0
  """,
  since = "1.4.0",
  group = "math_funcs")
case class Hypot(left: Expression, right: Expression)
  extends BinaryMathExpression(math.hypot, "HYPOT") {
  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Hypot =
    copy(left = newLeft, right = newRight)
}


/**
 * Computes the logarithm of a number.
 *
 * @param left the logarithm base, default to e.
 * @param right the number to compute the logarithm of.
 */
@ExpressionDescription(
  usage = "_FUNC_(base, expr) - Returns the logarithm of `expr` with `base`.",
  examples = """
    Examples:
      > SELECT _FUNC_(10, 100);
       2.0
  """,
  since = "1.5.0",
  group = "math_funcs")
case class Logarithm(left: Expression, right: Expression)
  extends BinaryMathExpression((c1, c2) => StrictMath.log(c2) / StrictMath.log(c1), "LOG") {

  /**
   * Natural log, i.e. using e as the base.
   */
  def this(child: Expression) = {
    this(EulerNumber(), child)
  }

  override def nullable: Boolean = true

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val dLeft = input1.asInstanceOf[Double]
    val dRight = input2.asInstanceOf[Double]
    // Unlike Hive, we support Log base in (0.0, 1.0]
    if (dLeft <= 0.0 || dRight <= 0.0) null else StrictMath.log(dRight) / StrictMath.log(dLeft)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (left.isInstanceOf[EulerNumber]) {
      nullSafeCodeGen(ctx, ev, (c1, c2) =>
        s"""
          if ($c2 <= 0.0) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = java.lang.StrictMath.log($c2);
          }
        """)
    } else {
      nullSafeCodeGen(ctx, ev, (c1, c2) =>
        s"""
          if ($c1 <= 0.0 || $c2 <= 0.0) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = java.lang.StrictMath.log($c2) / java.lang.StrictMath.log($c1);
          }
        """)
    }
  }

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Logarithm = copy(left = newLeft, right = newRight)
}

/**
 * Round the `child`'s result to `scale` decimal place when `scale` >= 0
 * or round at integral part when `scale` < 0.
 *
 * Child of IntegralType would round to itself when `scale` >= 0.
 * Child of FractionalType whose value is NaN or Infinite would always round to itself.
 *
 * Round's dataType would always equal to `child`'s dataType except for DecimalType,
 * which would lead scale decrease from the origin DecimalType.
 *
 * @param child expr to be round, all [[NumericType]] is allowed as Input
 * @param scale new scale to be round to, this should be a constant int at runtime
 * @param mode rounding mode (e.g. HALF_UP, HALF_EVEN)
 * @param modeStr rounding mode string name (e.g. "ROUND_HALF_UP", "ROUND_HALF_EVEN")
 */
abstract class RoundBase(child: Expression, scale: Expression,
    mode: BigDecimal.RoundingMode.Value, modeStr: String)
  extends BinaryExpression with Serializable with ImplicitCastInputTypes with SupportQueryContext {

  override def left: Expression = child
  override def right: Expression = scale

  protected def ansiEnabled: Boolean = false

  // round of Decimal would eval to null if it fails to `changePrecision`
  override def nullable: Boolean = true

  override def foldable: Boolean = child.foldable

  override lazy val dataType: DataType = child.dataType match {
    case DecimalType.Fixed(p, s) =>
      // After rounding we may need one more digit in the integral part,
      // e.g. `ceil(9.9, 0)` -> `10`, `ceil(99, -1)` -> `100`.
      val integralLeastNumDigits = p - s + 1
      if (_scale < 0) {
        // negative scale means we need to adjust `-scale` number of digits before the decimal
        // point, which means we need at lease `-scale + 1` digits (after rounding).
        val newPrecision = math.max(integralLeastNumDigits, -_scale + 1)
        // We have to accept the risk of overflow as we can't exceed the max precision.
        DecimalType(math.min(newPrecision, DecimalType.MAX_PRECISION), 0)
      } else {
        val newScale = math.min(s, _scale)
        // We have to accept the risk of overflow as we can't exceed the max precision.
        DecimalType(math.min(integralLeastNumDigits + newScale, 38), newScale)
      }
    case t => t
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case TypeCheckSuccess =>
        if (scale.foldable) {
          TypeCheckSuccess
        } else {
          DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> toSQLId("scale"),
              "inputType" -> toSQLType(scale.dataType),
              "inputExpr" -> toSQLExpr(scale)))
        }
      case f => f
    }
  }

  // Avoid repeated evaluation since `scale` is a constant int,
  // avoid unnecessary `child` evaluation in both codegen and non-codegen eval
  // by checking if scaleV == null as well.
  private lazy val scaleV: Any = scale.eval(EmptyRow)
  protected lazy val _scale: Int = scaleV.asInstanceOf[Int]

  override def initQueryContext(): Option[QueryContext] = {
    if (ansiEnabled) {
      Some(origin.context)
    } else {
      None
    }
  }

  override def eval(input: InternalRow): Any = {
    if (scaleV == null) { // if scale is null, no need to eval its child at all
      null
    } else {
      val evalE = child.eval(input)
      if (evalE == null) {
        null
      } else {
        nullSafeEval(evalE)
      }
    }
  }

  // not overriding since _scale is a constant int at runtime
  def nullSafeEval(input1: Any): Any = {
    dataType match {
      case DecimalType.Fixed(p, s) =>
        val decimal = input1.asInstanceOf[Decimal]
        if (_scale >= 0) {
          // Overflow cannot happen, so no need to control nullOnOverflow
          decimal.toPrecision(decimal.precision, s, mode)
        } else {
          Decimal(decimal.toBigDecimal.setScale(_scale, mode), p, s)
        }
      case ByteType if ansiEnabled =>
        MathUtils.withOverflow(
          f = BigDecimal(input1.asInstanceOf[Byte]).setScale(_scale, mode).toByteExact,
          hint = tryHint(),
          context = getContextOrNull())
      case ByteType =>
        BigDecimal(input1.asInstanceOf[Byte]).setScale(_scale, mode).toByte
      case ShortType if ansiEnabled =>
        MathUtils.withOverflow(
          f = BigDecimal(input1.asInstanceOf[Short]).setScale(_scale, mode).toShortExact,
          hint = tryHint(),
          context = getContextOrNull())
      case ShortType =>
        BigDecimal(input1.asInstanceOf[Short]).setScale(_scale, mode).toShort
      case IntegerType if ansiEnabled =>
        MathUtils.withOverflow(
          f = BigDecimal(input1.asInstanceOf[Int]).setScale(_scale, mode).toIntExact,
          hint = tryHint(),
          context = getContextOrNull())
      case IntegerType =>
        BigDecimal(input1.asInstanceOf[Int]).setScale(_scale, mode).toInt
      case LongType if ansiEnabled =>
        MathUtils.withOverflow(
          f = BigDecimal(input1.asInstanceOf[Long]).setScale(_scale, mode).toLongExact,
          hint = tryHint(),
          context = getContextOrNull())
      case LongType =>
        BigDecimal(input1.asInstanceOf[Long]).setScale(_scale, mode).toLong
      case FloatType =>
        val f = input1.asInstanceOf[Float]
        if (f.isNaN || f.isInfinite) {
          f
        } else {
          BigDecimal(f.toDouble).setScale(_scale, mode).toFloat
        }
      case DoubleType =>
        val d = input1.asInstanceOf[Double]
        if (d.isNaN || d.isInfinite) {
          d
        } else {
          BigDecimal(d).setScale(_scale, mode).toDouble
        }
    }
  }

  protected def tryHint(): String = ""

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ce = child.genCode(ctx)

    def codegenForIntegralType(dt: String): String = {
      if (_scale < 0) {
        if (ansiEnabled) {
          val errorContext = getContextOrNullCode(ctx)
          val evalCode = s"""
            |${ev.value} = new java.math.BigDecimal(${ce.value}).
            |setScale(${_scale}, java.math.BigDecimal.${modeStr}).${dt}ValueExact();
            |""".stripMargin
          MathUtils.withOverflowCode(evalCode, tryHint(), errorContext)
        } else {
          s"""
             |${ev.value} = new java.math.BigDecimal(${ce.value}).
             |setScale(${_scale}, java.math.BigDecimal.${modeStr}).${dt}Value();
             |""".stripMargin
        }
      } else {
        s"${ev.value} = ${ce.value};"
      }
    }

    val evaluationCode = dataType match {
      case DecimalType.Fixed(p, s) =>
        if (_scale >= 0) {
          s"""
            ${ev.value} = ${ce.value}.toPrecision(${ce.value}.precision(), $s,
            Decimal.$modeStr(), true, null);
            ${ev.isNull} = ${ev.value} == null;"""
       } else {
          s"""
            ${ev.value} = new Decimal().set(${ce.value}.toBigDecimal()
            .setScale(${_scale}, Decimal.$modeStr()), $p, $s);
            ${ev.isNull} = ${ev.value} == null;"""
        }
      case ByteType =>
        codegenForIntegralType("byte")
      case ShortType =>
        codegenForIntegralType("short")
      case IntegerType =>
        codegenForIntegralType("int")
      case LongType =>
        codegenForIntegralType("long")
      case FloatType => // if child eval to NaN or Infinity, just return it.
        s"""
          if (Float.isNaN(${ce.value}) || Float.isInfinite(${ce.value})) {
            ${ev.value} = ${ce.value};
          } else {
            ${ev.value} = java.math.BigDecimal.valueOf(${ce.value}).
              setScale(${_scale}, java.math.BigDecimal.${modeStr}).floatValue();
          }"""
      case DoubleType => // if child eval to NaN or Infinity, just return it.
        s"""
          if (Double.isNaN(${ce.value}) || Double.isInfinite(${ce.value})) {
            ${ev.value} = ${ce.value};
          } else {
            ${ev.value} = java.math.BigDecimal.valueOf(${ce.value}).
              setScale(${_scale}, java.math.BigDecimal.${modeStr}).doubleValue();
          }"""
    }

    val javaType = CodeGenerator.javaType(dataType)
    if (scaleV == null) { // if scale is null, no need to eval its child at all
      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};""")
    } else {
      ev.copy(code = code"""
        ${ce.code}
        boolean ${ev.isNull} = ${ce.isNull};
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (!${ev.isNull}) {
          $evaluationCode
        }""")
    }
  }
}

/**
 * Round an expression to d decimal places using HALF_UP rounding mode.
 * round(2.5) == 3.0, round(3.5) == 4.0.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr, d) - Returns `expr` rounded to `d` decimal places using HALF_UP rounding mode.",
  examples = """
    Examples:
      > SELECT _FUNC_(2.5, 0);
       3
  """,
  since = "1.5.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class Round(
    child: Expression,
    scale: Expression,
    override val ansiEnabled: Boolean = SQLConf.get.ansiEnabled)
  extends RoundBase(child, scale, BigDecimal.RoundingMode.HALF_UP, "ROUND_HALF_UP") {
  def this(child: Expression) = this(child, Literal(0), SQLConf.get.ansiEnabled)

  def this(child: Expression, scale: Expression) = this(child, scale, SQLConf.get.ansiEnabled)

  override def tryHint(): String = "try_round"

  override def flatArguments: Iterator[Any] = Iterator(child, scale)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Round =
    copy(child = newLeft, scale = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr, d) - Returns `expr` rounded to `d` decimal places using HALF_UP rounding mode.",
  examples = """
    Examples:
      > SELECT _FUNC_(2.5, 0);
       3
  """,
  since = "4.0.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class TryRound(override val child: Expression, scale: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {

  def this(child: Expression, scale: Expression) = this(child, scale, Round(child, scale, false))
  def this(child: Expression) = this(child, Literal(0))

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(replacement = newChild)
  }

  override def parameters: Seq[Expression] = Seq(child, scale)

  override def prettyName: String = "try_round"
}

/**
 * Round an expression to d decimal places using HALF_EVEN rounding mode,
 * also known as Gaussian rounding or bankers' rounding.
 * round(2.5) = 2.0, round(3.5) = 4.0.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr, d) - Returns `expr` rounded to `d` decimal places using HALF_EVEN rounding mode.",
  examples = """
    Examples:
      > SELECT _FUNC_(2.5, 0);
       2
      > SELECT _FUNC_(25, -1);
       20
  """,
  since = "2.0.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class BRound(
    child: Expression,
    scale: Expression,
    override val ansiEnabled: Boolean = SQLConf.get.ansiEnabled)
  extends RoundBase(child, scale, BigDecimal.RoundingMode.HALF_EVEN, "ROUND_HALF_EVEN") {
  def this(child: Expression) = this(child, Literal(0), SQLConf.get.ansiEnabled)

  def this(child: Expression, scale: Expression) = this(child, scale, SQLConf.get.ansiEnabled)

  override def tryHint(): String = "try_bround"

  override def flatArguments: Iterator[Any] = Iterator(child, scale)

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): BRound = copy(child = newLeft, scale = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr, d) - Returns `expr` rounded to `d` decimal places using HALF_EVEN rounding mode.",
  examples = """
    Examples:
      > SELECT _FUNC_(2.5, 0);
       2
  """,
  since = "4.0.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class TryBRound(override val child: Expression, scale: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {

  def this(child: Expression, scale: Expression) = this(child, scale, BRound(child, scale, false))
  def this(child: Expression) = this(child, Literal(0))

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(replacement = newChild)
  }

  override def parameters: Seq[Expression] = Seq(child, scale)

  override def prettyName: String = "try_bround"
}

object WidthBucket {
  def computeBucketNumber(value: Double, min: Double, max: Double, numBucket: Long): jl.Long = {
    if (isNull(value, min, max, numBucket)) {
      null
    } else {
      computeBucketNumberNotNull(value, min, max, numBucket)
    }
  }

  /** This function is called by generated Java code, so it needs to be public. */
  def isNull(value: Double, min: Double, max: Double, numBucket: Long): Boolean = {
    numBucket <= 0 ||
      numBucket == Long.MaxValue ||
      jl.Double.isNaN(value) ||
      min == max ||
      jl.Double.isNaN(min) || jl.Double.isInfinite(min) ||
      jl.Double.isNaN(max) || jl.Double.isInfinite(max)
  }

  /** This function is called by generated Java code, so it needs to be public. */
  def computeBucketNumberNotNull(
      value: Double, min: Double, max: Double, numBucket: Long): jl.Long = {
    val lower = Math.min(min, max)
    val upper = Math.max(min, max)

    if (min < max) {
      if (value < lower) {
        0L
      } else if (value >= upper) {
        numBucket + 1L
      } else {
        (numBucket.toDouble * (value - lower) / (upper - lower)).toLong + 1L
      }
    } else { // `min > max` case
      if (value > upper) {
        0L
      } else if (value <= lower) {
        numBucket + 1L
      } else {
        (numBucket.toDouble * (upper - value) / (upper - lower)).toLong + 1L
      }
    }
  }
}

/**
 * Returns the bucket number into which the value of this expression would fall
 * after being evaluated. Note that input arguments must follow conditions listed below;
 * otherwise, the method will return null.
 *  - `numBucket` must be greater than zero and be less than Long.MaxValue
 *  - `value`, `min`, and `max` cannot be NaN
 *  - `min` bound cannot equal `max`
 *  - `min` and `max` must be finite
 *
 * Note: If `minValue` > `maxValue`, a return value is as follows;
 *  if `value` > `minValue`, it returns 0.
 *  if `value` <= `maxValue`, it returns `numBucket` + 1.
 *  otherwise, it returns (`numBucket` * (`minValue` - `value`) / (`minValue` - `maxValue`)) + 1
 *
 * @param value is the expression to compute a bucket number in the histogram
 * @param minValue is the minimum value of the histogram
 * @param maxValue is the maximum value of the histogram
 * @param numBucket is the number of buckets
 */
@ExpressionDescription(
  usage = """
    _FUNC_(value, min_value, max_value, num_bucket) - Returns the bucket number to which
      `value` would be assigned in an equiwidth histogram with `num_bucket` buckets,
      in the range `min_value` to `max_value`."
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(5.3, 0.2, 10.6, 5);
       3
      > SELECT _FUNC_(-2.1, 1.3, 3.4, 3);
       0
      > SELECT _FUNC_(8.1, 0.0, 5.7, 4);
       5
      > SELECT _FUNC_(-0.9, 5.2, 0.5, 2);
       3
      > SELECT _FUNC_(INTERVAL '0' YEAR, INTERVAL '0' YEAR, INTERVAL '10' YEAR, 10);
       1
      > SELECT _FUNC_(INTERVAL '1' YEAR, INTERVAL '0' YEAR, INTERVAL '10' YEAR, 10);
       2
      > SELECT _FUNC_(INTERVAL '0' DAY, INTERVAL '0' DAY, INTERVAL '10' DAY, 10);
       1
      > SELECT _FUNC_(INTERVAL '1' DAY, INTERVAL '0' DAY, INTERVAL '10' DAY, 10);
       2
  """,
  since = "3.1.0",
  group = "math_funcs")
case class WidthBucket(
    value: Expression,
    minValue: Expression,
    maxValue: Expression,
    numBucket: Expression)
  extends QuaternaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(
    TypeCollection(DoubleType, YearMonthIntervalType, DayTimeIntervalType),
    TypeCollection(DoubleType, YearMonthIntervalType, DayTimeIntervalType),
    TypeCollection(DoubleType, YearMonthIntervalType, DayTimeIntervalType),
    LongType)

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case TypeCheckSuccess =>
        (value.dataType, minValue.dataType, maxValue.dataType) match {
          case (_: YearMonthIntervalType, _: YearMonthIntervalType, _: YearMonthIntervalType) =>
            TypeCheckSuccess
          case (_: DayTimeIntervalType, _: DayTimeIntervalType, _: DayTimeIntervalType) =>
            TypeCheckSuccess
          case _ =>
            val types = Seq(value.dataType, minValue.dataType, maxValue.dataType)
            TypeUtils.checkForSameTypeInputExpr(types, prettyName)
        }
      case f => f
    }
  }

  override def dataType: DataType = LongType
  override def nullable: Boolean = true
  override def prettyName: String = "width_bucket"

  override protected def nullSafeEval(input: Any, min: Any, max: Any, numBucket: Any): Any = {
    WidthBucket.computeBucketNumber(
      input.asInstanceOf[Number].doubleValue(),
      min.asInstanceOf[Number].doubleValue(),
      max.asInstanceOf[Number].doubleValue(),
      numBucket.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (input, min, max, numBucket) => {
      s"""${ev.isNull} = org.apache.spark.sql.catalyst.expressions.WidthBucket
         |  .isNull($input, $min, $max, $numBucket);
         |if (!${ev.isNull}) {
         |  ${ev.value} = org.apache.spark.sql.catalyst.expressions.WidthBucket
         |    .computeBucketNumberNotNull($input, $min, $max, $numBucket);
         |}""".stripMargin
    })
  }

  override def first: Expression = value
  override def second: Expression = minValue
  override def third: Expression = maxValue
  override def fourth: Expression = numBucket

  override protected def withNewChildrenInternal(
      first: Expression, second: Expression, third: Expression, fourth: Expression): WidthBucket =
    copy(value = first, minValue = second, maxValue = third, numBucket = fourth)
}
