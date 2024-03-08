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

import scala.math.{max, min}

import org.apache.spark.{QueryContext, SparkException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast.{toSQLId, toSQLType}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TreePattern.{BINARY_ARITHMETIC, TreePattern, UNARY_POSITIVE}
import org.apache.spark.sql.catalyst.types.{PhysicalDecimalType, PhysicalFractionalType, PhysicalIntegerType, PhysicalIntegralType, PhysicalLongType}
import org.apache.spark.sql.catalyst.util.{IntervalMathUtils, IntervalUtils, MathUtils, TypeUtils}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the negated value of `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       -1
  """,
  since = "1.0.0",
  group = "math_funcs")
case class UnaryMinus(
    child: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(child: Expression) = this(child, SQLConf.get.ansiEnabled)

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def toString: String = s"-$child"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case _: DecimalType => defineCodeGen(ctx, ev, c => s"$c.unary_$$minus()")
    case ByteType | ShortType | IntegerType | LongType if failOnError =>
      val typeUtils = TypeUtils.getClass.getCanonicalName.stripSuffix("$")
      val refDataType = ctx.addReferenceObj("refDataType", dataType, dataType.getClass.getName)
      nullSafeCodeGen(ctx, ev, eval => {
        val javaBoxedType = CodeGenerator.boxedType(dataType)
        s"""
           |${ev.value} = ($javaBoxedType)$typeUtils.getNumeric(
           |  $refDataType, $failOnError).negate($eval);
         """.stripMargin
      })
    case dt: NumericType => nullSafeCodeGen(ctx, ev, eval => {
      val originValue = ctx.freshName("origin")
      // codegen would fail to compile if we just write (-($c))
      // for example, we could not write --9223372036854775808L in code
      s"""
        ${CodeGenerator.javaType(dt)} $originValue = (${CodeGenerator.javaType(dt)})($eval);
        ${ev.value} = (${CodeGenerator.javaType(dt)})(-($originValue));
      """})
    case _: CalendarIntervalType =>
      val iu = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      val method = if (failOnError) "negateExact" else "negate"
      defineCodeGen(ctx, ev, c => s"$iu.$method($c)")
    case _: AnsiIntervalType =>
      nullSafeCodeGen(ctx, ev, eval => {
        val mathUtils = IntervalMathUtils.getClass.getCanonicalName.stripSuffix("$")
        s"${ev.value} = $mathUtils.negateExact($eval);"
      })
  }

  protected override def nullSafeEval(input: Any): Any = dataType match {
    case CalendarIntervalType if failOnError =>
      IntervalUtils.negateExact(input.asInstanceOf[CalendarInterval])
    case CalendarIntervalType => IntervalUtils.negate(input.asInstanceOf[CalendarInterval])
    case _: DayTimeIntervalType => IntervalMathUtils.negateExact(input.asInstanceOf[Long])
    case _: YearMonthIntervalType => IntervalMathUtils.negateExact(input.asInstanceOf[Int])
    case _ => numeric.negate(input)
  }

  override def sql: String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("-") match {
      case "-" => s"(- ${child.sql})"
      case funcName => s"$funcName(${child.sql})"
    }
  }

  override protected def withNewChildInternal(newChild: Expression): UnaryMinus =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the value of `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       1
  """,
  since = "1.5.0",
  group = "math_funcs")
case class UnaryPositive(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  final override val nodePatterns: Seq[TreePattern] = Seq(UNARY_POSITIVE)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => c)

  protected override def nullSafeEval(input: Any): Any = input

  override def sql: String = s"(+ ${child.sql})"

  override protected def withNewChildInternal(newChild: Expression): UnaryPositive =
    copy(child = newChild)
}

/**
 * A function that get the absolute value of the numeric or interval value.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the absolute value of the numeric or interval value.",
  examples = """
    Examples:
      > SELECT _FUNC_(-1);
       1
      > SELECT _FUNC_(INTERVAL -'1-1' YEAR TO MONTH);
       1-1
  """,
  since = "1.2.0",
  group = "math_funcs")
case class Abs(child: Expression, failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(child: Expression) = this(child, SQLConf.get.ansiEnabled)

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndAnsiInterval)

  override def dataType: DataType = child.dataType

  private lazy val numeric = (dataType match {
    case _: DayTimeIntervalType => LongExactNumeric
    case _: YearMonthIntervalType => IntegerExactNumeric
    case _ => TypeUtils.getNumeric(dataType, failOnError)
  }).asInstanceOf[Numeric[Any]]

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case _: DecimalType =>
      defineCodeGen(ctx, ev, c => s"$c.abs()")

    case ByteType | ShortType | IntegerType | LongType if failOnError =>
      val typeUtils = TypeUtils.getClass.getCanonicalName.stripSuffix("$")
      val refDataType = ctx.addReferenceObj("refDataType", dataType, dataType.getClass.getName)
      nullSafeCodeGen(ctx, ev, eval => {
        val javaBoxedType = CodeGenerator.boxedType(dataType)
        s"""
           |${ev.value} = ($javaBoxedType)$typeUtils.getNumeric(
           |  $refDataType, $failOnError).abs($eval);
         """.stripMargin
      })

    case _: AnsiIntervalType =>
      val mathUtils = MathUtils.getClass.getCanonicalName.stripSuffix("$")
      defineCodeGen(ctx, ev, c => s"$c < 0 ? $mathUtils.negateExact($c) : $c")

    case dt: NumericType =>
      defineCodeGen(ctx, ev, c => s"(${CodeGenerator.javaType(dt)})(java.lang.Math.abs($c))")
  }

  protected override def nullSafeEval(input: Any): Any = numeric.abs(input)

  override def flatArguments: Iterator[Any] = Iterator(child)

  override protected def withNewChildInternal(newChild: Expression): Abs = copy(child = newChild)
}

abstract class BinaryArithmetic extends BinaryOperator
  with NullIntolerant with SupportQueryContext {

  protected val evalMode: EvalMode.Value

  private lazy val internalDataType: DataType = (left.dataType, right.dataType) match {
    case (DecimalType.Fixed(p1, s1), DecimalType.Fixed(p2, s2)) =>
      resultDecimalType(p1, s1, p2, s2)
    case _ => left.dataType
  }

  protected def failOnError: Boolean = evalMode match {
    // The TRY mode executes as if it would fail on errors, except that it would capture the errors
    // and return null results.
    case EvalMode.ANSI | EvalMode.TRY => true
    case _ => false
  }

  override def checkInputDataTypes(): TypeCheckResult = (left.dataType, right.dataType) match {
    case (l: DecimalType, r: DecimalType) if inputType.acceptsType(l) && inputType.acceptsType(r) =>
      // We allow decimal type inputs with different precision and scale, and use special formulas
      // to calculate the result precision and scale.
      TypeCheckResult.TypeCheckSuccess
    case _ => super.checkInputDataTypes()
  }

  override def dataType: DataType = internalDataType

  // When `spark.sql.decimalOperations.allowPrecisionLoss` is set to true, if the precision / scale
  // needed are out of the range of available values, the scale is reduced up to 6, in order to
  // prevent the truncation of the integer part of the decimals.
  protected def allowPrecisionLoss: Boolean = SQLConf.get.decimalOperationsAllowPrecisionLoss

  protected def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    throw SparkException.internalError(
      s"${getClass.getSimpleName} must override `resultDecimalType`.")
  }

  override def nullable: Boolean = super.nullable || evalMode == EvalMode.TRY || {
    if (left.dataType.isInstanceOf[DecimalType]) {
      // For decimal arithmetic, we may return null even if both inputs are not null, if overflow
      // happens and this `failOnError` flag is false.
      evalMode != EvalMode.ANSI
    } else {
      // For non-decimal arithmetic, the calculation always return non-null result when inputs are
      // not null. If overflow happens, we return either the overflowed value or fail.
      false
    }
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(BINARY_ARITHMETIC)

  override def initQueryContext(): Option[QueryContext] = {
    if (failOnError) {
      Some(origin.context)
    } else {
      None
    }
  }

  protected def checkDecimalOverflow(value: Decimal, precision: Int, scale: Int): Decimal = {
    value.toPrecision(precision, scale, Decimal.ROUND_HALF_UP, !failOnError, getContextOrNull())
  }

  /** Name of the function for this expression on a [[Decimal]] type. */
  def decimalMethod: String =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError(this.getClass.getName,
      "decimalMethod", "genCode")

  /** Name of the function for this expression on a [[CalendarInterval]] type. */
  def calendarIntervalMethod: String =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError(this.getClass.getName,
      "calendarIntervalMethod", "genCode")

  protected def isAnsiInterval: Boolean = dataType.isInstanceOf[AnsiIntervalType]

  // Name of the function for the exact version of this expression in [[Math]].
  // If the option "spark.sql.ansi.enabled" is enabled and there is corresponding
  // function in [[Math]], the exact function will be called instead of evaluation with [[symbol]].
  def exactMathMethod: Option[String] = None

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case DecimalType.Fixed(precision, scale) =>
      val errorContextCode = getContextOrNullCode(ctx, failOnError)
      val updateIsNull = if (failOnError) {
        ""
      } else {
        s"${ev.isNull} = ${ev.value} == null;"
      }
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
           |${ev.value} = $eval1.$decimalMethod($eval2).toPrecision(
           |  $precision, $scale, Decimal.ROUND_HALF_UP(), ${!failOnError}, $errorContextCode);
           |$updateIsNull
       """.stripMargin
      })
    case CalendarIntervalType =>
      val iu = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$iu.$calendarIntervalMethod($eval1, $eval2)")
    case _: AnsiIntervalType =>
      assert(exactMathMethod.isDefined,
        s"The expression '$nodeName' must override the exactMathMethod() method " +
        "if it is supposed to operate over interval types.")
      val mathUtils = IntervalMathUtils.getClass.getCanonicalName.stripSuffix("$")
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$mathUtils.${exactMathMethod.get}($eval1, $eval2)")
    // byte and short are casted into int when add, minus, times or divide
    case ByteType | ShortType =>
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        val tmpResult = ctx.freshName("tmpResult")
        val overflowCheck = if (failOnError) {
          val javaType = CodeGenerator.boxedType(dataType)
          s"""
             |if ($tmpResult < $javaType.MIN_VALUE || $tmpResult > $javaType.MAX_VALUE) {
             |  throw QueryExecutionErrors.binaryArithmeticCauseOverflowError(
             |  $eval1, "$symbol", $eval2);
             |}
           """.stripMargin
        } else {
          ""
        }
        s"""
           |${CodeGenerator.JAVA_INT} $tmpResult = $eval1 $symbol $eval2;
           |$overflowCheck
           |${ev.value} = (${CodeGenerator.javaType(dataType)})($tmpResult);
         """.stripMargin
      })
    case IntegerType | LongType if failOnError && exactMathMethod.isDefined =>
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        val errorContext = getContextOrNullCode(ctx)
        val mathUtils = MathUtils.getClass.getCanonicalName.stripSuffix("$")
        s"""
           |${ev.value} = $mathUtils.${exactMathMethod.get}($eval1, $eval2, $errorContext);
         """.stripMargin
      })

    case IntegerType | LongType | DoubleType | FloatType =>
      // When Double/Float overflows, there can be 2 cases:
      // - precision loss: according to SQL standard, the number is truncated;
      // - returns (+/-)Infinite: same behavior also other DBs have (e.g. Postgres)
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
           |${ev.value} = $eval1 $symbol $eval2;
         """.stripMargin
      })
  }

  override def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String): ExprCode = {
    if (evalMode == EvalMode.TRY) {
      val tryBlock: (String, String) => String = (eval1, eval2) => {
        s"""
           |try {
           | ${f(eval1, eval2)}
           |} catch (Exception e) {
           | ${ev.isNull} = true;
           |}
           |""".stripMargin
      }
      super.nullSafeCodeGen(ctx, ev, tryBlock)
    } else {
      super.nullSafeCodeGen(ctx, ev, f)
    }
  }

  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        if (evalMode == EvalMode.TRY) {
          try {
            nullSafeEval(value1, value2)
          } catch {
            case _: Exception =>
              null
          }
        } else {
          nullSafeEval(value1, value2)
        }
      }
    }
  }
}

object BinaryArithmetic {
  def unapply(e: BinaryArithmetic): Option[(Expression, Expression)] = Some((e.left, e.right))
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`+`expr2`.",
  examples = """
    Examples:
      > SELECT 1 _FUNC_ 2;
       3
  """,
  since = "1.0.0",
  group = "math_funcs")
case class Add(
    left: Expression,
    right: Expression,
    evalMode: EvalMode.Value = EvalMode.fromSQLConf(SQLConf.get)) extends BinaryArithmetic
  with CommutativeExpression {

  def this(left: Expression, right: Expression) =
    this(left, right, EvalMode.fromSQLConf(SQLConf.get))

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  override def decimalMethod: String = "$plus"

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: max(s1, s2) + max(p1-s1, p2-s2) + 1
  // Result Scale:     max(s1, s2)
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = max(p1 - s1, p2 - s2) + resultScale + 1
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }

  override def calendarIntervalMethod: String = if (failOnError) "addExact" else "add"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = dataType match {
    case DecimalType.Fixed(precision, scale) =>
      checkDecimalOverflow(numeric.plus(input1, input2).asInstanceOf[Decimal], precision, scale)
    case CalendarIntervalType if failOnError =>
      IntervalUtils.addExact(
        input1.asInstanceOf[CalendarInterval], input2.asInstanceOf[CalendarInterval])
    case CalendarIntervalType =>
      IntervalUtils.add(
        input1.asInstanceOf[CalendarInterval], input2.asInstanceOf[CalendarInterval])
    case _: DayTimeIntervalType =>
      IntervalMathUtils.addExact(input1.asInstanceOf[Long], input2.asInstanceOf[Long])
    case _: YearMonthIntervalType =>
      IntervalMathUtils.addExact(input1.asInstanceOf[Int], input2.asInstanceOf[Int])
    case _: IntegerType if failOnError =>
      MathUtils.addExact(input1.asInstanceOf[Int], input2.asInstanceOf[Int], getContextOrNull())
    case _: LongType if failOnError =>
      MathUtils.addExact(input1.asInstanceOf[Long], input2.asInstanceOf[Long], getContextOrNull())
    case _ => numeric.plus(input1, input2)
  }

  override def exactMathMethod: Option[String] = Some("addExact")

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Add =
    copy(left = newLeft, right = newRight)

  override lazy val canonicalized: Expression = {
    // TODO: do not reorder consecutive `Add`s with different `evalMode`
    val reorderResult = buildCanonicalizedPlan(
      { case Add(l, r, _) => Seq(l, r) },
      { case (l: Expression, r: Expression) => Add(l, r, evalMode)},
      Some(evalMode)
    )
    if (resolved && reorderResult.resolved && reorderResult.dataType == dataType) {
      reorderResult
    } else {
      // SPARK-40903: Avoid reordering decimal Add for canonicalization if the result data type is
      // changed, which may cause data checking error within ComplexTypeMergingExpression.
      withCanonicalizedChildren
    }
  }
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`-`expr2`.",
  examples = """
    Examples:
      > SELECT 2 _FUNC_ 1;
       1
  """,
  since = "1.0.0",
  group = "math_funcs")
case class Subtract(
    left: Expression,
    right: Expression,
    evalMode: EvalMode.Value = EvalMode.fromSQLConf(SQLConf.get)) extends BinaryArithmetic {

  def this(left: Expression, right: Expression) =
    this(left, right, EvalMode.fromSQLConf(SQLConf.get))

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  override def decimalMethod: String = "$minus"

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: max(s1, s2) + max(p1-s1, p2-s2) + 1
  // Result Scale:     max(s1, s2)
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = max(p1 - s1, p2 - s2) + resultScale + 1
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }

  override def calendarIntervalMethod: String = if (failOnError) "subtractExact" else "subtract"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = dataType match {
    case DecimalType.Fixed(precision, scale) =>
      checkDecimalOverflow(numeric.minus(input1, input2).asInstanceOf[Decimal], precision, scale)
    case CalendarIntervalType if failOnError =>
      IntervalUtils.subtractExact(
        input1.asInstanceOf[CalendarInterval], input2.asInstanceOf[CalendarInterval])
    case CalendarIntervalType =>
      IntervalUtils.subtract(
        input1.asInstanceOf[CalendarInterval], input2.asInstanceOf[CalendarInterval])
    case _: DayTimeIntervalType =>
      IntervalMathUtils.subtractExact(input1.asInstanceOf[Long], input2.asInstanceOf[Long])
    case _: YearMonthIntervalType =>
      IntervalMathUtils.subtractExact(input1.asInstanceOf[Int], input2.asInstanceOf[Int])
    case _: IntegerType if failOnError =>
      MathUtils.subtractExact(
        input1.asInstanceOf[Int],
        input2.asInstanceOf[Int],
        getContextOrNull())
    case _: LongType if failOnError =>
      MathUtils.subtractExact(
        input1.asInstanceOf[Long],
        input2.asInstanceOf[Long],
        getContextOrNull())
    case _ => numeric.minus(input1, input2)
  }

  override def exactMathMethod: Option[String] = Some("subtractExact")

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Subtract = copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`*`expr2`.",
  examples = """
    Examples:
      > SELECT 2 _FUNC_ 3;
       6
  """,
  since = "1.0.0",
  group = "math_funcs")
case class Multiply(
    left: Expression,
    right: Expression,
    evalMode: EvalMode.Value = EvalMode.fromSQLConf(SQLConf.get)) extends BinaryArithmetic
  with CommutativeExpression {

  def this(left: Expression, right: Expression) =
    this(left, right, EvalMode.fromSQLConf(SQLConf.get))

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"
  override def decimalMethod: String = "$times"

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: p1 + p2 + 1
  // Result Scale:     s1 + s2
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = s1 + s2
    val resultPrecision = p1 + p2 + 1
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = dataType match {
    case DecimalType.Fixed(precision, scale) =>
      checkDecimalOverflow(numeric.times(input1, input2).asInstanceOf[Decimal], precision, scale)
    case _: IntegerType if failOnError =>
      MathUtils.multiplyExact(
        input1.asInstanceOf[Int],
        input2.asInstanceOf[Int],
        getContextOrNull())
    case _: LongType if failOnError =>
      MathUtils.multiplyExact(
        input1.asInstanceOf[Long],
        input2.asInstanceOf[Long],
        getContextOrNull())
    case _ => numeric.times(input1, input2)
  }

  override def exactMathMethod: Option[String] = Some("multiplyExact")

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Multiply = copy(left = newLeft, right = newRight)

  override lazy val canonicalized: Expression = {
    // TODO: do not reorder consecutive `Multiply`s with different `evalMode`
    buildCanonicalizedPlan(
      { case Multiply(l, r, _) => Seq(l, r) },
      { case (l: Expression, r: Expression) => Multiply(l, r, evalMode)},
      Some(evalMode)
    )
  }
}

// Common base trait for Divide and Remainder, since these two classes are almost identical
trait DivModLike extends BinaryArithmetic {

  protected def decimalToDataTypeCodeGen(decimalResult: String): String = decimalResult

  // Whether we should check overflow or not in ANSI mode.
  protected def checkDivideOverflow: Boolean = false

  override def nullable: Boolean = true

  private lazy val isZero: Any => Boolean = right.dataType match {
    case _: DecimalType => x => x.asInstanceOf[Decimal].isZero
    case _ => x => x == 0
  }

  final override def eval(input: InternalRow): Any = {
    // evaluate right first as we have a chance to skip left if right is 0
    val input2 = right.eval(input)
    if (input2 == null || (!failOnError && isZero(input2))) {
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        if (isZero(input2)) {
          // when we reach here, failOnError must be true.
          throw QueryExecutionErrors.divideByZeroError(getContextOrNull())
        }
        if (checkDivideOverflow && input1 == Long.MinValue && input2 == -1) {
          throw QueryExecutionErrors.overflowInIntegralDivideError(getContextOrNull())
        }
        evalOperation(input1, input2)
      }
    }
  }

  def evalOperation(left: Any, right: Any): Any

  /**
   * Special case handling due to division/remainder by 0 => null or ArithmeticException.
   */
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val operandsDataType = left.dataType
    val isZero = if (operandsDataType.isInstanceOf[DecimalType]) {
      s"${eval2.value}.isZero()"
    } else {
      s"${eval2.value} == 0"
    }
    val javaType = CodeGenerator.javaType(dataType)
    val errorContextCode = getContextOrNullCode(ctx, failOnError)
    val operation = super.dataType match {
      case DecimalType.Fixed(precision, scale) =>
        val decimalValue = ctx.freshName("decimalValue")
        s"""
           |Decimal $decimalValue = ${eval1.value}.$decimalMethod(${eval2.value}).toPrecision(
           |  $precision, $scale, Decimal.ROUND_HALF_UP(), ${!failOnError}, $errorContextCode);
           |if ($decimalValue != null) {
           |  ${ev.value} = ${decimalToDataTypeCodeGen(s"$decimalValue")};
           |} else {
           |  ${ev.isNull} = true;
           |}
           |""".stripMargin
      case _ => s"${ev.value} = ($javaType)(${eval1.value} $symbol ${eval2.value});"
    }
    val checkIntegralDivideOverflow = if (checkDivideOverflow) {
      s"""
        |if (${eval1.value} == ${Long.MinValue}L && ${eval2.value} == -1)
        |  throw QueryExecutionErrors.overflowInIntegralDivideError($errorContextCode);
        |""".stripMargin
    } else {
      ""
    }

    // evaluate right first as we have a chance to skip left if right is 0
    if (!left.nullable && !right.nullable) {
      val divByZero = if (failOnError) {
        s"throw QueryExecutionErrors.divideByZeroError($errorContextCode);"
      } else {
        s"${ev.isNull} = true;"
      }
      ev.copy(code = code"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if ($isZero) {
          $divByZero
        } else {
          ${eval1.code}
          $checkIntegralDivideOverflow
          $operation
        }""")
    } else {
      val nullOnErrorCondition = if (failOnError) "" else s" || $isZero"
      val failOnErrorBranch = if (failOnError) {
        s"if ($isZero) throw QueryExecutionErrors.divideByZeroError($errorContextCode);"
      } else {
        ""
      }
      ev.copy(code = code"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (${eval2.isNull}$nullOnErrorCondition) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          if (${eval1.isNull}) {
            ${ev.isNull} = true;
          } else {
            $failOnErrorBranch
            $checkIntegralDivideOverflow
            $operation
          }
        }""")
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`/`expr2`. It always performs floating point division.",
  examples = """
    Examples:
      > SELECT 3 _FUNC_ 2;
       1.5
      > SELECT 2L _FUNC_ 2L;
       1.0
  """,
  since = "1.0.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class Divide(
    left: Expression,
    right: Expression,
    evalMode: EvalMode.Value = EvalMode.fromSQLConf(SQLConf.get)) extends DivModLike {

  def this(left: Expression, right: Expression) =
    this(left, right, EvalMode.fromSQLConf(SQLConf.get))

  // `try_divide` has exactly the same behavior as the legacy divide, so here it only executes
  // the error code path when `evalMode` is `ANSI`.
  protected override def failOnError: Boolean = evalMode == EvalMode.ANSI

  override def inputType: AbstractDataType = TypeCollection(DoubleType, DecimalType)

  override def symbol: String = "/"
  override def decimalMethod: String = "$div"

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: p1 - s1 + s2 + max(6, s1 + p2 + 1)
  // Result Scale:     max(6, s1 + p2 + 1)
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    if (allowPrecisionLoss) {
      val intDig = p1 - s1 + s2
      val scale = max(DecimalType.MINIMUM_ADJUSTED_SCALE, s1 + p2 + 1)
      val prec = intDig + scale
      DecimalType.adjustPrecisionScale(prec, scale)
    } else {
      var intDig = min(DecimalType.MAX_SCALE, p1 - s1 + s2)
      var decDig = min(DecimalType.MAX_SCALE, max(DecimalType.MINIMUM_ADJUSTED_SCALE, s1 + p2 + 1))
      val diff = (intDig + decDig) - DecimalType.MAX_SCALE
      if (diff > 0) {
        decDig -= diff / 2 + 1
        intDig = DecimalType.MAX_SCALE - decDig
      }
      DecimalType.bounded(intDig + decDig, decDig)
    }
  }

  private lazy val div: (Any, Any) => Any = dataType match {
    case d @ DecimalType.Fixed(precision, scale) =>
      val fractional = PhysicalDecimalType(precision, scale).fractional
      (l, r) => {
      val value = fractional.asInstanceOf[Fractional[Any]].div(l, r)
      checkDecimalOverflow(value.asInstanceOf[Decimal], precision, scale)
    }
    case ft: FractionalType => PhysicalFractionalType.fractional(ft).div
  }

  override def evalOperation(left: Any, right: Any): Any = div(left, right)

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Divide = copy(left = newLeft, right = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Divide `expr1` by `expr2`. It returns NULL if an operand is NULL or `expr2` is 0. The result is casted to long.",
  examples = """
    Examples:
      > SELECT 3 _FUNC_ 2;
       1
      > SELECT INTERVAL '1-1' YEAR TO MONTH _FUNC_ INTERVAL '-1' MONTH;
       -13
  """,
  since = "3.0.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class IntegralDivide(
    left: Expression,
    right: Expression,
    evalMode: EvalMode.Value = EvalMode.fromSQLConf(SQLConf.get)) extends DivModLike {

  def this(left: Expression, right: Expression) = this(left, right,
    EvalMode.fromSQLConf(SQLConf.get))

  override def checkDivideOverflow: Boolean = left.dataType match {
    case LongType if failOnError => true
    case _ => false
  }

  override def inputType: AbstractDataType = TypeCollection(
    LongType, DecimalType, YearMonthIntervalType, DayTimeIntervalType)

  override def dataType: DataType = LongType

  override def symbol: String = "/"
  override def decimalMethod: String = "quot"
  override def decimalToDataTypeCodeGen(decimalResult: String): String = s"$decimalResult.toLong()"

  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    // This follows division rule
    val intDig = p1 - s1 + s2
    // No precision loss can happen as the result scale is 0.
    // If intDig is 0 that means the result data is 0, to be safe we use decimal(1, 0)
    // to represent 0.
    DecimalType.bounded(if (intDig == 0) 1 else intDig, 0)
  }

  override def sqlOperator: String = "div"

  private lazy val div: (Any, Any) => Any = {
    val integral = left.dataType match {
      case i: IntegralType =>
        PhysicalIntegralType.integral(i)
      case DecimalType.Fixed(p, s) =>
        PhysicalDecimalType(p, s).asIntegral.asInstanceOf[Integral[Any]]
      case _: YearMonthIntervalType =>
        PhysicalIntegerType.integral.asInstanceOf[Integral[Any]]
      case _: DayTimeIntervalType =>
        PhysicalLongType.integral.asInstanceOf[Integral[Any]]
    }
    (x, y) => {
      val res = super.dataType match {
        case DecimalType.Fixed(precision, scale) =>
          checkDecimalOverflow(integral.quot(x, y).asInstanceOf[Decimal], precision, scale)
        case _ => integral.quot(x, y)
      }
      if (res == null) {
        null
      } else {
        integral.toLong(res)
      }
    }
  }

  override def evalOperation(left: Any, right: Any): Any = div(left, right)

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): IntegralDivide =
    copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns the remainder after `expr1`/`expr2`.",
  examples = """
    Examples:
      > SELECT 2 % 1.8;
       0.2
      > SELECT MOD(2, 1.8);
       0.2
  """,
  since = "1.0.0",
  group = "math_funcs")
case class Remainder(
    left: Expression,
    right: Expression,
    evalMode: EvalMode.Value = EvalMode.fromSQLConf(SQLConf.get)) extends DivModLike {

  def this(left: Expression, right: Expression) =
    this(left, right, EvalMode.fromSQLConf(SQLConf.get))

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "%"
  override def decimalMethod: String = "remainder"

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: min(p1-s1, p2-s2) + max(s1, s2)
  // Result Scale:     max(s1, s2)
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = min(p1 - s1, p2 - s2) + resultScale
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }

  override def toString: String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse(sqlOperator) match {
      case operator if operator == sqlOperator => s"($left $sqlOperator $right)"
      case funcName => s"$funcName($left, $right)"
    }
  }
  override def sql: String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse(sqlOperator) match {
      case operator if operator == sqlOperator => s"(${left.sql} $sqlOperator ${right.sql})"
      case funcName => s"$funcName(${left.sql}, ${right.sql})"
    }
  }

  private lazy val mod: (Any, Any) => Any = dataType match {
    // special cases to make float/double primitive types faster
    case DoubleType =>
      (left, right) => left.asInstanceOf[Double] % right.asInstanceOf[Double]
    case FloatType =>
      (left, right) => left.asInstanceOf[Float] % right.asInstanceOf[Float]

    // catch-all cases
    case i: IntegralType =>
      val integral = PhysicalIntegralType.integral(i)
      (left, right) => integral.rem(left, right)

    case d @ DecimalType.Fixed(precision, scale) =>
      val integral = PhysicalDecimalType(precision, scale).asIntegral.asInstanceOf[Integral[Any]]
      (left, right) =>
        checkDecimalOverflow(integral.rem(left, right).asInstanceOf[Decimal], precision, scale)
  }

  override def evalOperation(left: Any, right: Any): Any = mod(left, right)

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Remainder = copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns the positive value of `expr1` mod `expr2`.",
  examples = """
    Examples:
      > SELECT _FUNC_(10, 3);
       1
      > SELECT _FUNC_(-10, 3);
       2
  """,
  since = "1.5.0",
  group = "math_funcs")
case class Pmod(
    left: Expression,
    right: Expression,
    evalMode: EvalMode.Value = EvalMode.fromSQLConf(SQLConf.get)) extends BinaryArithmetic {

  def this(left: Expression, right: Expression) =
    this(left, right, EvalMode.fromSQLConf(SQLConf.get))

  override def toString: String = s"pmod($left, $right)"

  override def symbol: String = "pmod"

  override def inputType: AbstractDataType = NumericType

  override def nullable: Boolean = true

  override def decimalMethod: String = "remainder"

  // This follows Remainder rule
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = min(p1 - s1, p2 - s2) + resultScale
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }

  private lazy val isZero: Any => Boolean = right.dataType match {
    case _: DecimalType => x => x.asInstanceOf[Decimal].isZero
    case _ => x => x == 0
  }

  private lazy val pmodFunc: (Any, Any) => Any = dataType match {
    case _: IntegerType => (l, r) => pmod(l.asInstanceOf[Int], r.asInstanceOf[Int])
    case _: LongType => (l, r) => pmod(l.asInstanceOf[Long], r.asInstanceOf[Long])
    case _: ShortType => (l, r) => pmod(l.asInstanceOf[Short], r.asInstanceOf[Short])
    case _: ByteType => (l, r) => pmod(l.asInstanceOf[Byte], r.asInstanceOf[Byte])
    case _: FloatType => (l, r) => pmod(l.asInstanceOf[Float], r.asInstanceOf[Float])
    case _: DoubleType => (l, r) => pmod(l.asInstanceOf[Double], r.asInstanceOf[Double])
    case DecimalType.Fixed(precision, scale) => (l, r) => checkDecimalOverflow(
      pmod(l.asInstanceOf[Decimal], r.asInstanceOf[Decimal]), precision, scale)
  }

  final override def eval(input: InternalRow): Any = {
    // evaluate right first as we have a chance to skip left if right is 0
    val input2 = right.eval(input)
    if (input2 == null || (!failOnError && isZero(input2))) {
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        if (isZero(input2)) {
          // when we reach here, failOnError must bet true.
          throw QueryExecutionErrors.divideByZeroError(getContextOrNull())
        }
        pmodFunc(input1, input2)
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val isZero = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval2.value}.isZero()"
    } else {
      s"${eval2.value} == 0"
    }
    val remainder = ctx.freshName("remainder")
    val javaType = CodeGenerator.javaType(dataType)
    val errorContext = getContextOrNullCode(ctx)
    val result = dataType match {
      case DecimalType.Fixed(precision, scale) =>
        val decimalAdd = "$plus"
        s"""
           |$javaType $remainder = ${eval1.value}.$decimalMethod(${eval2.value});
           |if ($remainder.compare(new org.apache.spark.sql.types.Decimal().set(0)) < 0) {
           |  ${ev.value}=($remainder.$decimalAdd(${eval2.value})).$decimalMethod(${eval2.value});
           |} else {
           |  ${ev.value}=$remainder;
           |}
           |${ev.value} = ${ev.value}.toPrecision(
           |  $precision, $scale, Decimal.ROUND_HALF_UP(), ${!failOnError}, $errorContext);
           |${ev.isNull} = ${ev.value} == null;
           |""".stripMargin

      // byte and short are casted into int when add, minus, times or divide
      case ByteType | ShortType =>
        s"""
          $javaType $remainder = ($javaType)(${eval1.value} % ${eval2.value});
          if ($remainder < 0) {
            ${ev.value}=($javaType)(($remainder + ${eval2.value}) % ${eval2.value});
          } else {
            ${ev.value}=$remainder;
          }
        """
      case _ =>
        s"""
          $javaType $remainder = ${eval1.value} % ${eval2.value};
          if ($remainder < 0) {
            ${ev.value}=($remainder + ${eval2.value}) % ${eval2.value};
          } else {
            ${ev.value}=$remainder;
          }
        """
    }

    // evaluate right first as we have a chance to skip left if right is 0
    if (!left.nullable && !right.nullable) {
      val divByZero = if (failOnError) {
        s"throw QueryExecutionErrors.divideByZeroError($errorContext);"
      } else {
        s"${ev.isNull} = true;"
      }
      ev.copy(code = code"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if ($isZero) {
          $divByZero
        } else {
          ${eval1.code}
          $result
        }""")
    } else {
      val nullOnErrorCondition = if (failOnError) "" else s" || $isZero"
      val failOnErrorBranch = if (failOnError) {
        s"if ($isZero) throw QueryExecutionErrors.divideByZeroError($errorContext);"
      } else {
        ""
      }
      ev.copy(code = code"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (${eval2.isNull}$nullOnErrorCondition) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          if (${eval1.isNull}) {
            ${ev.isNull} = true;
          } else {
            $failOnErrorBranch
            $result
          }
        }""")
    }
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
    if (r != null && r.compare(Decimal.ZERO) < 0) {(r + n) % n} else r
  }

  override def sql: String = s"$prettyName(${left.sql}, ${right.sql})"

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Pmod =
    copy(left = newLeft, right = newRight)
}

/**
 * A function that returns the least value of all parameters, skipping null values.
 * It takes at least 2 parameters, and returns null iff all parameters are null.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, ...) - Returns the least value of all parameters, skipping null values.",
  examples = """
    Examples:
      > SELECT _FUNC_(10, 9, 2, 4, 3);
       2
  """,
  since = "1.5.0",
  group = "math_funcs")
case class Least(children: Seq[Expression]) extends ComplexTypeMergingExpression
  with CommutativeExpression {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName), Seq("> 1"), children.length
      )
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      DataTypeMismatch(
        errorSubClass = "DATA_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> toSQLId(prettyName),
          "dataType" -> children.map(_.dataType).map(toSQLType).mkString("[", ", ", "]")
        )
      )
    } else {
      TypeUtils.checkForOrderingExpr(dataType, prettyName)
    }
  }

  override def eval(input: InternalRow): Any = {
    children.foldLeft[Any](null)((r, c) => {
      val evalc = c.eval(input)
      if (evalc != null) {
        if (r == null || ordering.lt(evalc, r)) evalc else r
      } else {
        r
      }
    })
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    ev.isNull = JavaCode.isNullGlobal(ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, ev.isNull))
    val evals = evalChildren.map(eval =>
      s"""
         |${eval.code}
         |${ctx.reassignIfSmaller(dataType, ev, eval)}
      """.stripMargin
    )

    val resultType = CodeGenerator.javaType(dataType)
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = evals,
      funcName = "least",
      extraArguments = Seq(resultType -> ev.value),
      returnType = resultType,
      makeSplitFunction = body =>
        s"""
          |$body
          |return ${ev.value};
        """.stripMargin,
      foldFunctions = _.map(funcCall => s"${ev.value} = $funcCall;").mkString("\n"))
    ev.copy(code =
      code"""
         |${ev.isNull} = true;
         |$resultType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |$codes
      """.stripMargin)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Least =
    copy(children = newChildren)

  override lazy val canonicalized: Expression = {
    Least(orderCommutative({ case Least(children) => children }))
  }
}

/**
 * A function that returns the greatest value of all parameters, skipping null values.
 * It takes at least 2 parameters, and returns null iff all parameters are null.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, ...) - Returns the greatest value of all parameters, skipping null values.",
  examples = """
    Examples:
      > SELECT _FUNC_(10, 9, 2, 4, 3);
       10
  """,
  since = "1.5.0",
  group = "math_funcs")
case class Greatest(children: Seq[Expression]) extends ComplexTypeMergingExpression
  with CommutativeExpression {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName), Seq("> 1"), children.length
      )
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      DataTypeMismatch(
        errorSubClass = "DATA_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> toSQLId(prettyName),
          "dataType" -> children.map(_.dataType).map(toSQLType).mkString("[", ", ", "]")
        )
      )
    } else {
      TypeUtils.checkForOrderingExpr(dataType, prettyName)
    }
  }

  override def eval(input: InternalRow): Any = {
    children.foldLeft[Any](null)((r, c) => {
      val evalc = c.eval(input)
      if (evalc != null) {
        if (r == null || ordering.gt(evalc, r)) evalc else r
      } else {
        r
      }
    })
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    ev.isNull = JavaCode.isNullGlobal(ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, ev.isNull))
    val evals = evalChildren.map(eval =>
      s"""
         |${eval.code}
         |${ctx.reassignIfGreater(dataType, ev, eval)}
      """.stripMargin
    )

    val resultType = CodeGenerator.javaType(dataType)
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = evals,
      funcName = "greatest",
      extraArguments = Seq(resultType -> ev.value),
      returnType = resultType,
      makeSplitFunction = body =>
        s"""
           |$body
           |return ${ev.value};
        """.stripMargin,
      foldFunctions = _.map(funcCall => s"${ev.value} = $funcCall;").mkString("\n"))
    ev.copy(code =
      code"""
         |${ev.isNull} = true;
         |$resultType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |$codes
      """.stripMargin)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Greatest =
    copy(children = newChildren)

  override lazy val canonicalized: Expression = {
    Greatest(orderCommutative({ case Greatest(children) => children }))
  }
}
