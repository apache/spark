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

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CharType, DataType, DateType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampLTZNanosType, TimestampNTZNanosType, TimestampNTZType, TimestampType, TimeType, VarcharType}
import org.apache.spark.unsafe.types.UTF8String

class ExprUtilsSuite extends SparkFunSuite {

  private val a = AttributeReference("a", LongType)()
  private val ansi = NumericEvalContext(EvalMode.ANSI)
  private val legacy = NumericEvalContext(EvalMode.LEGACY)
  private val tryMode = NumericEvalContext(EvalMode.TRY)

  // Evaluates `build` over input columns of type `t` on every pair of `values`, and NULL when
  // the columns are nullable, without and with code generation.
  private def evalPairs(t: DataType, values: Seq[Any])(
      build: (Expression, Expression) => Expression): Unit = {
    Seq(true, false).foreach { nullable =>
      val e = build(BoundReference(0, t, nullable), BoundReference(1, t, nullable))
      val projection = GenerateUnsafeProjection.generate(Seq(e))
      val inputs = if (nullable) values :+ null else values
      for (l <- inputs; r <- inputs) {
        val row = InternalRow(l, r)
        e.eval(row)
        projection(row)
      }
    }
  }

  private def evalEach(t: DataType, values: Seq[Any])(build: Expression => Expression): Unit =
    evalPairs(t, values)((l, _) => build(l))

  test("canEvaluateUnconditionally: allowlisted total expressions") {
    assert(ExprUtils.canEvaluateUnconditionally(a))
    assert(ExprUtils.canEvaluateUnconditionally(Literal(1L)))
    assert(ExprUtils.canEvaluateUnconditionally(
      And(LessThan(a, Literal(5L)), IsNotNull(a))))
    assert(ExprUtils.canEvaluateUnconditionally(
      GetStructField(
        AttributeReference("s", StructType(StructField("f1", IntegerType) :: Nil))(), 0)))
    assert(ExprUtils.canEvaluateUnconditionally(Coalesce(Seq(a, Literal(0L)))))
    assert(ExprUtils.canEvaluateUnconditionally(In(a, Seq(Literal(1L), Literal(2L)))))
  }

  test("canEvaluateUnconditionally: expressions that can throw are excluded") {
    // Arithmetic can throw (overflow, div-by-zero in ANSI mode) even though it does not
    // override `throwable` and inherits false from its non-throwing children, which is
    // why the allowlist is used instead of the throwable flag.
    val arithmetic = LessThan(Add(a, Literal(1L), ansi), Literal(5L))
    assert(!arithmetic.throwable)
    assert(!ExprUtils.canEvaluateUnconditionally(arithmetic))
    assert(!ExprUtils.canEvaluateUnconditionally(
      EqualTo(Remainder(a, Literal(3L), ansi), Literal(0L))))
    assert(!ExprUtils.canEvaluateUnconditionally(
      Cast(AttributeReference("s", StringType)(), IntegerType, evalMode = EvalMode.ANSI)))
    // GetArrayItem/ElementAt can throw on invalid ordinals in ANSI mode.
    val arr = AttributeReference("arr", ArrayType(IntegerType))()
    assert(!ExprUtils.canEvaluateUnconditionally(GetArrayItem(arr, Literal(0))))
    // An allowlisted predicate over a child that is not allowlisted is still excluded.
    assert(!ExprUtils.canEvaluateUnconditionally(IsNotNull(Add(a, Literal(1L), ansi))))
  }

  test("SPARK-60882: canEvaluateUnconditionally: size and numeric up-casts are included") {
    val arr = AttributeReference("arr", ArrayType(IntegerType))()
    val i = AttributeReference("i", IntegerType)()
    assert(ExprUtils.canEvaluateUnconditionally(GreaterThan(Size(arr), Literal(0))))
    assert(ExprUtils.canEvaluateUnconditionally(
      EqualTo(Cast(i, LongType, evalMode = EvalMode.ANSI), a)))
    assert(ExprUtils.canEvaluateUnconditionally(Cast(i, DoubleType, evalMode = EvalMode.ANSI)))
    assert(ExprUtils.canEvaluateUnconditionally(
      Cast(i, DecimalType(20, 0), evalMode = EvalMode.ANSI)))
    // A decimal to an integral type is an up-cast too, but can overflow in ANSI mode.
    assert(Cast.canUpCast(DecimalType(19, 0), LongType))
    assert(!ExprUtils.canEvaluateUnconditionally(Cast(AttributeReference("d", DecimalType(19, 0))(),
      LongType, evalMode = EvalMode.ANSI)))
    intercept[ArithmeticException](Cast(Literal(Decimal(BigDecimal("9" * 19)), DecimalType(19, 0)),
      LongType, evalMode = EvalMode.ANSI).eval())
    // Narrowing casts can fail in ANSI mode. LONG to TIMESTAMP is an up-cast too, but only the
    // numeric ones are taken.
    assert(!ExprUtils.canEvaluateUnconditionally(Cast(a, IntegerType, evalMode = EvalMode.ANSI)))
    assert(Cast.canUpCast(LongType, TimestampType))
    assert(!ExprUtils.canEvaluateUnconditionally(
      Cast(a, TimestampType, evalMode = EvalMode.ANSI)))
    // A size over an expression that can fail is still excluded.
    assert(!ExprUtils.canEvaluateUnconditionally(Size(ElementAt(
      AttributeReference("nested", ArrayType(ArrayType(IntegerType)))(), Literal(1)))))
  }

  test("SPARK-60882: canEvaluateUnconditionally: string prefix, suffix, containment and " +
    "length checks and a field of a struct built from plain values are included") {
    val s = AttributeReference("s", StringType)()
    // `LIKE 'ab%c'` becomes the last one.
    Seq(StartsWith(s, Literal("abc")), EndsWith(s, Literal("abc")), Contains(s, s),
        GreaterThan(Length(s), Literal(1)),
        And(GreaterThanOrEqual(OctetLength(s), Literal(3)),
          And(StartsWith(s, Literal("ab")), EndsWith(s, Literal("c"))))).foreach {
      p => assert(ExprUtils.canEvaluateUnconditionally(p), p)
    }
    val bin = AttributeReference("bin", BinaryType)()
    assert(ExprUtils.canEvaluateUnconditionally(GreaterThan(Length(bin), Literal(1))))
    assert(ExprUtils.canEvaluateUnconditionally(GreaterThan(OctetLength(bin), Literal(1))))
    // They do not raise for any collation, also on empty, ignorable and invalid UTF-8 strings,
    // which ICU can not search for: a combining accent after a letter and alone, a zero-width
    // space, NUL, U+FFFD, a bad lead byte, a truncated sequence and one above U+10FFFF.
    val ignorable = Seq(Seq('a', 0x0301), Seq(0x0301), Seq(0x200B), Seq(0x0000), Seq(0xFFFD))
      .map(_.map(_.toChar).mkString)
    val invalid = Seq(Array(0xFF, 'a'), Array('a', 0xE2), Array(0xF4, 0xBF, 0xBF, 0xBF))
      .map(b => UTF8String.fromBytes(b.map(_.toByte)))
    val values = (Seq("", " ", "a", "abc") ++ ignorable).map(UTF8String.fromString) ++ invalid
    Seq("UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI", "UNICODE_AI", "UNICODE_CI_AI",
        "UTF8_BINARY_RTRIM", "UNICODE_RTRIM", "UTF8_LCASE_RTRIM", "sr_CI_AI").foreach {
      collation =>
        val collated = StringType(collation)
        evalEach(collated, values)(Length(_))
        evalEach(collated, values)(OctetLength(_))
        Seq[(Expression, Expression) => Expression](StartsWith(_, _), EndsWith(_, _),
          Contains(_, _)).foreach(evalPairs(collated, values))
    }
    // LIKE and RLIKE raise on an invalid pattern.
    assert(!ExprUtils.canEvaluateUnconditionally(Like(s, Literal("abc%"), '\\')))
    assert(!ExprUtils.canEvaluateUnconditionally(RLike(s, Literal("abc"))))
    assert(!ExprUtils.canEvaluateUnconditionally(StartsWith(Upper(s), Literal("abc"))))
    assert(!ExprUtils.canEvaluateUnconditionally(Length(Upper(s))))

    // A struct built from plain values, and a field of it, but not when another field can fail.
    def field(values: Expression*): Expression = GetStructField(CreateNamedStruct(
      values.zipWithIndex.flatMap { case (v, i) => Seq(Literal(s"f$i"), v) }), 0)
    assert(ExprUtils.canEvaluateUnconditionally(GreaterThan(field(a, s), Literal(1L))))
    assert(!ExprUtils.canEvaluateUnconditionally(GreaterThan(field(a, Add(a, Literal(1L), ansi)),
      Literal(1L))))
    assert(ExprUtils.canEvaluateUnconditionally(CreateNamedStruct(Seq(Literal("f0"), a))))
    assert(!ExprUtils.canEvaluateUnconditionally(
      CreateNamedStruct(Seq(Literal("f0"), Add(a, Literal(1L), ansi)))))
  }

  test("SPARK-60882: canEvaluateUnconditionally: numeric arithmetic outside ANSI mode") {
    val i = AttributeReference("i", IntegerType)()
    def binary(ctx: NumericEvalContext): Seq[Expression] = Seq(Add(a, a, ctx),
      Subtract(a, a, ctx), Multiply(a, a, ctx), Divide(Cast(a, DoubleType), Literal(0.0), ctx),
      IntegralDivide(a, a, ctx), Remainder(a, a, ctx), Pmod(i, i, ctx))
    binary(legacy).foreach(e => assert(ExprUtils.canEvaluateUnconditionally(e), e))
    binary(ansi).foreach(e => assert(!ExprUtils.canEvaluateUnconditionally(e), e))
    assert(ExprUtils.canEvaluateUnconditionally(UnaryMinus(a, failOnError = false)))
    assert(ExprUtils.canEvaluateUnconditionally(Abs(a, failOnError = false)))
    assert(!ExprUtils.canEvaluateUnconditionally(UnaryMinus(a, failOnError = true)))
    assert(!ExprUtils.canEvaluateUnconditionally(Abs(a, failOnError = true)))
    // Not over a child that can fail.
    val failing = Add(a, a, ansi)
    assert(!ExprUtils.canEvaluateUnconditionally(Add(failing, a, legacy)))
    assert(!ExprUtils.canEvaluateUnconditionally(UnaryMinus(failing, failOnError = false)))
    assert(!ExprUtils.canEvaluateUnconditionally(Abs(failing, failOnError = false)))
    // TRY mode catches the errors of Add, Subtract and Multiply, and Divide and Remainder only
    // check a zero divisor in ANSI mode, but IntegralDivide and Pmod raise there.
    Seq(Add(a, a, tryMode), Subtract(a, a, tryMode), Multiply(a, a, tryMode),
        Divide(Cast(a, DoubleType), Literal(0.0), tryMode), Remainder(a, a, tryMode))
      .foreach(e => assert(ExprUtils.canEvaluateUnconditionally(e), e))
    Seq(IntegralDivide(Literal(1L), Literal(0L), tryMode), Pmod(Literal(1), Literal(0), tryMode))
      .foreach { e =>
        assert(!ExprUtils.canEvaluateUnconditionally(e), e)
        intercept[ArithmeticException](e.eval())
      }
    // IntegralDivide, Remainder and Pmod over decimals raise when the integral quotient has more
    // digits than the MathContext of `Decimal` holds.
    val big = Literal(Decimal(BigDecimal("9" * 38)), DecimalType(38, 0))
    val small = Literal(Decimal(BigDecimal("0.007"), 3, 3), DecimalType(3, 3))
    Seq(IntegralDivide(big, small, legacy), Remainder(big, small, legacy),
        Remainder(big, small, tryMode), Pmod(big, small, legacy)).foreach { e =>
      assert(!ExprUtils.canEvaluateUnconditionally(e), e)
      intercept[ArithmeticException](e.eval())
    }
    assert(ExprUtils.canEvaluateUnconditionally(Divide(big, small, legacy)))
    // Divide and the other arithmetic over decimals of different scales do not raise.
    Seq(legacy, tryMode).foreach { ctx =>
      assert(Divide(big, small, ctx).eval() == null)
    }
    // Arithmetic over intervals overflows in every mode.
    val maxInterval = Literal(Long.MaxValue, DayTimeIntervalType())
    val intervalAdd = Add(maxInterval, maxInterval, legacy)
    assert(!ExprUtils.canEvaluateUnconditionally(intervalAdd))
    intercept[ArithmeticException](intervalAdd.eval())

    // None of the accepted ones raises, also on the values that do in ANSI mode.
    val longs = (LongType, Seq(Long.MinValue, -1L, 0L, 1L, Long.MaxValue))
    val ints = (IntegerType, Seq(Int.MinValue, -1, 0, 1, Int.MaxValue))
    val shorts = (ShortType, Seq(Short.MinValue, -1.toShort, 0.toShort, Short.MaxValue))
    val bytes = (ByteType, Seq(Byte.MinValue, -1.toByte, 0.toByte, Byte.MaxValue))
    val floats = (FloatType, Seq(Float.NegativeInfinity, -1.0f, 0.0f, Float.NaN, Float.MaxValue))
    val doubles =
      (DoubleType, Seq(Double.NegativeInfinity, -1.0, 0.0, Double.NaN, Double.MaxValue))
    val decimals = (DecimalType(38, 0),
      Seq("-" + "9" * 38, "0", "1", "9" * 38).map(d => Decimal(BigDecimal(d))))
    // A small divisor makes the quotient overflow.
    val smallDecimals = (DecimalType(38, 37),
      Seq("-1", "0", "1E-37", "9." + "9" * 37).map(d => Decimal(BigDecimal(d), 38, 37)))
    val nonDecimals = Seq(longs, ints, shorts, bytes, floats, doubles)
    for ((t, values) <- nonDecimals ++ Seq(decimals, smallDecimals)) {
      Seq(legacy, tryMode).foreach { ctx =>
        Seq[(Expression, Expression) => Expression](Add(_, _, ctx), Subtract(_, _, ctx),
          Multiply(_, _, ctx)).foreach(evalPairs(t, values))
      }
      evalPairs(t, values)((l, _) => UnaryMinus(l, failOnError = false))
      evalPairs(t, values)((l, _) => Abs(l, failOnError = false))
    }
    for ((t, values) <- nonDecimals) {
      Seq(legacy, tryMode).foreach(ctx => evalPairs(t, values)(Remainder(_, _, ctx)))
      evalPairs(t, values)(Pmod(_, _, legacy))
    }
    evalPairs(longs._1, longs._2)(IntegralDivide(_, _, legacy))
    for ((t, values) <- Seq(doubles, decimals, smallDecimals); ctx <- Seq(legacy, tryMode)) {
      evalPairs(t, values)(Divide(_, _, ctx))
    }
    // Decimals of different precisions and scales, also without precision loss.
    for (allowLoss <- Seq(true, false); mode <- Seq(EvalMode.LEGACY, EvalMode.TRY);
        (l, r) <- Seq((decimals, smallDecimals), (smallDecimals, decimals),
          (decimals, (DecimalType(3, 3), Seq(Decimal(BigDecimal("0.007"), 3, 3)))))) {
      val ctx = NumericEvalContext(mode, allowLoss)
      Seq[(Expression, Expression) => Expression](Add(_, _, ctx), Subtract(_, _, ctx),
        Multiply(_, _, ctx), Divide(_, _, ctx)).foreach { build =>
        val e = build(BoundReference(0, l._1, nullable = true), BoundReference(1, r._1,
          nullable = true))
        val projection = GenerateUnsafeProjection.generate(Seq(e))
        for (lv <- l._2 :+ null; rv <- r._2 :+ null) {
          val row = InternalRow(lv, rv)
          e.eval(row)
          projection(row)
        }
      }
    }
  }

  test("SPARK-60882: canEvaluateUnconditionally: casts that can not fail") {
    val s = AttributeReference("s", StringType)()
    val ts = AttributeReference("ts", TimestampType)()
    val date = AttributeReference("date", DateType)()
    def cast(e: Expression, to: DataType, mode: EvalMode.Value): Cast =
      Cast(e, to, Some("America/Los_Angeles"), mode)
    // In every mode: a timestamp to a date, and a number or a boolean to a string.
    Seq(EvalMode.ANSI, EvalMode.LEGACY, EvalMode.TRY).foreach { mode =>
      Seq[(Expression, DataType)]((ts, DateType),
          (AttributeReference("ntz", TimestampNTZType)(), DateType), (a, StringType),
          (AttributeReference("d", DecimalType(10, 2))(), StringType),
          (AttributeReference("b", BooleanType)(), StringType)).foreach { case (e, to) =>
        assert(ExprUtils.canEvaluateUnconditionally(cast(e, to, mode)), (e, to, mode))
      }
    }
    // Not to a CHAR or VARCHAR, which checks the length.
    Seq[DataType](CharType(3), VarcharType(3)).foreach { to =>
      assert(!ExprUtils.canEvaluateUnconditionally(cast(a, to, EvalMode.LEGACY)), to)
      intercept[SparkRuntimeException](cast(Literal(12345L), to, EvalMode.LEGACY).eval())
    }
    // In LEGACY mode, the casts that return NULL or wrap for a value they can not convert.
    Seq[(Expression, DataType)]((a, IntegerType), (a, ByteType), (a, DecimalType(5, 2)),
        (s, IntegerType), (s, DoubleType), (s, DecimalType(5, 2)), (s, BooleanType),
        (s, DateType), (s, TimestampType), (s, TimestampNTZType)).foreach { case (e, to) =>
      assert(ExprUtils.canEvaluateUnconditionally(cast(e, to, EvalMode.LEGACY)), (e, to))
      assert(!ExprUtils.canEvaluateUnconditionally(cast(e, to, EvalMode.ANSI)), (e, to))
    }
    // A date to a timestamp can overflow for a date far from the epoch, also in LEGACY mode,
    // although it is an up-cast.
    assert(Cast.canUpCast(DateType, TimestampType))
    val farDate = cast(Literal(Int.MaxValue, DateType), TimestampType, EvalMode.LEGACY)
    assert(!ExprUtils.canEvaluateUnconditionally(cast(date, TimestampType, EvalMode.LEGACY)))
    intercept[ArithmeticException](farDate.eval())
    // A try_cast to a CHAR or VARCHAR returns NULL for a string that is too long.
    assert(ExprUtils.canEvaluateUnconditionally(cast(a, VarcharType(3), EvalMode.TRY)))
    assert(cast(Literal(12345L), CharType(3), EvalMode.TRY).eval() == null)
    Seq[DataType](CharType(3), VarcharType(3)).foreach { to =>
      evalEach(LongType, Seq(12345L, Long.MinValue))(cast(_, to, EvalMode.TRY))
    }
    // Not over a child that can fail, in LEGACY mode either.
    assert(!ExprUtils.canEvaluateUnconditionally(
      cast(Add(a, a, ansi), IntegerType, EvalMode.LEGACY)))
    // TRY mode returns NULL for any error of the cast itself, but not of its input.
    assert(ExprUtils.canEvaluateUnconditionally(cast(date, TimestampType, EvalMode.TRY)))
    assert(cast(Literal(Int.MaxValue, DateType), TimestampType, EvalMode.TRY).eval() == null)
    assert(!ExprUtils.canEvaluateUnconditionally(
      cast(Add(a, a, ansi), IntegerType, EvalMode.TRY)))

    // None of the accepted ones raises.
    val strings = Seq("", " ", "x", "1e400", "-9" * 30, "9999-99-99", "+294247-01-10 04:00:54",
      "-290308-12-21 19:59:05", "25:61:61", "true", "1e-2147483647", "1e2147483647",
      "-1e-600000000", "0e-600000000").map(UTF8String.fromString)
    val numbers = Seq[(DataType, Seq[Any])]((LongType, Seq(Long.MinValue, Long.MaxValue)),
      (DoubleType, Seq(Double.NaN, Double.NegativeInfinity, Double.MaxValue)),
      (FloatType, Seq(Float.NaN, Float.MaxValue)),
      (DecimalType(38, 0), Seq(Decimal(BigDecimal("9" * 38)))))
    val integrals = Seq[(DataType, Seq[Any])]((IntegerType, Seq(Int.MinValue, Int.MaxValue)),
      (ShortType, Seq(Short.MinValue, Short.MaxValue)), (ByteType, Seq(Byte.MinValue)))
    for (mode <- Seq(EvalMode.LEGACY, EvalMode.TRY)) {
      for (to <- Seq(IntegerType, LongType, ShortType, ByteType, FloatType, DoubleType,
          DecimalType(5, 2), DecimalType(38, 0), BooleanType, DateType, TimestampType,
          TimestampNTZType)) {
        evalEach(StringType, strings)(cast(_, to, mode))
      }
      for ((from, values) <- numbers ++ integrals; to <- Seq(ByteType, ShortType, IntegerType,
          LongType, FloatType, DoubleType, DecimalType(5, 2))) {
        evalEach(from, values)(cast(_, to, mode))
      }
    }
    // The numeric widening casts in ANSI mode.
    for ((from, values) <- integrals :+ ((LongType, Seq(Long.MinValue, Long.MaxValue)));
        to <- Seq(LongType, DoubleType, DecimalType(20, 0)) if Cast.canUpCast(from, to)) {
      evalEach(from, values)(cast(_, to, EvalMode.ANSI))
    }
    evalEach(DecimalType(10, 2), Seq(Decimal(BigDecimal("-99999999.99"), 10, 2)))(
      cast(_, DecimalType(20, 4), EvalMode.ANSI))
    // A try_cast of a string to a time or a timestamp with nanoseconds takes the LEGACY path.
    for (to <- Seq(TimeType(6), TimestampNTZNanosType(9), TimestampLTZNanosType(9))) {
      evalEach(StringType, strings)(cast(_, to, EvalMode.TRY))
    }
    for (mode <- Seq(EvalMode.ANSI, EvalMode.LEGACY, EvalMode.TRY)) {
      for ((from, values) <- numbers :+ (BooleanType, Seq(true, false))) {
        evalEach(from, values)(cast(_, StringType, mode))
      }
      for (from <- Seq(TimestampType, TimestampNTZType)) {
        evalEach(from, Seq(Long.MinValue, 0L, Long.MaxValue))(cast(_, DateType, mode))
      }
    }
  }

  test("SPARK-60882: canEvaluateUnconditionally: substring and date and time fields") {
    val s = AttributeReference("s", StringType)()
    assert(ExprUtils.canEvaluateUnconditionally(Substring(s, Literal(2), Literal(3))))
    assert(!ExprUtils.canEvaluateUnconditionally(Substring(s, Cast(a, IntegerType,
      evalMode = EvalMode.ANSI), Literal(3))))
    val date = AttributeReference("date", DateType)()
    val ts = AttributeReference("ts", TimestampType)()
    val ntz = AttributeReference("ntz", TimestampNTZType)()
    def dateFields(d: Expression): Seq[Expression] = Seq(Year(d), YearOfWeek(d), Quarter(d),
      Month(d), DayOfMonth(d), DayOfYear(d), DayOfWeek(d), WeekDay(d), WeekOfYear(d),
      MonthName(d), DayName(d))
    def timeFields(t: Expression): Seq[Expression] = Seq(Hour(t, Some("UTC")),
      Minute(t, Some("Asia/Tokyo")), Second(t, Some("America/Los_Angeles")),
      SecondWithFraction(t, Some("UTC")))
    (dateFields(date) ++ timeFields(ts) ++ timeFields(ntz)).foreach { e =>
      assert(ExprUtils.canEvaluateUnconditionally(e), e)
    }
    // `year(ts)` reads the date of a timestamp, but `year(s)` casts a string to a date, which
    // can fail in ANSI mode.
    assert(ExprUtils.canEvaluateUnconditionally(
      EqualTo(Year(Cast(ts, DateType, Some("UTC"), EvalMode.ANSI)), Literal(2024))))
    assert(!ExprUtils.canEvaluateUnconditionally(
      Year(Cast(s, DateType, Some("UTC"), EvalMode.ANSI))))
    assert(!ExprUtils.canEvaluateUnconditionally(
      Hour(Cast(s, TimestampType, Some("UTC"), EvalMode.ANSI), Some("UTC"))))

    // None of them raises, also at the ends of the ranges of dates and timestamps.
    dateFields(date).indices.foreach { idx =>
      evalEach(DateType, Seq(Int.MinValue, 0, Int.MaxValue))(d => dateFields(d)(idx))
    }
    for (t <- Seq(TimestampType, TimestampNTZType); idx <- timeFields(ts).indices) {
      evalEach(t, Seq(Long.MinValue, 0L, Long.MaxValue))(v => timeFields(v)(idx))
    }
    val positions = Seq(Int.MinValue, -1, 0, 1, Int.MaxValue)
    // Also non-ASCII strings, which take another path, and invalid UTF-8.
    val nonAscii = Seq(0xE9, 'a', 0x4E2D).map(_.toChar).mkString
    val strings = Seq("", "abc", nonAscii).map(UTF8String.fromString) ++
      Seq(Array(0xFF, 'a'), Array('a', 0xE2)).map(b => UTF8String.fromBytes(b.map(_.toByte)))
    for (pos <- positions; len <- positions) {
      evalEach(StringType, strings)(Substring(_, Literal(pos), Literal(len)))
      evalEach(BinaryType, strings.map(_.getBytes))(Substring(_, Literal(pos), Literal(len)))
    }
  }

  test("canEvaluateUnconditionally: non-deterministic expressions are excluded") {
    assert(!ExprUtils.canEvaluateUnconditionally(LessThan(Rand(Literal(0L)), Literal(0.5))))
  }
}
