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
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CharType, DataType, DateType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TestUDT, TimestampLTZNanosType, TimestampNTZNanosType, TimestampNTZType, TimestampType, TimeType, VarcharType, VariantType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.UTF8String

class ExprUtilsSuite extends SparkFunSuite with SQLHelper {

  private val a = AttributeReference("a", LongType)()
  private val ansi = NumericEvalContext(EvalMode.ANSI)
  private val legacy = NumericEvalContext(EvalMode.LEGACY)
  private val tryMode = NumericEvalContext(EvalMode.TRY)

  // Evaluates `build` over input columns of the given types on every combination of their
  // values, and NULL when the columns are nullable, without and with code generation.
  private def evalRows(columns: (DataType, Seq[Any])*)(
      build: Seq[Expression] => Expression): Unit = {
    Seq(true, false).foreach { nullable =>
      val e = build(columns.zipWithIndex.map { case ((t, _), i) => BoundReference(i, t, nullable) })
      val projection = GenerateUnsafeProjection.generate(Seq(e))
      val inputs = columns.map { case (_, values) => if (nullable) values :+ null else values }
      inputs.foldLeft(Seq(Seq.empty[Any])) { (rows, columnValues) =>
        for (r <- rows; v <- columnValues) yield r :+ v
      }.foreach { rowValues =>
        val row = InternalRow.fromSeq(rowValues)
        e.eval(row)
        projection(row)
      }
    }
  }

  private def evalPairs(t: DataType, values: Seq[Any])(
      build: (Expression, Expression) => Expression): Unit =
    evalRows((t, values), (t, values))(c => build(c(0), c(1)))

  private def evalEach(t: DataType, values: Seq[Any])(build: Expression => Expression): Unit =
    evalRows((t, values))(c => build(c(0)))

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
    // A decimal to an integral type is an up-cast only when every value fits: DECIMAL(18, 0) to
    // BIGINT, but not DECIMAL(19, 0), which can overflow in ANSI mode.
    for ((precision, to) <- Seq((18, LongType), (9, IntegerType), (4, ShortType), (2, ByteType))) {
      assert(Cast.canUpCast(DecimalType(precision, 0), to), to)
      val d = AttributeReference("d", DecimalType(precision, 0))()
      assert(ExprUtils.canEvaluateUnconditionally(Cast(d, to, evalMode = EvalMode.ANSI)), to)
      evalEach(DecimalType(precision, 0),
        Seq("-" + "9" * precision, "9" * precision).map(v => Decimal(BigDecimal(v))))(
        Cast(_, to, evalMode = EvalMode.ANSI))
    }
    assert(!Cast.canUpCast(DecimalType(19, 0), LongType))
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
    "length checks, COLLATE and a field of a struct built from plain values are included") {
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
    // Also with an explicit COLLATE: `contains(s COLLATE UNICODE_CI, s)` is analyzed to a
    // `Collate`, and a cast of the other side to the collation, in the session's eval mode.
    def collate(e: Expression): Expression = Collate(e, ResolvedCollation("UNICODE_CI"))
    assert(ExprUtils.canEvaluateUnconditionally(
      Contains(collate(s), Cast(s, StringType("UNICODE_CI"), evalMode = EvalMode.ANSI))))
    assert(!ExprUtils.canEvaluateUnconditionally(Contains(collate(Upper(s)), collate(s))))
    Seq("UTF8_BINARY", "UNICODE_CI", "UTF8_LCASE_RTRIM").foreach { collation =>
      evalEach(StringType, values)(Collate(_, ResolvedCollation(collation)))
    }

    // A struct built from plain values, and a field of it, but not when another field can fail.
    def field(values: Expression*): Expression = GetStructField(CreateNamedStruct(
      values.zipWithIndex.flatMap { case (v, i) => Seq(Literal(s"f$i"), v) }), 0)
    assert(ExprUtils.canEvaluateUnconditionally(GreaterThan(field(a, s), Literal(1L))))
    assert(!ExprUtils.canEvaluateUnconditionally(GreaterThan(field(a, Add(a, Literal(1L), ansi)),
      Literal(1L))))
    assert(ExprUtils.canEvaluateUnconditionally(CreateNamedStruct(Seq(Literal("f0"), a))))
    assert(!ExprUtils.canEvaluateUnconditionally(
      CreateNamedStruct(Seq(Literal("f0"), Add(a, Literal(1L), ansi)))))
    // So is an array, as both are marked `NoThrow`, but not a map, which raises on a NULL key or,
    // by default, a duplicate key (SPARK-60849).
    assert(!ExprUtils.canEvaluateUnconditionally(CreateMap(Seq(a, Literal(1)))))
    intercept[SparkRuntimeException](
      CreateMap(Seq(Literal(1L), Literal(1), Literal(1L), Literal(2))).eval())
    assert(ExprUtils.canEvaluateUnconditionally(
      EqualTo(CreateArray(Seq(a, Literal(1L))), CreateArray(Seq(Literal(1L), Literal(2L))))))
    assert(!ExprUtils.canEvaluateUnconditionally(CreateArray(Seq(a, Add(a, Literal(1L), ansi)))))
  }

  test("SPARK-60882: canEvaluateUnconditionally: numeric arithmetic that can not fail") {
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
    // In ANSI mode too, Add, Subtract and Multiply over FLOAT and DOUBLE return an infinity or NaN,
    // and UnaryMinus and Abs over FLOAT, DOUBLE and DECIMAL do not check for an overflow. Divide,
    // Remainder and Pmod raise on a zero divisor, and Add over the integral types or DECIMAL on an
    // overflow.
    Seq(FloatType, DoubleType).foreach { t =>
      val x = AttributeReference("x", t)()
      Seq(Add(x, x, ansi), Subtract(x, x, ansi), Multiply(x, x, ansi)).foreach { e =>
        assert(ExprUtils.canEvaluateUnconditionally(e), e)
      }
      Seq(Divide(x, x, ansi), Remainder(x, x, ansi), Pmod(x, x, ansi)).foreach { e =>
        assert(!ExprUtils.canEvaluateUnconditionally(e), e)
      }
    }
    Seq(FloatType, DoubleType, DecimalType(10, 2)).foreach { t =>
      val x = AttributeReference("x", t)()
      assert(ExprUtils.canEvaluateUnconditionally(UnaryMinus(x, failOnError = true)), t)
      assert(ExprUtils.canEvaluateUnconditionally(Abs(x, failOnError = true)), t)
    }
    Seq(IntegerType, ShortType, ByteType).foreach { t =>
      val x = AttributeReference("x", t)()
      assert(!ExprUtils.canEvaluateUnconditionally(Add(x, x, ansi)), t)
      assert(!ExprUtils.canEvaluateUnconditionally(UnaryMinus(x, failOnError = true)), t)
    }
    intercept[ArithmeticException](Divide(Literal(1.0), Literal(0.0), ansi).eval())
    val maxDecimal = Literal(Decimal(BigDecimal("9" * 38)), DecimalType(38, 0))
    val decimalAdd = Add(maxDecimal, maxDecimal, ansi)
    assert(!ExprUtils.canEvaluateUnconditionally(decimalAdd))
    intercept[ArithmeticException](decimalAdd.eval())
    // Analysis casts both sides to one type; this only pins that every child must be a FLOAT or a
    // DOUBLE.
    assert(!ExprUtils.canEvaluateUnconditionally(Add(Literal(1.0), maxDecimal, ansi)))
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
    // Also over decimals whose integral quotient has more digits than the MathContext of
    // `Decimal` holds (SPARK-61050).
    val big = Literal(Decimal(BigDecimal("9" * 38)), DecimalType(38, 0))
    val small = Literal(Decimal(BigDecimal("0.007"), 3, 3), DecimalType(3, 3))
    Seq(IntegralDivide(big, small, legacy), Remainder(big, small, legacy),
        Remainder(big, small, tryMode), Pmod(big, small, legacy), Divide(big, small, legacy),
        Divide(big, small, tryMode)).foreach { e =>
      assert(ExprUtils.canEvaluateUnconditionally(e), e)
      e.eval()
    }
    // But not over a decimal of a negative scale, for which `Decimal.%` can raise, also in TRY
    // mode.
    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
      val veryNegative = DecimalType(1, -2147483000)
      Seq(legacy, tryMode).foreach { ctx =>
        assert(!ExprUtils.canEvaluateUnconditionally(Remainder(
          AttributeReference("n", veryNegative)(), Literal(Decimal(3), DecimalType(1, 0)), ctx)))
        intercept[ArithmeticException](Remainder(BoundReference(0, veryNegative, true),
          BoundReference(1, DecimalType(1, 0), true), ctx)
          .eval(InternalRow(Decimal.createUnsafe(1L, 1, -2147483000), Decimal(3))))
      }
      // UnaryMinus and Abs only negate one.
      val negativeScale = DecimalType(3, -2)
      Seq[Expression => Expression](UnaryMinus(_, failOnError = true),
          Abs(_, failOnError = true)).foreach { build =>
        val n = AttributeReference("n", negativeScale)()
        assert(ExprUtils.canEvaluateUnconditionally(build(n)))
        evalEach(negativeScale, Seq(Decimal.createUnsafe(-999L, 3, -2)))(build)
      }
    }
    // Arithmetic over DAY TO SECOND and YEAR TO MONTH intervals overflows in every mode, also
    // UnaryMinus and Abs.
    val maxInterval = Literal(Long.MaxValue, DayTimeIntervalType())
    val intervalAdd = Add(maxInterval, maxInterval, legacy)
    assert(!ExprUtils.canEvaluateUnconditionally(intervalAdd))
    intercept[ArithmeticException](intervalAdd.eval())
    Seq(Literal(Long.MinValue, DayTimeIntervalType()),
        Literal(Int.MinValue, YearMonthIntervalType())).foreach { minInterval =>
      Seq(UnaryMinus(minInterval, failOnError = false), Abs(minInterval, failOnError = false))
        .foreach { e =>
          assert(!ExprUtils.canEvaluateUnconditionally(e), e)
          intercept[ArithmeticException](e.eval())
        }
    }

    // None of the accepted ones raises, also on the values that do in ANSI mode.
    val longs = (LongType, Seq(Long.MinValue, -1L, 0L, 1L, Long.MaxValue))
    val ints = (IntegerType, Seq(Int.MinValue, -1, 0, 1, Int.MaxValue))
    val shorts = (ShortType, Seq(Short.MinValue, -1.toShort, 0.toShort, Short.MaxValue))
    val bytes = (ByteType, Seq(Byte.MinValue, -1.toByte, 0.toByte, Byte.MaxValue))
    val floats = (FloatType, Seq(Float.NegativeInfinity, -1.0f, 0.0f, Float.NaN, Float.MaxValue,
      Float.PositiveInfinity))
    val doubles = (DoubleType, Seq(Double.NegativeInfinity, -1.0, 0.0, Double.NaN,
      Double.MaxValue, Double.PositiveInfinity))
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
    for ((t, values) <- Seq(floats, doubles)) {
      Seq[(Expression, Expression) => Expression](Add(_, _, ansi), Subtract(_, _, ansi),
        Multiply(_, _, ansi)).foreach(evalPairs(t, values))
    }
    for ((t, values) <- Seq(floats, doubles, decimals, smallDecimals)) {
      evalEach(t, values)(UnaryMinus(_, failOnError = true))
      evalEach(t, values)(Abs(_, failOnError = true))
    }
    for ((t, values) <- nonDecimals ++ Seq(decimals, smallDecimals)) {
      Seq(legacy, tryMode).foreach(ctx => evalPairs(t, values)(Remainder(_, _, ctx)))
      evalPairs(t, values)(Pmod(_, _, legacy))
    }
    for ((t, values) <- Seq(longs, decimals, smallDecimals)) {
      evalPairs(t, values)(IntegralDivide(_, _, legacy))
    }
    for ((t, values) <- Seq(doubles, decimals, smallDecimals); ctx <- Seq(legacy, tryMode)) {
      evalPairs(t, values)(Divide(_, _, ctx))
    }
    // Decimals of different precisions and scales, also without precision loss.
    for (allowLoss <- Seq(true, false); mode <- Seq(EvalMode.LEGACY, EvalMode.TRY);
        (l, r) <- Seq((decimals, smallDecimals), (smallDecimals, decimals),
          (decimals, (DecimalType(3, 3), Seq(Decimal(BigDecimal("0.007"), 3, 3)))))) {
      val ctx = NumericEvalContext(mode, allowLoss)
      Seq[(Expression, Expression) => Expression](Add(_, _, ctx), Subtract(_, _, ctx),
        Multiply(_, _, ctx), Divide(_, _, ctx), Remainder(_, _, ctx)).foreach { build =>
        evalRows(l, r)(c => build(c(0), c(1)))
      }
      if (mode == EvalMode.LEGACY) {
        Seq[(Expression, Expression) => Expression](IntegralDivide(_, _, ctx), Pmod(_, _, ctx))
          .foreach(build => evalRows(l, r)(c => build(c(0), c(1))))
      }
    }
  }

  test("SPARK-60882: canEvaluateUnconditionally: casts that can not fail") {
    val s = AttributeReference("s", StringType)()
    val ts = AttributeReference("ts", TimestampType)()
    val date = AttributeReference("date", DateType)()
    def cast(e: Expression, to: DataType, mode: EvalMode.Value): Cast =
      Cast(e, to, Some("America/Los_Angeles"), mode)
    // In every mode: a timestamp to a date, a decimal to a FLOAT or DOUBLE, and a number, a
    // boolean or a string to a STRING of any collation.
    Seq(EvalMode.ANSI, EvalMode.LEGACY, EvalMode.TRY).foreach { mode =>
      Seq[(Expression, DataType)]((ts, DateType),
          (AttributeReference("ntz", TimestampNTZType)(), DateType), (a, StringType),
          (AttributeReference("d", DecimalType(10, 2))(), StringType),
          (AttributeReference("b", BooleanType)(), StringType),
          (AttributeReference("d", DecimalType(38, 0))(), DoubleType),
          (AttributeReference("d", DecimalType(10, 2))(), FloatType), (s, StringType),
          (s, StringType("UNICODE_CI")),
          (AttributeReference("v", VarcharType(3))(), StringType("UTF8_LCASE"))).foreach {
        case (e, to) =>
          assert(ExprUtils.canEvaluateUnconditionally(cast(e, to, mode)), (e, to, mode))
      }
    }
    // A FLOAT or DOUBLE to a number or a boolean in LEGACY and TRY mode, but not to a string.
    for (mode <- Seq(EvalMode.LEGACY, EvalMode.TRY); from <- Seq(FloatType, DoubleType);
        to <- Seq[DataType](LongType, DecimalType(10, 2), BooleanType)) {
      assert(ExprUtils.canEvaluateUnconditionally(
        cast(AttributeReference("x", from)(), to, mode)), (from, to, mode))
    }
    // Not from or to a decimal of a negative scale (`spark.sql.legacy.allowNegativeScaleOfDecimal`)
    // outside TRY mode, nor from a string in TRY mode, which runs the LEGACY cast: `Decimal.toLong`
    // raises for one.
    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
      val negativeScale = DecimalType(3, -2)
      Seq[(Expression, DataType)]((AttributeReference("n", negativeScale)(), LongType),
          (a, negativeScale)).foreach { case (e, to) =>
        assert(!ExprUtils.canEvaluateUnconditionally(cast(e, to, EvalMode.LEGACY)), to)
        assert(ExprUtils.canEvaluateUnconditionally(cast(e, to, EvalMode.TRY)), to)
      }
      assert(!ExprUtils.canEvaluateUnconditionally(cast(s, negativeScale, EvalMode.TRY)))
      // But a widening between decimals, which never calls `Decimal.toLong`, also to a negative
      // scale or through a `BigDecimal`, and a cast to a FLOAT or DOUBLE, in every mode.
      for (mode <- Seq(EvalMode.ANSI, EvalMode.LEGACY, EvalMode.TRY);
          (from, to, unscaled) <- Seq[(DecimalType, DataType, Long)](
            (negativeScale, DecimalType(10, 0), 999L),
            (DecimalType(3, -3), DecimalType(5, -2), 999L),
            (DecimalType(1, -36), DecimalType(38, 0), 9L),
            (negativeScale, DoubleType, 999L), (negativeScale, FloatType, 999L))) {
        assert(ExprUtils.canEvaluateUnconditionally(
          cast(AttributeReference("n", from)(), to, mode)), (from, to, mode))
        evalEach(from, Seq(-unscaled, unscaled).map(Decimal.createUnsafe(_, from.precision,
          from.scale)))(cast(_, to, mode))
      }
      intercept[ArrayIndexOutOfBoundsException](cast(BoundReference(0, negativeScale, true),
        LongType, EvalMode.LEGACY).eval(InternalRow(Decimal.createUnsafe(123L, 3, -2))))
      // The accepted TRY casts do not raise.
      evalEach(negativeScale, Seq(Decimal.createUnsafe(123L, 3, -2)))(
        cast(_, LongType, EvalMode.TRY))
      evalEach(LongType, Seq(Long.MinValue, 0L, Long.MaxValue))(
        cast(_, negativeScale, EvalMode.TRY))
    }
    // Not a FLOAT or DOUBLE to a string, also inside a struct, an array or a map, or to a
    // variant, in any mode: the string tells -0.0 from 0.0, which are equal.
    assert(EqualTo(Literal(-0.0), Literal(0.0)).eval() == true)
    assert(cast(Literal(-0.0), StringType, EvalMode.ANSI).eval() != cast(Literal(0.0), StringType,
      EvalMode.ANSI).eval())
    val doubles = AttributeReference("ds", ArrayType(DoubleType))()
    Seq[(Expression, DataType)]((AttributeReference("f", FloatType)(), StringType),
        (AttributeReference("x", DoubleType)(), StringType("UNICODE_CI")),
        (doubles, StringType), (doubles, ArrayType(StringType)),
        (AttributeReference("sd", StructType(Seq(StructField("v", DoubleType))))(),
          StructType(Seq(StructField("v", StringType)))),
        (AttributeReference("md", MapType(IntegerType, DoubleType))(),
          MapType(IntegerType, StringType)),
        (AttributeReference("x", DoubleType)(), VariantType)).foreach { case (e, to) =>
      Seq(EvalMode.ANSI, EvalMode.LEGACY, EvalMode.TRY).foreach { mode =>
        assert(!ExprUtils.canEvaluateUnconditionally(cast(e, to, mode)), (e, to, mode))
      }
    }
    // Nor from or to a UDT, also inside an array or a struct, in any mode: it renders its value
    // with its own `stringifyValue`, e.g. this vector of -0.0 as '(-0.0)'.
    val udt = new TestUDT.MyDenseVectorUDT()
    Seq[(Expression, DataType)]((AttributeReference("v", udt)(), StringType),
        (AttributeReference("vs", ArrayType(udt))(), StringType),
        (AttributeReference("sv", StructType(Seq(StructField("v", udt))))(), StringType),
        (AttributeReference("v", udt)(), udt)).foreach { case (e, to) =>
      Seq(EvalMode.ANSI, EvalMode.LEGACY, EvalMode.TRY).foreach { mode =>
        assert(!ExprUtils.canEvaluateUnconditionally(cast(e, to, mode)), (e, to, mode))
      }
    }
    // Not to a CHAR or VARCHAR, which checks the length.
    Seq[DataType](CharType(3), VarcharType(3)).foreach { to =>
      assert(!ExprUtils.canEvaluateUnconditionally(cast(a, to, EvalMode.LEGACY)), to)
      intercept[SparkRuntimeException](cast(Literal(12345L), to, EvalMode.LEGACY).eval())
      assert(!ExprUtils.canEvaluateUnconditionally(cast(s, to, EvalMode.ANSI)), to)
      intercept[SparkRuntimeException](cast(Literal("abcd"), to, EvalMode.ANSI).eval())
    }
    // Not a decimal to a narrower decimal, which raises in ANSI mode.
    assert(!ExprUtils.canEvaluateUnconditionally(
      cast(AttributeReference("d", DecimalType(38, 0))(), DecimalType(5, 2), EvalMode.ANSI)))
    intercept[ArithmeticException](cast(Literal(Decimal(BigDecimal("9" * 38)), DecimalType(38, 0)),
      DecimalType(5, 2), EvalMode.ANSI).eval())
    // In LEGACY mode, the casts that return NULL or wrap for a value they can not convert.
    Seq[(Expression, DataType)]((a, IntegerType), (a, ByteType), (a, DecimalType(5, 2)),
        (s, IntegerType), (s, DoubleType), (s, DecimalType(5, 2)), (s, BooleanType),
        (s, DateType), (s, TimestampType), (s, TimestampNTZType)).foreach { case (e, to) =>
      assert(ExprUtils.canEvaluateUnconditionally(cast(e, to, EvalMode.LEGACY)), (e, to))
      assert(!ExprUtils.canEvaluateUnconditionally(cast(e, to, EvalMode.ANSI)), (e, to))
    }
    // And the other casts `Cast.isNoThrowLegacyCast` accepts.
    Seq[(Expression, DataType)]((AttributeReference("b", BooleanType)(), IntegerType),
        (a, BooleanType)).foreach { case (e, to) =>
      assert(Cast.isNoThrowLegacyCast(e.dataType, to), (e, to))
      assert(ExprUtils.canEvaluateUnconditionally(cast(e, to, EvalMode.LEGACY)), (e, to))
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
      for ((from, values) <- numbers ++ integrals :+ ((BooleanType, Seq(true, false)));
          to <- Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
            DecimalType(5, 2), DecimalType(1, 1), BooleanType)) {
        evalEach(from, values)(cast(_, to, mode))
      }
      // From strings of other collations and from a VARCHAR.
      for (from <- Seq(StringType("UTF8_LCASE"), VarcharType(3)); to <- Seq(IntegerType,
          DateType)) {
        evalEach(from, strings)(cast(_, to, mode))
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
      for ((from, values) <- numbers.filterNot(n => n._1 == DoubleType || n._1 == FloatType) :+
          (BooleanType, Seq(true, false))) {
        evalEach(from, values)(cast(_, StringType, mode))
      }
      for (from <- Seq(TimestampType, TimestampNTZType)) {
        evalEach(from, Seq(Long.MinValue, 0L, Long.MaxValue))(cast(_, DateType, mode))
      }
      for ((from, values) <- Seq[(DataType, Seq[Any])]((DecimalType(38, 0),
            Seq("-" + "9" * 38, "9" * 38).map(d => Decimal(BigDecimal(d)))),
          (DecimalType(38, 37), Seq("-9." + "9" * 37, "9." + "9" * 37)
            .map(d => Decimal(BigDecimal(d), 38, 37))));
          to <- Seq(FloatType, DoubleType)) {
        evalEach(from, values)(cast(_, to, mode))
      }
      for (from <- Seq(StringType, StringType("UTF8_LCASE"), VarcharType(3), CharType(3));
          to <- Seq(StringType, StringType("UNICODE_CI"))) {
        evalEach(from, strings)(cast(_, to, mode))
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
    for (input <- Seq((StringType, strings), (BinaryType, strings.map(_.getBytes)))) {
      evalRows(input, (IntegerType, positions), (IntegerType, positions))(c =>
        Substring(c(0), c(1), c(2)))
    }
  }

  test("canEvaluateUnconditionally: non-deterministic expressions are excluded") {
    assert(!ExprUtils.canEvaluateUnconditionally(LessThan(Rand(Literal(0L)), Literal(0.5))))
  }
}
