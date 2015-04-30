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

import java.sql.{Date, Timestamp}

import scala.collection.immutable.HashSet

import org.scalactic.TripleEqualsSupport.Spread
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.analysis.UnresolvedGetField
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.mathfuncs._
import org.apache.spark.sql.types._


class ExpressionEvaluationBaseSuite extends FunSuite {

  def evaluate(expression: Expression, inputRow: Row = EmptyRow): Any = {
    expression.eval(inputRow)
  }

  def checkEvaluation(expression: Expression, expected: Any, inputRow: Row = EmptyRow): Unit = {
    val actual = try evaluate(expression, inputRow) catch {
      case e: Exception => fail(s"Exception evaluating $expression", e)
    }
    if(actual != expected) {
      val input = if(inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(s"Incorrect Evaluation: $expression, actual: $actual, expected: $expected$input")
    }
  }

  def checkDoubleEvaluation(
      expression: Expression,
      expected: Spread[Double],
      inputRow: Row = EmptyRow): Unit = {
    val actual = try evaluate(expression, inputRow) catch {
      case e: Exception => fail(s"Exception evaluating $expression", e)
    }
    actual.asInstanceOf[Double] shouldBe expected
  }
}

class ExpressionEvaluationSuite extends ExpressionEvaluationBaseSuite {

  def create_row(values: Any*): Row = {
    new GenericRow(values.map(CatalystTypeConverters.convertToCatalyst).toArray)
  }

  test("literals") {
    checkEvaluation(Literal(1), 1)
    checkEvaluation(Literal(true), true)
    checkEvaluation(Literal(0L), 0L)
    checkEvaluation(Literal("test"), "test")
    checkEvaluation(Literal(1) + Literal(1), 2)
  }

  test("unary BitwiseNOT") {
    checkEvaluation(BitwiseNot(1), -2)
    assert(BitwiseNot(1).dataType === IntegerType)
    assert(BitwiseNot(1).eval(EmptyRow).isInstanceOf[Int])
    checkEvaluation(BitwiseNot(1.toLong), -2.toLong)
    assert(BitwiseNot(1.toLong).dataType === LongType)
    assert(BitwiseNot(1.toLong).eval(EmptyRow).isInstanceOf[Long])
    checkEvaluation(BitwiseNot(1.toShort), -2.toShort)
    assert(BitwiseNot(1.toShort).dataType === ShortType)
    assert(BitwiseNot(1.toShort).eval(EmptyRow).isInstanceOf[Short])
    checkEvaluation(BitwiseNot(1.toByte), -2.toByte)
    assert(BitwiseNot(1.toByte).dataType === ByteType)
    assert(BitwiseNot(1.toByte).eval(EmptyRow).isInstanceOf[Byte])
  }

  // scalastyle:off
  /**
   * Checks for three-valued-logic.  Based on:
   * http://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_.283VL.29
   * I.e. in flat cpo "False -> Unknown -> True",
   *   OR is lowest upper bound,
   *   AND is greatest lower bound.
   * p       q       p OR q  p AND q  p = q
   * True    True    True    True     True
   * True    False   True    False    False
   * True    Unknown True    Unknown  Unknown
   * False   True    True    False    False
   * False   False   False   False    True
   * False   Unknown Unknown False    Unknown
   * Unknown True    True    Unknown  Unknown
   * Unknown False   Unknown False    Unknown
   * Unknown Unknown Unknown Unknown  Unknown
   *
   * p       NOT p
   * True    False
   * False   True
   * Unknown Unknown
   */
  // scalastyle:on
  val notTrueTable =
    (true, false) ::
    (false, true) ::
    (null, null) :: Nil

  test("3VL Not") {
    notTrueTable.foreach {
      case (v, answer) =>
        checkEvaluation(!Literal.create(v, BooleanType), answer)
    }
  }

  booleanLogicTest("AND", _ && _,
    (true,  true,  true) ::
    (true,  false, false) ::
    (true,  null,  null) ::
    (false, true,  false) ::
    (false, false, false) ::
    (false, null,  false) ::
    (null,  true,  null) ::
    (null,  false, false) ::
    (null,  null,  null) :: Nil)

  booleanLogicTest("OR", _ || _,
    (true,  true,  true) ::
    (true,  false, true) ::
    (true,  null,  true) ::
    (false, true,  true) ::
    (false, false, false) ::
    (false, null,  null) ::
    (null,  true,  true) ::
    (null,  false, null) ::
    (null,  null,  null) :: Nil)

  booleanLogicTest("=", _ === _,
    (true,  true,  true) ::
    (true,  false, false) ::
    (true,  null,  null) ::
    (false, true,  false) ::
    (false, false, true) ::
    (false, null,  null) ::
    (null,  true,  null) ::
    (null,  false, null) ::
    (null,  null,  null) :: Nil)

  def booleanLogicTest(
      name: String,
      op: (Expression, Expression) => Expression,
      truthTable: Seq[(Any, Any, Any)]) {
    test(s"3VL $name") {
      truthTable.foreach {
        case (l,r,answer) =>
          val expr = op(Literal.create(l, BooleanType), Literal.create(r, BooleanType))
          checkEvaluation(expr, answer)
      }
    }
  }

  test("IN") {
    checkEvaluation(In(Literal(1), Seq(Literal(1), Literal(2))), true)
    checkEvaluation(In(Literal(2), Seq(Literal(1), Literal(2))), true)
    checkEvaluation(In(Literal(3), Seq(Literal(1), Literal(2))), false)
    checkEvaluation(
      In(Literal(1), Seq(Literal(1), Literal(2))) && In(Literal(2), Seq(Literal(1), Literal(2))),
      true)
  }

  test("Divide") {
    checkEvaluation(Divide(Literal(2), Literal(1)), 2)
    checkEvaluation(Divide(Literal(1.0), Literal(2.0)), 0.5)
    checkEvaluation(Divide(Literal(1), Literal(2)), 0)
    checkEvaluation(Divide(Literal(1), Literal(0)), null)
    checkEvaluation(Divide(Literal(1.0), Literal(0.0)), null)
    checkEvaluation(Divide(Literal(0.0), Literal(0.0)), null)
    checkEvaluation(Divide(Literal(0), Literal.create(null, IntegerType)), null)
    checkEvaluation(Divide(Literal(1), Literal.create(null, IntegerType)), null)
    checkEvaluation(Divide(Literal.create(null, IntegerType), Literal(0)), null)
    checkEvaluation(Divide(Literal.create(null, DoubleType), Literal(0.0)), null)
    checkEvaluation(Divide(Literal.create(null, IntegerType), Literal(1)), null)
    checkEvaluation(Divide(Literal.create(null, IntegerType), Literal.create(null, IntegerType)),
      null)
  }

  test("Remainder") {
    checkEvaluation(Remainder(Literal(2), Literal(1)), 0)
    checkEvaluation(Remainder(Literal(1.0), Literal(2.0)), 1.0)
    checkEvaluation(Remainder(Literal(1), Literal(2)), 1)
    checkEvaluation(Remainder(Literal(1), Literal(0)), null)
    checkEvaluation(Remainder(Literal(1.0), Literal(0.0)), null)
    checkEvaluation(Remainder(Literal(0.0), Literal(0.0)), null)
    checkEvaluation(Remainder(Literal(0), Literal.create(null, IntegerType)), null)
    checkEvaluation(Remainder(Literal(1), Literal.create(null, IntegerType)), null)
    checkEvaluation(Remainder(Literal.create(null, IntegerType), Literal(0)), null)
    checkEvaluation(Remainder(Literal.create(null, DoubleType), Literal(0.0)), null)
    checkEvaluation(Remainder(Literal.create(null, IntegerType), Literal(1)), null)
    checkEvaluation(Remainder(Literal.create(null, IntegerType), Literal.create(null, IntegerType)),
      null)
  }

  test("INSET") {
    val hS = HashSet[Any]() + 1 + 2
    val nS = HashSet[Any]() + 1 + 2 + null
    val one = Literal(1)
    val two = Literal(2)
    val three = Literal(3)
    val nl = Literal(null)
    val s = Seq(one, two)
    val nullS = Seq(one, two, null)
    checkEvaluation(InSet(one, hS), true)
    checkEvaluation(InSet(two, hS), true)
    checkEvaluation(InSet(two, nS), true)
    checkEvaluation(InSet(nl, nS), true)
    checkEvaluation(InSet(three, hS), false)
    checkEvaluation(InSet(three, nS), false)
    checkEvaluation(InSet(one, hS) && InSet(two, hS), true)
  }

  test("MaxOf") {
    checkEvaluation(MaxOf(1, 2), 2)
    checkEvaluation(MaxOf(2, 1), 2)
    checkEvaluation(MaxOf(1L, 2L), 2L)
    checkEvaluation(MaxOf(2L, 1L), 2L)

    checkEvaluation(MaxOf(Literal.create(null, IntegerType), 2), 2)
    checkEvaluation(MaxOf(2, Literal.create(null, IntegerType)), 2)
  }

  test("MinOf") {
    checkEvaluation(MinOf(1, 2), 1)
    checkEvaluation(MinOf(2, 1), 1)
    checkEvaluation(MinOf(1L, 2L), 1L)
    checkEvaluation(MinOf(2L, 1L), 1L)

    checkEvaluation(MinOf(Literal.create(null, IntegerType), 1), 1)
    checkEvaluation(MinOf(1, Literal.create(null, IntegerType)), 1)
  }

  test("LIKE literal Regular Expression") {
    checkEvaluation(Literal.create(null, StringType).like("a"), null)
    checkEvaluation(Literal.create("a", StringType).like(Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create(null, StringType).like(Literal.create(null, StringType)), null)
    checkEvaluation("abdef" like "abdef", true)
    checkEvaluation("a_%b" like "a\\__b", true)
    checkEvaluation("addb" like "a_%b", true)
    checkEvaluation("addb" like "a\\__b", false)
    checkEvaluation("addb" like "a%\\%b", false)
    checkEvaluation("a_%b" like "a%\\%b", true)
    checkEvaluation("addb" like "a%", true)
    checkEvaluation("addb" like "**", false)
    checkEvaluation("abc" like "a%", true)
    checkEvaluation("abc"  like "b%", false)
    checkEvaluation("abc"  like "bc%", false)
    checkEvaluation("a\nb" like "a_b", true)
    checkEvaluation("ab" like "a%b", true)
    checkEvaluation("a\nb" like "a%b", true)
  }

  test("LIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abcd" like regEx, null, create_row(null))
    checkEvaluation("abdef" like regEx, true, create_row("abdef"))
    checkEvaluation("a_%b" like regEx, true, create_row("a\\__b"))
    checkEvaluation("addb" like regEx, true, create_row("a_%b"))
    checkEvaluation("addb" like regEx, false, create_row("a\\__b"))
    checkEvaluation("addb" like regEx, false, create_row("a%\\%b"))
    checkEvaluation("a_%b" like regEx, true, create_row("a%\\%b"))
    checkEvaluation("addb" like regEx, true, create_row("a%"))
    checkEvaluation("addb" like regEx, false, create_row("**"))
    checkEvaluation("abc" like regEx, true, create_row("a%"))
    checkEvaluation("abc" like regEx, false, create_row("b%"))
    checkEvaluation("abc" like regEx, false, create_row("bc%"))
    checkEvaluation("a\nb" like regEx, true, create_row("a_b"))
    checkEvaluation("ab" like regEx, true, create_row("a%b"))
    checkEvaluation("a\nb" like regEx, true, create_row("a%b"))

    checkEvaluation(Literal.create(null, StringType) like regEx, null, create_row("bc%"))
  }

  test("RLIKE literal Regular Expression") {
    checkEvaluation(Literal.create(null, StringType) rlike "abdef", null)
    checkEvaluation("abdef" rlike Literal.create(null, StringType), null)
    checkEvaluation(Literal.create(null, StringType) rlike Literal.create(null, StringType), null)
    checkEvaluation("abdef" rlike "abdef", true)
    checkEvaluation("abbbbc" rlike "a.*c", true)

    checkEvaluation("fofo" rlike "^fo", true)
    checkEvaluation("fo\no" rlike "^fo\no$", true)
    checkEvaluation("Bn" rlike "^Ba*n", true)
    checkEvaluation("afofo" rlike "fo", true)
    checkEvaluation("afofo" rlike "^fo", false)
    checkEvaluation("Baan" rlike "^Ba?n", false)
    checkEvaluation("axe" rlike "pi|apa", false)
    checkEvaluation("pip" rlike "^(pi)*$", false)

    checkEvaluation("abc"  rlike "^ab", true)
    checkEvaluation("abc"  rlike "^bc", false)
    checkEvaluation("abc"  rlike "^ab", true)
    checkEvaluation("abc"  rlike "^bc", false)

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" rlike "**")
    }
  }

  test("RLIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abdef" rlike regEx, true, create_row("abdef"))
    checkEvaluation("abbbbc" rlike regEx, true, create_row("a.*c"))
    checkEvaluation("fofo" rlike regEx, true, create_row("^fo"))
    checkEvaluation("fo\no" rlike regEx, true, create_row("^fo\no$"))
    checkEvaluation("Bn" rlike regEx, true, create_row("^Ba*n"))

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" rlike regEx, create_row("**"))
    }
  }

  test("data type casting") {

    val sd = "1970-01-01"
    val d = Date.valueOf(sd)
    val zts = sd + " 00:00:00"
    val sts = sd + " 00:00:02"
    val nts = sts + ".1"
    val ts = Timestamp.valueOf(nts)

    checkEvaluation("abdef" cast StringType, "abdef")
    checkEvaluation("abdef" cast DecimalType.Unlimited, null)
    checkEvaluation("abdef" cast TimestampType, null)
    checkEvaluation("12.65" cast DecimalType.Unlimited, Decimal(12.65))

    checkEvaluation(Literal(1) cast LongType, 1)
    checkEvaluation(Cast(Literal(1000) cast TimestampType, LongType), 1.toLong)
    checkEvaluation(Cast(Literal(-1200) cast TimestampType, LongType), -2.toLong)
    checkEvaluation(Cast(Literal(1.toDouble) cast TimestampType, DoubleType), 1.toDouble)
    checkEvaluation(Cast(Literal(1.toDouble) cast TimestampType, DoubleType), 1.toDouble)

    checkEvaluation(Cast(Literal(sd) cast DateType, StringType), sd)
    checkEvaluation(Cast(Literal(d) cast StringType, DateType), 0)
    checkEvaluation(Cast(Literal(nts) cast TimestampType, StringType), nts)
    checkEvaluation(Cast(Literal(ts) cast StringType, TimestampType), ts)
    // all convert to string type to check
    checkEvaluation(
      Cast(Cast(Literal(nts) cast TimestampType, DateType), StringType), sd)
    checkEvaluation(
      Cast(Cast(Literal(ts) cast DateType, TimestampType), StringType), zts)

    checkEvaluation(Cast("abdef" cast BinaryType, StringType), "abdef")

    checkEvaluation(Cast(Cast(Cast(Cast(
      Cast("5" cast ByteType, ShortType), IntegerType), FloatType), DoubleType), LongType), 5)
    checkEvaluation(Cast(Cast(Cast(Cast(Cast("5" cast
      ByteType, TimestampType), DecimalType.Unlimited), LongType), StringType), ShortType), 0)
    checkEvaluation(Cast(Cast(Cast(Cast(Cast("5" cast
      TimestampType, ByteType), DecimalType.Unlimited), LongType), StringType), ShortType), null)
    checkEvaluation(Cast(Cast(Cast(Cast(Cast("5" cast
      DecimalType.Unlimited, ByteType), TimestampType), LongType), StringType), ShortType), 0)
    checkEvaluation(Literal(true) cast IntegerType, 1)
    checkEvaluation(Literal(false) cast IntegerType, 0)
    checkEvaluation(Cast(Literal(1) cast BooleanType, IntegerType), 1)
    checkEvaluation(Cast(Literal(0) cast BooleanType, IntegerType), 0)
    checkEvaluation("23" cast DoubleType, 23d)
    checkEvaluation("23" cast IntegerType, 23)
    checkEvaluation("23" cast FloatType, 23f)
    checkEvaluation("23" cast DecimalType.Unlimited, Decimal(23))
    checkEvaluation("23" cast ByteType, 23.toByte)
    checkEvaluation("23" cast ShortType, 23.toShort)
    checkEvaluation("2012-12-11" cast DoubleType, null)
    checkEvaluation(Literal(123) cast IntegerType, 123)

    checkEvaluation(Literal(23d) + Cast(true, DoubleType), 24d)
    checkEvaluation(Literal(23) + Cast(true, IntegerType), 24)
    checkEvaluation(Literal(23f) + Cast(true, FloatType), 24f)
    checkEvaluation(Literal(Decimal(23)) + Cast(true, DecimalType.Unlimited), Decimal(24))
    checkEvaluation(Literal(23.toByte) + Cast(true, ByteType), 24.toByte)
    checkEvaluation(Literal(23.toShort) + Cast(true, ShortType), 24.toShort)

    intercept[Exception] {evaluate(Literal(1) cast BinaryType, null)}

    assert(("abcdef" cast StringType).nullable === false)
    assert(("abcdef" cast BinaryType).nullable === false)
    assert(("abcdef" cast BooleanType).nullable === false)
    assert(("abcdef" cast TimestampType).nullable === true)
    assert(("abcdef" cast LongType).nullable === true)
    assert(("abcdef" cast IntegerType).nullable === true)
    assert(("abcdef" cast ShortType).nullable === true)
    assert(("abcdef" cast ByteType).nullable === true)
    assert(("abcdef" cast DecimalType.Unlimited).nullable === true)
    assert(("abcdef" cast DecimalType(4, 2)).nullable === true)
    assert(("abcdef" cast DoubleType).nullable === true)
    assert(("abcdef" cast FloatType).nullable === true)

    checkEvaluation(Cast(Literal.create(null, IntegerType), ShortType), null)
  }

  test("date") {
    val d1 = DateUtils.fromJavaDate(Date.valueOf("1970-01-01"))
    val d2 = DateUtils.fromJavaDate(Date.valueOf("1970-01-02"))
    checkEvaluation(Literal(d1) < Literal(d2), true)
  }

  test("casting to fixed-precision decimals") {
    // Overflow and rounding for casting to fixed-precision decimals:
    // - Values should round with HALF_UP mode by default when you lower scale
    // - Values that would overflow the target precision should turn into null
    // - Because of this, casts to fixed-precision decimals should be nullable

    assert(Cast(Literal(123), DecimalType.Unlimited).nullable === false)
    assert(Cast(Literal(10.03f), DecimalType.Unlimited).nullable === true)
    assert(Cast(Literal(10.03), DecimalType.Unlimited).nullable === true)
    assert(Cast(Literal(Decimal(10.03)), DecimalType.Unlimited).nullable === false)

    assert(Cast(Literal(123), DecimalType(2, 1)).nullable === true)
    assert(Cast(Literal(10.03f), DecimalType(2, 1)).nullable === true)
    assert(Cast(Literal(10.03), DecimalType(2, 1)).nullable === true)
    assert(Cast(Literal(Decimal(10.03)), DecimalType(2, 1)).nullable === true)

    checkEvaluation(Cast(Literal(123), DecimalType.Unlimited), Decimal(123))
    checkEvaluation(Cast(Literal(123), DecimalType(3, 0)), Decimal(123))
    checkEvaluation(Cast(Literal(123), DecimalType(3, 1)), null)
    checkEvaluation(Cast(Literal(123), DecimalType(2, 0)), null)

    checkEvaluation(Cast(Literal(10.03), DecimalType.Unlimited), Decimal(10.03))
    checkEvaluation(Cast(Literal(10.03), DecimalType(4, 2)), Decimal(10.03))
    checkEvaluation(Cast(Literal(10.03), DecimalType(3, 1)), Decimal(10.0))
    checkEvaluation(Cast(Literal(10.03), DecimalType(2, 0)), Decimal(10))
    checkEvaluation(Cast(Literal(10.03), DecimalType(1, 0)), null)
    checkEvaluation(Cast(Literal(10.03), DecimalType(2, 1)), null)
    checkEvaluation(Cast(Literal(10.03), DecimalType(3, 2)), null)
    checkEvaluation(Cast(Literal(Decimal(10.03)), DecimalType(3, 1)), Decimal(10.0))
    checkEvaluation(Cast(Literal(Decimal(10.03)), DecimalType(3, 2)), null)

    checkEvaluation(Cast(Literal(10.05), DecimalType.Unlimited), Decimal(10.05))
    checkEvaluation(Cast(Literal(10.05), DecimalType(4, 2)), Decimal(10.05))
    checkEvaluation(Cast(Literal(10.05), DecimalType(3, 1)), Decimal(10.1))
    checkEvaluation(Cast(Literal(10.05), DecimalType(2, 0)), Decimal(10))
    checkEvaluation(Cast(Literal(10.05), DecimalType(1, 0)), null)
    checkEvaluation(Cast(Literal(10.05), DecimalType(2, 1)), null)
    checkEvaluation(Cast(Literal(10.05), DecimalType(3, 2)), null)
    checkEvaluation(Cast(Literal(Decimal(10.05)), DecimalType(3, 1)), Decimal(10.1))
    checkEvaluation(Cast(Literal(Decimal(10.05)), DecimalType(3, 2)), null)

    checkEvaluation(Cast(Literal(9.95), DecimalType(3, 2)), Decimal(9.95))
    checkEvaluation(Cast(Literal(9.95), DecimalType(3, 1)), Decimal(10.0))
    checkEvaluation(Cast(Literal(9.95), DecimalType(2, 0)), Decimal(10))
    checkEvaluation(Cast(Literal(9.95), DecimalType(2, 1)), null)
    checkEvaluation(Cast(Literal(9.95), DecimalType(1, 0)), null)
    checkEvaluation(Cast(Literal(Decimal(9.95)), DecimalType(3, 1)), Decimal(10.0))
    checkEvaluation(Cast(Literal(Decimal(9.95)), DecimalType(1, 0)), null)

    checkEvaluation(Cast(Literal(-9.95), DecimalType(3, 2)), Decimal(-9.95))
    checkEvaluation(Cast(Literal(-9.95), DecimalType(3, 1)), Decimal(-10.0))
    checkEvaluation(Cast(Literal(-9.95), DecimalType(2, 0)), Decimal(-10))
    checkEvaluation(Cast(Literal(-9.95), DecimalType(2, 1)), null)
    checkEvaluation(Cast(Literal(-9.95), DecimalType(1, 0)), null)
    checkEvaluation(Cast(Literal(Decimal(-9.95)), DecimalType(3, 1)), Decimal(-10.0))
    checkEvaluation(Cast(Literal(Decimal(-9.95)), DecimalType(1, 0)), null)

    checkEvaluation(Cast(Literal(Double.NaN), DecimalType.Unlimited), null)
    checkEvaluation(Cast(Literal(1.0 / 0.0), DecimalType.Unlimited), null)
    checkEvaluation(Cast(Literal(Float.NaN), DecimalType.Unlimited), null)
    checkEvaluation(Cast(Literal(1.0f / 0.0f), DecimalType.Unlimited), null)

    checkEvaluation(Cast(Literal(Double.NaN), DecimalType(2, 1)), null)
    checkEvaluation(Cast(Literal(1.0 / 0.0), DecimalType(2, 1)), null)
    checkEvaluation(Cast(Literal(Float.NaN), DecimalType(2, 1)), null)
    checkEvaluation(Cast(Literal(1.0f / 0.0f), DecimalType(2, 1)), null)
  }

  test("timestamp") {
    val ts1 = new Timestamp(12)
    val ts2 = new Timestamp(123)
    checkEvaluation(Literal("ab") < Literal("abc"), true)
    checkEvaluation(Literal(ts1) < Literal(ts2), true)
  }

  test("date casting") {
    val d = Date.valueOf("1970-01-01")
    checkEvaluation(Cast(Literal(d), ShortType), null)
    checkEvaluation(Cast(Literal(d), IntegerType), null)
    checkEvaluation(Cast(Literal(d), LongType), null)
    checkEvaluation(Cast(Literal(d), FloatType), null)
    checkEvaluation(Cast(Literal(d), DoubleType), null)
    checkEvaluation(Cast(Literal(d), DecimalType.Unlimited), null)
    checkEvaluation(Cast(Literal(d), DecimalType(10, 2)), null)
    checkEvaluation(Cast(Literal(d), StringType), "1970-01-01")
    checkEvaluation(Cast(Cast(Literal(d), TimestampType), StringType), "1970-01-01 00:00:00")
  }

  test("timestamp casting") {
    val millis = 15 * 1000 + 2
    val seconds = millis * 1000 + 2
    val ts = new Timestamp(millis)
    val tss = new Timestamp(seconds)
    checkEvaluation(Cast(ts, ShortType), 15)
    checkEvaluation(Cast(ts, IntegerType), 15)
    checkEvaluation(Cast(ts, LongType), 15)
    checkEvaluation(Cast(ts, FloatType), 15.002f)
    checkEvaluation(Cast(ts, DoubleType), 15.002)
    checkEvaluation(Cast(Cast(tss, ShortType), TimestampType), ts)
    checkEvaluation(Cast(Cast(tss, IntegerType), TimestampType), ts)
    checkEvaluation(Cast(Cast(tss, LongType), TimestampType), ts)
    checkEvaluation(Cast(Cast(millis.toFloat / 1000, TimestampType), FloatType),
      millis.toFloat / 1000)
    checkEvaluation(Cast(Cast(millis.toDouble / 1000, TimestampType), DoubleType),
      millis.toDouble / 1000)
    checkEvaluation(Cast(Literal(Decimal(1)) cast TimestampType, DecimalType.Unlimited), Decimal(1))

    // A test for higher precision than millis
    checkEvaluation(Cast(Cast(0.00000001, TimestampType), DoubleType), 0.00000001)

    checkEvaluation(Cast(Literal(Double.NaN), TimestampType), null)
    checkEvaluation(Cast(Literal(1.0 / 0.0), TimestampType), null)
    checkEvaluation(Cast(Literal(Float.NaN), TimestampType), null)
    checkEvaluation(Cast(Literal(1.0f / 0.0f), TimestampType), null)
  }

  test("array casting") {
    val array = Literal.create(Seq("123", "abc", "", null),
      ArrayType(StringType, containsNull = true))
    val array_notNull = Literal.create(Seq("123", "abc", ""),
      ArrayType(StringType, containsNull = false))

    {
      val cast = Cast(array, ArrayType(IntegerType, containsNull = true))
      assert(cast.resolved === true)
      checkEvaluation(cast, Seq(123, null, null, null))
    }
    {
      val cast = Cast(array, ArrayType(IntegerType, containsNull = false))
      assert(cast.resolved === false)
    }
    {
      val cast = Cast(array, ArrayType(BooleanType, containsNull = true))
      assert(cast.resolved === true)
      checkEvaluation(cast, Seq(true, true, false, null))
    }
    {
      val cast = Cast(array, ArrayType(BooleanType, containsNull = false))
      assert(cast.resolved === false)
    }

    {
      val cast = Cast(array_notNull, ArrayType(IntegerType, containsNull = true))
      assert(cast.resolved === true)
      checkEvaluation(cast, Seq(123, null, null))
    }
    {
      val cast = Cast(array_notNull, ArrayType(IntegerType, containsNull = false))
      assert(cast.resolved === false)
    }
    {
      val cast = Cast(array_notNull, ArrayType(BooleanType, containsNull = true))
      assert(cast.resolved === true)
      checkEvaluation(cast, Seq(true, true, false))
    }
    {
      val cast = Cast(array_notNull, ArrayType(BooleanType, containsNull = false))
      assert(cast.resolved === true)
      checkEvaluation(cast, Seq(true, true, false))
    }

    {
      val cast = Cast(array, IntegerType)
      assert(cast.resolved === false)
    }
  }

  test("map casting") {
    val map = Literal.create(
      Map("a" -> "123", "b" -> "abc", "c" -> "", "d" -> null),
      MapType(StringType, StringType, valueContainsNull = true))
    val map_notNull = Literal.create(
      Map("a" -> "123", "b" -> "abc", "c" -> ""),
      MapType(StringType, StringType, valueContainsNull = false))

    {
      val cast = Cast(map, MapType(StringType, IntegerType, valueContainsNull = true))
      assert(cast.resolved === true)
      checkEvaluation(cast, Map("a" -> 123, "b" -> null, "c" -> null, "d" -> null))
    }
    {
      val cast = Cast(map, MapType(StringType, IntegerType, valueContainsNull = false))
      assert(cast.resolved === false)
    }
    {
      val cast = Cast(map, MapType(StringType, BooleanType, valueContainsNull = true))
      assert(cast.resolved === true)
      checkEvaluation(cast, Map("a" -> true, "b" -> true, "c" -> false, "d" -> null))
    }
    {
      val cast = Cast(map, MapType(StringType, BooleanType, valueContainsNull = false))
      assert(cast.resolved === false)
    }
    {
      val cast = Cast(map, MapType(IntegerType, StringType, valueContainsNull = true))
      assert(cast.resolved === false)
    }

    {
      val cast = Cast(map_notNull, MapType(StringType, IntegerType, valueContainsNull = true))
      assert(cast.resolved === true)
      checkEvaluation(cast, Map("a" -> 123, "b" -> null, "c" -> null))
    }
    {
      val cast = Cast(map_notNull, MapType(StringType, IntegerType, valueContainsNull = false))
      assert(cast.resolved === false)
    }
    {
      val cast = Cast(map_notNull, MapType(StringType, BooleanType, valueContainsNull = true))
      assert(cast.resolved === true)
      checkEvaluation(cast, Map("a" -> true, "b" -> true, "c" -> false))
    }
    {
      val cast = Cast(map_notNull, MapType(StringType, BooleanType, valueContainsNull = false))
      assert(cast.resolved === true)
      checkEvaluation(cast, Map("a" -> true, "b" -> true, "c" -> false))
    }
    {
      val cast = Cast(map_notNull, MapType(IntegerType, StringType, valueContainsNull = true))
      assert(cast.resolved === false)
    }

    {
      val cast = Cast(map, IntegerType)
      assert(cast.resolved === false)
    }
  }

  test("struct casting") {
    val struct = Literal.create(
      Row("123", "abc", "", null),
      StructType(Seq(
        StructField("a", StringType, nullable = true),
        StructField("b", StringType, nullable = true),
        StructField("c", StringType, nullable = true),
        StructField("d", StringType, nullable = true))))
    val struct_notNull = Literal.create(
      Row("123", "abc", ""),
      StructType(Seq(
        StructField("a", StringType, nullable = false),
        StructField("b", StringType, nullable = false),
        StructField("c", StringType, nullable = false))))

    {
      val cast = Cast(struct, StructType(Seq(
        StructField("a", IntegerType, nullable = true),
        StructField("b", IntegerType, nullable = true),
        StructField("c", IntegerType, nullable = true),
        StructField("d", IntegerType, nullable = true))))
      assert(cast.resolved === true)
      checkEvaluation(cast, Row(123, null, null, null))
    }
    {
      val cast = Cast(struct, StructType(Seq(
        StructField("a", IntegerType, nullable = true),
        StructField("b", IntegerType, nullable = true),
        StructField("c", IntegerType, nullable = false),
        StructField("d", IntegerType, nullable = true))))
      assert(cast.resolved === false)
    }
    {
      val cast = Cast(struct, StructType(Seq(
        StructField("a", BooleanType, nullable = true),
        StructField("b", BooleanType, nullable = true),
        StructField("c", BooleanType, nullable = true),
        StructField("d", BooleanType, nullable = true))))
      assert(cast.resolved === true)
      checkEvaluation(cast, Row(true, true, false, null))
    }
    {
      val cast = Cast(struct, StructType(Seq(
        StructField("a", BooleanType, nullable = true),
        StructField("b", BooleanType, nullable = true),
        StructField("c", BooleanType, nullable = false),
        StructField("d", BooleanType, nullable = true))))
      assert(cast.resolved === false)
    }

    {
      val cast = Cast(struct_notNull, StructType(Seq(
        StructField("a", IntegerType, nullable = true),
        StructField("b", IntegerType, nullable = true),
        StructField("c", IntegerType, nullable = true))))
      assert(cast.resolved === true)
      checkEvaluation(cast, Row(123, null, null))
    }
    {
      val cast = Cast(struct_notNull, StructType(Seq(
        StructField("a", IntegerType, nullable = true),
        StructField("b", IntegerType, nullable = true),
        StructField("c", IntegerType, nullable = false))))
      assert(cast.resolved === false)
    }
    {
      val cast = Cast(struct_notNull, StructType(Seq(
        StructField("a", BooleanType, nullable = true),
        StructField("b", BooleanType, nullable = true),
        StructField("c", BooleanType, nullable = true))))
      assert(cast.resolved === true)
      checkEvaluation(cast, Row(true, true, false))
    }
    {
      val cast = Cast(struct_notNull, StructType(Seq(
        StructField("a", BooleanType, nullable = true),
        StructField("b", BooleanType, nullable = true),
        StructField("c", BooleanType, nullable = false))))
      assert(cast.resolved === true)
      checkEvaluation(cast, Row(true, true, false))
    }

    {
      val cast = Cast(struct, StructType(Seq(
        StructField("a", StringType, nullable = true),
        StructField("b", StringType, nullable = true),
        StructField("c", StringType, nullable = true))))
      assert(cast.resolved === false)
    }
    {
      val cast = Cast(struct, IntegerType)
      assert(cast.resolved === false)
    }
  }

  test("complex casting") {
    val complex = Literal.create(
      Row(
        Seq("123", "abc", ""),
        Map("a" -> "123", "b" -> "abc", "c" -> ""),
        Row(0)),
      StructType(Seq(
        StructField("a",
          ArrayType(StringType, containsNull = false), nullable = true),
        StructField("m",
          MapType(StringType, StringType, valueContainsNull = false), nullable = true),
        StructField("s",
          StructType(Seq(
            StructField("i", IntegerType, nullable = true)))))))

    val cast = Cast(complex, StructType(Seq(
      StructField("a",
        ArrayType(IntegerType, containsNull = true), nullable = true),
      StructField("m",
        MapType(StringType, BooleanType, valueContainsNull = false), nullable = true),
      StructField("s",
        StructType(Seq(
          StructField("l", LongType, nullable = true)))))))

    assert(cast.resolved === true)
    checkEvaluation(cast, Row(
      Seq(123, null, null),
      Map("a" -> true, "b" -> true, "c" -> false),
      Row(0L)))
  }

  test("null checking") {
    val row = create_row("^Ba*n", null, true, null)
    val c1 = 'a.string.at(0)
    val c2 = 'a.string.at(1)
    val c3 = 'a.boolean.at(2)
    val c4 = 'a.boolean.at(3)

    checkEvaluation(c1.isNull, false, row)
    checkEvaluation(c1.isNotNull, true, row)

    checkEvaluation(c2.isNull, true, row)
    checkEvaluation(c2.isNotNull, false, row)

    checkEvaluation(Literal.create(1, ShortType).isNull, false)
    checkEvaluation(Literal.create(1, ShortType).isNotNull, true)

    checkEvaluation(Literal.create(null, ShortType).isNull, true)
    checkEvaluation(Literal.create(null, ShortType).isNotNull, false)

    checkEvaluation(Coalesce(c1 :: c2 :: Nil), "^Ba*n", row)
    checkEvaluation(Coalesce(Literal.create(null, StringType) :: Nil), null, row)
    checkEvaluation(Coalesce(Literal.create(null, StringType) :: c1 :: c2 :: Nil), "^Ba*n", row)

    checkEvaluation(
      If(c3, Literal.create("a", StringType), Literal.create("b", StringType)), "a", row)
    checkEvaluation(If(c3, c1, c2), "^Ba*n", row)
    checkEvaluation(If(c4, c2, c1), "^Ba*n", row)
    checkEvaluation(If(Literal.create(null, BooleanType), c2, c1), "^Ba*n", row)
    checkEvaluation(If(Literal.create(true, BooleanType), c1, c2), "^Ba*n", row)
    checkEvaluation(If(Literal.create(false, BooleanType), c2, c1), "^Ba*n", row)
    checkEvaluation(If(Literal.create(false, BooleanType),
      Literal.create("a", StringType), Literal.create("b", StringType)), "b", row)

    checkEvaluation(c1 in (c1, c2), true, row)
    checkEvaluation(
      Literal.create("^Ba*n", StringType) in (Literal.create("^Ba*n", StringType)), true, row)
    checkEvaluation(
      Literal.create("^Ba*n", StringType) in (Literal.create("^Ba*n", StringType), c2), true, row)
  }

  test("case when") {
    val row = create_row(null, false, true, "a", "b", "c")
    val c1 = 'a.boolean.at(0)
    val c2 = 'a.boolean.at(1)
    val c3 = 'a.boolean.at(2)
    val c4 = 'a.string.at(3)
    val c5 = 'a.string.at(4)
    val c6 = 'a.string.at(5)

    checkEvaluation(CaseWhen(Seq(c1, c4, c6)), "c", row)
    checkEvaluation(CaseWhen(Seq(c2, c4, c6)), "c", row)
    checkEvaluation(CaseWhen(Seq(c3, c4, c6)), "a", row)
    checkEvaluation(CaseWhen(Seq(Literal.create(null, BooleanType), c4, c6)), "c", row)
    checkEvaluation(CaseWhen(Seq(Literal.create(false, BooleanType), c4, c6)), "c", row)
    checkEvaluation(CaseWhen(Seq(Literal.create(true, BooleanType), c4, c6)), "a", row)

    checkEvaluation(CaseWhen(Seq(c3, c4, c2, c5, c6)), "a", row)
    checkEvaluation(CaseWhen(Seq(c2, c4, c3, c5, c6)), "b", row)
    checkEvaluation(CaseWhen(Seq(c1, c4, c2, c5, c6)), "c", row)
    checkEvaluation(CaseWhen(Seq(c1, c4, c2, c5)), null, row)

    assert(CaseWhen(Seq(c2, c4, c6)).nullable === true)
    assert(CaseWhen(Seq(c2, c4, c3, c5, c6)).nullable === true)
    assert(CaseWhen(Seq(c2, c4, c3, c5)).nullable === true)

    val c4_notNull = 'a.boolean.notNull.at(3)
    val c5_notNull = 'a.boolean.notNull.at(4)
    val c6_notNull = 'a.boolean.notNull.at(5)

    assert(CaseWhen(Seq(c2, c4_notNull, c6_notNull)).nullable === false)
    assert(CaseWhen(Seq(c2, c4, c6_notNull)).nullable === true)
    assert(CaseWhen(Seq(c2, c4_notNull, c6)).nullable === true)

    assert(CaseWhen(Seq(c2, c4_notNull, c3, c5_notNull, c6_notNull)).nullable === false)
    assert(CaseWhen(Seq(c2, c4, c3, c5_notNull, c6_notNull)).nullable === true)
    assert(CaseWhen(Seq(c2, c4_notNull, c3, c5, c6_notNull)).nullable === true)
    assert(CaseWhen(Seq(c2, c4_notNull, c3, c5_notNull, c6)).nullable === true)

    assert(CaseWhen(Seq(c2, c4_notNull, c3, c5_notNull)).nullable === true)
    assert(CaseWhen(Seq(c2, c4, c3, c5_notNull)).nullable === true)
    assert(CaseWhen(Seq(c2, c4_notNull, c3, c5)).nullable === true)
  }

  test("complex type") {
    val row = create_row(
      "^Ba*n",                                // 0
      null.asInstanceOf[UTF8String],          // 1
      create_row("aa", "bb"),     // 2
      Map("aa"->"bb"),                        // 3
      Seq("aa", "bb")                         // 4
    )

    val typeS = StructType(
      StructField("a", StringType, true) :: StructField("b", StringType, true) :: Nil
    )
    val typeMap = MapType(StringType, StringType)
    val typeArray = ArrayType(StringType)

    checkEvaluation(GetItem(BoundReference(3, typeMap, true),
      Literal("aa")), "bb", row)
    checkEvaluation(GetItem(Literal.create(null, typeMap), Literal("aa")), null, row)
    checkEvaluation(
      GetItem(Literal.create(null, typeMap), Literal.create(null, StringType)), null, row)
    checkEvaluation(GetItem(BoundReference(3, typeMap, true),
      Literal.create(null, StringType)), null, row)

    checkEvaluation(GetItem(BoundReference(4, typeArray, true),
      Literal(1)), "bb", row)
    checkEvaluation(GetItem(Literal.create(null, typeArray), Literal(1)), null, row)
    checkEvaluation(
      GetItem(Literal.create(null, typeArray), Literal.create(null, IntegerType)), null, row)
    checkEvaluation(GetItem(BoundReference(4, typeArray, true),
      Literal.create(null, IntegerType)), null, row)

    def quickBuildGetField(expr: Expression, fieldName: String): StructGetField = {
      expr.dataType match {
        case StructType(fields) =>
          val field = fields.find(_.name == fieldName).get
          StructGetField(expr, field, fields.indexOf(field))
      }
    }

    def quickResolve(u: UnresolvedGetField): StructGetField = {
      quickBuildGetField(u.child, u.fieldName)
    }

    checkEvaluation(quickBuildGetField(BoundReference(2, typeS, nullable = true), "a"), "aa", row)
    checkEvaluation(quickBuildGetField(Literal.create(null, typeS), "a"), null, row)

    val typeS_notNullable = StructType(
      StructField("a", StringType, nullable = false)
        :: StructField("b", StringType, nullable = false) :: Nil
    )

    assert(quickBuildGetField(BoundReference(2,typeS, nullable = true), "a").nullable === true)
    assert(quickBuildGetField(BoundReference(2, typeS_notNullable, nullable = false), "a").nullable
      === false)

    assert(quickBuildGetField(Literal.create(null, typeS), "a").nullable === true)
    assert(quickBuildGetField(Literal.create(null, typeS_notNullable), "a").nullable === true)

    checkEvaluation('c.map(typeMap).at(3).getItem("aa"), "bb", row)
    checkEvaluation('c.array(typeArray.elementType).at(4).getItem(1), "bb", row)
    checkEvaluation(quickResolve('c.struct(typeS).at(2).getField("a")), "aa", row)
  }

  test("arithmetic") {
    val row = create_row(1, 2, 3, null)
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.int.at(2)
    val c4 = 'a.int.at(3)

    checkEvaluation(UnaryMinus(c1), -1, row)
    checkEvaluation(UnaryMinus(Literal.create(100, IntegerType)), -100)

    checkEvaluation(Add(c1, c4), null, row)
    checkEvaluation(Add(c1, c2), 3, row)
    checkEvaluation(Add(c1, Literal.create(null, IntegerType)), null, row)
    checkEvaluation(Add(Literal.create(null, IntegerType), c2), null, row)
    checkEvaluation(
      Add(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null, row)

    checkEvaluation(-c1, -1, row)
    checkEvaluation(c1 + c2, 3, row)
    checkEvaluation(c1 - c2, -1, row)
    checkEvaluation(c1 * c2, 2, row)
    checkEvaluation(c1 / c2, 0, row)
    checkEvaluation(c1 % c2, 1, row)
  }

  test("fractional arithmetic") {
    val row = create_row(1.1, 2.0, 3.1, null)
    val c1 = 'a.double.at(0)
    val c2 = 'a.double.at(1)
    val c3 = 'a.double.at(2)
    val c4 = 'a.double.at(3)

    checkEvaluation(UnaryMinus(c1), -1.1, row)
    checkEvaluation(UnaryMinus(Literal.create(100.0, DoubleType)), -100.0)
    checkEvaluation(Add(c1, c4), null, row)
    checkEvaluation(Add(c1, c2), 3.1, row)
    checkEvaluation(Add(c1, Literal.create(null, DoubleType)), null, row)
    checkEvaluation(Add(Literal.create(null, DoubleType), c2), null, row)
    checkEvaluation(
      Add(Literal.create(null, DoubleType), Literal.create(null, DoubleType)), null, row)

    checkEvaluation(-c1, -1.1, row)
    checkEvaluation(c1 + c2, 3.1, row)
    checkDoubleEvaluation(c1 - c2, (-0.9 +- 0.001), row)
    checkDoubleEvaluation(c1 * c2, (2.2 +- 0.001), row)
    checkDoubleEvaluation(c1 / c2, (0.55 +- 0.001), row)
    checkDoubleEvaluation(c3 % c2, (1.1 +- 0.001), row)
  }

  test("BinaryComparison") {
    val row = create_row(1, 2, 3, null, 3, null)
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.int.at(2)
    val c4 = 'a.int.at(3)
    val c5 = 'a.int.at(4)
    val c6 = 'a.int.at(5)

    checkEvaluation(LessThan(c1, c4), null, row)
    checkEvaluation(LessThan(c1, c2), true, row)
    checkEvaluation(LessThan(c1, Literal.create(null, IntegerType)), null, row)
    checkEvaluation(LessThan(Literal.create(null, IntegerType), c2), null, row)
    checkEvaluation(
      LessThan(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null, row)

    checkEvaluation(c1 < c2, true, row)
    checkEvaluation(c1 <= c2, true, row)
    checkEvaluation(c1 > c2, false, row)
    checkEvaluation(c1 >= c2, false, row)
    checkEvaluation(c1 === c2, false, row)
    checkEvaluation(c1 !== c2, true, row)
    checkEvaluation(c4 <=> c1, false, row)
    checkEvaluation(c1 <=> c4, false, row)
    checkEvaluation(c4 <=> c6, true, row)
    checkEvaluation(c3 <=> c5, true, row)
    checkEvaluation(Literal(true) <=> Literal.create(null, BooleanType), false, row)
    checkEvaluation(Literal.create(null, BooleanType) <=> Literal(true), false, row)
  }

  test("StringComparison") {
    val row = create_row("abc", null)
    val c1 = 'a.string.at(0)
    val c2 = 'a.string.at(1)

    checkEvaluation(c1 contains "b", true, row)
    checkEvaluation(c1 contains "x", false, row)
    checkEvaluation(c2 contains "b", null, row)
    checkEvaluation(c1 contains Literal.create(null, StringType), null, row)

    checkEvaluation(c1 startsWith "a", true, row)
    checkEvaluation(c1 startsWith "b", false, row)
    checkEvaluation(c2 startsWith "a", null, row)
    checkEvaluation(c1 startsWith Literal.create(null, StringType), null, row)

    checkEvaluation(c1 endsWith "c", true, row)
    checkEvaluation(c1 endsWith "b", false, row)
    checkEvaluation(c2 endsWith "b", null, row)
    checkEvaluation(c1 endsWith Literal.create(null, StringType), null, row)
  }

  test("Substring") {
    val row = create_row("example", "example".toArray.map(_.toByte))

    val s = 'a.string.at(0)

    // substring from zero position with less-than-full length
    checkEvaluation(
      Substring(s, Literal.create(0, IntegerType), Literal.create(2, IntegerType)), "ex", row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(2, IntegerType)), "ex", row)

    // substring from zero position with full length
    checkEvaluation(
      Substring(s, Literal.create(0, IntegerType), Literal.create(7, IntegerType)), "example", row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(7, IntegerType)), "example", row)

    // substring from zero position with greater-than-full length
    checkEvaluation(Substring(s, Literal.create(0, IntegerType), Literal.create(100, IntegerType)),
      "example", row)
    checkEvaluation(Substring(s, Literal.create(1, IntegerType), Literal.create(100, IntegerType)),
      "example", row)

    // substring from nonzero position with less-than-full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(2, IntegerType)),
      "xa", row)

    // substring from nonzero position with full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(6, IntegerType)),
      "xample", row)

    // substring from nonzero position with greater-than-full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(100, IntegerType)),
      "xample", row)

    // zero-length substring (within string bounds)
    checkEvaluation(Substring(s, Literal.create(0, IntegerType), Literal.create(0, IntegerType)),
      "", row)

    // zero-length substring (beyond string bounds)
    checkEvaluation(Substring(s, Literal.create(100, IntegerType), Literal.create(4, IntegerType)),
      "", row)

    // substring(null, _, _) -> null
    checkEvaluation(Substring(s, Literal.create(100, IntegerType), Literal.create(4, IntegerType)),
      null, create_row(null))

    // substring(_, null, _) -> null
    checkEvaluation(Substring(s, Literal.create(null, IntegerType), Literal.create(4, IntegerType)),
      null, row)

    // substring(_, _, null) -> null
    checkEvaluation(
      Substring(s, Literal.create(100, IntegerType), Literal.create(null, IntegerType)),
      null,
      row)

    // 2-arg substring from zero position
    checkEvaluation(
      Substring(s, Literal.create(0, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "example",
      row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "example",
      row)

    // 2-arg substring from nonzero position
    checkEvaluation(
      Substring(s, Literal.create(2, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "xample",
      row)

    val s_notNull = 'a.string.notNull.at(0)

    assert(Substring(s, Literal.create(0, IntegerType), Literal.create(2, IntegerType)).nullable
      === true)
    assert(
      Substring(s_notNull, Literal.create(0, IntegerType), Literal.create(2, IntegerType)).nullable
        === false)
    assert(Substring(s_notNull,
      Literal.create(null, IntegerType), Literal.create(2, IntegerType)).nullable === true)
    assert(Substring(s_notNull,
      Literal.create(0, IntegerType), Literal.create(null, IntegerType)).nullable === true)

    checkEvaluation(s.substr(0, 2), "ex", row)
    checkEvaluation(s.substr(0), "example", row)
    checkEvaluation(s.substring(0, 2), "ex", row)
    checkEvaluation(s.substring(0), "example", row)
  }

  test("SQRT") {
    val inputSequence = (1 to (1<<24) by 511).map(_ * (1L<<24))
    val expectedResults = inputSequence.map(l => math.sqrt(l.toDouble))
    val rowSequence = inputSequence.map(l => create_row(l.toDouble))
    val d = 'a.double.at(0)

    for ((row, expected) <- rowSequence zip expectedResults) {
      checkEvaluation(Sqrt(d), expected, row)
    }

    checkEvaluation(Sqrt(Literal.create(null, DoubleType)), null, create_row(null))
    checkEvaluation(Sqrt(-1), null, EmptyRow)
    checkEvaluation(Sqrt(-1.5), null, EmptyRow)
  }

  test("Bitwise operations") {
    val row = create_row(1, 2, 3, null)
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.int.at(2)
    val c4 = 'a.int.at(3)

    checkEvaluation(BitwiseAnd(c1, c4), null, row)
    checkEvaluation(BitwiseAnd(c1, c2), 0, row)
    checkEvaluation(BitwiseAnd(c1, Literal.create(null, IntegerType)), null, row)
    checkEvaluation(
      BitwiseAnd(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null, row)

    checkEvaluation(BitwiseOr(c1, c4), null, row)
    checkEvaluation(BitwiseOr(c1, c2), 3, row)
    checkEvaluation(BitwiseOr(c1, Literal.create(null, IntegerType)), null, row)
    checkEvaluation(
      BitwiseOr(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null, row)

    checkEvaluation(BitwiseXor(c1, c4), null, row)
    checkEvaluation(BitwiseXor(c1, c2), 3, row)
    checkEvaluation(BitwiseXor(c1, Literal.create(null, IntegerType)), null, row)
    checkEvaluation(
      BitwiseXor(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null, row)

    checkEvaluation(BitwiseNot(c4), null, row)
    checkEvaluation(BitwiseNot(c1), -2, row)
    checkEvaluation(BitwiseNot(Literal.create(null, IntegerType)), null, row)

    checkEvaluation(c1 & c2, 0, row)
    checkEvaluation(c1 | c2, 3, row)
    checkEvaluation(c1 ^ c2, 3, row)
    checkEvaluation(~c1, -2, row)
  }

  /**
   * Used for testing math functions for DataFrames. 
   * @param c The DataFrame function
   * @param f The functions in scala.math
   * @param domain The set of values to run the function with
   * @param expectNull Whether the given values should return null or not
   * @tparam T Generic type for primitives
   */
  def unaryMathFunctionEvaluation[@specialized(Int, Double, Float, Long) T](
      c: Expression => Expression, 
      f: T => T,
      domain: Iterable[T] = (-20 to 20).map(_ * 0.1),
      expectNull: Boolean = false): Unit = {
    if (expectNull) {
      domain.foreach { value =>
        checkEvaluation(c(Literal(value)), null, EmptyRow)
      }
    } else {
      domain.foreach { value =>
        checkEvaluation(c(Literal(value)), f(value), EmptyRow)
      }
    }
    checkEvaluation(c(Literal.create(null, DoubleType)), null, create_row(null))
  }

  test("sin") {
    unaryMathFunctionEvaluation(Sin, math.sin)
  }

  test("asin") {
    unaryMathFunctionEvaluation(Asin, math.asin, (-10 to 10).map(_ * 0.1))
    unaryMathFunctionEvaluation(Asin, math.asin, (11 to 20).map(_ * 0.1), true)
  }

  test("sinh") {
    unaryMathFunctionEvaluation(Sinh, math.sinh)
  }

  test("cos") {
    unaryMathFunctionEvaluation(Cos, math.cos)
  }

  test("acos") {
    unaryMathFunctionEvaluation(Acos, math.acos, (-10 to 10).map(_ * 0.1))
    unaryMathFunctionEvaluation(Acos, math.acos, (11 to 20).map(_ * 0.1), true)
  }

  test("cosh") {
    unaryMathFunctionEvaluation(Cosh, math.cosh)
  }

  test("tan") {
    unaryMathFunctionEvaluation(Tan, math.tan)
  }

  test("atan") {
    unaryMathFunctionEvaluation(Atan, math.atan)
  }

  test("tanh") {
    unaryMathFunctionEvaluation(Tanh, math.tanh)
  }

  test("toDeg") {
    unaryMathFunctionEvaluation(ToDegrees, math.toDegrees)
  }

  test("toRad") {
    unaryMathFunctionEvaluation(ToRadians, math.toRadians)
  }

  test("cbrt") {
    unaryMathFunctionEvaluation(Cbrt, math.cbrt)
  }

  test("ceil") {
    unaryMathFunctionEvaluation(Ceil, math.ceil)
  }

  test("floor") {
    unaryMathFunctionEvaluation(Floor, math.floor)
  }

  test("rint") {
    unaryMathFunctionEvaluation(Rint, math.rint)
  }

  test("exp") {
    unaryMathFunctionEvaluation(Exp, math.exp)
  }

  test("expm1") {
    unaryMathFunctionEvaluation(Expm1, math.expm1)
  }

  test("signum") {
    unaryMathFunctionEvaluation[Double](Signum, math.signum)
  }

  test("log") {
    unaryMathFunctionEvaluation(Log, math.log, (0 to 20).map(_ * 0.1))
    unaryMathFunctionEvaluation(Log, math.log, (-5 to -1).map(_ * 0.1), true)
  }

  test("log10") {
    unaryMathFunctionEvaluation(Log10, math.log10, (0 to 20).map(_ * 0.1))
    unaryMathFunctionEvaluation(Log10, math.log10, (-5 to -1).map(_ * 0.1), true)
  }

  test("log1p") {
    unaryMathFunctionEvaluation(Log1p, math.log1p, (-1 to 20).map(_ * 0.1))
    unaryMathFunctionEvaluation(Log1p, math.log1p, (-10 to -2).map(_ * 1.0), true)
  }

  /**
   * Used for testing math functions for DataFrames.
   * @param c The DataFrame function
   * @param f The functions in scala.math
   * @param domain The set of values to run the function with
   */
  def binaryMathFunctionEvaluation(
      c: (Expression, Expression) => Expression,
      f: (Double, Double) => Double,
      domain: Iterable[(Double, Double)] = (-20 to 20).map(v => (v * 0.1, v * -0.1)),
      expectNull: Boolean = false): Unit = {
    if (expectNull) {
      domain.foreach { case (v1, v2) =>
        checkEvaluation(c(v1, v2), null, create_row(null))
      }
    } else {
      domain.foreach { case (v1, v2) =>
        checkEvaluation(c(v1, v2), f(v1 + 0.0, v2 + 0.0), EmptyRow)
        checkEvaluation(c(v2, v1), f(v2 + 0.0, v1 + 0.0), EmptyRow)
      }
    }
    checkEvaluation(c(Literal.create(null, DoubleType), 1.0), null, create_row(null))
    checkEvaluation(c(1.0, Literal.create(null, DoubleType)), null, create_row(null))
  }

  test("pow") {
    binaryMathFunctionEvaluation(Pow, math.pow, (-5 to 5).map(v => (v * 1.0, v * 1.0)))
    binaryMathFunctionEvaluation(Pow, math.pow, Seq((-1.0, 0.9), (-2.2, 1.7), (-2.2, -1.7)), true)
  }

  test("hypot") {
    binaryMathFunctionEvaluation(Hypot, math.hypot)
  }

  test("atan2") {
    binaryMathFunctionEvaluation(Atan2, math.atan2)
  }
}

// TODO: Make the tests work with codegen.
class ExpressionEvaluationWithoutCodeGenSuite extends ExpressionEvaluationBaseSuite {

  test("CreateStruct") {
    val row = Row(1, 2, 3)
    val c1 = 'a.int.at(0).as("a")
    val c3 = 'c.int.at(2).as("c")
    checkEvaluation(CreateStruct(Seq(c1, c3)), Row(1, 3), row)
  }
}
