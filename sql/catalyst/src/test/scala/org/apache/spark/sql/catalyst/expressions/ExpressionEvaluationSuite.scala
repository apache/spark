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

import java.sql.Timestamp

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.types._

/* Implicit conversions */
import org.apache.spark.sql.catalyst.dsl.expressions._

class ExpressionEvaluationSuite extends FunSuite {

  test("literals") {
    assert((Literal(1) + Literal(1)).eval(null) === 2)
  }

  /**
   * Checks for three-valued-logic.  Based on:
   * http://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_.283VL.29
   *
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

  val notTrueTable =
    (true, false) ::
    (false, true) ::
    (null, null) :: Nil

  test("3VL Not") {
    notTrueTable.foreach {
      case (v, answer) =>
        val expr = Not(Literal(v, BooleanType))
        val result = expr.eval(null)
        if (result != answer)
          fail(s"$expr should not evaluate to $result, expected: $answer")    }
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
          val expr = op(Literal(l, BooleanType), Literal(r, BooleanType))
          val result = expr.eval(null)
          if (result != answer)
            fail(s"$expr should not evaluate to $result, expected: $answer")
      }
    }
  }

  def evaluate(expression: Expression, inputRow: Row = EmptyRow): Any = {
    expression.eval(inputRow)
  }

  def checkEvaluation(
      expression: Expression,
      dataType: DataType,
      foldable: Boolean,
      nullable: Boolean,
      expected: Any,
      inputRow: Row = EmptyRow): Unit = {
    assert(expression.dataType === dataType)
    assert(expression.foldable === foldable)
    assert(expression.nullable === nullable)
    val actual = try evaluate(expression, inputRow) catch {
      case e: Exception => fail(s"Exception evaluating $expression", e)
    }
    if(actual != expected) {
      val input = if(inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(s"Incorrect Evaluation: $expression, actual: $actual, expected: $expected$input")
    }
  }

  test("LIKE literal Regular Expression") {
    checkEvaluation(Literal(null, StringType).like("a"), BooleanType, true, true, null)
    checkEvaluation(Literal(null, StringType).like(Literal(null, StringType)),
      BooleanType, true, true, null)
    checkEvaluation("abdef" like "abdef", BooleanType, true, true, true)
    checkEvaluation("a_%b" like "a\\__b", BooleanType, true, true, true)
    checkEvaluation("addb" like "a_%b", BooleanType, true, true, true)
    checkEvaluation("addb" like "a\\__b", BooleanType, true, true, false)
    checkEvaluation("addb" like "a%\\%b", BooleanType, true, true, false)
    checkEvaluation("a_%b" like "a%\\%b", BooleanType, true, true, true)
    checkEvaluation("addb" like "a%", BooleanType, true, true, true)
    checkEvaluation("addb" like "**", BooleanType, true, true, false)
    checkEvaluation("abc" like "a%", BooleanType, true, true, true)
    checkEvaluation("abc" like "b%", BooleanType, true, true, false)
    checkEvaluation("abc" like "bc%", BooleanType, true, true, false)
  }

  test("LIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abcd" like regEx, BooleanType, false, true, null,
      new GenericRow(Array[Any](null)))
    checkEvaluation("abdef" like regEx, BooleanType, false, true, true,
      new GenericRow(Array[Any]("abdef")))
    checkEvaluation("a_%b" like regEx, BooleanType, false, true, true,
      new GenericRow(Array[Any]("a\\__b")))
    checkEvaluation("addb" like regEx, BooleanType, false, true, true,
      new GenericRow(Array[Any]("a_%b")))
    checkEvaluation("addb" like regEx, BooleanType, false, true, false,
      new GenericRow(Array[Any]("a\\__b")))
    checkEvaluation("addb" like regEx, BooleanType, false, true, false,
      new GenericRow(Array[Any]("a%\\%b")))
    checkEvaluation("a_%b" like regEx, BooleanType, false, true, true,
      new GenericRow(Array[Any]("a%\\%b")))
    checkEvaluation("addb" like regEx, BooleanType, false, true, true,
      new GenericRow(Array[Any]("a%")))
    checkEvaluation("addb" like regEx, BooleanType, false, true, false,
      new GenericRow(Array[Any]("**")))
    checkEvaluation("abc" like regEx, BooleanType, false, true, true,
      new GenericRow(Array[Any]("a%")))
    checkEvaluation("abc" like regEx, BooleanType, false, true, false,
      new GenericRow(Array[Any]("b%")))
    checkEvaluation("abc" like regEx, BooleanType, false, true, false,
      new GenericRow(Array[Any]("bc%")))
  }

  test("RLIKE literal Regular Expression") {
    checkEvaluation("abdef" rlike "abdef", BooleanType, true, true, true)
    checkEvaluation("abbbbc" rlike "a.*c", BooleanType, true, true, true)

    checkEvaluation("fofo" rlike "^fo", BooleanType, true, true, true)
    checkEvaluation("fo\no" rlike "^fo\no$", BooleanType, true, true, true)
    checkEvaluation("Bn" rlike "^Ba*n", BooleanType, true, true, true)
    checkEvaluation("afofo" rlike "fo", BooleanType, true, true, true)
    checkEvaluation("afofo" rlike "^fo", BooleanType, true, true, false)
    checkEvaluation("Baan" rlike "^Ba?n", BooleanType, true, true, false)
    checkEvaluation("axe" rlike "pi|apa", BooleanType, true, true, false)
    checkEvaluation("pip" rlike "^(pi)*$", BooleanType, true, true, false)

    checkEvaluation("abc" rlike "^ab", BooleanType, true, true, true)
    checkEvaluation("abc" rlike "^bc", BooleanType, true, true, false)
    checkEvaluation("abc" rlike "^ab", BooleanType, true, true, true)
    checkEvaluation("abc" rlike "^bc", BooleanType, true, true, false)

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" rlike "**")
    }
  }

  test("RLIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abdef" rlike regEx, BooleanType, false, true, true,
      new GenericRow(Array[Any]("abdef")))
    checkEvaluation("abbbbc" rlike regEx, BooleanType, false, true, true,
      new GenericRow(Array[Any]("a.*c")))
    checkEvaluation("fofo" rlike regEx, BooleanType, false, true, true,
      new GenericRow(Array[Any]("^fo")))
    checkEvaluation("fo\no" rlike regEx, BooleanType, false, true, true,
      new GenericRow(Array[Any]("^fo\no$")))
    checkEvaluation("Bn" rlike regEx, BooleanType, false, true, true,
      new GenericRow(Array[Any]("^Ba*n")))

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" rlike regEx, new GenericRow(Array[Any]("**")))
    }
  }

  test("data type casting") {

    val sts = "1970-01-01 00:00:01.0"
    val ts = Timestamp.valueOf(sts)

    checkEvaluation("abdef" cast StringType, StringType, true, false, "abdef")
    checkEvaluation("abdef" cast DecimalType, DecimalType, true, true, null)
    checkEvaluation("abdef" cast TimestampType, TimestampType, true, true, null)
    checkEvaluation("12.65" cast DecimalType, DecimalType, true, true, BigDecimal(12.65))

    checkEvaluation(Literal(1) cast LongType, LongType, true, false, 1)
    checkEvaluation(Cast(Literal(1) cast TimestampType, LongType), LongType, true, false, 1)
    checkEvaluation(Cast(Literal(1.toDouble) cast TimestampType, DoubleType),
      DoubleType, true, false, 1.toDouble)

    checkEvaluation(Cast(Literal(sts) cast TimestampType, StringType),
      StringType, true, true, sts)
    checkEvaluation(Cast(Literal(ts) cast StringType, TimestampType),
      TimestampType, true, true, ts)

    checkEvaluation(Cast("abdef" cast BinaryType, StringType), StringType, true, false, "abdef")

    checkEvaluation(Cast(Cast(Cast(Cast(
      Cast("5" cast ByteType, ShortType), IntegerType), FloatType), DoubleType), LongType),
      LongType, true, true, 5)
    checkEvaluation(Cast(Cast(Cast(Cast(
      Cast("5" cast ByteType, TimestampType), DecimalType), LongType), StringType), ShortType),
      ShortType, true, true, 5)
    checkEvaluation(Cast(Cast(Cast(Cast(
      Cast("5" cast TimestampType, ByteType), DecimalType), LongType), StringType), ShortType),
      ShortType, true, true, null)
    checkEvaluation(Cast(Cast(Cast(Cast(
      Cast("5" cast DecimalType, ByteType), TimestampType), LongType), StringType), ShortType),
      ShortType, true, true, 5)
    checkEvaluation(Literal(true) cast IntegerType, IntegerType, true, false, 1)
    checkEvaluation(Literal(false) cast IntegerType, IntegerType, true, false, 0)
    checkEvaluation(Cast(Literal(1) cast BooleanType, IntegerType), IntegerType, true, false, 1)
    checkEvaluation(Cast(Literal(0) cast BooleanType, IntegerType), IntegerType, true, false, 0)
    checkEvaluation("23" cast DoubleType, DoubleType, true, true, 23)
    checkEvaluation("23" cast IntegerType, IntegerType, true, true, 23)
    checkEvaluation("23" cast FloatType, FloatType, true, true, 23)
    checkEvaluation("23" cast DecimalType, DecimalType, true, true, 23)
    checkEvaluation("23" cast ByteType, ByteType, true, true, 23)
    checkEvaluation("23" cast ShortType, ShortType, true, true, 23)
    checkEvaluation("2012-12-11" cast DoubleType, DoubleType, true, true, null)
    checkEvaluation(Literal(123) cast IntegerType, IntegerType, true, false, 123)

    intercept[Exception] { evaluate(Literal(1) cast BinaryType, null) }
  }

  test("timestamp") {
    val ts1 = new Timestamp(12)
    val ts2 = new Timestamp(123)
    checkEvaluation(Literal("ab") < Literal("abc"), BooleanType, true, false, true)
    checkEvaluation(Literal(ts1) < Literal(ts2), BooleanType, true, false, true)
  }

  test("timestamp casting") {
    val millis = 15 * 1000 + 2
    val ts = new Timestamp(millis)
    val ts1 = new Timestamp(15 * 1000) // a timestamp without the milliseconds part
    checkEvaluation(Cast(ts, ShortType), ShortType, true, false, 15)
    checkEvaluation(Cast(ts, IntegerType), IntegerType, true, false, 15)
    checkEvaluation(Cast(ts, LongType), LongType, true, false, 15)
    checkEvaluation(Cast(ts, FloatType), FloatType, true, false, 15.002f)
    checkEvaluation(Cast(ts, DoubleType), DoubleType, true, false, 15.002)
    checkEvaluation(Cast(Cast(ts, ShortType), TimestampType), TimestampType, true, false, ts1)
    checkEvaluation(Cast(Cast(ts, IntegerType), TimestampType), TimestampType, true, false, ts1)
    checkEvaluation(Cast(Cast(ts, LongType), TimestampType), TimestampType, true, false, ts1)
    checkEvaluation(Cast(Cast(millis.toFloat / 1000, TimestampType), FloatType),
      FloatType, true, false, millis.toFloat / 1000)
    checkEvaluation(Cast(Cast(millis.toDouble / 1000, TimestampType), DoubleType),
      DoubleType, true, false, millis.toDouble / 1000)
    checkEvaluation(Cast(Literal(BigDecimal(1)) cast TimestampType, DecimalType),
      DecimalType, true, false, 1)

    // A test for higher precision than millis
    checkEvaluation(Cast(Cast(0.00000001, TimestampType), DoubleType),
      DoubleType, true, false, 0.00000001)
  }
}

