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
   * I.e. in flat cpo "False -> Unknown -> True", OR is lowest upper bound, AND is greatest lower bound.
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
        val expr = ! Literal(v, BooleanType)
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
          checkEvaluation(expr, answer)
      }
    }
  }

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

  test("LIKE literal Regular Expression") {
    checkEvaluation(Literal(null, StringType).like("a"), null)
    checkEvaluation(Literal("a", StringType).like(Literal(null, StringType)), null)
    checkEvaluation(Literal(null, StringType).like(Literal(null, StringType)), null)
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
  }

  test("LIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abcd" like regEx, null, new GenericRow(Array[Any](null)))
    checkEvaluation("abdef" like regEx, true, new GenericRow(Array[Any]("abdef")))
    checkEvaluation("a_%b" like regEx, true, new GenericRow(Array[Any]("a\\__b")))
    checkEvaluation("addb" like regEx, true, new GenericRow(Array[Any]("a_%b")))
    checkEvaluation("addb" like regEx, false, new GenericRow(Array[Any]("a\\__b")))
    checkEvaluation("addb" like regEx, false, new GenericRow(Array[Any]("a%\\%b")))
    checkEvaluation("a_%b" like regEx, true, new GenericRow(Array[Any]("a%\\%b")))
    checkEvaluation("addb" like regEx, true, new GenericRow(Array[Any]("a%")))
    checkEvaluation("addb" like regEx, false, new GenericRow(Array[Any]("**")))
    checkEvaluation("abc" like regEx, true, new GenericRow(Array[Any]("a%")))
    checkEvaluation("abc" like regEx, false, new GenericRow(Array[Any]("b%")))
    checkEvaluation("abc" like regEx, false, new GenericRow(Array[Any]("bc%")))
    
    checkEvaluation(Literal(null, StringType) like regEx, null, new GenericRow(Array[Any]("bc%")))
  }

  test("RLIKE literal Regular Expression") {
    checkEvaluation(Literal(null, StringType) rlike "abdef", null)
    checkEvaluation("abdef" rlike Literal(null, StringType), null)
    checkEvaluation(Literal(null, StringType) rlike Literal(null, StringType), null)
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
    checkEvaluation("abdef" rlike regEx, true, new GenericRow(Array[Any]("abdef")))
    checkEvaluation("abbbbc" rlike regEx, true, new GenericRow(Array[Any]("a.*c")))
    checkEvaluation("fofo" rlike regEx, true, new GenericRow(Array[Any]("^fo")))
    checkEvaluation("fo\no" rlike regEx, true, new GenericRow(Array[Any]("^fo\no$")))
    checkEvaluation("Bn" rlike regEx, true, new GenericRow(Array[Any]("^Ba*n")))

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" rlike regEx, new GenericRow(Array[Any]("**")))
    }
  }

  test("data type casting") {

    val sts = "1970-01-01 00:00:01.0"
    val ts = Timestamp.valueOf(sts)

    checkEvaluation("abdef" cast StringType, "abdef")
    checkEvaluation("abdef" cast DecimalType, null)
    checkEvaluation("abdef" cast TimestampType, null)
    checkEvaluation("12.65" cast DecimalType, BigDecimal(12.65))

    checkEvaluation(Literal(1) cast LongType, 1)
    checkEvaluation(Cast(Literal(1) cast TimestampType, LongType), 1)
    checkEvaluation(Cast(Literal(1.toDouble) cast TimestampType, DoubleType), 1.toDouble)

    checkEvaluation(Cast(Literal(sts) cast TimestampType, StringType), sts)
    checkEvaluation(Cast(Literal(ts) cast StringType, TimestampType), ts)

    checkEvaluation(Cast("abdef" cast BinaryType, StringType), "abdef")

    checkEvaluation(Cast(Cast(Cast(Cast(
      Cast("5" cast ByteType, ShortType), IntegerType), FloatType), DoubleType), LongType), 5)
    checkEvaluation(Cast(Cast(Cast(Cast(
      Cast("5" cast ByteType, TimestampType), DecimalType), LongType), StringType), ShortType), 5)
    checkEvaluation(Cast(Cast(Cast(Cast(
      Cast("5" cast TimestampType, ByteType), DecimalType), LongType), StringType), ShortType), null)
    checkEvaluation(Cast(Cast(Cast(Cast(
      Cast("5" cast DecimalType, ByteType), TimestampType), LongType), StringType), ShortType), 5)
    checkEvaluation(Literal(true) cast IntegerType, 1)
    checkEvaluation(Literal(false) cast IntegerType, 0)
    checkEvaluation(Cast(Literal(1) cast BooleanType, IntegerType), 1)
    checkEvaluation(Cast(Literal(0) cast BooleanType, IntegerType), 0)
    checkEvaluation("23" cast DoubleType, 23)
    checkEvaluation("23" cast IntegerType, 23)
    checkEvaluation("23" cast FloatType, 23)
    checkEvaluation("23" cast DecimalType, 23)
    checkEvaluation("23" cast ByteType, 23)
    checkEvaluation("23" cast ShortType, 23)
    checkEvaluation("2012-12-11" cast DoubleType, null)
    checkEvaluation(Literal(123) cast IntegerType, 123)

    checkEvaluation(Literal(23d) + Cast(true, DoubleType), 24)
    checkEvaluation(Literal(23) + Cast(true, IntegerType), 24)
    checkEvaluation(Literal(23f) + Cast(true, FloatType), 24)
    checkEvaluation(Literal(BigDecimal(23)) + Cast(true, DecimalType), 24)
    checkEvaluation(Literal(23.toByte) + Cast(true, ByteType), 24)
    checkEvaluation(Literal(23.toShort) + Cast(true, ShortType), 24)

    intercept[Exception] {evaluate(Literal(1) cast BinaryType, null)}

    assert(("abcdef" cast StringType).nullable === false)
    assert(("abcdef" cast BinaryType).nullable === false)
    assert(("abcdef" cast BooleanType).nullable === false)
    assert(("abcdef" cast TimestampType).nullable === true)
    assert(("abcdef" cast LongType).nullable === true)
    assert(("abcdef" cast IntegerType).nullable === true)
    assert(("abcdef" cast ShortType).nullable === true)
    assert(("abcdef" cast ByteType).nullable === true)
    assert(("abcdef" cast DecimalType).nullable === true)
    assert(("abcdef" cast DoubleType).nullable === true)
    assert(("abcdef" cast FloatType).nullable === true)

    checkEvaluation(Cast(Literal(null, IntegerType), ShortType), null)
  }

  test("timestamp") {
    val ts1 = new Timestamp(12)
    val ts2 = new Timestamp(123)
    checkEvaluation(Literal("ab") < Literal("abc"), true)
    checkEvaluation(Literal(ts1) < Literal(ts2), true)
  }

  test("timestamp casting") {
    val millis = 15 * 1000 + 2
    val ts = new Timestamp(millis)
    val ts1 = new Timestamp(15 * 1000)  // a timestamp without the milliseconds part
    checkEvaluation(Cast(ts, ShortType), 15)
    checkEvaluation(Cast(ts, IntegerType), 15)
    checkEvaluation(Cast(ts, LongType), 15)
    checkEvaluation(Cast(ts, FloatType), 15.002f)
    checkEvaluation(Cast(ts, DoubleType), 15.002)
    checkEvaluation(Cast(Cast(ts, ShortType), TimestampType), ts1)
    checkEvaluation(Cast(Cast(ts, IntegerType), TimestampType), ts1)
    checkEvaluation(Cast(Cast(ts, LongType), TimestampType), ts1)
    checkEvaluation(Cast(Cast(millis.toFloat / 1000, TimestampType), FloatType),
      millis.toFloat / 1000)
    checkEvaluation(Cast(Cast(millis.toDouble / 1000, TimestampType), DoubleType),
      millis.toDouble / 1000)
    checkEvaluation(Cast(Literal(BigDecimal(1)) cast TimestampType, DecimalType), 1)

    // A test for higher precision than millis
    checkEvaluation(Cast(Cast(0.00000001, TimestampType), DoubleType), 0.00000001)
  }
  
  test("null checking") {
    val row = new GenericRow(Array[Any]("^Ba*n", null, true, null))
    val c1 = 'a.string.at(0)
    val c2 = 'a.string.at(1)
    val c3 = 'a.boolean.at(2)
    val c4 = 'a.boolean.at(3)

    checkEvaluation(IsNull(c1), false, row)
    checkEvaluation(IsNotNull(c1), true, row)

    checkEvaluation(IsNull(c2), true, row)
    checkEvaluation(IsNotNull(c2), false, row)

    checkEvaluation(IsNull(Literal(1, ShortType)), false)
    checkEvaluation(IsNotNull(Literal(1, ShortType)), true)

    checkEvaluation(IsNull(Literal(null, ShortType)), true)
    checkEvaluation(IsNotNull(Literal(null, ShortType)), false)
    
    checkEvaluation(Coalesce(c1 :: c2 :: Nil), "^Ba*n", row)
    checkEvaluation(Coalesce(Literal(null, StringType) :: Nil), null, row)
    checkEvaluation(Coalesce(Literal(null, StringType) :: c1 :: c2 :: Nil), "^Ba*n", row)

    checkEvaluation(If(c3, Literal("a", StringType), Literal("b", StringType)), "a", row)
    checkEvaluation(If(c3, c1, c2), "^Ba*n", row)
    checkEvaluation(If(c4, c2, c1), "^Ba*n", row)
    checkEvaluation(If(Literal(null, BooleanType), c2, c1), "^Ba*n", row)
    checkEvaluation(If(Literal(true, BooleanType), c1, c2), "^Ba*n", row)
    checkEvaluation(If(Literal(false, BooleanType), c2, c1), "^Ba*n", row)
    checkEvaluation(If(Literal(false, BooleanType), 
      Literal("a", StringType), Literal("b", StringType)), "b", row)

    checkEvaluation(In(c1, c1 :: c2 :: Nil), true, row)
    checkEvaluation(In(Literal("^Ba*n", StringType), 
      Literal("^Ba*n", StringType) :: Nil), true, row)
    checkEvaluation(In(Literal("^Ba*n", StringType),
      Literal("^Ba*n", StringType) :: c2 :: Nil), true, row)
  }

  test("complex type") {
    val row = new GenericRow(Array[Any](
      "^Ba*n",                                  // 0 
      null.asInstanceOf[String],                // 1
      new GenericRow(Array[Any]("aa", "bb")),   // 2
      Map("aa"->"bb"),                          // 3
      Seq("aa", "bb")                           // 4
    ))

    val typeS = StructType(
      StructField("a", StringType, true) :: StructField("b", StringType, true) :: Nil
    )
    val typeMap = MapType(StringType, StringType)
    val typeArray = ArrayType(StringType)

    checkEvaluation(GetItem(BoundReference(3, AttributeReference("c", typeMap)()), 
      Literal("aa")), "bb", row)
    checkEvaluation(GetItem(Literal(null, typeMap), Literal("aa")), null, row)
    checkEvaluation(GetItem(Literal(null, typeMap), Literal(null, StringType)), null, row)
    checkEvaluation(GetItem(BoundReference(3, AttributeReference("c", typeMap)()), 
      Literal(null, StringType)), null, row)

    checkEvaluation(GetItem(BoundReference(4, AttributeReference("c", typeArray)()), 
      Literal(1)), "bb", row)
    checkEvaluation(GetItem(Literal(null, typeArray), Literal(1)), null, row)
    checkEvaluation(GetItem(Literal(null, typeArray), Literal(null, IntegerType)), null, row)
    checkEvaluation(GetItem(BoundReference(4, AttributeReference("c", typeArray)()), 
      Literal(null, IntegerType)), null, row)

    checkEvaluation(GetField(BoundReference(2, AttributeReference("c", typeS)()), "a"), "aa", row)
    checkEvaluation(GetField(Literal(null, typeS), "a"), null, row)

    val typeS_notNullable = StructType(
      StructField("a", StringType, nullable = false)
        :: StructField("b", StringType, nullable = false) :: Nil
    )

    assert(GetField(BoundReference(2,
      AttributeReference("c", typeS)()), "a").nullable === true)
    assert(GetField(BoundReference(2,
      AttributeReference("c", typeS_notNullable, nullable = false)()), "a").nullable === false)

    assert(GetField(Literal(null, typeS), "a").nullable === true)
    assert(GetField(Literal(null, typeS_notNullable), "a").nullable === true)
  }

  test("arithmetic") {
    val row = new GenericRow(Array[Any](1, 2, 3, null))
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.int.at(2)
    val c4 = 'a.int.at(3)

    checkEvaluation(UnaryMinus(c1), -1, row)
    checkEvaluation(UnaryMinus(Literal(100, IntegerType)), -100)

    checkEvaluation(Add(c1, c4), null, row)
    checkEvaluation(Add(c1, c2), 3, row)
    checkEvaluation(Add(c1, Literal(null, IntegerType)), null, row)
    checkEvaluation(Add(Literal(null, IntegerType), c2), null, row)
    checkEvaluation(Add(Literal(null, IntegerType), Literal(null, IntegerType)), null, row)

    checkEvaluation(-c1, -1, row)
    checkEvaluation(c1 + c2, 3, row)
    checkEvaluation(c1 - c2, -1, row)
    checkEvaluation(c1 * c2, 2, row)
    checkEvaluation(c1 / c2, 0, row)
    checkEvaluation(c1 % c2, 1, row)
  }

  test("BinaryComparison") {
    val row = new GenericRow(Array[Any](1, 2, 3, null))
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.int.at(2)
    val c4 = 'a.int.at(3)

    checkEvaluation(LessThan(c1, c4), null, row)
    checkEvaluation(LessThan(c1, c2), true, row)
    checkEvaluation(LessThan(c1, Literal(null, IntegerType)), null, row)
    checkEvaluation(LessThan(Literal(null, IntegerType), c2), null, row)
    checkEvaluation(LessThan(Literal(null, IntegerType), Literal(null, IntegerType)), null, row)

    checkEvaluation(c1 < c2, true, row)
    checkEvaluation(c1 <= c2, true, row)
    checkEvaluation(c1 > c2, false, row)
    checkEvaluation(c1 >= c2, false, row)
    checkEvaluation(c1 === c2, false, row)
    checkEvaluation(c1 !== c2, true, row)
  }
}

