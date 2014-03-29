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

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.types._

/* Implicit conversions */
import org.apache.spark.sql.catalyst.dsl.expressions._

class ExpressionEvaluationSuite extends FunSuite {

  test("literals") {
    assert((Literal(1) + Literal(1)).apply(null) === 2)
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
        val result = expr.apply(null)
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

  def booleanLogicTest(name: String, op: (Expression, Expression) => Expression,  truthTable: Seq[(Any, Any, Any)]) {
    test(s"3VL $name") {
      truthTable.foreach {
        case (l,r,answer) =>
          val expr = op(Literal(l, BooleanType), Literal(r, BooleanType))
          val result = expr.apply(null)
          if (result != answer)
            fail(s"$expr should not evaluate to $result, expected: $answer")
      }
    }
  }

  def evaluate(expression: Expression, inputRow: Row = EmptyRow): Any = {
    expression.apply(inputRow)
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
  }

  test("RLIKE literal Regular Expression") {
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
}

