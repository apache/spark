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

package org.apache.spark.sql
package catalyst
package expressions

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
  
  val c1 = BoundReference(0, AttributeReference("a", StringType)())  // null
  val c2 = BoundReference(1, AttributeReference("b", StringType)())  // "addb"
  val c3 = BoundReference(2, AttributeReference("c", StringType)())  // "a"
  val c4 = BoundReference(3, AttributeReference("d", StringType)())  // "abdef"
  val c5 = BoundReference(4, AttributeReference("e", StringType)())  // "a_%b"
  val c6 = BoundReference(5, AttributeReference("f", StringType)())  // "a\\__b"
  val c7 = BoundReference(6, AttributeReference("g", StringType)())  // "a%\\%b"
  val c8 = BoundReference(7, AttributeReference("h", StringType)())  // "a%"
  val c9 = BoundReference(8, AttributeReference("i", StringType)())  // "**"

  val cs1: String = null
  val cs2 = "addb"
  val cs3 = "a"
  val cs4 = "abdef"
  val cs5 = "a_%b"
  val cs6 = "a\\__b"
  val cs7 = "a%\\%b"
  val cs8 = "a%"
  val cs9 = "**"
  val regexData: Row = new GenericRow(Array[Any](cs1, cs2, cs3, cs4, cs5, cs6, cs7, cs8, cs9))
    
  regexTest(regexData, "Like - pattern with Dynamic regex string", Like(_, _), 
    (c1, c3, null) :: // null, "a"
    (c1, c1, null) :: // null, null
    (c4, c4, true) ::  // "abdef", "abdef"
    (c5, c6, true) ::  // "a_%b", "a\\__b"
    (c2, c5, true) ::  // "addb", "a_%b"
    (c2, c6, false) :: // "addb", "a\\__b"
    (c2, c7, false) :: // "addb", "a%\\%b"
    (c5, c7, true) ::  // "a_%b", "a%\\%b"
    (c2, c8, true) ::  // "addb", "a%"
    (c2, c9, false) ::  // "addb", "**"
    Nil
  )
  
  regexTest(regexData, "Like - pattern with Literal regex string", Like(_, _), 
    (Literal(cs1), Literal(cs3), null) :: // null, "a"
    (Literal(cs1), Literal(cs1), null) :: // null, null
    (Literal(cs4), Literal(cs4), true) ::  // "abdef", "abdef"
    (Literal(cs5), Literal(cs6), true) ::  // "a_%b", "a\\__b"
    (Literal(cs2), Literal(cs5), true) ::  // "addb", "a_%b"
    (Literal(cs2), Literal(cs6), false) :: // "addb", "a\\__b"
    (Literal(cs2), Literal(cs7), false) :: // "addb", "a%\\%b"
    (Literal(cs5), Literal(cs7), true) ::  // "a_%b", "a%\\%b"
    (Literal(cs2), Literal(cs8), true) ::  // "addb", "a%"
    (Literal(cs2), Literal(cs9), false) ::  // "addb", "**"
    Nil
  )
  
  regexTest(regexData, "RLike - pattern with Literal regex string", RLike(_, _), 
    (Literal(cs4), Literal(cs4), true) ::  // "abdef", "abdef"
    (Literal("abbbbc"), Literal("a.*c"), true) ::
    (Literal("abbbbc"), Literal("**"), classOf[java.util.regex.PatternSyntaxException]) ::
    Nil
  )
  
  def regexTest(row: Row, name: String, op: (Expression, Expression) => Expression,  
    truthTable: Seq[(Expression, Expression, Any)]) {

    test(s"regex: $name") {
      truthTable.foreach {
        case (l, r, null) =>
          val expr = op(l, r)
          val result = expr.apply(row)
          if (result != null) fail(s"$expr should not evaluate to $result, expected: null")
        case (l, r, answer: Class[_]) =>
          val expr = op(l, r)
          try{
            expr.apply(row)
            // will fail if no exception thrown
            fail(s"$expr should throw exception ${answer.getCanonicalName()}, but it didn't")
          } catch {
            // raise by fail() method
            case x if (x.isInstanceOf[org.scalatest.exceptions.TestFailedException]) => throw x
            // the same exception as expected it, do nothing
            case x if answer.getCanonicalName() == x.getClass().getCanonicalName() =>
            case x => fail(s"$expr should not throw exception $x, expected: $answer")
          }
        case (l, r, answer) =>
          val expr = op(l, r)
          val result = expr.apply(row)
          if (result != answer)
            fail(s"$expr should not evaluate to $result, expected: $answer")
      }
    }
  }
}
