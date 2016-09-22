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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types.IntegerType

class ExpressionSetSuite extends SparkFunSuite {

  val aUpper = AttributeReference("A", IntegerType)(exprId = ExprId(1))
  val aLower = AttributeReference("a", IntegerType)(exprId = ExprId(1))
  val fakeA = AttributeReference("a", IntegerType)(exprId = ExprId(3))

  val bUpper = AttributeReference("B", IntegerType)(exprId = ExprId(2))
  val bLower = AttributeReference("b", IntegerType)(exprId = ExprId(2))

  val aAndBSet = AttributeSet(aUpper :: bUpper :: Nil)

  def setTest(size: Int, exprs: Expression*): Unit = {
    test(s"expect $size: ${exprs.mkString(", ")}") {
      val set = ExpressionSet(exprs)
      if (set.size != size) {
        fail(set.toDebugString)
      }
    }
  }

  def setTestIgnore(size: Int, exprs: Expression*): Unit =
    ignore(s"expect $size: ${exprs.mkString(", ")}") {}

  // Commutative
  setTest(1, aUpper + 1, aLower + 1)
  setTest(2, aUpper + 1, aLower + 2)
  setTest(2, aUpper + 1, fakeA + 1)
  setTest(2, aUpper + 1, bUpper + 1)

  setTest(1, aUpper + aLower, aLower + aUpper)
  setTest(1, aUpper + bUpper, bUpper + aUpper)
  setTest(1,
    aUpper + bUpper + 3,
    bUpper + 3 + aUpper,
    bUpper + aUpper + 3,
    Literal(3) + aUpper + bUpper)
  setTest(1,
    aUpper * bUpper * 3,
    bUpper * 3 * aUpper,
    bUpper * aUpper * 3,
    Literal(3) * aUpper * bUpper)
  setTest(1, aUpper === bUpper, bUpper === aUpper)

  setTest(1, aUpper + 1 === bUpper, bUpper === Literal(1) + aUpper)


  // Not commutative
  setTest(2, aUpper - bUpper, bUpper - aUpper)

  // Reversible
  setTest(1, aUpper > bUpper, bUpper < aUpper)
  setTest(1, aUpper >= bUpper, bUpper <= aUpper)

  // `Not` canonicalization
  setTest(1, Not(aUpper > 1), aUpper <= 1, Not(Literal(1) < aUpper), Literal(1) >= aUpper)
  setTest(1, Not(aUpper < 1), aUpper >= 1, Not(Literal(1) > aUpper), Literal(1) <= aUpper)
  setTest(1, Not(aUpper >= 1), aUpper < 1, Not(Literal(1) <= aUpper), Literal(1) > aUpper)
  setTest(1, Not(aUpper <= 1), aUpper > 1, Not(Literal(1) >= aUpper), Literal(1) < aUpper)

  test("add to / remove from set") {
    val initialSet = ExpressionSet(aUpper + 1 :: Nil)

    assert((initialSet + (aUpper + 1)).size == 1)
    assert((initialSet + (aUpper + 2)).size == 2)
    assert((initialSet - (aUpper + 1)).size == 0)
    assert((initialSet - (aUpper + 2)).size == 1)

    assert((initialSet + (aLower + 1)).size == 1)
    assert((initialSet - (aLower + 1)).size == 0)

  }
}
