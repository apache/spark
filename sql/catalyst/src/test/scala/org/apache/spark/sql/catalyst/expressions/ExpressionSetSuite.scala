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

  setTest(1, aUpper > bUpper && aUpper <= 10, aUpper <= 10 && aUpper > bUpper)
  setTest(1,
    aUpper > bUpper && bUpper > 100 && aUpper <= 10,
    bUpper > 100 && aUpper <= 10 && aUpper > bUpper)

  setTest(1, aUpper > bUpper || aUpper <= 10, aUpper <= 10 || aUpper > bUpper)
  setTest(1,
    aUpper > bUpper || bUpper > 100 || aUpper <= 10,
    bUpper > 100 || aUpper <= 10 || aUpper > bUpper)

  setTest(1,
    bUpper > 100 || aUpper <= 10 && aUpper > bUpper,
    bUpper > 100 || (aUpper <= 10 && aUpper > bUpper))

  setTest(1,
    aUpper > 10 && bUpper < 10 || aUpper >= bUpper,
    (bUpper < 10 && aUpper > 10) || aUpper >= bUpper)

  setTest(1,
    bUpper > 100 || aUpper < 100 && bUpper <= aUpper || aUpper >= 10 && bUpper >= 50,
    (aUpper >= 10 && bUpper >= 50) || bUpper > 100 || (aUpper < 100 && bUpper <= aUpper),
    (bUpper >= 50 && aUpper >= 10) || (bUpper <= aUpper && aUpper < 100) || bUpper > 100)

  setTest(1,
    bUpper > 100 && aUpper < 100 && bUpper <= aUpper || aUpper >= 10 && bUpper >= 50,
    (aUpper >= 10 && bUpper >= 50) || (aUpper < 100 && bUpper > 100 && bUpper <= aUpper),
    (bUpper >= 50 && aUpper >= 10) || (bUpper <= aUpper && aUpper < 100 && bUpper > 100))

  setTest(1,
    aUpper >= 10 || bUpper <= 10 && aUpper === bUpper && aUpper < 100 || bUpper >= 100,
    aUpper === bUpper && aUpper < 100 && bUpper <= 10 || bUpper >= 100 || aUpper >= 10,
    aUpper < 100 && bUpper <= 10 && aUpper === bUpper || aUpper >= 10 || bUpper >= 100,
    ((bUpper <= 10 && aUpper === bUpper) && aUpper < 100) || (aUpper >= 10 || bUpper >= 100))

  // Don't reorder non-deterministic expression in AND/OR.
  setTest(2, Rand(1L) > aUpper && aUpper <= 10, aUpper <= 10 && Rand(1L) > aUpper)
  setTest(2,
    aUpper > bUpper &&
      bUpper > 100 &&
      Rand(1L) > aUpper,
    bUpper > 100 &&
      Rand(1L) > aUpper &&
      aUpper > bUpper)

  setTest(2, Rand(1L) > aUpper || aUpper <= 10, aUpper <= 10 || Rand(1L) > aUpper)
  setTest(2,
    aUpper > bUpper ||
      aUpper <= Rand(1L) ||
      aUpper <= 10,
    aUpper <= Rand(1L) ||
      aUpper <= 10 ||
      aUpper > bUpper)

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
