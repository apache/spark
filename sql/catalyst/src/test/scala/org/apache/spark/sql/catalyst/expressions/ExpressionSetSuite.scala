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

  // An [AttributeReference] with almost the maximum hashcode, to make testing canonicalize rules
  // like `case GreaterThan(l, r) if l.hashcode > r.hashcode => GreaterThan(r, l)` easier
  val maxHash = AttributeReference("none", IntegerType)(exprId =
    new ExprId(4, NamedExpression.jvmId) {
      // This `hashCode` is carefully picked to make `maxHash.hashCode` becomes `Int.MaxValue`.
      override def hashCode: Int = 1394598635
      // We are implementing this equals() only because the style-checking rule "you should
      // implement equals and hashCode together" requires us to
      override def equals(obj: Any): Boolean = super.equals(obj)
    })
  assert(maxHash.hashCode() == Int.MaxValue)

  // An [AttributeReference] with almost the minimum hashcode, to make testing canonicalize rules
  // like `case GreaterThan(l, r) if l.hashcode > r.hashcode => GreaterThan(r, l)` easier
  val minHash = AttributeReference("none", IntegerType)(exprId =
    new ExprId(5, NamedExpression.jvmId) {
      // This `hashCode` is carefully picked to make `minHash.hashCode` becomes `Int.MinValue`.
      override def hashCode: Int = -462684520
      // We are implementing this equals() only because the style-checking rule "you should
      // implement equals and hashCode together" requires us to
      override def equals(obj: Any): Boolean = super.equals(obj)
    })
  assert(minHash.hashCode() == Int.MinValue)

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
  setTest(1, Not(maxHash > 1), maxHash <= 1, Not(Literal(1) < maxHash), Literal(1) >= maxHash)
  setTest(1, Not(minHash > 1), minHash <= 1, Not(Literal(1) < minHash), Literal(1) >= minHash)
  setTest(1, Not(maxHash < 1), maxHash >= 1, Not(Literal(1) > maxHash), Literal(1) <= maxHash)
  setTest(1, Not(minHash < 1), minHash >= 1, Not(Literal(1) > minHash), Literal(1) <= minHash)
  setTest(1, Not(maxHash >= 1), maxHash < 1, Not(Literal(1) <= maxHash), Literal(1) > maxHash)
  setTest(1, Not(minHash >= 1), minHash < 1, Not(Literal(1) <= minHash), Literal(1) > minHash)
  setTest(1, Not(maxHash <= 1), maxHash > 1, Not(Literal(1) >= maxHash), Literal(1) < maxHash)
  setTest(1, Not(minHash <= 1), minHash > 1, Not(Literal(1) >= minHash), Literal(1) < minHash)

  // Reordering AND/OR expressions
  setTest(1, aUpper > bUpper && aUpper <= 10, aUpper <= 10 && aUpper > bUpper)
  setTest(1,
    aUpper > bUpper && bUpper > 100 && aUpper <= 10,
    bUpper > 100 && aUpper <= 10 && aUpper > bUpper)

  setTest(1, aUpper > bUpper || aUpper <= 10, aUpper <= 10 || aUpper > bUpper)
  setTest(1,
    aUpper > bUpper || bUpper > 100 || aUpper <= 10,
    bUpper > 100 || aUpper <= 10 || aUpper > bUpper)

  setTest(1,
    (aUpper <= 10 && aUpper > bUpper) || bUpper > 100,
    bUpper > 100 || (aUpper <= 10 && aUpper > bUpper))

  setTest(1,
    aUpper >= bUpper || (aUpper > 10 && bUpper < 10),
    (bUpper < 10 && aUpper > 10) || aUpper >= bUpper)

  // More complicated cases mixing AND/OR
  // Three predicates in the following:
  //   (bUpper > 100)
  //   (aUpper < 100 && bUpper <= aUpper)
  //   (aUpper >= 10 && bUpper >= 50)
  // They can be reordered and the sub-predicates contained in each of them can be reordered too.
  setTest(1,
    (bUpper > 100) || (aUpper < 100 && bUpper <= aUpper) || (aUpper >= 10 && bUpper >= 50),
    (aUpper >= 10 && bUpper >= 50) || (bUpper > 100) || (aUpper < 100 && bUpper <= aUpper),
    (bUpper >= 50 && aUpper >= 10) || (bUpper <= aUpper && aUpper < 100) || (bUpper > 100))

  // Two predicates in the following:
  //   (bUpper > 100 && aUpper < 100 && bUpper <= aUpper)
  //   (aUpper >= 10 && bUpper >= 50)
  setTest(1,
    (bUpper > 100 && aUpper < 100 && bUpper <= aUpper) || (aUpper >= 10 && bUpper >= 50),
    (aUpper >= 10 && bUpper >= 50) || (aUpper < 100 && bUpper > 100 && bUpper <= aUpper),
    (bUpper >= 50 && aUpper >= 10) || (bUpper <= aUpper && aUpper < 100 && bUpper > 100))

  // Three predicates in the following:
  //   (aUpper >= 10)
  //   (bUpper <= 10 && aUpper === bUpper && aUpper < 100)
  //   (bUpper >= 100)
  setTest(1,
    (aUpper >= 10) || (bUpper <= 10 && aUpper === bUpper && aUpper < 100) || (bUpper >= 100),
    (aUpper === bUpper && aUpper < 100 && bUpper <= 10) || (bUpper >= 100) || (aUpper >= 10),
    (aUpper < 100 && bUpper <= 10 && aUpper === bUpper) || (aUpper >= 10) || (bUpper >= 100),
    ((bUpper <= 10 && aUpper === bUpper) && aUpper < 100) || ((aUpper >= 10) || (bUpper >= 100)))

  // Don't reorder non-deterministic expression in AND/OR.
  setTest(2, Rand(1L) > aUpper && aUpper <= 10, aUpper <= 10 && Rand(1L) > aUpper)
  setTest(2,
    aUpper > bUpper && bUpper > 100 && Rand(1L) > aUpper,
    bUpper > 100 && Rand(1L) > aUpper && aUpper > bUpper)

  setTest(2, Rand(1L) > aUpper || aUpper <= 10, aUpper <= 10 || Rand(1L) > aUpper)
  setTest(2,
    aUpper > bUpper || aUpper <= Rand(1L) || aUpper <= 10,
    aUpper <= Rand(1L) || aUpper <= 10 || aUpper > bUpper)

  // Keep all the non-deterministic expressions even they are semantically equal.
  setTest(2, Rand(1L), Rand(1L))

  setTest(2,
    (aUpper > bUpper || bUpper > 100) && aUpper === Rand(1L),
    (bUpper > 100 || aUpper > bUpper) && aUpper === Rand(1L))

  setTest(2,
    Rand(1L) > aUpper || (aUpper <= Rand(1L) && aUpper > bUpper) || (aUpper > 10 && bUpper > 10),
    Rand(1L) > aUpper || (aUpper <= Rand(1L) && aUpper > bUpper) || (bUpper > 10 && aUpper > 10))

  // Same predicates as above, but a negative case when we reorder non-deterministic
  // expression in (aUpper <= Rand(1L) && aUpper > bUpper).
  setTest(2,
    Rand(1L) > aUpper || (aUpper <= Rand(1L) && aUpper > bUpper) || (aUpper > 10 && bUpper > 10),
    Rand(1L) > aUpper || (aUpper > bUpper && aUpper <= Rand(1L)) || (aUpper > 10 && bUpper > 10))

  test("add to / remove from set") {
    val initialSet = ExpressionSet(aUpper + 1 :: Nil)

    assert((initialSet + (aUpper + 1)).size == 1)
    assert((initialSet + (aUpper + 2)).size == 2)
    assert((initialSet - (aUpper + 1)).size == 0)
    assert((initialSet - (aUpper + 2)).size == 1)

    assert((initialSet + (aLower + 1)).size == 1)
    assert((initialSet - (aLower + 1)).size == 0)

  }

  test("add multiple elements to set") {
    val initialSet = ExpressionSet(aUpper + 1 :: Nil)
    val setToAddWithSameExpression = ExpressionSet(aUpper + 1 :: aUpper + 2 :: Nil)
    val setToAddWithOutSameExpression = ExpressionSet(aUpper + 3 :: aUpper + 4 :: Nil)

    assert((initialSet ++ setToAddWithSameExpression).size == 2)
    assert((initialSet ++ setToAddWithOutSameExpression).size == 3)
  }

  test("add single element to set with non-deterministic expressions") {
    val initialSet = ExpressionSet(aUpper + 1 :: Rand(0) :: Nil)

    assert((initialSet + (aUpper + 1)).size == 2)
    assert((initialSet + Rand(0)).size == 3)
    assert((initialSet + (aUpper + 2)).size == 3)
  }

  test("remove single element to set with non-deterministic expressions") {
    val initialSet = ExpressionSet(aUpper + 1 :: Rand(0) :: Nil)

    assert((initialSet - (aUpper + 1)).size == 1)
    assert((initialSet - Rand(0)).size == 2)
    assert((initialSet - (aUpper + 2)).size == 2)
  }

  test("add multiple elements to set with non-deterministic expressions") {
    val initialSet = ExpressionSet(aUpper + 1 :: Rand(0) :: Nil)
    val setToAddWithSameDeterministicExpression = ExpressionSet(aUpper + 1 :: Rand(0) :: Nil)
    val setToAddWithOutSameExpression = ExpressionSet(aUpper + 3 :: aUpper + 4 :: Nil)

    assert((initialSet ++ setToAddWithSameDeterministicExpression).size == 3)
    assert((initialSet ++ setToAddWithOutSameExpression).size == 4)
  }

  test("remove multiple elements to set with non-deterministic expressions") {
    val initialSet = ExpressionSet(aUpper + 1 :: Rand(0) :: Nil)
    val setToRemoveWithSameDeterministicExpression = ExpressionSet(aUpper + 1 :: Rand(0) :: Nil)
    val setToRemoveWithOutSameExpression = ExpressionSet(aUpper + 3 :: aUpper + 4 :: Nil)

    assert((initialSet -- setToRemoveWithSameDeterministicExpression).size == 1)
    assert((initialSet -- setToRemoveWithOutSameExpression).size == 2)
  }

  test("simpleString limits the number of expressions recursively") {
    val expressionSet =
      ExpressionSet(InSet(aUpper, Set(0, 1)) :: Rand(1) :: Rand(2) :: Rand(3) :: Nil)
    assert(expressionSet.simpleString(1) ==
      "Set(A#1 INSET 0, ... 1 more fields, ... 3 more fields)")
    assert(expressionSet.simpleString(2) == "Set(A#1 INSET 0, 1, rand(1), ... 2 more fields)")
    assert(expressionSet.simpleString(3) ==
      "Set(A#1 INSET 0, 1, rand(1), rand(2), ... 1 more fields)")
    assert(expressionSet.simpleString(4) == expressionSet.toString)

    // Only one expression, but the simple string for this expression must be truncated.
    val expressionSetTwo = ExpressionSet(InSet(aUpper, Set(0, 1, 2, 3, 4)) :: Nil)
    assert(expressionSetTwo.simpleString(1) == "Set(A#1 INSET 0, ... 4 more fields)")
  }
}
