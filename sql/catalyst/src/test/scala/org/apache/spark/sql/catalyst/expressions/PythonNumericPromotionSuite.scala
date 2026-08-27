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
import org.apache.spark.sql.types.{ByteType, DataType, DecimalType, DoubleType, IntegerType, LongType, ShortType, StringType}

/**
 * The promotion rule on its own, without a session. The point of these is that the widths are
 * derived arithmetic rather than a lookup table someone can quietly edit: each expectation below
 * is the narrowest Catalyst type that holds the worst case the *input* types allow, so if the
 * rule drifts the numbers stop lining up.
 */
class PythonNumericPromotionSuite extends SparkFunSuite {

  private def dec(p: Int): DataType = DecimalType(p, 0)

  test("addition widens far enough for the sum of the operands' extremes") {
    // Two tinyints reach 256, which a tinyint cannot hold but a smallint can. Two bigints reach
    // 2**64, which needs 20 digits -- one more than LongType has.
    assert(PythonNumericPromotion.forAddition(ByteType, ByteType) === Some(ShortType))
    assert(PythonNumericPromotion.forAddition(ShortType, ShortType) === Some(IntegerType))
    assert(PythonNumericPromotion.forAddition(IntegerType, IntegerType) === Some(LongType))
    // Two bigints reach 2**64, which nothing integral holds, so they are left alone -- LongType
    // is the ceiling on purpose, see narrowestFor.
    assert(PythonNumericPromotion.forAddition(LongType, LongType).isEmpty)
    assert(PythonNumericPromotion.forAddition(ByteType, LongType).isEmpty)
  }

  test("multiplication widens by the product, which climbs much faster") {
    // 128 * 128 = 16384, so two tinyints still fit a smallint, and two ints reach 2**62, which
    // needs the whole of LongType. Two bigints reach 2**126, which nothing integral holds -- and
    // LongType is the ceiling on purpose, see narrowestFor -- so they are left alone.
    assert(PythonNumericPromotion.forMultiplication(ByteType, ByteType) === Some(ShortType))
    assert(PythonNumericPromotion.forMultiplication(IntegerType, IntegerType) === Some(LongType))
    assert(PythonNumericPromotion.forMultiplication(LongType, LongType).isEmpty)
  }

  test("negation needs one more value than the type holds") {
    // Two's complement reaches one further down than up, so `abs` of a width's minimum does not
    // fit that width -- the case that made `abs(x)` raise on a smallint holding -32768.
    assert(PythonNumericPromotion.forNegation(ByteType) === Some(ShortType))
    assert(PythonNumericPromotion.forNegation(ShortType) === Some(IntegerType))
    assert(PythonNumericPromotion.forNegation(IntegerType) === Some(LongType))
    // And 2**63 overruns Long.MaxValue by exactly one, so bigint has nowhere to go: abs of
    // Long.MinValue keeps raising, which no amount of intermediate widening could fix anyway
    // since the value does not fit a LongType return either.
    assert(PythonNumericPromotion.forNegation(LongType).isEmpty)
  }

  test("nothing is promoted when there is nothing to gain or nothing to promote") {
    // Doubles saturate to infinity, which is what Python does too, so there is no overflow to
    // avoid and no reason to touch them.
    assert(PythonNumericPromotion.forAddition(DoubleType, DoubleType).isEmpty)
    assert(PythonNumericPromotion.forMultiplication(IntegerType, DoubleType).isEmpty)
    assert(PythonNumericPromotion.forNegation(DoubleType).isEmpty)
    // Non-numerics are not our business at all.
    assert(PythonNumericPromotion.forAddition(StringType, StringType).isEmpty)
    // A decimal operand is not promoted at all -- decimal arithmetic needs the coercion rule
    // our replacement never sees, so we leave it to the ordinary operator.
    assert(PythonNumericPromotion.forNegation(dec(38)).isEmpty)
  }

  test("a chain climbs until it runs out of integral room, then gives up") {
    // int32 * int32 lands on LongType, and multiplying that again has nowhere left to go. Giving
    // up leaves the operator to raise on overflow exactly as it did before promotion existed --
    // no worse, and never a wrong answer.
    assert(PythonNumericPromotion.forMultiplication(ByteType, ByteType) === Some(ShortType))
    assert(PythonNumericPromotion.forMultiplication(ShortType, ShortType) === Some(IntegerType))
    assert(PythonNumericPromotion.forMultiplication(IntegerType, IntegerType) === Some(LongType))
    assert(PythonNumericPromotion.forMultiplication(LongType, IntegerType).isEmpty)
  }

  test("the widened form casts both operands, not the result") {
    // The whole bug this fixes: `a + b` on two int columns already *reported* bigint, because
    // the transpiler casts to the declared return type at the end. The addition still happened
    // in IntegerType and overflowed long before that cast. So assert the casts are on the way in.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    PythonNumericPromotion.widened(
      a, b, PythonNumericPromotion.forAddition, Add(_, _)) match {
      case Add(Cast(l, LongType, _, _), Cast(r, LongType, _, _), _) =>
        assert(l === a && r === b)
      case other =>
        fail(s"expected both operands cast to bigint before the Add, got $other")
    }
  }

  test("the widened form leaves the operator alone when no promotion applies") {
    val a = AttributeReference("a", DoubleType)()
    val b = AttributeReference("b", DoubleType)()
    val result = PythonNumericPromotion.widened(
      a, b, PythonNumericPromotion.forAddition, Add(_, _))
    assert(result === Add(a, b), "a double add should not collect casts it cannot use")
  }
}
