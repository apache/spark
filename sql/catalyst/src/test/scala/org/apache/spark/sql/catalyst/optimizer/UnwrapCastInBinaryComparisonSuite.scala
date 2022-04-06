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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.immutable.HashSet

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.IntegralLiteralTestUtils._
import org.apache.spark.sql.catalyst.optimizer.UnwrapCastInBinaryComparison._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class UnwrapCastInBinaryComparisonSuite extends PlanTest with ExpressionEvalHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: List[Batch] =
      Batch("Unwrap casts in binary comparison", FixedPoint(10),
        NullPropagation, UnwrapCastInBinaryComparison) :: Nil
  }

  val testRelation: LocalRelation = LocalRelation($"a".short, $"b".float,
    $"c".decimal(5, 2), $"d".boolean)
  val f: BoundReference = $"a".short.canBeNull.at(0)
  val f2: BoundReference = $"b".float.canBeNull.at(1)
  val f3: BoundReference = $"c".decimal(5, 2).canBeNull.at(2)
  val f4: BoundReference = $"d".boolean.canBeNull.at(3)

  test("unwrap casts when literal == max") {
    val v = Short.MaxValue
    assertEquivalent(castInt(f) > v.toInt, falseIfNotNull(f))
    assertEquivalent(castInt(f) >= v.toInt, f === v)
    assertEquivalent(castInt(f) === v.toInt, f === v)
    assertEquivalent(castInt(f) <=> v.toInt, f <=> v)
    assertEquivalent(castInt(f) <= v.toInt, trueIfNotNull(f))
    assertEquivalent(castInt(f) < v.toInt, f =!= v)

    val d = Float.NaN
    assertEquivalent(castDouble(f2) > d.toDouble, falseIfNotNull(f2))
    assertEquivalent(castDouble(f2) >= d.toDouble, f2 === d)
    assertEquivalent(castDouble(f2) === d.toDouble, f2 === d)
    assertEquivalent(castDouble(f2) <=> d.toDouble, f2 <=> d)
    assertEquivalent(castDouble(f2) <= d.toDouble, trueIfNotNull(f2))
    assertEquivalent(castDouble(f2) < d.toDouble, f2 =!= d)
  }

  test("unwrap casts when literal > max") {
    val v: Int = positiveInt
    assertEquivalent(castInt(f) > v, falseIfNotNull(f))
    assertEquivalent(castInt(f) >= v, falseIfNotNull(f))
    assertEquivalent(castInt(f) === v, falseIfNotNull(f))
    assertEquivalent(castInt(f) <=> v, false)
    assertEquivalent(castInt(f) <= v, trueIfNotNull(f))
    assertEquivalent(castInt(f) < v, trueIfNotNull(f))
  }

  test("unwrap casts when literal == min") {
    val v = Short.MinValue
    assertEquivalent(castInt(f) > v.toInt, f =!= v)
    assertEquivalent(castInt(f) >= v.toInt, trueIfNotNull(f))
    assertEquivalent(castInt(f) === v.toInt, f === v)
    assertEquivalent(castInt(f) <=> v.toInt, f <=> v)
    assertEquivalent(castInt(f) <= v.toInt, f === v)
    assertEquivalent(castInt(f) < v.toInt, falseIfNotNull(f))

    val d = Float.NegativeInfinity
    assertEquivalent(castDouble(f2) > d.toDouble, f2 =!= d)
    assertEquivalent(castDouble(f2) >= d.toDouble, trueIfNotNull(f2))
    assertEquivalent(castDouble(f2) === d.toDouble, f2 === d)
    assertEquivalent(castDouble(f2) <=> d.toDouble, f2 <=> d)
    assertEquivalent(castDouble(f2) <= d.toDouble, f2 === d)
    assertEquivalent(castDouble(f2) < d.toDouble, falseIfNotNull(f2))

    // Double.NegativeInfinity == Float.NegativeInfinity
    val d2 = Double.NegativeInfinity
    assertEquivalent(castDouble(f2) > d2, f2 =!= d)
    assertEquivalent(castDouble(f2) >= d2, trueIfNotNull(f2))
    assertEquivalent(castDouble(f2) === d2, f2 === d)
    assertEquivalent(castDouble(f2) <=> d2, f2 <=> d)
    assertEquivalent(castDouble(f2) <= d2, f2 === d)
    assertEquivalent(castDouble(f2) < d2, falseIfNotNull(f2))
  }

  test("unwrap casts when literal < min") {
    val v: Int = negativeInt
    assertEquivalent(castInt(f) > v, trueIfNotNull(f))
    assertEquivalent(castInt(f) >= v, trueIfNotNull(f))
    assertEquivalent(castInt(f) === v, falseIfNotNull(f))
    assertEquivalent(castInt(f) <=> v, false)
    assertEquivalent(castInt(f) <= v, falseIfNotNull(f))
    assertEquivalent(castInt(f) < v, falseIfNotNull(f))
  }

  test("unwrap casts when literal is within range (min, max) or fromType has no range") {
    Seq(300, 500, 32766, -6000, -32767).foreach(v => {
      assertEquivalent(castInt(f) > v, f > v.toShort)
      assertEquivalent(castInt(f) >= v, f >= v.toShort)
      assertEquivalent(castInt(f) === v, f === v.toShort)
      assertEquivalent(castInt(f) <=> v, f <=> v.toShort)
      assertEquivalent(castInt(f) <= v, f <= v.toShort)
      assertEquivalent(castInt(f) < v, f < v.toShort)
    })

    Seq(3.14.toFloat.toDouble, -1000.0.toFloat.toDouble,
      20.0.toFloat.toDouble, -2.414.toFloat.toDouble,
      Float.MinValue.toDouble, Float.MaxValue.toDouble, Float.PositiveInfinity.toDouble
    ).foreach(v => {
      assertEquivalent(castDouble(f2) > v, f2 > v.toFloat)
      assertEquivalent(castDouble(f2) >= v, f2 >= v.toFloat)
      assertEquivalent(castDouble(f2) === v, f2 === v.toFloat)
      assertEquivalent(castDouble(f2) <=> v, f2 <=> v.toFloat)
      assertEquivalent(castDouble(f2) <= v, f2 <= v.toFloat)
      assertEquivalent(castDouble(f2) < v, f2 < v.toFloat)
    })

    Seq(decimal2(100.20), decimal2(-200.50)).foreach(v => {
      assertEquivalent(castDecimal2(f3) > v, f3 > decimal(v))
      assertEquivalent(castDecimal2(f3) >= v, f3 >= decimal(v))
      assertEquivalent(castDecimal2(f3) === v, f3 === decimal(v))
      assertEquivalent(castDecimal2(f3) <=> v, f3 <=> decimal(v))
      assertEquivalent(castDecimal2(f3) <= v, f3 <= decimal(v))
      assertEquivalent(castDecimal2(f3) < v, f3 < decimal(v))
    })
  }

  test("unwrap cast when literal is within range (min, max) AND has round up or down") {
    // Cases for rounding down
    var doubleValue = 100.6
    assertEquivalent(castDouble(f) > doubleValue, f > doubleValue.toShort)
    assertEquivalent(castDouble(f) >= doubleValue, f > doubleValue.toShort)
    assertEquivalent(castDouble(f) === doubleValue, falseIfNotNull(f))
    assertEquivalent(castDouble(f) <=> doubleValue, false)
    assertEquivalent(castDouble(f) <= doubleValue, f <= doubleValue.toShort)
    assertEquivalent(castDouble(f) < doubleValue, f <= doubleValue.toShort)

    // Cases for rounding up: 3.14 will be rounded to 3.14000010... after casting to float
    doubleValue = 3.14
    assertEquivalent(castDouble(f2) > doubleValue, f2 >= doubleValue.toFloat)
    assertEquivalent(castDouble(f2) >= doubleValue, f2 >= doubleValue.toFloat)
    assertEquivalent(castDouble(f2) === doubleValue, falseIfNotNull(f2))
    assertEquivalent(castDouble(f2) <=> doubleValue, false)
    assertEquivalent(castDouble(f2) <= doubleValue, f2 < doubleValue.toFloat)
    assertEquivalent(castDouble(f2) < doubleValue, f2 < doubleValue.toFloat)

    // Another case: 400.5678 is rounded up to 400.57
    val decimalValue = decimal2(400.5678)
    assertEquivalent(castDecimal2(f3) > decimalValue, f3 >= decimal(decimalValue))
    assertEquivalent(castDecimal2(f3) >= decimalValue, f3 >= decimal(decimalValue))
    assertEquivalent(castDecimal2(f3) === decimalValue, falseIfNotNull(f3))
    assertEquivalent(castDecimal2(f3) <=> decimalValue, false)
    assertEquivalent(castDecimal2(f3) <= decimalValue, f3 < decimal(decimalValue))
    assertEquivalent(castDecimal2(f3) < decimalValue, f3 < decimal(decimalValue))
  }

  test("unwrap casts when cast is on rhs") {
    val v = Short.MaxValue
    assertEquivalent(Literal(v.toInt) < castInt(f), falseIfNotNull(f))
    assertEquivalent(Literal(v.toInt) <= castInt(f), Literal(v) === f)
    assertEquivalent(Literal(v.toInt) === castInt(f), Literal(v) === f)
    assertEquivalent(Literal(v.toInt) <=> castInt(f), Literal(v) <=> f)
    assertEquivalent(Literal(v.toInt) >= castInt(f), trueIfNotNull(f))
    assertEquivalent(Literal(v.toInt) > castInt(f), f =!= v)

    assertEquivalent(Literal(30) <= castInt(f), Literal(30.toShort, ShortType) <= f)
  }

 test("unwrap cast should skip when expression is non-deterministic or foldable") {
   Seq(positiveLong, negativeLong).foreach (v => {
     val e = Cast(Rand(0), LongType) <=> v
      assertEquivalent(e, e, evaluate = false)
      val e2 = Cast(Literal(30), LongType) >= v
      assertEquivalent(e2, e2, evaluate = false)
    })
  }

  test("unwrap casts when literal is null") {
    val intLit = Literal.create(null, IntegerType)
    val nullLit = Literal.create(null, BooleanType)
    assertEquivalent(castInt(f) > intLit, nullLit)
    assertEquivalent(castInt(f) >= intLit, nullLit)
    assertEquivalent(castInt(f) === intLit, nullLit)
    assertEquivalent(castInt(f) <=> intLit, IsNull(castInt(f)))
    assertEquivalent(castInt(f) <= intLit, nullLit)
    assertEquivalent(castInt(f) < intLit, nullLit)
  }

  test("unwrap casts should skip if downcast failed") {
    Seq("true", "false").foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled) {
        val decimalValue = decimal2(123456.1234)
        assertEquivalent(castDecimal2(f3) === decimalValue, castDecimal2(f3) === decimalValue)
      }
    }
  }

  test("unwrap cast should skip if cannot coerce type") {
    if (!conf.ansiEnabled) {
      assertEquivalent(Cast(f, ByteType) > 100.toByte, Cast(f, ByteType) > 100.toByte)
    }
  }

  test("test getRange()") {
    assert(Some((Byte.MinValue, Byte.MaxValue)) === getRange(ByteType))
    assert(Some((Short.MinValue, Short.MaxValue)) === getRange(ShortType))
    assert(Some((Int.MinValue, Int.MaxValue)) === getRange(IntegerType))
    assert(Some((Long.MinValue, Long.MaxValue)) === getRange(LongType))

    val floatRange = getRange(FloatType)
    assert(floatRange.isDefined)
    val (floatMin, floatMax) = floatRange.get
    assert(floatMin.isInstanceOf[Float])
    assert(floatMin.asInstanceOf[Float].isNegInfinity)
    assert(floatMax.isInstanceOf[Float])
    assert(floatMax.asInstanceOf[Float].isNaN)

    val doubleRange = getRange(DoubleType)
    assert(doubleRange.isDefined)
    val (doubleMin, doubleMax) = doubleRange.get
    assert(doubleMin.isInstanceOf[Double])
    assert(doubleMin.asInstanceOf[Double].isNegInfinity)
    assert(doubleMax.isInstanceOf[Double])
    assert(doubleMax.asInstanceOf[Double].isNaN)

    assert(getRange(DecimalType(5, 2)).isEmpty)
  }

  test("SPARK-35316: unwrap should support In/InSet predicate.") {
    val longLit = Literal.create(null, LongType)
    val intLit = Literal.create(null, IntegerType)
    val shortLit = Literal.create(null, ShortType)

    def checkInAndInSet(in: In, expected: Expression): Unit = {
      assertEquivalent(in, expected)
      val toInSet = (in: In) => InSet(in.value, HashSet() ++ in.list.map(_.eval()))
      val expectedInSet = expected match {
        case expectedIn: In =>
          toInSet(expectedIn)
        case Or(falseIfNotNull: And, expectedIn: In) =>
          Or(falseIfNotNull, toInSet(expectedIn))
      }
      assertEquivalent(toInSet(in), expectedInSet)
    }

    checkInAndInSet(
      In(Cast(f, LongType), Seq(1.toLong, 2.toLong, 3.toLong)),
      f.in(1.toShort, 2.toShort, 3.toShort))

    // in.list contains the value which out of `fromType` range
    checkInAndInSet(
      In(Cast(f, LongType), Seq(1.toLong, Int.MaxValue.toLong, Long.MaxValue)),
      Or(falseIfNotNull(f), f.in(1.toShort)))

    // in.list only contains the value which out of `fromType` range
    checkInAndInSet(
      In(Cast(f, LongType), Seq(Int.MaxValue.toLong, Long.MaxValue)),
      Or(falseIfNotNull(f), f.in()))

    // in.list is empty
    checkInAndInSet(
      In(Cast(f, IntegerType), Seq.empty), Cast(f, IntegerType).in())

    // in.list contains null value
    checkInAndInSet(
      In(Cast(f, IntegerType), Seq(intLit)), In(Cast(f, IntegerType), Seq(intLit)))
    checkInAndInSet(
      In(Cast(f, IntegerType), Seq(intLit, intLit)), In(Cast(f, IntegerType), Seq(intLit, intLit)))
    checkInAndInSet(
      In(Cast(f, IntegerType), Seq(intLit, 1)), f.in(shortLit, 1.toShort))
    checkInAndInSet(
      In(Cast(f, LongType), Seq(longLit, 1.toLong, Long.MaxValue)),
      Or(falseIfNotNull(f), f.in(shortLit, 1.toShort))
    )
  }

  test("SPARK-36130: unwrap In should skip when in.list contains an expression that " +
    "is not literal") {
    val add = Cast(f2, DoubleType) + 1.0d
    val doubleLit = Literal.create(null, DoubleType)
    assertEquivalent(In(Cast(f2, DoubleType), Seq(add)), In(Cast(f2, DoubleType), Seq(add)))
    assertEquivalent(
      In(Cast(f2, DoubleType), Seq(doubleLit, add)),
      In(Cast(f2, DoubleType), Seq(doubleLit, add)))
    assertEquivalent(
      In(Cast(f2, DoubleType), Seq(doubleLit, 1.0d, add)),
      In(Cast(f2, DoubleType), Seq(doubleLit, 1.0d, add)))
    assertEquivalent(
      In(Cast(f2, DoubleType), Seq(1.0d, add)),
      In(Cast(f2, DoubleType), Seq(1.0d, add)))
    assertEquivalent(
      In(Cast(f2, DoubleType), Seq(0.0d, 1.0d, add)),
      In(Cast(f2, DoubleType), Seq(0.0d, 1.0d, add)))
  }

  test("SPARK-36607: Support BooleanType in UnwrapCastInBinaryComparison") {
    assert(Some((false, true)) === getRange(BooleanType))

    val n = -1
    assertEquivalent(castInt(f4) > n, trueIfNotNull(f4))
    assertEquivalent(castInt(f4) >= n, trueIfNotNull(f4))
    assertEquivalent(castInt(f4) === n, falseIfNotNull(f4))
    assertEquivalent(castInt(f4) <=> n, false)
    assertEquivalent(castInt(f4) <= n, falseIfNotNull(f4))
    assertEquivalent(castInt(f4) < n, falseIfNotNull(f4))

    val z = 0
    assertEquivalent(castInt(f4) > z, f4 =!= false)
    assertEquivalent(castInt(f4) >= z, trueIfNotNull(f4))
    assertEquivalent(castInt(f4) === z, f4 === false)
    assertEquivalent(castInt(f4) <=> z, f4 <=> false)
    assertEquivalent(castInt(f4) <= z, f4 === false)
    assertEquivalent(castInt(f4) < z, falseIfNotNull(f4))

    val o = 1
    assertEquivalent(castInt(f4) > o, falseIfNotNull(f4))
    assertEquivalent(castInt(f4) >= o, f4 === true)
    assertEquivalent(castInt(f4) === o, f4 === true)
    assertEquivalent(castInt(f4) <=> o, f4 <=> true)
    assertEquivalent(castInt(f4) <= o, trueIfNotNull(f4))
    assertEquivalent(castInt(f4) < o, f4 =!= true)

    val t = 2
    assertEquivalent(castInt(f4) > t, falseIfNotNull(f4))
    assertEquivalent(castInt(f4) >= t, falseIfNotNull(f4))
    assertEquivalent(castInt(f4) === t, falseIfNotNull(f4))
    assertEquivalent(castInt(f4) <=> t, false)
    assertEquivalent(castInt(f4) <= t, trueIfNotNull(f4))
    assertEquivalent(castInt(f4) < t, trueIfNotNull(f4))
  }

  private def castInt(e: Expression): Expression = Cast(e, IntegerType)
  private def castDouble(e: Expression): Expression = Cast(e, DoubleType)
  private def castDecimal2(e: Expression): Expression = Cast(e, DecimalType(10, 4))

  private def decimal(v: Decimal): Decimal = Decimal(v.toJavaBigDecimal, 5, 2)
  private def decimal2(v: BigDecimal): Decimal = Decimal(v, 10, 4)

  private def assertEquivalent(e1: Expression, e2: Expression, evaluate: Boolean = true): Unit = {
    val plan = testRelation.where(e1).analyze
    val actual = Optimize.execute(plan)
    val expected = testRelation.where(e2).analyze
    comparePlans(actual, expected)

    if (evaluate) {
      Seq(
        (100.toShort, 3.14.toFloat, decimal2(100), true),
        (-300.toShort, 3.1415927.toFloat, decimal2(-3000.50), false),
        (null, Float.NaN, decimal2(12345.6789), null),
        (null, null, null, null),
        (Short.MaxValue, Float.PositiveInfinity, decimal2(Short.MaxValue), true),
        (Short.MinValue, Float.NegativeInfinity, decimal2(Short.MinValue), false),
        (0.toShort, Float.MaxValue, decimal2(0), null),
        (0.toShort, Float.MinValue, decimal2(0.01), null)
      ).foreach(v => {
        val row = create_row(v._1, v._2, v._3, v._4)
        checkEvaluation(e1, e2.eval(row), row)
      })
    }
  }
}
