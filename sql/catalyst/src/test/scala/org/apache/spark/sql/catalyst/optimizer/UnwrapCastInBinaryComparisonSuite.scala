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

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}

import scala.collection.immutable.HashSet

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.IntegralLiteralTestUtils._
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral
import org.apache.spark.sql.catalyst.optimizer.UnwrapCastInBinaryComparison._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class UnwrapCastInBinaryComparisonSuite extends PlanTest with ExpressionEvalHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: List[Batch] =
      Batch("Unwrap casts in binary comparison", FixedPoint(10),
        UnwrapCastInBinaryComparison) :: Nil
  }

  val testRelation: LocalRelation = LocalRelation($"a".short, $"b".float,
    $"c".decimal(5, 2), $"d".boolean, $"e".timestamp, $"f".timestampNTZ, $"g".date,
    AttributeReference("h", TimeType(6))())
  val f: BoundReference = $"a".short.canBeNull.at(0)
  val f2: BoundReference = $"b".float.canBeNull.at(1)
  val f3: BoundReference = $"c".decimal(5, 2).canBeNull.at(2)
  val f4: BoundReference = $"d".boolean.canBeNull.at(3)
  val f5: BoundReference = $"e".timestamp.notNull.at(4)
  val f6: BoundReference = $"f".timestampNTZ.canBeNull.at(5)
  val f7: BoundReference = $"g".date.canBeNull.at(6)
  val f8: BoundReference = AttributeReference("h", TimeType(6))().canBeNull.at(7)

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

  test("SPARK-42741: Do not unwrap casts in binary comparison when literal is null") {
    val intLit = Literal.create(null, IntegerType)
    Seq(castInt(f) > intLit, castInt(f) >= intLit, castInt(f) === intLit, castInt(f) <=> intLit,
      castInt(f) <= intLit, castInt(f) < intLit).foreach { be =>
      assertEquivalent(be, be)
    }
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

    checkInAndInSet(
      In(Cast(f, LongType), Seq(1.toLong, 2.toLong, 3.toLong)),
      f.in(1.toShort, 2.toShort, 3.toShort))

    // in.list contains the value which out of `fromType` range
    checkInAndInSet(
      In(Cast(f, LongType), Seq(1.toLong, Int.MaxValue.toLong, Long.MaxValue)),
      f.in(1.toShort))

    // in.list only contains the value which out of `fromType` range
    checkInAndInSet(
      In(Cast(f, LongType), Seq(Int.MaxValue.toLong, Long.MaxValue)),
      falseIfNotNull(f))

    // in.list is empty
    checkInAndInSet(
      In(Cast(f, IntegerType), Seq.empty), Cast(f, IntegerType).in())

    // in.list contains null value
    checkInAndInSet(
      In(Cast(f, IntegerType), Seq(intLit)), f.in(intLit.cast(ShortType)))
    checkInAndInSet(
      In(Cast(f, IntegerType), Seq(intLit, intLit)),
      f.in(intLit.cast(ShortType), intLit.cast(ShortType)))
    checkInAndInSet(
      In(Cast(f, IntegerType), Seq(intLit, 1)), f.in(intLit.cast(ShortType), 1.toShort))
    checkInAndInSet(
      In(Cast(f, LongType), Seq(longLit, 1.toLong, Long.MaxValue)),
      f.in(longLit.cast(ShortType), 1.toShort)
    )
    checkInAndInSet(
      In(Cast(f, LongType), Seq(longLit, Long.MaxValue)),
      f.in(longLit.cast(ShortType))
    )
  }

  test("SPARK-39896: unwrap cast when the literal of In/InSet downcast failed") {
    val decimalValue = decimal2(123456.1234)
    val decimalValue2 = decimal2(100.20)
    checkInAndInSet(
      In(castDecimal2(f3), Seq(decimalValue, decimalValue2)),
      f3.in(decimal(decimalValue2)))
  }

  test("SPARK-39896: unwrap cast when the literal of In/Inset has round up or down") {

    val doubleValue = 1.0
    val doubleValue1 = 100.6
    checkInAndInSet(
      In(castDouble(f), Seq(doubleValue1, doubleValue)),
      f.in(doubleValue.toShort))

    // Cases for rounding up: 3.14 will be rounded to 3.14000010... after casting to float
    val doubleValue2 = 3.14
    checkInAndInSet(
      In(castDouble(f2), Seq(doubleValue2, doubleValue)),
      f2.in(doubleValue.toFloat))

    // Another case: 400.5678 is rounded up to 400.57
    val decimalValue1 = decimal2(400.5678)
    val decimalValue2 = decimal2(1.0)
    checkInAndInSet(
      In(castDecimal2(f3), Seq(decimalValue1, decimalValue2)),
      f3.in(decimal(decimalValue2)))
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

  test("SPARK-42597: Support unwrap date to timestamp type") {
    val dateLit = Literal.create(LocalDate.of(2023, 1, 1), DateType)
    val dateAddOne = DateAdd(dateLit, Literal(1))
    val nullLit = Literal.create(null, DateType)

    assertEquivalent(
      castDate(f5) > dateLit || castDate(f6) > dateLit,
      f5 >= castTimestamp(dateAddOne) || f6 >= castTimestampNTZ(dateAddOne))
    assertEquivalent(
      castDate(f5) >= dateLit || castDate(f6) >= dateLit,
      f5 >= castTimestamp(dateLit) || f6 >= castTimestampNTZ(dateLit))
    assertEquivalent(
      castDate(f5) < dateLit || castDate(f6) < dateLit,
      f5 < castTimestamp(dateLit) || f6 < castTimestampNTZ(dateLit))
    assertEquivalent(
      castDate(f5) <= dateLit || castDate(f6) <= dateLit,
      f5 < castTimestamp(dateAddOne) || f6 < castTimestampNTZ(dateAddOne))
    assertEquivalent(
      castDate(f5) === dateLit || castDate(f6) === dateLit,
      (f5 >= castTimestamp(dateLit) && f5 < castTimestamp(dateAddOne)) ||
        (f6 >= castTimestampNTZ(dateLit) && f6 < castTimestampNTZ(dateAddOne)))
    assertEquivalent(
      castDate(f5) <=> dateLit || castDate(f6) <=> dateLit,
      (f5 >= castTimestamp(dateLit) && f5 < castTimestamp(dateAddOne)) || castDate(f6) <=> dateLit)
    assertEquivalent(
      dateLit < castDate(f5) || dateLit < castDate(f6),
      castTimestamp(dateAddOne) <= f5 || castTimestampNTZ(dateAddOne) <= f6)

    // Null date literal should be handled by NullPropagation
    assertEquivalent(castDate(f5) > nullLit || castDate(f6) > nullLit,
      castDate(f5) > nullLit || castDate(f6) > nullLit)
  }

  test("SPARK-46069: Support unwrap timestamp type to date type") {
    def doTest(tsLit: Literal, timePartsAllZero: Boolean): Unit = {
      val tz = Some(conf.sessionLocalTimeZone)
      val floorDate = Literal(Cast(tsLit, DateType, tz).eval(), DateType)
      val dateToTsCast = Cast(f7, tsLit.dataType, tz)

      assertEquivalent(dateToTsCast > tsLit, f7 > floorDate)
      assertEquivalent(dateToTsCast <= tsLit, f7 <= floorDate)
      if (timePartsAllZero) {
        assertEquivalent(dateToTsCast >= tsLit, f7 >= floorDate)
        assertEquivalent(dateToTsCast < tsLit, f7 < floorDate)
        assertEquivalent(dateToTsCast === tsLit, f7 === floorDate)
        assertEquivalent(dateToTsCast <=> tsLit, f7 <=> floorDate)
      } else {
        assertEquivalent(dateToTsCast >= tsLit, f7 > floorDate)
        assertEquivalent(dateToTsCast < tsLit, f7 <= floorDate)
        assertEquivalent(dateToTsCast === tsLit, f7.isNull && Literal(null, BooleanType))
        assertEquivalent(dateToTsCast <=> tsLit, FalseLiteral)
      }
    }

    // Test timestamp with all its time parts as 0.
    val micros = SparkDateTimeUtils.daysToMicros(19704, ZoneId.of(conf.sessionLocalTimeZone))
    val instant = java.time.Instant.ofEpochSecond(micros / 1000000)
    val tsLit = Literal.create(instant, TimestampType)
    doTest(tsLit, timePartsAllZero = true)

    val tsNTZ = LocalDateTime.of(2023, 12, 13, 0, 0, 0, 0)
    val tsNTZLit = Literal.create(tsNTZ, TimestampNTZType)
    doTest(tsNTZLit, timePartsAllZero = true)

    // Test timestamp with non-zero time parts.
    val tsLit2 = Literal.create(instant.plusSeconds(30), TimestampType)
    doTest(tsLit2, timePartsAllZero = false)
    val tsNTZLit2 = Literal.create(tsNTZ.withSecond(30), TimestampNTZType)
    doTest(tsNTZLit2, timePartsAllZero = false)
  }

  test("SPARK-62378: Support unwrap date type to string type") {
    val maxDateLit = Literal.create(LocalDate.of(9999, 12, 31), DateType)
    // The range-predicate rewrite only applies when fromExp is an Attribute (see
    // unwrapStringToDate's doc), so it needs a genuine AttributeReference here -- unlike f7 (a
    // BoundReference, used for everything else in this file), g is Unevaluable until bound, so
    // these range-predicate assertions compare plan structure only (evaluate = false).
    val g: AttributeReference = testRelation.output(6).asInstanceOf[AttributeReference]

    // DateType's [0001-01-01, 9999-12-31] contract is not enforced anywhere (e.g. DateAdd does
    // unchecked Int arithmetic), so the unwrap must guard against out-of-range column values at
    // runtime. Outside the range, Cast(Date, String) gains a sign prefix. For years <= 0, the
    // formatted string always sorts before in-range literals, matching date order. For years
    // >= 10000, however, the '+' prefix sorts before in-range digits, flipping the comparison.
    // The range guard thus only needs to protect against years >= 10000 (g > maxDateLit).
    def boundedRewrite(rewritten: Expression, original: Predicate): Expression = {
      original match {
        case _: LessThan | _: LessThanOrEqual =>
          Or(GreaterThan(g, maxDateLit), rewritten)
        case _: GreaterThan | _: GreaterThanOrEqual =>
          And(LessThanOrEqual(g, maxDateLit), rewritten)
      }
    }

    val dateToStrCastAttr = Cast(g, StringType)
    val dateLit = Literal.create(LocalDate.of(2023, 1, 1), DateType)

    assertEquivalent(dateToStrCastAttr > "2023-01-01",
      boundedRewrite(g > dateLit, dateToStrCastAttr > "2023-01-01"), evaluate = false)
    assertEquivalent(dateToStrCastAttr >= "2023-01-01",
      boundedRewrite(g >= dateLit, dateToStrCastAttr >= "2023-01-01"), evaluate = false)
    assertEquivalent(dateToStrCastAttr < "2023-01-01",
      boundedRewrite(g < dateLit, dateToStrCastAttr < "2023-01-01"), evaluate = false)
    assertEquivalent(dateToStrCastAttr <= "2023-01-01",
      boundedRewrite(g <= dateLit, dateToStrCastAttr <= "2023-01-01"), evaluate = false)

    // Literal-on-the-left is canonicalized to Cast-on-the-left by unwrapCast's generic `swap`
    // before this case ever runs, and `swap` has no case for `If`, so the resulting plan keeps
    // the canonicalized (not the original) orientation on both branches.
    assertEquivalent("2023-01-01" >= dateToStrCastAttr,
      boundedRewrite(g <= dateLit, dateToStrCastAttr <= "2023-01-01"), evaluate = false)

    // Unlike the range predicates above, `=`/`<=>` need no Attribute restriction or runtime range
    // guard: day-count equality between `fromExp` and the literal's decoded value agrees with
    // string equality between their `Cast(_, StringType)` forms regardless of DateType's
    // documented range or fromExp's shape, since that formatting stays injective outside the
    // range -- it just stops being order-preserving -- and the rewrite embeds fromExp only once.
    // So this keeps using f7 (a BoundReference, not an Attribute) and can check runtime
    // evaluation too.
    val dateToStrCast = Cast(f7, StringType)
    assertEquivalent(dateToStrCast === "2023-01-01", f7 === dateLit)
    assertEquivalent(dateToStrCast <=> "2023-01-01", f7 <=> dateLit)

    // A non-canonical literal (missing zero-padding) can still parse to a valid date, but
    // string-ordering it against canonical date strings is not equivalent to date-ordering it.
    // These fail the round-trip check before fromExp's shape is even considered, so f7 is fine.
    val nonCanonical = dateToStrCast <= "2023-1-1"
    assertEquivalent(nonCanonical, nonCanonical, evaluate = false)

    val unparseable = dateToStrCast <= "foobar"
    assertEquivalent(unparseable, unparseable, evaluate = false)

    val nullLit = dateToStrCast <= Literal.create(null, StringType)
    assertEquivalent(nullLit, nullLit, evaluate = false)
  }

  test("SPARK-62378: range guard is unsound under legacy time parser policy") {
    // The range-guard rewrite assumes: for ANY date outside DateType's documented
    // [0001-01-01, 9999-12-31] range, Cast(date, String) sorts before EVERY in-range, canonical
    // date string, regardless of the out-of-range date's actual magnitude. Under the default
    // (CORRECTED/EXCEPTION) formatter that holds, because every out-of-range year gets a '+'/'-'
    // sign prefix, and '+'/'-' (0x2B/0x2D) sort (ASCII) before any digit (0x30-0x39).
    //
    // Under `spark.sql.legacy.timeParserPolicy=LEGACY`, Cast(date, String) instead uses
    // `java.text.SimpleDateFormat("yyyy-MM-dd")`, whose "yyyy" pattern does not sign-prefix or
    // truncate -- it just prints however many digits the year needs. A big enough out-of-range
    // year (e.g. 99999) then formats as a plain digit string that can sort AFTER an in-range
    // literal, breaking the invariant the rewrite depends on. This is why unwrapStringToDate must
    // return None entirely under LEGACY, rather than only guarding the range predicates.
    val bigYearDays = LocalDate.of(99999, 1, 1).toEpochDay.toInt
    val bigYearRow = create_row(null, null, null, null, null, null, bigYearDays, null)

    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> LegacyBehaviorPolicy.LEGACY.toString) {
      val formatted = Cast(f7, StringType, ansiEnabled = false).eval(bigYearRow)
      assert(formatted === UTF8String.fromString("99999-01-01"),
        "precondition: LEGACY must not sign-prefix out-of-range years")

      // Ground truth: "99999-01-01" does not sort before "2024-01-01" ('9' > '2'), so the
      // original (un-rewritten) predicate is false for this row.
      val original = Cast(f7, StringType) < "2024-01-01"
      assert(original.eval(bigYearRow) == false)

      // What the range-guard rewrite would produce if it fired here: it assumes every
      // out-of-range column value makes `<` trivially true, via `f7 > maxDateLit`.
      val maxDateLit =
        Literal(SparkDateTimeUtils.localDateToDays(LocalDate.of(9999, 12, 31)), DateType)
      val dateLit = Literal(SparkDateTimeUtils.localDateToDays(LocalDate.of(2024, 1, 1)), DateType)
      val wouldBeRewritten = Or(GreaterThan(f7, maxDateLit), LessThan(f7, dateLit))
      assert(wouldBeRewritten.eval(bigYearRow) == true)

      // The two disagree -- proving the rewrite would be unsound here -- so the rule must skip
      // it entirely under LEGACY, which it does: the optimized plan is left untouched.
      assert(original.eval(bigYearRow) != wouldBeRewritten.eval(bigYearRow))
      assertEquivalent(original, original, evaluate = false)
    }
  }

  test("SPARK-62378: range predicates require fromExp to be an Attribute") {
    // The range-predicate rewrite embeds `fromExp` twice (once in the range guard, once more
    // in the rewritten comparison), so it only applies when `fromExp` is an Attribute --
    // cheap and safe to re-read. A non-Attribute fromExp like `DateAdd(f7, 1)` is left untouched
    // for the range predicates, but `=`/`<=>` embed `fromExp` only once either way and still
    // rewrite.
    val computedDate = DateAdd(f7, Literal(1))
    val dateToStrCast = Cast(computedDate, StringType)
    val dateLit = Literal.create(LocalDate.of(2023, 1, 1), DateType)

    val gt = dateToStrCast > "2023-01-01"
    assertEquivalent(gt, gt)
    val lte = dateToStrCast <= "2023-01-01"
    assertEquivalent(lte, lte)

    assertEquivalent(dateToStrCast === "2023-01-01", computedDate === dateLit)
    assertEquivalent(dateToStrCast <=> "2023-01-01", computedDate <=> dateLit)
  }

  test("SPARK-62378: range-predicate rewrite's runtime guard is correct for an " +
    "out-of-range column value") {
    // The range-predicate rewrite only fires for an Attribute fromExp, so build the plan with a
    // genuine AttributeReference (g), then bind both the original and optimized conditions back
    // to BoundReferences (matching testRelation's schema order) so `.eval(row)` works below.
    val g: AttributeReference = testRelation.output(6).asInstanceOf[AttributeReference]
    val dateToStrCast = Cast(g, StringType)
    val original = dateToStrCast <= "2024-01-01"
    val optimized = Optimize.execute(testRelation.where(original).analyze)
      .asInstanceOf[Filter].condition
    val boundOriginal = BindReferences.bindReference(original, testRelation.output)
    val boundOptimized = BindReferences.bindReference(optimized, testRelation.output)

    // date_add(DATE'9999-12-31', 100) is allowed by Spark (DateAdd does unchecked Int
    // arithmetic), landing in year ~10000; Cast(Date, String) then formats it as the
    // non-fixed-width "+10000-04-09", which sorts lexicographically BEFORE "2024-01-01"
    // despite being chronologically after it.
    val outOfRangeDays = LocalDate.of(9999, 12, 31).plusDays(100).toEpochDay.toInt
    val outOfRangeRow = create_row(null, null, null, null, null, null, outOfRangeDays, null)
    assert(boundOriginal.eval(outOfRangeRow) == true)
    checkEvaluation(boundOptimized, boundOriginal.eval(outOfRangeRow), outOfRangeRow)

    val inRangeDays = LocalDate.of(2023, 1, 1).toEpochDay.toInt
    val inRangeRow = create_row(null, null, null, null, null, null, inRangeDays, null)
    checkEvaluation(boundOptimized, boundOriginal.eval(inRangeRow), inRangeRow)

    val nullRow = create_row(null, null, null, null, null, null, null, null)
    checkEvaluation(boundOptimized, boundOriginal.eval(nullRow), nullRow)
  }

  test("SPARK-62378: falls back when the literal is outside DateType's documented " +
    "[0001-01-01, 9999-12-31] range") {
    // '+10000-04-09' parses to a valid Date (year 10000) and, unlike dates within the documented
    // range, round-trips back through Cast(Date, String) to the exact same string, so it passes
    // unwrapStringToDate's canonicality check. But its day-count sorts AFTER every in-range date,
    // while its formatted string ('+10000-04-09') sorts BEFORE every in-range, canonical date
    // string (ASCII '+' < '0'-'9'). A range-predicate rewrite that only checks fromExp's range
    // (not the literal's) would therefore flip the comparison's result for an in-range column --
    // e.g. "2024-01-01" <= "+10000-04-09" is false by string order, but 2024-01-01 <= DATE
    // '10000-04-09' is true by date order. The rewrite must decline to fire in this case.
    val g: AttributeReference = testRelation.output(6).asInstanceOf[AttributeReference]
    val dateToStrCast = Cast(g, StringType)
    val outOfRangeLit = "+10000-04-09"

    for (original <- Seq(
        dateToStrCast < outOfRangeLit,
        dateToStrCast <= outOfRangeLit,
        dateToStrCast > outOfRangeLit,
        dateToStrCast >= outOfRangeLit)) {
      // No rewrite at all: the optimized plan keeps the original Cast-based comparison.
      assertEquivalent(original, original, evaluate = false)
    }

    val boundOriginal = BindReferences.bindReference(
      dateToStrCast <= outOfRangeLit, testRelation.output)
    val inRangeDays = LocalDate.of(2024, 1, 1).toEpochDay.toInt
    val inRangeRow = create_row(null, null, null, null, null, null, inRangeDays, null)
    assert(boundOriginal.eval(inRangeRow) == false)
  }

  test("SPARK-62378: falls back when the literal's year is <= 0") {
    // '-0005-01-01' round-trips through Cast(Date, String) exactly (like '+10000-04-09' above),
    // but unlike that over-range case, a negative-year literal alone doesn't break order against
    // an in-range column -- it only breaks order against another negative-year, out-of-range
    // column: "-0010-01-01" < "-0005-01-01" by date order (year -10 is earlier), but
    // "-0010-01-01" > "-0005-01-01" by string order (same '-' prefix, then digit-magnitude
    // comparison: "0010" > "0005"). So the rewrite must decline for negative-year literals too,
    // not just over-range ones -- this is why `literalInRange` checks the literal's lower bound.
    val g: AttributeReference = testRelation.output(6).asInstanceOf[AttributeReference]
    val dateToStrCast = Cast(g, StringType)
    val outOfRangeLit = "-0005-01-01"

    for (original <- Seq(
        dateToStrCast < outOfRangeLit,
        dateToStrCast <= outOfRangeLit,
        dateToStrCast > outOfRangeLit,
        dateToStrCast >= outOfRangeLit)) {
      assertEquivalent(original, original, evaluate = false)
    }

    val boundOriginal = BindReferences.bindReference(
      dateToStrCast < outOfRangeLit, testRelation.output)
    val alsoOutOfRangeDays = LocalDate.of(-10, 1, 1).toEpochDay.toInt
    val alsoOutOfRangeRow = create_row(null, null, null, null, null, null, alsoOutOfRangeDays, null)
    // Ground truth: "-0010-01-01" does not sort before "-0005-01-01" ('1' > '0' at the first
    // differing digit), so the original predicate is false for this row.
    assert(boundOriginal.eval(alsoOutOfRangeRow) == false)

    // What a max-bound-only `literalInRange` would (wrongly) produce: `g > maxDateLit` is false
    // (g is very negative), but `g < literalDate` is true (year -10 is chronologically before
    // year -5) -- the opposite of the correct answer above.
    val maxDateLit =
      Literal(SparkDateTimeUtils.localDateToDays(LocalDate.of(9999, 12, 31)), DateType)
    val literalDate = Literal(LocalDate.of(-5, 1, 1).toEpochDay.toInt, DateType)
    val wouldBeRewritten = Or(GreaterThan(g, maxDateLit), LessThan(g, literalDate))
    val boundWouldBeRewritten = BindReferences.bindReference(wouldBeRewritten, testRelation.output)
    assert(boundWouldBeRewritten.eval(alsoOutOfRangeRow) == true)
    assert(boundOriginal.eval(alsoOutOfRangeRow) != boundWouldBeRewritten.eval(alsoOutOfRangeRow))
  }

  test("SPARK-62378: needs no range guard for `=`/`<=>` outside DateType's " +
    "documented [0001-01-01, 9999-12-31] range") {
    val dateToStrCast = Cast(f7, StringType)
    val outOfRangeDays = LocalDate.of(9999, 12, 31).plusDays(100).toEpochDay.toInt
    val outOfRangeRow = create_row(null, null, null, null, null, null, outOfRangeDays, null)

    for (original <- Seq(dateToStrCast === "2024-01-01", dateToStrCast <=> "2024-01-01")) {
      val optimized = Optimize.execute(testRelation.where(original).analyze)
        .asInstanceOf[Filter].condition
      assert(original.eval(outOfRangeRow) == false)
      checkEvaluation(optimized, original.eval(outOfRangeRow), outOfRangeRow)
    }
  }

  test("SPARK-46502: Support unwrap timestamp_ntz to timestamp type") {
    def doTest(tsLit: Literal): Unit = {
      val tz = Some(conf.sessionLocalTimeZone)
      val tsNtzData = Cast(tsLit, TimestampNTZType, tz)
      val tsNTzToTsCast = Cast(f6, tsLit.dataType, tz)

      assertEquivalent(tsNTzToTsCast > tsLit, f6 > tsNtzData)
      assertEquivalent(tsNTzToTsCast <= tsLit, f6 <= tsNtzData)
      assertEquivalent(tsNTzToTsCast >= tsLit, f6 >= tsNtzData)
      assertEquivalent(tsNTzToTsCast < tsLit, f6 < tsNtzData)
      assertEquivalent(tsNTzToTsCast === tsLit, f6 === tsNtzData)
      assertEquivalent(tsNTzToTsCast <=> tsLit, f6 <=> tsNtzData)
    }

    val micros = SparkDateTimeUtils.daysToMicros(19704, ZoneId.of(conf.sessionLocalTimeZone))
    val instant = java.time.Instant.ofEpochSecond(micros / 1000000)
    val tsLit = Literal.create(instant, TimestampType)
    doTest(tsLit)

    val ts = LocalDateTime.of(2023, 12, 13, 0, 0, 0, 0)
    val tsLit2 = Literal.create(ts, TimestampType)
    doTest(tsLit2)

    val tsLit3 = Literal.create(instant.plusSeconds(30), TimestampType)
    doTest(tsLit3)
    val tsLit4 = Literal.create(ts.withSecond(30), TimestampNTZType)
    doTest(tsLit4)
  }

  test("SPARK-46502: Support unwrap timestamp to timestamp_ntz type") {
    def doTest(tsLit: Literal): Unit = {
      val tz = Some(conf.sessionLocalTimeZone)
      val tsData = Cast(tsLit, TimestampType, tz)
      val tsToTsNtzCast = Cast(f5, tsLit.dataType, tz)

      assertEquivalent(tsToTsNtzCast > tsLit, f5 > tsData)
      assertEquivalent(tsToTsNtzCast <= tsLit, f5 <= tsData)
      assertEquivalent(tsToTsNtzCast >= tsLit, f5 >= tsData)
      assertEquivalent(tsToTsNtzCast < tsLit, f5 < tsData)
      assertEquivalent(tsToTsNtzCast === tsLit, f5 === tsData)
      assertEquivalent(tsToTsNtzCast <=> tsLit, f5 <=> tsData)
    }

    val micros = SparkDateTimeUtils.daysToMicros(19704, ZoneId.of(conf.sessionLocalTimeZone))
    val instant = java.time.Instant.ofEpochSecond(micros / 1000000)
    val tsNtzLit = Literal.create(instant, TimestampNTZType)
    doTest(tsNtzLit)

    val ts = LocalDateTime.of(2023, 12, 13, 0, 0, 0, 0)
    val tsNtzLit2 = Literal.create(ts, TimestampNTZType)
    doTest(tsNtzLit2)

    val tsNtzLit3 = Literal.create(instant.plusSeconds(30), TimestampNTZType)
    doTest(tsNtzLit3)
    val tsNtzLit4 = Literal.create(ts.withSecond(30), TimestampNTZType)
    doTest(tsNtzLit4)
  }

  test("SPARK-60355: Support unwrap time type to time type with a larger precision") {
    // The literal is representable as TIME(6), so the cast is moved to the literal side.
    Seq(time("12:00:00.1", 9), time("00:00:00", 9), time("23:59:59.999999", 7)).foreach { lit =>
      val v = Literal(lit.value, f8.dataType)
      assertEquivalent(Cast(f8, lit.dataType) > lit, f8 > v)
      assertEquivalent(Cast(f8, lit.dataType) >= lit, f8 >= v)
      assertEquivalent(Cast(f8, lit.dataType) === lit, f8 === v)
      assertEquivalent(Cast(f8, lit.dataType) <=> lit, f8 <=> v)
      assertEquivalent(Cast(f8, lit.dataType) <= lit, f8 <= v)
      assertEquivalent(Cast(f8, lit.dataType) < lit, f8 < v)
      assertEquivalent(lit < Cast(f8, lit.dataType), v < f8)
    }

    // The literal has more fractional digits than TIME(6), so it is truncated to TIME(6), which
    // rounds it down.
    Seq(
      time("12:00:00.100000001", 9) -> time("12:00:00.1", 6),
      time("23:59:59.999999999", 9) -> time("23:59:59.999999", 6),
      time("12:00:00.1000005", 7) -> time("12:00:00.1", 6)
    ).foreach { case (lit, v) =>
      assertEquivalent(Cast(f8, lit.dataType) > lit, f8 > v)
      assertEquivalent(Cast(f8, lit.dataType) >= lit, f8 > v)
      assertEquivalent(Cast(f8, lit.dataType) === lit, falseIfNotNull(f8))
      assertEquivalent(Cast(f8, lit.dataType) <=> lit, FalseLiteral)
      assertEquivalent(Cast(f8, lit.dataType) <= lit, f8 <= v)
      assertEquivalent(Cast(f8, lit.dataType) < lit, f8 <= v)
      assertEquivalent(lit < Cast(f8, lit.dataType), v < f8)
    }

    // Neither a null literal nor a cast to a smaller precision is unwrapped.
    Seq(
      Cast(f8, TimeType(9)) === Literal(null, TimeType(9)),
      Cast(f8, TimeType(3)) === time("12:00:00.1", 3)
    ).foreach { e =>
      assertEquivalent(e, e)
    }

    checkInAndInSet(
      In(Cast(f8, TimeType(9)),
        Seq(time("12:00:00.1", 9), time("12:00:00.100000001", 9), time("00:00:00", 9))),
      f8.in(time("12:00:00.1", 6), time("00:00:00", 6)))
    checkInAndInSet(
      In(Cast(f8, TimeType(9)), Seq(time("12:00:00.100000001", 9))),
      falseIfNotNull(f8))
    val nullLit = Literal(null, TimeType(9))
    checkInAndInSet(
      In(Cast(f8, TimeType(9)), Seq(nullLit, time("12:00:00.1", 9))),
      f8.in(nullLit.cast(TimeType(6)), time("12:00:00.1", 6)))
  }

  private val ts1 = LocalDateTime.of(2023, 1, 1, 23, 59, 59, 99999000)
  private val ts2 = LocalDateTime.of(2023, 1, 1, 23, 59, 59, 999998000)
  private val ts3 = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999999)
  private val ts4 = LocalDateTime.of(1, 1, 1, 0, 0, 0, 0)

  private val dt1 = java.sql.Date.valueOf("2023-01-01")

  private val tm1 = LocalTime.of(12, 0, 0, 100000000)
  private val tm2 = LocalTime.of(12, 0, 0, 100001000)
  private val tm3 = LocalTime.of(23, 59, 59, 999999000)
  private val tm4 = LocalTime.MIDNIGHT
  private val tm5 = LocalTime.of(12, 0, 0, 99999000)

  private def time(s: String, precision: Int): Literal =
    Literal(LocalTime.parse(s).toNanoOfDay, TimeType(precision))

  private def castInt(e: Expression): Expression = Cast(e, IntegerType)
  private def castDouble(e: Expression): Expression = Cast(e, DoubleType)
  private def castDecimal2(e: Expression): Expression = Cast(e, DecimalType(10, 4))
  private def castDate(e: Expression): Expression =
    Cast(e, DateType, Some(conf.sessionLocalTimeZone))
  private def castTimestamp(e: Expression): Expression =
    Cast(e, TimestampType, Some(conf.sessionLocalTimeZone))
  private def castTimestampNTZ(e: Expression): Expression =
    Cast(e, TimestampNTZType, Some(conf.sessionLocalTimeZone))

  private def decimal(v: Decimal): Decimal = Decimal(v.toJavaBigDecimal, 5, 2)
  private def decimal2(v: BigDecimal): Decimal = Decimal(v, 10, 4)

  private def assertEquivalent(e1: Expression, e2: Expression, evaluate: Boolean = true): Unit = {
    val plan = testRelation.where(e1).analyze
    val actual = Optimize.execute(plan)
    val expected = testRelation.where(e2).analyze
    comparePlans(actual, expected)

    if (evaluate) {
      Seq(
        (100.toShort, 3.14.toFloat, decimal2(100), true, ts1, ts1, dt1, tm1),
        (-300.toShort, 3.1415927.toFloat, decimal2(-3000.50), false, ts2, ts2, dt1, tm2),
        (null, Float.NaN, decimal2(12345.6789), null, null, null, null, null),
        (null, null, null, null, null, null, null, null),
        (Short.MaxValue, Float.PositiveInfinity, decimal2(Short.MaxValue), true, ts3, ts3, dt1,
          tm3),
        (Short.MinValue, Float.NegativeInfinity, decimal2(Short.MinValue), false, ts4, ts4, dt1,
          tm4),
        (0.toShort, Float.MaxValue, decimal2(0), null, null, null, null, tm5),
        (0.toShort, Float.MinValue, decimal2(0.01), null, null, null, null, null)
      ).foreach(v => {
        val row = create_row(v._1, v._2, v._3, v._4, v._5, v._6, v._7, v._8)
        checkEvaluation(e1, e2.eval(row), row)
      })
    }
  }

  private def checkInAndInSet(in: In, expected: Expression): Unit = {
    assertEquivalent(in, expected)
    val toInSet = (in: In) => InSet(in.value, HashSet() ++ in.list.map(_.eval()))
    val expectedInSet = expected match {
      case expectedIn: In =>
        toInSet(expectedIn)
      case falseIfNotNull: And =>
        falseIfNotNull
    }
    assertEquivalent(toInSet(in), expectedInSet)
  }
}
