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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Cast, Expression, Literal, TimestampAdd, TimestampAddInterval, TimestampAddYMInterval}
import org.apache.spark.sql.catalyst.optimizer.SimpleTestOptimizer
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, Filter, LeafNode, Statistics}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_DAY
import org.apache.spark.sql.types.{CalendarIntervalType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MetadataBuilder, TimestampType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.CalendarInterval

class StreamingJoinHelperSuite extends AnalysisTest {

  test("state watermark extraction preserves arithmetic term multiplicity") {
    val leftTime = AttributeReference("leftTime", TimestampType)()
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val rightTime = AttributeReference("rightTime", TimestampType, metadata = metadata)()
    val left = leftTime.cast(LongType)
    val right = rightTime.cast(LongType)

    val cases = Seq(
      (left > right + 2 + 2, Some(14000L)),
      (left > right - 2 - 2, Some(6000L)),
      (left + left > right, None),
      (left > right + right, None),
      (left > right - right, None))
    cases.foreach { case (condition, expected) =>
      withClue(condition.sql) {
        assert(StreamingJoinHelper.getStateValueWatermark(
          AttributeSet(Seq(leftTime)), AttributeSet(Seq(rightTime)),
          Some(condition), Some(10000L)) === expected)
      }
    }
  }

  test("find casts that drop part of a time") {
    val leftTime = AttributeReference("leftTime", TimestampType)()
    val rightTime = AttributeReference("rightTime", TimestampType)()
    import StreamingJoinHelper.hasTruncatingCast

    assert(hasTruncatingCast(leftTime.cast(LongType) > rightTime.cast(LongType)))
    assert(hasTruncatingCast(leftTime.cast(IntegerType) > 0))
    assert(hasTruncatingCast(leftTime.cast(FloatType) > rightTime.cast(DoubleType)))
    assert(hasTruncatingCast(leftTime.cast(DecimalType(20, 3)) > 0))
    // Casts that keep every microsecond.
    assert(!hasTruncatingCast(leftTime.cast(DoubleType) > rightTime.cast(DoubleType)))
    assert(!hasTruncatingCast(leftTime.cast(DecimalType(20, 6)) > 0))
    assert(!hasTruncatingCast(Cast(leftTime, TimestampType) > rightTime))
    // A cast of an integral value drops nothing.
    assert(!hasTruncatingCast(leftTime.cast(DoubleType) >
      rightTime.cast(DoubleType) + Cast(Literal(1), LongType).cast(DoubleType)))
  }

  test("find whole days in a zone whose offset changes, and months") {
    val leftTime = AttributeReference("leftTime", TimestampType)()
    val rightTime = AttributeReference("rightTime", TimestampType)()
    val delay = AttributeReference("delay", DayTimeIntervalType())()
    def dayTime(micros: Long): Expression = Literal(micros, DayTimeIntervalType())
    def calendar(months: Int, days: Int, micros: Long): Expression =
      Literal(new CalendarInterval(months, days, micros), CalendarIntervalType)
    def bound(
        interval: Expression,
        zone: Option[String] = Some("America/Los_Angeles")): Expression =
      rightTime <= TimestampAddInterval(leftTime, interval, zone)
    import StreamingJoinHelper.hasCalendarInterval

    assert(hasCalendarInterval(bound(dayTime(MICROS_PER_DAY))))
    assert(hasCalendarInterval(bound(dayTime(-MICROS_PER_DAY))))
    assert(hasCalendarInterval(bound(calendar(0, 1, 0))))
    assert(hasCalendarInterval(bound(dayTime(MICROS_PER_DAY), None)))
    assert(hasCalendarInterval(bound(calendar(1, 0, 0), Some("UTC"))))
    assert(hasCalendarInterval(bound(delay, Some("UTC"))))
    assert(hasCalendarInterval(
      rightTime <= TimestampAddYMInterval(leftTime, Literal(1, YearMonthIntervalType()))))
    assert(hasCalendarInterval(rightTime <= TimestampAdd("DAY", Literal(1), leftTime)))
    // A day is 24 hours in a zone with a fixed offset.
    Seq("UTC", "Etc/GMT+5", "+05:00").foreach { zone =>
      assert(!hasCalendarInterval(bound(dayTime(MICROS_PER_DAY), Some(zone))), zone)
      assert(!hasCalendarInterval(bound(calendar(0, 1, 0), Some(zone))), zone)
    }
    // Spark adds these as fixed durations, or the expression has no interval.
    assert(!hasCalendarInterval(bound(dayTime(MICROS_PER_DAY - 1))))
    assert(!hasCalendarInterval(bound(calendar(0, 0, MICROS_PER_DAY))))
    assert(!hasCalendarInterval(rightTime <= TimestampAdd("HOUR", Literal(25), leftTime)))
    assert(!hasCalendarInterval(rightTime >= leftTime))
  }

  test("V4 range scan offsets come only from comparisons that give exact offsets") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val leftTime = AttributeReference("leftTime", TimestampType, metadata = metadata)()
    val rightTime = AttributeReference("rightTime", TimestampType, metadata = metadata)()
    val leftExpiry = AttributeReference("leftExpiry", TimestampType)()
    val leftValue = AttributeReference("leftValue", DoubleType)()
    def plus(t: Expression, micros: Long, zone: String = "America/Los_Angeles"): Expression =
      TimestampAddInterval(t, Literal(micros, DayTimeIntervalType()), Some(zone))
    def offsets(condition: Expression, leftProbes: Boolean = true): Option[(Long, Long)] =
      if (leftProbes) {
        StreamingJoinHelper.getRangeScanOffsets(Some(condition), leftTime, rightTime)
      } else {
        StreamingJoinHelper.getRangeScanOffsets(Some(condition), rightTime, leftTime)
      }
    val tenMinutes = 600000000L
    // Each end is the solved bound, less 1 ms for a non-strict comparison, widened by 1 ms.
    val exact = rightTime >= leftTime && rightTime <= plus(leftTime, tenMinutes)
    assert(offsets(exact) === Some((-2000L, 600002000L)))
    assert(offsets(exact, leftProbes = false) === Some((-600002000L, 2000L)))

    // Whole days are exact in a zone with a fixed offset.
    val oneDay = rightTime >= leftTime && rightTime <= plus(leftTime, MICROS_PER_DAY, "UTC")
    assert(offsets(oneDay) === Some((-2000L, 86400002000L)))
    assert(offsets(rightTime >= leftTime && rightTime <= plus(leftTime, MICROS_PER_DAY))
      === None)

    // A comparison the window does not need drops neither end.
    Seq(
      rightTime <= plus(leftExpiry, MICROS_PER_DAY),
      rightTime.cast(LongType) <= leftTime.cast(LongType) + 7200L,
      leftValue.cast(LongType) > 0L).foreach { other =>
      assert(offsets(exact && other) === Some((-2000L, 600002000L)), other.sql)
    }

    // A comparison of one event time with a constant gives an absolute time, not an offset,
    // and a sum of the two event times moves the other way.
    Seq(
      leftTime.cast(DoubleType) >= 1.7e9,
      leftTime.cast(DoubleType) > 0.0,
      leftTime.cast(DecimalType(20, 6)) >= Literal(BigDecimal(1.7e9))).foreach { constant =>
      assert(offsets(rightTime >= leftTime && constant) === None, constant.sql)
    }
    assert(offsets(leftTime.cast(DoubleType) + rightTime.cast(DoubleType) > 100.0 &&
      rightTime <= plus(leftTime, tenMinutes)) === None)

    // A bound that is not a whole number of milliseconds: -0.5 ms is solved as 0.
    val subMillisecond = rightTime > plus(leftTime, -500L) && rightTime < plus(leftTime, 1000000L)
    assert(offsets(subMillisecond) === Some((-1000L, 1001000L)))

    // Casts that drop part of a time give no bound.
    assert(offsets(rightTime.cast(LongType) >= leftTime.cast(LongType) + 60L &&
      rightTime.cast(LongType) <= leftTime.cast(LongType) + 120L) === None)
  }

  test("extract watermark from time condition") {
    val attributesToFindConstraintFor = Seq(
      AttributeReference("leftTime", TimestampType)(),
      AttributeReference("leftOther", IntegerType)())
    val metadataWithWatermark = new MetadataBuilder()
      .putLong(EventTimeWatermark.delayKey, 1000)
      .build()
    val attributesWithWatermark = Seq(
      AttributeReference("rightTime", TimestampType, metadata = metadataWithWatermark)(),
      AttributeReference("rightOther", IntegerType)())

    case class DummyLeafNode() extends LeafNode {
      override def output: Seq[Attribute] =
        attributesToFindConstraintFor ++ attributesWithWatermark
      // override computeStats to avoid UnsupportedOperationException.
      override def computeStats(): Statistics = Statistics(sizeInBytes = BigInt(0))
    }

    def watermarkFrom(
        conditionStr: String,
        rightWatermark: Option[Long] = Some(10000)): Option[Long] = {
      val conditionExpr = Some(conditionStr).map { str =>
        val plan =
          Filter(
            CatalystSqlParser.parseExpression(str),
            DummyLeafNode())
        val optimized = SimpleTestOptimizer.execute(SimpleAnalyzer.execute(plan))
        optimized.asInstanceOf[Filter].condition
      }
      StreamingJoinHelper.getStateValueWatermark(
        AttributeSet(attributesToFindConstraintFor), AttributeSet(attributesWithWatermark),
        conditionExpr, rightWatermark)
    }

    // Test comparison directionality. E.g. if leftTime < rightTime and rightTime > watermark,
    // then cannot define constraint on leftTime.
    assert(watermarkFrom("leftTime > rightTime") === Some(10000))
    assert(watermarkFrom("leftTime >= rightTime") === Some(9999))
    assert(watermarkFrom("leftTime < rightTime") === None)
    assert(watermarkFrom("leftTime <= rightTime") === None)
    assert(watermarkFrom("rightTime > leftTime") === None)
    assert(watermarkFrom("rightTime >= leftTime") === None)
    assert(watermarkFrom("rightTime < leftTime") === Some(10000))
    assert(watermarkFrom("rightTime <= leftTime") === Some(9999))

    // Test type conversions
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS LONG) < CAST(rightTime AS LONG)") === None)
    assert(watermarkFrom("CAST(leftTime AS DOUBLE) > CAST(rightTime AS DOUBLE)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS DOUBLE)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS FLOAT)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS DOUBLE) > CAST(rightTime AS FLOAT)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS STRING) > CAST(rightTime AS STRING)") === None)

    // Test with timestamp type + calendar interval on either side of equation
    // Note: timestamptype and calendar interval don't commute, so less valid combinations to test.
    assert(watermarkFrom("leftTime > rightTime + interval 1 second") === Some(11000))
    assert(watermarkFrom("leftTime + interval 2 seconds > rightTime ") === Some(8000))
    assert(watermarkFrom("leftTime > rightTime - interval 3 second") === Some(7000))
    assert(watermarkFrom("rightTime < leftTime - interval 3 second") === Some(13000))
    assert(watermarkFrom("rightTime - interval 1 second < leftTime - interval 3 second")
      === Some(12000))

    assert(watermarkFrom("leftTime > rightTime + interval '0 00:00:01' day to second")
      === Some(11000))
    assert(watermarkFrom("leftTime + interval '00:00:02' hour to second > rightTime ")
      === Some(8000))
    assert(watermarkFrom("leftTime > rightTime - interval '00:03' minute to second")
      === Some(7000))
    assert(watermarkFrom("rightTime < leftTime - interval '1 20:30:40' day to second")
      === Some(160250000))
    assert(watermarkFrom(
      "rightTime - interval 1 second < leftTime - interval '20:15:32' hour to second")
      === Some(72941000))

    // Test with casted long type + constants on either side of equation
    // Note: long type and constants commute, so more combinations to test.
    // -- Constants on the right
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) + 1") === Some(11000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) - 1") === Some(9000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST((rightTime + interval 1 second) AS LONG)")
      === Some(11000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > 2 + CAST(rightTime AS LONG)") === Some(12000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > -0.5 + CAST(rightTime AS LONG)") === Some(9500))
    assert(watermarkFrom("CAST(leftTime AS LONG) - CAST(rightTime AS LONG) > 2") === Some(12000))
    assert(watermarkFrom("-CAST(rightTime AS DOUBLE) + CAST(leftTime AS LONG) > 0.1")
      === Some(10100))
    assert(watermarkFrom("0 > CAST(rightTime AS LONG) - CAST(leftTime AS LONG) + 0.2")
      === Some(10200))
    // -- Constants on the left
    assert(watermarkFrom("CAST(leftTime AS LONG) + 2 > CAST(rightTime AS LONG)") === Some(8000))
    assert(watermarkFrom("1 + CAST(leftTime AS LONG) > CAST(rightTime AS LONG)") === Some(9000))
    assert(watermarkFrom("CAST((leftTime  + interval 3 second) AS LONG) > CAST(rightTime AS LONG)")
      === Some(7000))
    assert(watermarkFrom("CAST(leftTime AS LONG) - 2 > CAST(rightTime AS LONG)") === Some(12000))
    assert(watermarkFrom("CAST(leftTime AS LONG) + 0.5 > CAST(rightTime AS LONG)") === Some(9500))
    assert(watermarkFrom("CAST(leftTime AS LONG) - CAST(rightTime AS LONG) - 2 > 0")
      === Some(12000))
    assert(watermarkFrom("-CAST(rightTime AS LONG) + CAST(leftTime AS LONG) - 0.1 > 0")
      === Some(10100))
    // -- Constants on both sides, mixed types
    assert(watermarkFrom("CAST(leftTime AS LONG) - 2.0 > CAST(rightTime AS LONG) + 1")
      === Some(13000))

    // Test multiple conditions, should return minimum watermark
    assert(watermarkFrom(
      "leftTime > rightTime - interval 3 second AND rightTime < leftTime + interval 2 seconds") ===
      Some(7000))  // first condition wins
    assert(watermarkFrom(
      "leftTime > rightTime - interval 3 second AND rightTime < leftTime + interval 4 seconds") ===
      Some(6000))  // second condition wins

    // Test invalid comparisons
    assert(watermarkFrom("cast(leftTime AS LONG) > leftOther") === None)      // non-time attributes
    assert(watermarkFrom("leftOther > rightOther") === None)                  // non-time attributes
    assert(watermarkFrom("leftOther > rightOther AND leftTime > rightTime") === Some(10000))
    assert(watermarkFrom("cast(rightTime AS DOUBLE) < rightOther") === None)  // non-time attributes
    assert(watermarkFrom("leftTime > rightTime + interval 1 month") === None) // month not allowed

    // Test static comparisons
    assert(watermarkFrom("cast(leftTime AS LONG) > 10") === Some(10000))

    // Test non-positive results
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) - 10") === Some(0))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) - 100") === Some(-90000))
  }
}
