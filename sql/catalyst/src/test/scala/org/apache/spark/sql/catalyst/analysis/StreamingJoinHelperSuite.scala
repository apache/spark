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

import java.time.Duration

import org.apache.logging.log4j.Level

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Cast, Expression, Literal, TimestampAddInterval}
import org.apache.spark.sql.catalyst.optimizer.SimpleTestOptimizer
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, Filter, LeafNode, Statistics}
import org.apache.spark.sql.types.{DataType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MetadataBuilder, TimestampNTZType, TimestampType}

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

  test("state watermark extraction ignores columns outside both attribute sets") {
    val leftTime = AttributeReference("leftTime", TimestampType)()
    val leftOffset = AttributeReference("leftOffset", LongType)()
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val rightTime = AttributeReference("rightTime", TimestampType, metadata = metadata)()
    val rightDelay = AttributeReference("rightDelay", DayTimeIntervalType())()
    val conditions = Seq(
      leftTime > TimestampAddInterval(rightTime, rightDelay),
      leftTime.cast(LongType) + leftOffset > rightTime.cast(LongType))
    val logAppender = new LogAppender("state watermark extraction")
    withLogAppender(logAppender, level = Some(Level.WARN)) {
      conditions.foreach { condition =>
        withClue(condition.sql) {
          // Eviction solves one column of an input at a time, so the other columns of the input
          // are in neither attribute set, and their values are unknown.
          assert(StreamingJoinHelper.getStateValueWatermark(
            AttributeSet(Seq(leftTime)), AttributeSet(Seq(rightTime)),
            Some(condition), Some(10000L)) === None)
        }
      }
    }
    assert(!logAppender.loggingEvents.exists(
      _.getMessage.getFormattedMessage.contains("Error trying to extract state constraint")))
  }

  test("a cutoff solved for another column than the event time keeps the earlier minimum") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val leftTime = AttributeReference("leftTime", TimestampType, metadata = metadata)()
    val expiry = AttributeReference("expiry", TimestampType)()
    val rightTime = AttributeReference("rightTime", TimestampType, metadata = metadata)()
    def cutoff(condition: Expression): Option[Long] = StreamingJoinHelper.getStateValueWatermark(
      AttributeSet(Seq(leftTime, expiry)), AttributeSet(Seq(rightTime)), Some(condition),
      Some(100000L))
    val constant = leftTime.cast(LongType) > 10
    // The cutoff from expiry applies to leftTime on the whole-input path, so the constant caps it.
    assert(cutoff(rightTime <= expiry && constant) === Some(10000L))
    // A cutoff on leftTime itself is not capped.
    assert(cutoff(leftTime > rightTime && constant) === Some(100000L))
    assert(cutoff(leftTime.cast(LongType) + rightTime.cast(LongType) > 100) === None)
  }

  test("a cutoff through a truncating cast keeps the earlier minimum") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val leftTime = AttributeReference("leftTime", TimestampType, metadata = metadata)()
    val rightTime = AttributeReference("rightTime", TimestampType, metadata = metadata)()
    def cutoff(condition: Expression): Option[Long] = StreamingJoinHelper.getStateValueWatermark(
      AttributeSet(Seq(leftTime)), AttributeSet(Seq(rightTime)), Some(condition), Some(100000L))
    val truncating = leftTime.cast(LongType) >= rightTime.cast(LongType) - 5
    assert(cutoff(truncating) === Some(94999L))
    assert(cutoff(truncating && leftTime.cast(LongType) > 10) === Some(10000L))
    // Casts that round, or keep too few digits, count too. One that keeps every digit does not.
    Seq[(DataType, Option[Long])](DecimalType(10, 0) -> Some(10000L), FloatType -> Some(10000L),
        DecimalType(16, 6) -> Some(94999L)).foreach { case (dataType, expected) =>
      assert(cutoff(leftTime.cast(dataType) >= rightTime.cast(dataType) - 5 &&
        leftTime.cast(LongType) > 10) === expected, dataType)
    }
    // A cast to DECIMAL keeps every digit of a TIMESTAMP only with a scale of 6 or more, and a cast
    // from DECIMAL only with a scale at least that of its input. One from DOUBLE can round.
    Seq[(Expression => Expression, Option[Long])](
        (_.cast(DecimalType(16, 5)), Some(10000L)),
        (_.cast(DecimalType(20, 6)).cast(DecimalType(20, 3)), Some(10000L)),
        (_.cast(DoubleType).cast(DecimalType(20, 6)), Some(10000L)),
        (_.cast(DecimalType(20, 6)).cast(DecimalType(22, 6)), Some(94999L)))
      .foreach { case (cast, expected) =>
        assert(cutoff(cast(leftTime) >= cast(rightTime) - 5 &&
          leftTime.cast(LongType) > 10) === expected, cast(leftTime))
      }
    // The sum gives no cutoff that rises with the watermark, so it does not count.
    assert(cutoff(leftTime > rightTime &&
      leftTime.cast(LongType) + rightTime.cast(LongType) > 100) === Some(100000L))
  }

  test("a cutoff through whole days added to a TIMESTAMP keeps the earlier minimum") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val leftTime = AttributeReference("leftTime", TimestampType, metadata = metadata)()
    val rightTime = AttributeReference("rightTime", TimestampType, metadata = metadata)()
    def cutoff(condition: Expression, risingCutoffsOnly: Boolean = true): Option[Long] =
      StreamingJoinHelper.getStateValueWatermark(AttributeSet(Seq(leftTime)),
        AttributeSet(Seq(rightTime)), Some(condition), Some(100000000L), risingCutoffsOnly)
    def plus(time: Expression, interval: Duration, zone: String): Expression =
      TimestampAddInterval(time, Literal(interval), Some(zone))
    val day = Duration.ofDays(1)
    Seq(leftTime.cast(LongType) > 10,
        leftTime.cast(LongType) + rightTime.cast(LongType) > 100).foreach { cap =>
      Seq[String => Expression](
          zone => rightTime <= plus(leftTime, day, zone),
          zone => leftTime >= plus(rightTime, day.negated(), zone)).foreach { bound =>
        // Also in UTC, so that the result does not change with the session time zone.
        Seq("America/Los_Angeles", "UTC").foreach { zone =>
          val condition = bound(zone) && cap
          assert(cutoff(condition) === cutoff(condition, risingCutoffsOnly = false), condition)
          assert(cutoff(condition) !== Some(13599999L), condition)
        }
      }
      // Less than a day is added as an exact number of microseconds.
      assert(cutoff(rightTime <= plus(leftTime, Duration.ofHours(23), "America/Los_Angeles") &&
        cap) === Some(17199999L))
    }
  }

  test("state value watermark column skips another column with an inexact cutoff when the " +
      "event time is held back") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val leftTime = AttributeReference("leftTime", TimestampType, metadata = metadata)()
    val expiry = AttributeReference("expiry", TimestampType)()
    val rightTime = AttributeReference("rightTime", TimestampType, metadata = metadata)()
    def column(condition: Expression): Option[Attribute] =
      StreamingJoinHelper.getStateValueWatermarkColumn(Seq(leftTime, expiry),
        AttributeSet(Seq(rightTime)), Some(condition), Some(100000000L)).map(_._1)
    val inexact = expiry.cast(LongType) >= rightTime.cast(LongType)
    // The pick orders state format 4 state, so it does not change with the session time zone.
    def dayInterval(zone: String): Expression =
      rightTime <= TimestampAddInterval(expiry, Literal(Duration.ofDays(1)), Some(zone))
    Seq(leftTime.cast(LongType) > 10,
        leftTime.cast(LongType) + rightTime.cast(LongType) > 100).foreach { cap =>
      assert(column(inexact && cap) === None, cap)
      Seq("Europe/Paris", "UTC").foreach { zone =>
        assert(column(dayInterval(zone) && cap) === None, zone)
      }
      assert(column(rightTime <= expiry && cap) === Some(expiry), cap)
    }
    // Without a cap on the event time, the inexact cutoff on expiry is all there is.
    assert(column(inexact) === Some(expiry))
  }

  test("state value watermark column prefers the event time and takes only TIMESTAMP columns") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val leftTime = AttributeReference("leftTime", TimestampType, metadata = metadata)()
    val expiry = AttributeReference("expiry", TimestampType)()
    val leftNtz = AttributeReference("leftNtz", TimestampNTZType)()
    val rightTime = AttributeReference("rightTime", TimestampType, metadata = metadata)()
    def column(condition: Expression): Option[Attribute] =
      StreamingJoinHelper.getStateValueWatermarkColumn(Seq(expiry, leftTime, leftNtz),
        AttributeSet(Seq(rightTime)), Some(condition), Some(10000L)).map(_._1)
    // expiry comes first in input order, but state format 4 probes by the event time.
    assert(column(expiry > rightTime && leftTime > rightTime) === Some(leftTime))
    assert(column(expiry > rightTime) === Some(expiry))
    assert(column(Cast(leftNtz, TimestampType, Some("UTC")) > rightTime) === None)

    // Among other TIMESTAMP columns, the first in input order wins.
    val deadline = AttributeReference("deadline", TimestampType)()
    Seq(Seq(leftTime, expiry, deadline) -> expiry, Seq(leftTime, deadline, expiry) -> deadline)
      .foreach { case (inputAttributes, expected) =>
        assert(StreamingJoinHelper.getStateValueWatermarkColumn(inputAttributes,
          AttributeSet(Seq(rightTime)), Some(rightTime <= expiry && rightTime <= deadline),
          Some(10000L)).map(_._1) === Some(expected))
      }

    // A column that the condition bounds only from above is skipped.
    val validFrom = AttributeReference("validFrom", TimestampType)()
    val validTo = AttributeReference("validTo", TimestampType)()
    assert(StreamingJoinHelper.getStateValueWatermarkColumn(Seq(leftTime, validFrom, validTo),
      AttributeSet(Seq(rightTime)), Some(rightTime >= validFrom && rightTime <= validTo),
      Some(10000L)) === Some((validTo, 9999L)))
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

    // A cutoff needs a conjunct that can be rearranged to `leftTime > rightTime + c`, so that
    // it rises with the opposite watermark.
    assert(watermarkFrom("cast(leftTime AS LONG) > 10") === None)
    assert(watermarkFrom("CAST(leftTime AS LONG) + CAST(rightTime AS LONG) > 100") === None)
    assert(watermarkFrom("100 < CAST(rightTime AS LONG) + CAST(leftTime AS LONG)") === None)
    assert(watermarkFrom(
      "leftTime > rightTime + interval 5 seconds AND cast(leftTime AS LONG) > 10") ===
      Some(15000))
    assert(watermarkFrom("leftTime > rightTime + interval 95 seconds AND " +
      "CAST(leftTime AS LONG) + CAST(rightTime AS LONG) > 100") === Some(105000))

    // Test non-positive results
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) - 10") === Some(0))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) - 100") === Some(-90000))
  }
}
