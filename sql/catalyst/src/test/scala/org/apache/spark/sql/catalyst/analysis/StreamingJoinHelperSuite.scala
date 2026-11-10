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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, EqualTo, Expression, Literal, TimestampAddInterval}
import org.apache.spark.sql.catalyst.optimizer.SimpleTestOptimizer
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, Filter, LeafNode, Statistics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DayTimeIntervalType, IntegerType, LongType, MetadataBuilder, TimestampType}
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

  test("equality key watermark on the event time plus a constant") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val eventTime = AttributeReference("eventTime", TimestampType, metadata = metadata)()
    val other = AttributeReference("other", TimestampType)()

    case class DummyLeafNode() extends LeafNode {
      override def output: Seq[Attribute] = Seq(eventTime, other)
      override def computeStats(): Statistics = Statistics(sizeInBytes = BigInt(0))
    }

    def key(keyStr: String): Expression = {
      val plan = Filter(EqualTo(CatalystSqlParser.parseExpression(keyStr), Literal(null)),
        DummyLeafNode())
      SimpleAnalyzer.execute(plan).asInstanceOf[Filter].condition
        .asInstanceOf[EqualTo].left
    }

    val cases = Seq(
      // The event time itself is bounded by the watermark.
      ("eventTime", Some(10000L), Some(10000L)),
      // A constant shifts the bound, less 1 ms as for range conditions.
      ("eventTime + INTERVAL 2 SECONDS", Some(11999L), Some(7999L)),
      ("eventTime - INTERVAL 2 SECONDS", Some(7999L), Some(11999L)),
      // Keys that the watermark does not bound.
      ("other + INTERVAL 2 SECONDS", None, None),
      ("date_trunc('MINUTE', eventTime)", None, None),
      // Truncation can map a later event time below the watermark.
      ("CAST(CAST(eventTime AS BIGINT) AS TIMESTAMP)", None, None),
      ("CAST(CAST(eventTime AS BIGINT) + 10 AS TIMESTAMP)", None, None))
    cases.foreach { case (keyStr, keyWatermark, eventTimeWatermark) =>
      withClue(keyStr) {
        val keys = Seq(key(keyStr))
        assert(StreamingJoinHelper.getStateKeyWatermark(
          keys, Seq(eventTime, other), 0, Some(10000L)) === keyWatermark)
        assert(StreamingJoinHelper.getEventTimeWatermarkFromKey(
          keys, Seq(eventTime, other), 0, 10000L) === eventTimeWatermark)
        assert(StreamingJoinHelper.getStateKeyWatermarkOrdinal(
          keys, Seq(eventTime, other)) === keyWatermark.map(_ => 0))
      }
    }
  }

  test("equality key watermark with a day-sized shift needs a fixed-offset time zone") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val eventTime = AttributeReference("eventTime", TimestampType, metadata = metadata)()

    case class DummyLeafNode() extends LeafNode {
      override def output: Seq[Attribute] = Seq(eventTime)
      override def computeStats(): Statistics = Statistics(sizeInBytes = BigInt(0))
    }

    def resolve(keyStr: String): Expression = {
      val plan = Filter(EqualTo(CatalystSqlParser.parseExpression(keyStr), Literal(null)),
        DummyLeafNode())
      SimpleAnalyzer.execute(plan).asInstanceOf[Filter].condition.asInstanceOf[EqualTo].left
    }
    def watermark(key: Expression): Option[Long] =
      StreamingJoinHelper.getStateKeyWatermark(Seq(key), Seq(eventTime), 0, Some(10000L))

    // Spark adds whole days in the session time zone, where a day can be 23 or 25 hours.
    val day = 86400000L
    Seq(
      ("eventTime + INTERVAL 1 DAY", Some(10000L + day - 1)),
      ("eventTime - INTERVAL 36 HOURS", Some(10000L - day * 3 / 2 - 1))).foreach {
      case (keyStr, utcWatermark) =>
        Seq("UTC" -> utcWatermark, "America/Los_Angeles" -> None).foreach {
          case (zone, expected) =>
            withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
              withClue(s"$keyStr $zone") {
                assert(watermark(resolve(keyStr)) === expected)
              }
            }
        }
    }

    // A calendar interval counts its days the same way; its microseconds are always exact.
    Seq(
      (new CalendarInterval(0, 1, 0), "UTC", Some(10000L + day - 1)),
      (new CalendarInterval(0, 1, 0), "America/Los_Angeles", None),
      (new CalendarInterval(0, 0, 2000000L), "America/Los_Angeles", Some(11999L))).foreach {
      case (interval, zone, expected) =>
        withClue(s"$interval $zone") {
          assert(watermark(
            TimestampAddInterval(eventTime, Literal(interval), Some(zone))) === expected)
        }
    }

    // The smallest day-time interval is many days long, although its absolute value overflows.
    assert(watermark(TimestampAddInterval(eventTime,
      Literal(Long.MinValue, DayTimeIntervalType()), Some("America/Los_Angeles"))) === None)
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
