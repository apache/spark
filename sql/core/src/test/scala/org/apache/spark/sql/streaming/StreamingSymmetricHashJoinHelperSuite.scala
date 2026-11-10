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

package org.apache.spark.sql.streaming

import java.time.Duration

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal, TimestampAddInterval}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinConditionSplitPredicates, JoinStateKeyWatermarkPredicate, JoinStateValueWatermarkPredicate, JoinStateWatermarkPredicate}
import org.apache.spark.sql.types._

class StreamingSymmetricHashJoinHelperSuite extends StreamTest {

  private object WatermarkColumn extends Enumeration {
    val NoWatermark, JoinKey, Other = Value
  }

  val leftAttributeA = AttributeReference("a", IntegerType)()
  val leftAttributeB = AttributeReference("b", IntegerType)()
  val rightAttributeC = AttributeReference("c", IntegerType)()
  val rightAttributeD = AttributeReference("d", IntegerType)()

  val left = new LocalTableScanExec(Seq(leftAttributeA, leftAttributeB), Seq(), None)
  val right = new LocalTableScanExec(Seq(rightAttributeC, rightAttributeD), Seq(), None)

  test("empty") {
    val split = JoinConditionSplitPredicates(None, left, right)
    assert(split.leftSideOnly.isEmpty)
    assert(split.rightSideOnly.isEmpty)
    assert(split.bothSides.isEmpty)
    assert(split.full.isEmpty)
  }

  test("only literals") {
    // Literal-only conjuncts end up on the left side because that's the first bucket they fit in.
    // There's no semantic reason they couldn't be in any bucket.
    val predicate = Literal(1) < Literal(5) && Literal(6) < Literal(7) && Literal(0) === Literal(-1)
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(predicate))
    assert(split.rightSideOnly.contains(predicate))
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only left") {
    val predicate =
      leftAttributeA > Literal(1) && leftAttributeB > Literal(5) && leftAttributeA < leftAttributeB
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(predicate))
    assert(split.rightSideOnly.isEmpty)
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only right") {
    val predicate = rightAttributeC > Literal(1) && rightAttributeD > Literal(5) &&
      rightAttributeD < rightAttributeC
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.isEmpty)
    assert(split.rightSideOnly.contains(predicate))
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("mixed conjuncts") {
    val predicate =
      (leftAttributeA > leftAttributeB
        && rightAttributeC > rightAttributeD
        && leftAttributeA === rightAttributeC
        && Literal(1) === Literal(1))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      leftAttributeA > leftAttributeB && Literal(1) === Literal(1)))
    assert(split.rightSideOnly.contains(
      rightAttributeC > rightAttributeD && Literal(1) === Literal(1)))
    assert(split.bothSides.contains((leftAttributeA === rightAttributeC)))
    assert(split.full.contains(predicate))
  }

  test("conjuncts after nondeterministic") {
    val predicate =
      (rand(9) > Literal(0)
        && leftAttributeA > leftAttributeB
        && rightAttributeC > rightAttributeD
        && leftAttributeA === rightAttributeC
        && Literal(1) === Literal(1))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      leftAttributeA > leftAttributeB && Literal(1) === Literal(1)))
    assert(split.rightSideOnly.contains(
      rightAttributeC > rightAttributeD && Literal(1) === Literal(1)))
    assert(split.bothSides.contains(
      leftAttributeA === rightAttributeC && rand(9).expr > Literal(0)))
    assert(split.full.contains(predicate))
  }


  test("conjuncts before nondeterministic") {
    val randAttribute = rand(0)
    val predicate =
      (leftAttributeA > leftAttributeB
        && rightAttributeC > rightAttributeD
        && leftAttributeA === rightAttributeC
        && Literal(1) === Literal(1)
        && randAttribute > Literal(0))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      leftAttributeA > leftAttributeB && Literal(1) === Literal(1)))
    assert(split.rightSideOnly.contains(
      rightAttributeC > rightAttributeD && Literal(1) === Literal(1)))
    assert(split.bothSides.contains(
      leftAttributeA === rightAttributeC && randAttribute > Literal(0)))
    assert(split.full.contains(predicate))
  }

  for (watermarkColumn <- WatermarkColumn.values) {
    test(s"SPARK-60378: previous key scan bound with watermark on $watermarkColumn") {
      val watermark = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000L).build()
      val key = AttributeReference("key", TimestampType,
        metadata = if (watermarkColumn == WatermarkColumn.JoinKey) watermark else Metadata.empty)()
      val other = AttributeReference("other", TimestampType,
        metadata = if (watermarkColumn == WatermarkColumn.Other) watermark else Metadata.empty)()
      val rightKey = AttributeReference("rightKey", TimestampType, metadata = watermark)()
      val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
        Seq(key, other), Seq(rightKey), Seq(key), Seq(rightKey), None,
        Some(20000L), Some(10000L), false)
      val expectedPrevious =
        if (watermarkColumn == WatermarkColumn.JoinKey) Some(10000L) else None
      assert(predicates.left.exists {
        case JoinStateKeyWatermarkPredicate(_, 20000L, previous) => previous == expectedPrevious
        case _ => false
      })
    }
  }
  for (offset <- Seq(10, -10)) {
    test(s"previous key scan bound for a key shifted by $offset seconds") {
      val watermark = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000L).build()
      val lt = AttributeReference("lt", TimestampType, metadata = watermark)()
      val rt = AttributeReference("rt", TimestampType, metadata = watermark)()
      val rightKey = TimestampAddInterval(rt, Literal(Duration.ofSeconds(offset)), Some("UTC"))
      def leftPredicate(lateWatermark: Option[Long]): Option[JoinStateWatermarkPredicate] =
        StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
          Seq(lt), Seq(rt), Seq(lt), Seq(rightKey), None, Some(140000L), lateWatermark, false).left
      val shift = offset * 1000L
      // New keys at or below the late watermark fail late filtering, and the previous batch
      // evicted keys up to the shifted cutoff at that watermark.
      assert(leftPredicate(Some(90000L)).contains(JoinStateKeyWatermarkPredicate(
        leftPredicate(None).get.expr, 140000L + shift - 1,
        Some(math.min(90000L + shift - 1, 90000L)))))
      // IncrementalExecution passes no late watermark on the first batch and in legacy mode.
      assert(leftPredicate(None).exists {
        case JoinStateKeyWatermarkPredicate(_, stateWatermark, None) =>
          stateWatermark == 140000L + shift - 1
        case _ => false
      })
    }
  }

  for (boundEventTime <- Seq(false, true)) {
    test(s"range eviction keeps the bounded column, event-time bound=$boundEventTime") {
      val watermark = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000L).build()
      val start = AttributeReference("start", TimestampType, metadata = watermark)()
      val expiry = AttributeReference("expiry", TimestampType)()
      val time = AttributeReference("time", TimestampType, metadata = watermark)()
      val range = time >= start && time <= expiry
      val condition = Some(if (boundEventTime) range && time <= start else range)
      val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
        Seq(start, expiry), Seq(time), Seq(leftAttributeA), Seq(rightAttributeC), condition,
        Some(340000L), Some(100000L), false)
      val expectedAttribute = if (boundEventTime) start else expiry
      val predicate = predicates.left.get.asInstanceOf[JoinStateValueWatermarkPredicate]
      assert(predicate.expr.references.toSeq == Seq(expectedAttribute))
      assert(predicate.stateWatermark == 339999L)
      assert(predicate.prevStateWatermark ==
        (if (boundEventTime) Some(99999L) else None))
      val (outputBound, _) = StreamingSymmetricHashJoinHelper.getStateWatermark(
        Seq(start, expiry), Seq(time), Seq(leftAttributeA), Seq(rightAttributeC), condition,
        Some(340000L), false)
      assert(outputBound.contains(if (boundEventTime) 339999L else 0L))
    }
  }
  test("equality eviction uses the opposite input's watermark") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val lt = AttributeReference("lt", TimestampType)()
    val rt = AttributeReference("rt", TimestampType, metadata = metadata)()
    val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
      Seq(lt), Seq(rt), Seq(lt), Seq(rt), None, Some(20000), Some(10000), false)
    assert(predicates.right.isEmpty)
    assert(predicates.left.exists {
      case JoinStateKeyWatermarkPredicate(_, 20000, None) => true
      case _ => false
    })
    assert(StreamingSymmetricHashJoinHelper.getStateWatermark(
      Seq(lt), Seq(rt), Seq(lt), Seq(rt), None, Some(20000), false) === (None, Some(0L)))

    val watermarkedLeft = lt.withMetadata(metadata)
    val both = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
      Seq(watermarkedLeft), Seq(rt), Seq(watermarkedLeft), Seq(rt), None,
      Some(20000), Some(10000), false)
    assert(both.left.exists {
      case JoinStateKeyWatermarkPredicate(_, 20000, Some(10000)) => true
      case _ => false
    })
    assert(both.right.isDefined)
  }

  test("a bound on another timestamp does not advance the output watermark") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val lt = AttributeReference("lt", TimestampType)()
    val other = AttributeReference("other", TimestampType, metadata = metadata)()
    val rt = AttributeReference("rt", TimestampType, metadata = metadata)()
    val condition = Some(lt > rt - 5)
    val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
      Seq(lt, other), Seq(rt), Nil, Nil, condition, Some(20000), Some(10000), false)
    assert(predicates.left.exists {
      case JoinStateValueWatermarkPredicate(expr, 15000, None) => expr.references.contains(lt)
      case _ => false
    })
    assert(StreamingSymmetricHashJoinHelper.getStateWatermark(
      Seq(lt, other), Seq(rt), Nil, Nil, condition, Some(20000), false) ===
      (Some(0L), Some(0L)))
  }

  test("V4 lookups use the own event-time key when no opposite key bounds the state") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val lk = AttributeReference("lk", IntegerType)()
    val lt = AttributeReference("lt", TimestampType, metadata = metadata)()
    val expiry = AttributeReference("expiry", TimestampType)()
    val rk = AttributeReference("rk", IntegerType)()
    val rt = AttributeReference("rt", TimestampType)()
    val left = Seq(lk, lt, expiry)
    val leftKeys = Seq(lk, lt)
    val rightKeys = Seq(rk, rt)
    def lookupOrdinals(
        right: Seq[Attribute],
        condition: Option[Expression]): (Option[Int], Option[Int]) = {
      val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
        left, right, leftKeys, rightKeys, condition, Some(20000), Some(10000), false)
      (StreamingSymmetricHashJoinHelper.getStateKeyOrdinalForLookup(
        left, leftKeys, right, rightKeys, predicates.left),
        StreamingSymmetricHashJoinHelper.getStateKeyOrdinalForLookup(
          right, rightKeys, left, leftKeys, predicates.right))
    }

    // Only lt has a watermark. The right state is evicted on rt by key, and the left state is
    // not evicted but still stores and looks up its rows at lt.
    assert(lookupOrdinals(Seq(rk, rt), None) === (Some(1), Some(1)))
    // A left state evicted on expiry indexes expiry, so lt cannot serve its lookups.
    val rw = AttributeReference("rw", TimestampType, metadata = metadata)()
    assert(lookupOrdinals(Seq(rk, rt, rw), Some(rw <= expiry)) === (None, Some(1)))
  }

  test("range timestamp selection stays stable as the watermark advances") {
    val first = AttributeReference("first", TimestampType)()
    val second = AttributeReference("second", TimestampType)()
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val right = AttributeReference("right", TimestampType, metadata = metadata)()
    val condition = Some(first > right - 0.0001 && second > right + 0.0001)
    Seq(0L, 1L, 10000L).foreach { watermark =>
      val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
        Seq(first, second), Seq(right), Nil, Nil, condition, Some(watermark), None, false)
      assert(predicates.left.get.expr.references.toSeq == Seq(first))
    }
  }

  test("range eviction prefers event time in either conjunct order") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val eventTime = AttributeReference("eventTime", TimestampType, metadata = metadata)()
    val other = AttributeReference("other", TimestampType)()
    val right = AttributeReference("right", TimestampType, metadata = metadata)()
    Seq(other > right && eventTime > right,
      eventTime > right && other > right).foreach { condition =>
      val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
        Seq(other, eventTime), Seq(right), Nil, Nil, Some(condition), Some(20000L), None, false)
      assert(predicates.left.exists {
        case JoinStateValueWatermarkPredicate(expr, 20000L, _) =>
          expr.references.toSeq == Seq(eventTime)
        case _ => false
      })
    }
  }

  test("crossed ranges bound a different timestamp on each side") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val leftTime = AttributeReference("leftTime", TimestampType, metadata = metadata)()
    val leftOther = AttributeReference("leftOther", TimestampType)()
    val rightTime = AttributeReference("rightTime", TimestampType, metadata = metadata)()
    val rightOther = AttributeReference("rightOther", TimestampType)()
    val leftInterval = leftOther > rightTime - 5 && leftOther < rightTime + 10
    val rightInterval = rightOther > leftTime - 7 && rightOther < leftTime + 12
    Seq(leftInterval && rightInterval, rightInterval && leftInterval).foreach { condition =>
      val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
        Seq(leftTime, leftOther), Seq(rightTime, rightOther), Nil, Nil, Some(condition),
        Some(20000L), None, false)
      assert(predicates.left.exists {
        case JoinStateValueWatermarkPredicate(expr, 15000L, _) =>
          expr.references.toSeq == Seq(leftOther)
        case _ => false
      })
      assert(predicates.right.exists {
        case JoinStateValueWatermarkPredicate(expr, 13000L, _) =>
          expr.references.toSeq == Seq(rightOther)
        case _ => false
      })
    }
  }

  test("range eviction uses the event time selected for late input filtering") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val left = AttributeReference("left", TimestampType, metadata = metadata)()
    val first = AttributeReference("first", TimestampType, metadata = metadata)()
    val second = AttributeReference("second", TimestampType, metadata = metadata)()
    val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
      Seq(left), Seq(first, second), Nil, Nil, Some(left > second), Some(20000L),
      Some(10000L), useFirstEventTimeColumn = true)
    // Only first is filtered for lateness; an old second can arrive in a future row.
    assert(predicates.left.isEmpty)
  }
}
