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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinConditionSplitPredicates, JoinSide, JoinStateKeyWatermarkPredicate, LeftSide, RightSide}
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

  test("value predicate tests the column that the condition bounds, except in the legacy mode") {
    val watermark = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 0L).build()
    val leftTime = AttributeReference("leftTime", TimestampType, metadata = watermark)()
    val expiry = AttributeReference("expiry", TimestampType)()
    val rightTime = AttributeReference("rightTime", TimestampType, metadata = watermark)()
    val condition = rightTime >= leftTime && rightTime <= expiry
    // The legacy mode with a single global watermark applies the bound to the event time.
    Seq(false -> expiry, true -> leftTime).foreach { case (legacyMode, column) =>
      val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
        Seq(leftTime, expiry), Seq(rightTime), Nil, Nil, Some(condition), Some(100000L), None,
        useFirstEventTimeColumn = legacyMode)
      assert(predicates.left.get.expr.references === AttributeSet(column))
    }
  }

  test("equality key that evicts each side is the opposite watermarked key") {
    val watermark = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 0L).build()
    val la = AttributeReference("la", TimestampType, metadata = watermark)()
    val lb = AttributeReference("lb", TimestampType)()
    val ra = AttributeReference("ra", TimestampType)()
    val rb = AttributeReference("rb", TimestampType, metadata = watermark)()
    def ordinal(side: JoinSide, leftKeys: Seq[Expression], rightKeys: Seq[Expression],
        legacyMode: Boolean = false): Option[Int] =
      StreamingSymmetricHashJoinHelper.findStateKeyOrdinalForWatermark(
        side, leftKeys, rightKeys, useFirstEventTimeColumn = legacyMode)
    assert(ordinal(LeftSide, Seq(la, lb), Seq(ra, rb)) === Some(1))
    assert(ordinal(RightSide, Seq(la, lb), Seq(ra, rb)) === Some(0))
    // Without an opposite watermarked key, and in the legacy mode, both sides use the first
    // watermarked key of the left input, or else of the right input.
    assert(ordinal(LeftSide, Seq(la, lb), Seq(ra, ra)) === Some(0))
    assert(ordinal(RightSide, Seq(lb, lb), Seq(ra, rb)) === Some(1))
    Seq(LeftSide, RightSide).foreach { side =>
      assert(ordinal(side, Seq(la, lb), Seq(ra, rb), legacyMode = true) === Some(0))
    }
  }

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
}
