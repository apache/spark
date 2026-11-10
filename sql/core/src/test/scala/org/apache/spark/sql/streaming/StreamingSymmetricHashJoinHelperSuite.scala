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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinConditionSplitPredicates, JoinStateKeyWatermarkPredicate, JoinStateValueWatermarkPredicate}
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

}
