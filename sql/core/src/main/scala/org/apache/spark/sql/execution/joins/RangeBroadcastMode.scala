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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.{IntervalOverlapJoin, PartialRangeJoin, PointInRangeJoin, RangeJoin}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.types.PhysicalDataType

/**
 * Which [[RangeRelation]] a broadcast materializes.
 *
 * A point index has no direction, so one broadcast of a column serves both
 * `<` and `>`. The join records which side holds the lower bound and scans
 * accordingly.
 */
private[execution] sealed abstract class RangeIndexKind {
  def matches(rangeJoin: RangeJoin): Boolean = RangeIndexKind(rangeJoin) == this
}

private[execution] case object IntervalIndexKind extends RangeIndexKind

private[execution] case object PointIndexKind extends RangeIndexKind

private[execution] object RangeIndexKind {
  def apply(rangeJoin: RangeJoin): RangeIndexKind = rangeJoin match {
    case PointInRangeJoin | IntervalOverlapJoin => IntervalIndexKind
    case _: PartialRangeJoin => PointIndexKind
  }
}

/**
 * A [[BroadcastMode]] that materializes the build side into a [[RangeRelation]].
 *
 * Symmetric with `HashedRelationBroadcastMode`. `buildKeys` are bound to the
 * build row, so the index does not depend on attribute ids. Which join input is
 * broadcast, and the original join condition, live on [[BroadcastRangeJoinExec]].
 * Under AQE, `LogicalQueryStageStrategy` checks that this mode's bound keys are
 * the keys `ExtractRangeJoinKeys` finds.
 *
 * Bound keys are stored with nullability forced on. AQE can rewrite a stage's
 * nullability, and [[org.apache.spark.sql.catalyst.plans.physical.BroadcastPartitioning]]
 * decides reuse with `==`. Forcing it here keeps that check aligned with the
 * strategy, which compares modes the same way. The index already treats a null
 * key as non-matching.
 */
private[execution] case class RangeBroadcastMode private(
    buildKeys: Seq[Expression],
    indexKind: RangeIndexKind)
  extends BroadcastMode with Serializable {

  override def transform(rows: Array[InternalRow]): RangeRelation =
    transform(rows.iterator, Some(rows.length))

  override def transform(
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): RangeRelation = {
    indexKind match {
      case IntervalIndexKind =>
        require(buildKeys.nonEmpty && buildKeys.length <= 2,
          "An interval index expects (low, high), or one point used as both ends.")
      case PointIndexKind =>
        require(buildKeys.length == 1, "A point index expects one build key.")
    }

    val ordering = PhysicalDataType.ordering(buildKeys.head.dataType)
    val projection = new InterpretedProjection(buildKeys)
    val lowOf = RangeIndex.getValue(buildKeys.head.dataType, 0)

    indexKind match {
      case IntervalIndexKind =>
        val highOf =
          if (buildKeys.length == 1) lowOf else RangeIndex.getValue(buildKeys(1).dataType, 1)
        val intervals = rows.map { r =>
          val p = projection(r)
          (lowOf(p), highOf(p), r)
        }.toArray
        IntervalIndex.build(ordering, intervals)

      case PointIndexKind =>
        val points = rows.map(r => (lowOf(projection(r)), r)).toArray
        PointIndex.build(ordering, points)
    }
  }

  override lazy val canonicalized: RangeBroadcastMode =
    RangeBroadcastMode(buildKeys.map(_.canonicalized), indexKind)
}

private[execution] object RangeBroadcastMode {

  def apply(buildKeys: Seq[Expression], indexKind: RangeIndexKind): RangeBroadcastMode = {
    val normalizedKeys = buildKeys.map { key =>
      key.transform {
        case b: BoundReference => b.copy(nullable = true)
      }
    }
    new RangeBroadcastMode(normalizedKeys, indexKind)
  }
}
