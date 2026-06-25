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

import java.util.{Comparator, PriorityQueue => JPriorityQueue}

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, LeftOuter, NearestByDirection, NearestByDistance}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.{ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Heap entry storing an index into the broadcast array alongside its ranking value.
 * Using a case class with primitive `Int` field avoids boxing that `(Int, Any)` tuples incur.
 */
private[joins] case class HeapEntry(index: Int, rankingValue: Any)

/**
 * Physical operator for NearestByJoin that avoids materializing the full cross product.
 * For each left row, iterates all broadcast right rows maintaining a bounded priority
 * queue of size k, then emits the top-k matches directly.
 *
 * The right side is fully broadcast to all partitions. This operator only fires when
 * the right side fits within [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]. For right tables
 * exceeding this threshold, the existing cross-product + aggregate rewrite is used as
 * fallback. Tie-breaking among equal ranking values is non-deterministic (matches the
 * existing rewrite behavior).
 */
case class BroadcastNearestByJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    joinType: JoinType,
    numResults: Int,
    rankingExpression: Expression,
    direction: NearestByDirection) extends BaseJoinExec {

  override def condition: Option[Expression] = None
  override def leftKeys: Seq[Expression] = Seq.empty
  override def rightKeys: Seq[Expression] = Seq.empty

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType k=$numResults $direction ($opId)".trim
  }

  override def output: Seq[Attribute] = joinType match {
    case _: InnerLike | LeftOuter =>
      left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
    case other =>
      throw SparkException.internalError(
        s"$nodeName does not support join type: $other")
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "streamedRows" -> SQLMetrics.createMetric(sparkContext, "number of left rows processed"))

  override def requiredChildDistribution: Seq[Distribution] =
    UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val broadcastedRight = right.executeBroadcast[Array[InternalRow]]()
    val numOutput = longMetric("numOutputRows")
    val streamedRowsMetric = longMetric("streamedRows")
    val localJoinType = joinType
    val k = numResults
    val isDistance = direction == NearestByDistance
    val leftOutput = left.output
    val rightOutput = right.output
    val rankExpr = rankingExpression
    val allOutput = output
    val ordering = TypeUtils.getInterpretedOrdering(rankExpr.dataType)

    left.execute().mapPartitionsInternal { leftIter =>
      val rightRows = broadcastedRight.value
      if (rightRows.isEmpty && localJoinType != LeftOuter) {
        Iterator.empty
      } else {
        val joinedRow = new JoinedRow
        val rankingProj = UnsafeProjection.create(
          Seq(rankExpr), leftOutput ++ rightOutput)
        val resultProj = UnsafeProjection.create(allOutput, allOutput)
        val rankingNeedsCopy = !UnsafeRow.isFixedLength(rankExpr.dataType)

        // Hoist heap outside flatMap to reduce GC pressure
        val heap = if (isDistance) {
          new JPriorityQueue[HeapEntry](k + 1,
            new Comparator[HeapEntry] {
              override def compare(a: HeapEntry, b: HeapEntry): Int =
                ordering.compare(b.rankingValue, a.rankingValue)
            })
        } else {
          new JPriorityQueue[HeapEntry](k + 1,
            new Comparator[HeapEntry] {
              override def compare(a: HeapEntry, b: HeapEntry): Int =
                ordering.compare(a.rankingValue, b.rankingValue)
            })
        }

        leftIter.flatMap { leftRow =>
          streamedRowsMetric += 1
          heap.clear()

          var i = 0
          while (i < rightRows.length) {
            val rightRow = rightRows(i)
            joinedRow(leftRow, rightRow)
            val rankingRow = rankingProj(joinedRow)
            if (!rankingRow.isNullAt(0)) {
              val rawValue = rankingRow.get(0, rankExpr.dataType)
              val rankingValue = if (rankingNeedsCopy) {
                rankingRow.copy().get(0, rankExpr.dataType)
              } else {
                rawValue
              }
              heap.offer(HeapEntry(i, rankingValue))
              if (heap.size() > k) heap.poll()
            }
            i += 1
          }

          if (heap.isEmpty && localJoinType == LeftOuter) {
            val nullRight = new GenericInternalRow(rightOutput.size)
            joinedRow(leftRow, nullRight)
            numOutput += 1
            Iterator.single(resultProj(joinedRow).copy())
          } else {
            val results = new Array[HeapEntry](heap.size())
            var idx = heap.size() - 1
            while (!heap.isEmpty) {
              results(idx) = heap.poll()
              idx -= 1
            }
            results.iterator.map { entry =>
              joinedRow(leftRow, rightRows(entry.index))
              numOutput += 1
              resultProj(joinedRow).copy()
            }
          }
        }
      }
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): BroadcastNearestByJoinExec =
    copy(left = newLeft, right = newRight)
}
