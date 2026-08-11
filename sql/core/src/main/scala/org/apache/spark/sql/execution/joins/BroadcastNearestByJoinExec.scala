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
 * Mutable so that a fixed pool of entries can be reused across all left rows in a
 * partition: the pool is sized to min(k, rightRows.length) and allocated once per
 * partition, bounding total HeapEntry allocations to min(k, rightRows.length) per task
 * regardless of the number of left rows.
 */
private[joins] class HeapEntry(var index: Int, var rankingValue: Any) {
  HeapEntry.allocationCount += 1
}

private[joins] object HeapEntry {
  /**
   * Test-only allocation counter. Incremented by the HeapEntry constructor.
   * Used to verify the per-partition pooling invariant: total allocations should be
   * bounded by min(k, rightRows.length) per partition, NOT by leftRows * k.
   */
  @volatile var allocationCount: Long = 0L

  def resetAllocationCount(): Unit = { allocationCount = 0L }
}

/**
 * Physical operator for NearestByJoin that avoids materializing the full cross product.
 * For each left row, iterates all broadcast right rows maintaining a bounded priority
 * queue of size k, then emits the top-k matches directly.
 *
 * The right side is fully broadcast unconditionally when
 * `spark.sql.join.nearestBy.broadcast.enabled` is on -- except when the ranking
 * expression contains a scalar Python UDF referencing both children, in which case
 * [[org.apache.spark.sql.catalyst.optimizer.RewriteNearestByJoin]] falls back to the
 * aggregate rewrite for correctness (ExtractPythonUDFs cannot split a two-sided UDF
 * across a binary operator).
 * For nodes that reach this operator, there is no size test and no fallback.
 * A right side too large to broadcast will fail the query. Tie-breaking among equal
 * ranking values is non-deterministic (matches the rewrite).
 *
 * Because no `Join` node is built on the operator path, `CheckCartesianProducts` does not
 * apply and `spark.sql.crossJoin.enabled = false` does not reject NEAREST BY queries.
 * This is intentional: the operator produces at most k rows per left row (bounded), not a
 * true cross product.
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

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Ranking", rankingExpression.sql)}
       |${ExplainUtils.generateFieldString("NumResults", numResults.toString)}
       |${ExplainUtils.generateFieldString("Direction", direction.toString)}
       |${ExplainUtils.generateFieldString("Join type", joinType.toString)}
       |""".stripMargin
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

  // Matches BroadcastNestedLoopJoinExec: for BuildRight with InnerLike or LeftOuter,
  // the streamed (left) side's ordering is preserved because we iterate left rows in
  // order and emit each row's matches contiguously.
  override def outputOrdering: Seq[SortOrder] = left.outputOrdering

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

    left.execute().mapPartitionsWithIndexInternal { (index, leftIter) =>
      val rightRows = broadcastedRight.value
      if (rightRows.isEmpty && localJoinType != LeftOuter) {
        Iterator.empty
      } else {
        val joinedRow = new JoinedRow
        val rankingProj = UnsafeProjection.create(
          Seq(rankExpr), leftOutput ++ rightOutput)
        rankingProj.initialize(index)
        val resultProj = UnsafeProjection.create(allOutput, allOutput)
        val rankingNeedsCopy = !UnsafeRow.isFixedLength(rankExpr.dataType)

        // Pre-allocate the all-null right row for LEFT OUTER unmatched left rows.
        // Hoisted here so it is built once per partition rather than once per left row.
        val nullRight: InternalRow = new GenericInternalRow(rightOutput.size)

        // Pool of HeapEntry objects allocated ONCE per partition and reused across all
        // left rows. The pool size is min(k, rightRows.length) -- the maximum number of
        // entries that can simultaneously live in the heap. This bounds total HeapEntry
        // allocations to min(k, rightRows.length) per partition (per task), regardless
        // of the number of left rows processed.
        val poolSize = math.min(k, rightRows.length)
        val entryPool = new Array[HeapEntry](poolSize)
        var pi = 0
        while (pi < poolSize) {
          entryPool(pi) = new HeapEntry(0, null)
          pi += 1
        }

        // Hoist heap outside flatMap -- created once per partition, cleared per left row.
        val heapCapacity = poolSize + 1
        val heap = if (isDistance) {
          new JPriorityQueue[HeapEntry](heapCapacity,
            new Comparator[HeapEntry] {
              override def compare(a: HeapEntry, b: HeapEntry): Int =
                ordering.compare(b.rankingValue, a.rankingValue)
            })
        } else {
          new JPriorityQueue[HeapEntry](heapCapacity,
            new Comparator[HeapEntry] {
              override def compare(a: HeapEntry, b: HeapEntry): Int =
                ordering.compare(a.rankingValue, b.rankingValue)
            })
        }

        // Index into entryPool for the next unused pooled entry during heap filling.
        var nextPoolIdx = 0

        leftIter.flatMap { leftRow =>
          streamedRowsMetric += 1
          heap.clear()
          nextPoolIdx = 0

          var i = 0
          while (i < rightRows.length) {
            val rightRow = rightRows(i)
            joinedRow(leftRow, rightRow)
            val rankingRow = rankingProj(joinedRow)
            if (!rankingRow.isNullAt(0)) {
              val rawValue = rankingRow.get(0, rankExpr.dataType)
              // Only insert if the heap has room or the candidate beats the current worst.
              // This avoids unnecessary .copy() allocations for rows that would be
              // immediately evicted, and reduces PriorityQueue churn.
              // For distance (isDistance=true): smaller is better, worst=largest on peek.
              // For similarity: larger is better, worst=smallest on peek.
              val shouldRetain = heap.size() < k || (if (isDistance) {
                ordering.compare(rawValue, heap.peek().rankingValue) < 0
              } else {
                ordering.compare(rawValue, heap.peek().rankingValue) > 0
              })
              if (shouldRetain) {
                val rankingValue = if (rankingNeedsCopy) {
                  // Deep-copy variable-width values (UTF8String, BinaryView, structs,
                  // arrays, maps) without allocating the enclosing one-column UnsafeRow.
                  InternalRow.copyValue(rawValue)
                } else {
                  rawValue
                }
                if (heap.size() < k) {
                  // Heap has room -- use the next pooled entry.
                  val entry = entryPool(nextPoolIdx)
                  nextPoolIdx += 1
                  entry.index = i
                  entry.rankingValue = rankingValue
                  heap.offer(entry)
                } else {
                  // Heap is at capacity -- reuse the evicted (worst) entry.
                  val evicted = heap.poll()
                  evicted.index = i
                  evicted.rankingValue = rankingValue
                  heap.offer(evicted)
                }
              }
            }
            i += 1
          }

          // CORRECTNESS: results must be fully materialized (output rows copied) BEFORE
          // returning from this flatMap closure, because the pooled HeapEntry objects
          // will be reused for the next left row. The resultProj(joinedRow).copy() below
          // produces a standalone UnsafeRow that does not alias the pool or the heap.
          // The Array[InternalRow] is eagerly built and its iterator consumed lazily,
          // but since flatMap fully drains each inner iterator before advancing leftIter,
          // the pool is not touched until all output rows for this left row are consumed.
          if (heap.isEmpty && localJoinType == LeftOuter) {
            joinedRow(leftRow, nullRight)
            numOutput += 1
            Iterator.single(resultProj(joinedRow).copy())
          } else {
            val results = new Array[InternalRow](heap.size())
            var idx = heap.size() - 1
            while (!heap.isEmpty) {
              val entry = heap.poll()
              joinedRow(leftRow, rightRows(entry.index))
              numOutput += 1
              results(idx) = resultProj(joinedRow).copy()
              idx -= 1
            }
            results.iterator
          }
        }
      }
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): BroadcastNearestByJoinExec =
    copy(left = newLeft, right = newRight)
}
