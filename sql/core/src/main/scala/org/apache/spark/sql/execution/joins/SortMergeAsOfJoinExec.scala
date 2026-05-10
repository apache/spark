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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * Performs an AS-OF join using sort-merge. Both sides are co-partitioned
 * by the equi-join keys and sorted by (equi-join keys, as-of key).
 * For each left row, we scan the right side to find the nearest match
 * satisfying the as-of condition.
 *
 * Note: When there are no equi-keys, both sides are collected into a
 * single partition (AllTuples). The right side is fully buffered in
 * memory, so this operator is not suitable for large right-side tables
 * without equi-keys.
 */
case class SortMergeAsOfJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    leftAsOfExpr: Expression,
    rightAsOfExpr: Expression,
    asOfCondition: Expression,
    orderExpression: Expression,
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows"))

  override def output: Seq[Attribute] = joinType match {
    case LeftOuter =>
      left.output ++ right.output.map(_.withNullability(true))
    case _ =>
      left.output ++ right.output
  }

  override def outputOrdering: Seq[SortOrder] = {
    // Output preserves left-side ordering (equi-keys + as-of key)
    left.outputOrdering
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    if (leftKeys.isEmpty) {
      AllTuples :: AllTuples :: Nil
    } else {
      ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    val leftOrdering = leftKeys.map(SortOrder(_, Ascending)) :+
      SortOrder(leftAsOfExpr, Ascending)
    val rightOrdering = rightKeys.map(SortOrder(_, Ascending)) :+
      SortOrder(rightAsOfExpr, Ascending)
    leftOrdering :: rightOrdering :: Nil
  }

  override def outputPartitioning: Partitioning = left.outputPartitioning

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      val scanner = new SortMergeAsOfJoinScanner(
        leftIter,
        rightIter,
        left.output,
        right.output,
        leftKeys,
        rightKeys,
        asOfCondition,
        orderExpression,
        joinType,
        condition,
        numOutputRows
      )
      // Register cleanup to release the right-side buffer on task completion
      TaskContext.get().addTaskCompletionListener[Unit](_ => scanner.close())
      scanner.iterator
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SortMergeAsOfJoinExec = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * Performs the sort-merge AS-OF join scan.
 *
 * Both inputs are sorted by (equi-keys, as-of key) ascending. For each
 * left row within an equi-key group, we find the right row that satisfies
 * the as-of condition and minimizes the order expression (distance).
 *
 * Since the right side is sorted by as-of key within each group, for
 * backward joins we scan right-to-left and stop at the first match
 * (exploiting sort order for early termination).
 */
private[joins] class SortMergeAsOfJoinScanner(
    leftIter: Iterator[InternalRow],
    rightIter: Iterator[InternalRow],
    leftOutput: Seq[Attribute],
    rightOutput: Seq[Attribute],
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    asOfCondition: Expression,
    orderExpression: Expression,
    joinType: JoinType,
    residualCondition: Option[Expression],
    numOutputRows: SQLMetric) {

  private val joinedOutput = leftOutput ++ rightOutput
  private val joinedRow = new JoinedRow()
  private val resultProjection =
    UnsafeProjection.create(joinedOutput, joinedOutput)

  // Bound expressions for evaluating conditions on joined rows
  private val boundAsOfCond = bindReference(asOfCondition, joinedOutput)
  private val boundOrderExpr = bindReference(orderExpression, joinedOutput)
  private val boundResidualCond =
    residualCondition.map(bindReference(_, joinedOutput))

  // Key ordering for equi-join keys
  private val equiKeyOrdering: Option[BaseOrdering] =
    if (leftKeys.nonEmpty) {
      val keyAttributes = leftKeys.zipWithIndex.map { case (key, i) =>
        AttributeReference(s"key_$i", key.dataType, key.nullable)()
      }
      Some(GenerateOrdering.generate(
        keyAttributes.map(SortOrder(_, Ascending)), keyAttributes))
    } else {
      None
    }

  // Projections to extract equi-keys for comparison
  private val leftKeyProj = UnsafeProjection.create(leftKeys, leftOutput)
  private val rightKeyProj = UnsafeProjection.create(rightKeys, rightOutput)

  // Ordering for the distance metric
  private val distanceOrdering =
    TypeUtils.getInterpretedOrdering(orderExpression.dataType)

  // Null row for LeftOuter when no match is found
  private val nullRightRow = new GenericInternalRow(rightOutput.length)

  // Right-side buffer: holds right rows for the current equi-key group.
  // Rows are sorted by as-of key ascending (guaranteed by requiredChildOrdering).
  private val rightGroupBuffer = new ArrayBuffer[InternalRow]()
  private var rightGroupKey: UnsafeRow = _
  private var rightPeek: InternalRow = _
  private var rightDone: Boolean = !rightIter.hasNext

  // Initialize: read first right row
  if (!rightDone) {
    rightPeek = rightIter.next().copy()
  }

  /** Release resources held by this scanner. */
  def close(): Unit = {
    rightGroupBuffer.clear()
    rightGroupBuffer.trimToSize()
  }

  def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {
    private var nextRow: InternalRow = _
    private val leftIterBuffered = leftIter.buffered

    override def hasNext: Boolean = {
      if (nextRow != null) return true
      nextRow = findNext()
      nextRow != null
    }

    override def next(): InternalRow = {
      if (!hasNext) throw new NoSuchElementException
      val result = nextRow
      nextRow = null
      result
    }

    private def findNext(): InternalRow = {
      while (leftIterBuffered.hasNext) {
        val leftRow = leftIterBuffered.next()
        val leftKey = leftKeyProj(leftRow).copy()

        // Advance right side to the matching equi-key group
        advanceRightTo(leftKey)

        // Search for best match exploiting sort order
        val bestMatch = findBestInGroup(leftRow)

        if (bestMatch != null) {
          numOutputRows += 1
          joinedRow.withLeft(leftRow).withRight(bestMatch)
          return resultProjection(joinedRow).copy()
        } else if (joinType == LeftOuter) {
          numOutputRows += 1
          joinedRow.withLeft(leftRow).withRight(nullRightRow)
          return resultProjection(joinedRow).copy()
        }
        // Inner join: no match, skip
      }
      null
    }
  }

  /**
   * Advance the right side so that rightGroupBuffer contains all right
   * rows whose equi-key matches `leftKey`.
   */
  private def advanceRightTo(leftKey: UnsafeRow): Unit = {
    equiKeyOrdering match {
      case None =>
        // No equi-keys: buffer all right rows once.
        // WARNING: This loads the entire right partition into memory.
        if (rightGroupBuffer.isEmpty && !rightDone) {
          bufferAllRight()
        }
      case Some(ordering) =>
        // Check if current buffer already matches
        if (rightGroupKey != null &&
            ordering.compare(leftKey, rightGroupKey) == 0) {
          return
        }

        // Skip right rows with keys < leftKey
        while (!rightDone && rightPeek != null) {
          val rightKey = rightKeyProj(rightPeek)
          val cmp = ordering.compare(leftKey, rightKey)
          if (cmp > 0) {
            rightPeek = if (rightIter.hasNext) {
              rightIter.next().copy()
            } else {
              rightDone = true; null
            }
          } else if (cmp == 0) {
            bufferRightGroup(leftKey, ordering)
            return
          } else {
            rightGroupBuffer.clear()
            rightGroupKey = null
            return
          }
        }
        rightGroupBuffer.clear()
        rightGroupKey = null
    }
  }

  /** Buffer all right rows with the same equi-key as leftKey. */
  private def bufferRightGroup(
      leftKey: UnsafeRow, ordering: BaseOrdering): Unit = {
    rightGroupBuffer.clear()
    rightGroupKey = leftKey.copy()

    while (!rightDone && rightPeek != null) {
      val rightKey = rightKeyProj(rightPeek)
      if (ordering.compare(leftKey, rightKey) == 0) {
        rightGroupBuffer += rightPeek
        rightPeek = if (rightIter.hasNext) {
          rightIter.next().copy()
        } else {
          rightDone = true; null
        }
      } else {
        return
      }
    }
  }

  /** Buffer all remaining right rows (no equi-keys case). */
  private def bufferAllRight(): Unit = {
    rightGroupBuffer.clear()
    if (rightPeek != null) {
      rightGroupBuffer += rightPeek
      rightPeek = null
    }
    while (rightIter.hasNext) {
      rightGroupBuffer += rightIter.next().copy()
    }
    rightDone = true
  }

  /**
   * Find the best matching right row for the given left row within the
   * current group buffer.
   *
   * The buffer is sorted by as-of key ascending. We scan right-to-left
   * (from largest as-of key to smallest). For backward joins, the first
   * row satisfying the as-of condition is the best match (closest in
   * time), so we can stop early. For forward/nearest, we still need to
   * find the minimum distance but can stop once the distance starts
   * increasing (since the buffer is sorted).
   */
  private def findBestInGroup(leftRow: InternalRow): InternalRow = {
    var bestMatch: InternalRow = null
    var bestDistance: Any = null

    // Scan right-to-left to exploit sort order for early termination
    var i = rightGroupBuffer.size - 1
    while (i >= 0) {
      val rightRow = rightGroupBuffer(i)
      joinedRow.withLeft(leftRow).withRight(rightRow)

      // Check as-of condition
      val asOfSatisfied = boundAsOfCond.eval(joinedRow)
      if (asOfSatisfied != null && asOfSatisfied.asInstanceOf[Boolean]) {
        // Check residual condition (non-equi predicates only)
        val residualSatisfied = boundResidualCond.forall { cond =>
          val result = cond.eval(joinedRow)
          result != null && result.asInstanceOf[Boolean]
        }
        if (residualSatisfied) {
          val distance = boundOrderExpr.eval(joinedRow)
          if (distance != null) {
            if (bestMatch == null) {
              bestMatch = rightRow
              bestDistance = distance
            } else if (distanceOrdering.lt(distance, bestDistance)) {
              bestMatch = rightRow
              bestDistance = distance
            } else {
              // Distance is increasing (buffer is sorted), so we can
              // stop: no subsequent row will be closer.
              return bestMatch
            }
          }
        }
      } else if (bestMatch != null) {
        // As-of condition no longer satisfied and we already have a
        // match. Since buffer is sorted, earlier rows are further away,
        // so stop.
        return bestMatch
      }
      i -= 1
    }
    bestMatch
  }
}
