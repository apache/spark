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

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, GenerateOrdering}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * Performs an AS-OF join using sort-merge. Both sides are co-partitioned
 * by the equi-join keys and sorted by (equi-join keys, as-of key).
 * For each left row, we scan the right-side group buffer (forward-only)
 * to find the nearest match satisfying the as-of condition.
 *
 * The right-side buffer uses [[ExternalAppendOnlyUnsafeRowArray]] which
 * spills to disk when the in-memory threshold is exceeded, avoiding OOM
 * for skewed equi-key groups.
 *
 * Note: When there are no equi-keys, both sides are collected into a
 * single partition (AllTuples) and the entire right side is buffered.
 */
case class SortMergeAsOfJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    leftSortExprs: Seq[Expression],
    rightSortExprs: Seq[Expression],
    asOfCondition: Expression,
    orderExpression: Expression,
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false) extends ShuffledJoin with PredicateHelper {

  require(leftSortExprs.nonEmpty && rightSortExprs.nonEmpty &&
    leftSortExprs.length == rightSortExprs.length,
    s"$nodeName requires matching non-empty sort expressions on both sides")

  require(Seq(Inner, LeftOuter).exists(joinType == _),
    s"$nodeName does not support join type: $joinType")

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  override def supportCodegen: Boolean = false

  // Codegen stubs (not called since supportCodegen = false)
  override def inputRDDs(): Seq[RDD[InternalRow]] =
    left.execute() :: right.execute() :: Nil
  override protected def doProduce(ctx: CodegenContext): String =
    throw SparkException.internalError(s"$nodeName does not support codegen")

  override def requiredChildDistribution: Seq[Distribution] = {
    if (leftKeys.isEmpty) {
      AllTuples :: AllTuples :: Nil
    } else {
      super.requiredChildDistribution
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    val leftOrdering = leftKeys.map(SortOrder(_, Ascending)) ++
      leftSortExprs.map(SortOrder(_, Ascending))
    val rightOrdering = rightKeys.map(SortOrder(_, Ascending)) ++
      rightSortExprs.map(SortOrder(_, Ascending))
    leftOrdering :: rightOrdering :: Nil
  }

  override def outputOrdering: Seq[SortOrder] = left.outputOrdering

  // Detect from asOfCondition so composite MATCH_CONDITION sort keys still work. Only a first
  // `left op right` conjunct counts. Nearest has none: its condition is TRUE, NOT(l = r), or
  // starts with `right >= left - tolerance`.
  private val asOfDirection: AsOfJoinDirection =
    splitConjunctivePredicates(asOfCondition).head match {
      case c: BinaryComparison
          if c.left.references.subsetOf(left.outputSet) &&
            c.right.references.subsetOf(right.outputSet) =>
        c match {
          case _: GreaterThanOrEqual | _: GreaterThan => Backward
          case _: LessThanOrEqual | _: LessThan => Forward
          case _ => Nearest
        }
      case _ => Nearest
    }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val spillSize = longMetric("spillSize")
    val direction = asOfDirection
    val inMemoryThreshold = conf.sortMergeJoinExecBufferInMemoryThreshold
    val sizeInBytesSpillThreshold = conf.sortMergeJoinExecBufferSpillSizeThreshold
    val spillThreshold = conf.sortMergeJoinExecBufferSpillThreshold

    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      val scanner = new SortMergeAsOfJoinScanner(
        leftIter, rightIter,
        left.output, right.output,
        leftKeys, rightKeys,
        asOfCondition, orderExpression,
        joinType, condition,
        numOutputRows, spillSize, direction,
        inMemoryThreshold, sizeInBytesSpillThreshold, spillThreshold
      )
      TaskContext.get().addTaskCompletionListener[Unit](_ => scanner.close())
      scanner.iterator
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): SortMergeAsOfJoinExec = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * Performs the sort-merge AS-OF join scan using forward-only iteration.
 *
 * Both inputs are sorted by (equi-keys, as-of key) ascending. For each
 * left row within an equi-key group, we scan the right-side buffer
 * forward to find the best match:
 *  - Backward (left.t >= right.t): the last satisfying row is the closest.
 *  - Forward (left.t <= right.t): the first satisfying row is the closest.
 *  - Nearest: the satisfying row with the smallest distance.
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
    numOutputRows: SQLMetric,
    spillSize: SQLMetric,
    direction: AsOfJoinDirection,
    inMemoryThreshold: Int,
    sizeInBytesSpillThreshold: Long,
    spillThreshold: Int) {

  private val joinedOutput = leftOutput ++ rightOutput
  private val joinedRow = new JoinedRow()
  // Use nullable bound references so outer-join null padding is safe even when
  // right attributes are NOT NULL in the catalog schema.
  private val resultProjection = {
    val nullableRefs = joinedOutput.zipWithIndex.map { case (attr, i) =>
      BoundReference(i, attr.dataType, nullable = true)
    }
    UnsafeProjection.create(nullableRefs, joinedOutput)
  }

  private val boundAsOfCond = bindReference(asOfCondition, joinedOutput)
  // Lazy, since only Nearest reads the distance. Its type can have no ordering, for example
  // CalendarInterval from TIMESTAMP - TIMESTAMP with spark.sql.legacy.interval.enabled.
  private lazy val boundOrderExpr = bindReference(orderExpression, joinedOutput)
  private val boundResidualCond =
    residualCondition.map(bindReference(_, joinedOutput))

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

  private val leftKeyProj = UnsafeProjection.create(leftKeys, leftOutput)
  private val rightKeyProj = UnsafeProjection.create(rightKeys, rightOutput)

  // Lazy for the same reason as boundOrderExpr.
  private lazy val distanceOrdering =
    TypeUtils.getInterpretedOrdering(orderExpression.dataType)

  // Materialize an all-null right row as UnsafeRow. GenericInternalRow cannot be
  // passed through identity UnsafeProjection when right columns are NOT NULL.
  private val nullRightRow: InternalRow = {
    val nullableRefs = rightOutput.zipWithIndex.map { case (attr, i) =>
      BoundReference(i, attr.dataType, nullable = true)
    }
    val proj = UnsafeProjection.create(nullableRefs, rightOutput)
    proj(new GenericInternalRow(rightOutput.length))
  }

  // Spill-backed right-side buffer
  private val rightGroupBuffer = new ExternalAppendOnlyUnsafeRowArray(
    inMemoryThreshold, sizeInBytesSpillThreshold, spillThreshold, sizeInBytesSpillThreshold)

  private var rightGroupKey: UnsafeRow = _
  private var rightPeek: UnsafeRow = _
  private var rightDone: Boolean = !rightIter.hasNext

  // Projection to convert right rows to UnsafeRow for the buffer
  private val rightToUnsafe = UnsafeProjection.create(rightOutput, rightOutput)

  if (!rightDone) {
    rightPeek = rightToUnsafe(rightIter.next()).copy()
  }

  def close(): Unit = {
    spillSize += rightGroupBuffer.spillSize
    rightGroupBuffer.clear()
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

        // Skip left rows with null equi-keys (EqualTo semantics:
        // NULL = NULL -> NULL, i.e. no match)
        if (leftKeys.nonEmpty && leftKey.anyNull) {
          if (joinType == LeftOuter) {
            numOutputRows += 1
            joinedRow.withLeft(leftRow).withRight(nullRightRow)
            return resultProjection(joinedRow).copy()
          }
        } else {
          advanceRightTo(leftKey)

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
        }
      }
      null
    }
  }

  private def advanceRightTo(leftKey: UnsafeRow): Unit = {
    equiKeyOrdering match {
      case None =>
        if (rightGroupBuffer.isEmpty && !rightDone) {
          bufferAllRight()
        }
      case Some(ordering) =>
        if (rightGroupKey != null &&
            ordering.compare(leftKey, rightGroupKey) == 0) {
          return
        }

        while (!rightDone && rightPeek != null) {
          val rightKey = rightKeyProj(rightPeek)
          val cmp = ordering.compare(leftKey, rightKey)
          if (cmp > 0) {
            rightPeek = if (rightIter.hasNext) {
              rightToUnsafe(rightIter.next()).copy()
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

  private def bufferRightGroup(
      leftKey: UnsafeRow, ordering: BaseOrdering): Unit = {
    rightGroupBuffer.clear()
    rightGroupKey = leftKey.copy()

    while (!rightDone && rightPeek != null) {
      val rightKey = rightKeyProj(rightPeek)
      if (ordering.compare(leftKey, rightKey) == 0) {
        rightGroupBuffer.add(rightPeek)
        rightPeek = if (rightIter.hasNext) {
          rightToUnsafe(rightIter.next()).copy()
        } else {
          rightDone = true; null
        }
      } else {
        return
      }
    }
  }

  private def bufferAllRight(): Unit = {
    rightGroupBuffer.clear()
    if (rightPeek != null) {
      rightGroupBuffer.add(rightPeek)
      rightPeek = null
    }
    while (rightIter.hasNext) {
      rightGroupBuffer.add(rightToUnsafe(rightIter.next()))
    }
    rightDone = true
  }

  /** Finds the best right row in the current group for `leftRow`, or null if none passes. */
  private def findBestInGroup(leftRow: InternalRow): InternalRow = direction match {
    case Backward => findLastBackward(leftRow)
    case Forward => findFirstForward(leftRow)
    case Nearest => findNearest(leftRow)
  }

  /**
   * Retains `rightRow` as the best match found so far.
   *
   * While the right-side buffer is held in memory, its iterator yields the distinct rows stored
   * in the buffer and a match can be retained as is. Once the buffer is spill-backed the
   * iterator re-points a single [[UnsafeRow]] on every `next()`, so the match has to be copied
   * out before the scan advances. `needsCopy` is read once per scan by the callers.
   */
  private def retainMatch(rightRow: UnsafeRow, needsCopy: Boolean): UnsafeRow = {
    if (needsCopy) rightRow.copy() else rightRow
  }

  /** Whether `cond` is TRUE on the current joined row. FALSE and NULL both fail. */
  private def holds(cond: Expression): Boolean = {
    val result = cond.eval(joinedRow)
    result != null && result.asInstanceOf[Boolean]
  }

  /** Whether the residual ON condition, if any, holds on the current joined row. */
  private def residualHolds: Boolean = boundResidualCond.forall(holds)

  /**
   * Backward joins: keeps the last row that passes both conditions. The scan stops
   * once the as-of condition turns false after a match, since it stays false after that.
   */
  private def findLastBackward(leftRow: InternalRow): InternalRow = {
    var bestMatch: InternalRow = null
    val iter = rightGroupBuffer.generateIterator()
    val needsCopy = rightGroupBuffer.isSpillBacked

    joinedRow.withLeft(leftRow)
    while (iter.hasNext) {
      val rightRow = iter.next()
      joinedRow.withRight(rightRow)

      if (holds(boundAsOfCond)) {
        if (residualHolds) {
          // Last match wins (closest right.t to left.t)
          bestMatch = retainMatch(rightRow, needsCopy)
        }
      } else if (bestMatch != null) {
        // as-of condition transitioned true -> false (monotone for Backward).
        // No further rows can satisfy it.
        return bestMatch
      }
    }
    bestMatch
  }

  /**
   * Forward joins: returns the first row that passes both conditions. It picks by buffer
   * order, not by distance: the distance can rank STRUCT and ARRAY values wrongly, for
   * example ones with NULL parts or a leading STRING field.
   */
  private def findFirstForward(leftRow: InternalRow): InternalRow = {
    val iter = rightGroupBuffer.generateIterator()
    var asOfSeen = false

    joinedRow.withLeft(leftRow)
    while (iter.hasNext) {
      val rightRow = iter.next()
      joinedRow.withRight(rightRow)

      if (holds(boundAsOfCond)) {
        asOfSeen = true
        if (residualHolds) {
          // No copy: the caller projects this row before the buffer iterator moves again.
          return rightRow
        }
      } else if (asOfSeen) {
        // Past the tolerance bound, so no later row passes the as-of condition.
        return null
      }
    }
    null
  }

  /**
   * Nearest joins: keeps the row with the smallest distance. The scan stops once the
   * distance starts to grow past the minimum found so far.
   */
  private def findNearest(leftRow: InternalRow): InternalRow = {
    var bestMatch: InternalRow = null
    var bestDistance: Any = null
    val iter = rightGroupBuffer.generateIterator()
    val needsCopy = rightGroupBuffer.isSpillBacked

    joinedRow.withLeft(leftRow)
    while (iter.hasNext) {
      val rightRow = iter.next()
      joinedRow.withRight(rightRow)

      if (holds(boundAsOfCond)) {
        if (residualHolds) {
          val distance = boundOrderExpr.eval(joinedRow)
          if (distance != null) {
            if (bestMatch == null || distanceOrdering.lt(distance, bestDistance)) {
              bestMatch = retainMatch(rightRow, needsCopy)
              bestDistance = distance
            } else {
              // Distance is increasing past the minimum. Distance is
              // V-shaped, so once past the minimum no later row can beat it.
              return bestMatch
            }
          }
        }
      }
      // Do NOT early-terminate on as-of condition failure here.
      // For Nearest + !allowExactMatches, the condition is false at a
      // single interior point (right == left) with valid matches on
      // both sides. Distance-based termination above is sufficient.
    }
    bestMatch
  }
}
