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
    isSkewJoin: Boolean = false) extends ShuffledJoin {

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

  // Backward joins scan the right-side buffer forward and keep the last match.
  // Detect from asOfCondition so composite MATCH_CONDITION sort keys still work.
  private val isBackwardJoin: Boolean =
    isBackwardAsOfCondition(asOfCondition, left.output, right.output)

  private def isBackwardAsOfCondition(
      condition: Expression,
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute]): Boolean = {
    val leftAttrs = AttributeSet(leftOutput)
    val rightAttrs = AttributeSet(rightOutput)
    def isBackwardPredicate(expr: Expression): Boolean = expr match {
      case GreaterThanOrEqual(l, r)
          if l.references.subsetOf(leftAttrs) && r.references.subsetOf(rightAttrs) =>
        true
      case GreaterThan(l, r)
          if l.references.subsetOf(leftAttrs) && r.references.subsetOf(rightAttrs) =>
        true
      case And(c, _) => isBackwardPredicate(c)
      case _ => false
    }
    isBackwardPredicate(condition)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val spillSize = longMetric("spillSize")
    val isBackward = isBackwardJoin
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
        numOutputRows, spillSize, isBackward,
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
 * forward to find the best match.
 *
 * For Backward joins (left.t >= right.t), the forward scan keeps the
 * last as-of-satisfying row as the best match (since the buffer is
 * sorted ascending, the last satisfying row is the closest).
 *
 * For Forward/Nearest joins, the forward scan uses distance-based
 * early termination (stop when distance starts increasing).
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
    isBackwardJoin: Boolean,
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
  private val boundOrderExpr = bindReference(orderExpression, joinedOutput)
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

  private val distanceOrdering =
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

  /**
   * Find the best matching right row using forward-only scan.
   *
   * For Backward joins: keeps the last as-of-satisfying row (since the
   * buffer is sorted ascending, the last satisfying row has the largest
   * right.t <= left.t, which is the closest match). Early-terminates
   * when as-of condition transitions from true to false (monotone).
   *
   * For Forward/Nearest joins: uses distance-based early termination
   * (stop when distance starts increasing past the minimum).
   */
  private def findBestInGroup(leftRow: InternalRow): InternalRow = {
    if (isBackwardJoin) {
      findBestBackwardForward(leftRow)
    } else {
      findBestForwardNearest(leftRow)
    }
  }

  /**
   * Forward scan for Backward joins: last-match-wins.
   * Buffer is sorted ascending by as-of key. For left.t >= right.t,
   * as-of condition is monotone: true for right.t <= left.t, then false.
   * The last satisfying row is the closest match.
   */
  private def findBestBackwardForward(leftRow: InternalRow): InternalRow = {
    var bestMatch: InternalRow = null
    val iter = rightGroupBuffer.generateIterator()

    while (iter.hasNext) {
      val rightRow = iter.next()
      joinedRow.withLeft(leftRow).withRight(rightRow)

      val asOfSatisfied = boundAsOfCond.eval(joinedRow)
      if (asOfSatisfied != null && asOfSatisfied.asInstanceOf[Boolean]) {
        val residualSatisfied = boundResidualCond.forall { cond =>
          val result = cond.eval(joinedRow)
          result != null && result.asInstanceOf[Boolean]
        }
        if (residualSatisfied) {
          // Last match wins (closest right.t to left.t)
          bestMatch = rightRow.copy()
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
   * Forward scan for Forward/Nearest joins: distance-based termination.
   * Stop when distance starts increasing past the minimum found so far.
   */
  private def findBestForwardNearest(leftRow: InternalRow): InternalRow = {
    var bestMatch: InternalRow = null
    var bestDistance: Any = null
    val iter = rightGroupBuffer.generateIterator()

    while (iter.hasNext) {
      val rightRow = iter.next()
      joinedRow.withLeft(leftRow).withRight(rightRow)

      val asOfSatisfied = boundAsOfCond.eval(joinedRow)
      if (asOfSatisfied != null && asOfSatisfied.asInstanceOf[Boolean]) {
        val residualSatisfied = boundResidualCond.forall { cond =>
          val result = cond.eval(joinedRow)
          result != null && result.asInstanceOf[Boolean]
        }
        if (residualSatisfied) {
          val distance = boundOrderExpr.eval(joinedRow)
          if (distance != null) {
            if (bestMatch == null || distanceOrdering.lt(distance, bestDistance)) {
              bestMatch = rightRow.copy()
              bestDistance = distance
            } else {
              // Distance is increasing past the minimum. For Forward,
              // the as-of condition guarantees no closer row exists
              // further right. For Nearest, distance is V-shaped so
              // once past the minimum no later row can beat it.
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
