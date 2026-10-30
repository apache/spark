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

import java.math.{BigDecimal => JavaBigDecimal}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, GenerateOrdering}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.AsOfJoin
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{AnyTimestampNanoType, BinaryType, BooleanType, DataType,
  DateType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegralType,
  NullType, NumericType, StringType, TimestampNTZType, TimestampType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Performs an AS-OF join using sort-merge. Both sides are co-partitioned
 * by the equi-join keys and sorted by (equi-join keys, as-of key).
 * For each left row, we scan the right-side group buffer (forward-only)
 * to find the nearest match satisfying the as-of condition.
 *
 * The right-side buffer uses [[ExternalAppendOnlyUnsafeRowArray]] which
 * spills to disk when the in-memory threshold is exceeded, avoiding OOM
 * for skewed equi-key groups. Joins that can use a cursor scan (see
 * [[SortMergeAsOfJoinScanner.supportsCursorScan]]) read the right side once
 * and buffer nothing.
 *
 * Note: When there are no equi-keys, both sides are collected into a
 * single partition (AllTuples) and, without a cursor scan, the entire
 * right side is buffered.
 */
case class SortMergeAsOfJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    leftSortExprs: Seq[Expression],
    rightSortExprs: Seq[Expression],
    asOfCondition: Expression,
    direction: AsOfJoinDirection,
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false) extends ShuffledJoin {

  // The sides may have different key counts: each side is sorted on its own, and only the
  // equi-keys are compared across sides.
  require(leftSortExprs.nonEmpty && rightSortExprs.nonEmpty,
    s"$nodeName requires non-empty sort expressions on both sides")

  require(Seq(Inner, LeftOuter).exists(joinType == _),
    s"$nodeName does not support join type: $joinType")

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "numAsOfCursorTasks" -> SQLMetrics.createMetric(sparkContext, "number of cursor scan tasks"))

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

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val spillSize = longMetric("spillSize")
    val inMemoryThreshold = conf.sortMergeJoinExecBufferInMemoryThreshold
    val sizeInBytesSpillThreshold = conf.sortMergeJoinExecBufferSpillSizeThreshold
    val spillThreshold = conf.sortMergeJoinExecBufferSpillThreshold
    val numAsOfCursorTasks = longMetric("numAsOfCursorTasks")
    val useCursorScan = conf.sortMergeAsOfJoinCursorScanEnabled &&
      SortMergeAsOfJoinScanner.supportsCursorScan(
        direction, asOfCondition, condition, leftSortExprs, rightSortExprs)

    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      val scanner = new SortMergeAsOfJoinScanner(
        leftIter, rightIter,
        left.output, right.output,
        leftKeys, rightKeys,
        leftSortExprs, rightSortExprs,
        asOfCondition,
        joinType, condition,
        numOutputRows, spillSize, direction,
        inMemoryThreshold, sizeInBytesSpillThreshold, spillThreshold,
        useCursorScan, numAsOfCursorTasks
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
 *  - Nearest: the closest eligible rows before and after the left key are compared.
 *
 * With `useCursorScan`, the scan does not buffer the group. It walks the right input once and
 * keeps only the best row so far (see [[findWithCursor]]).
 */
private[joins] class SortMergeAsOfJoinScanner(
    leftIter: Iterator[InternalRow],
    rightIter: Iterator[InternalRow],
    leftOutput: Seq[Attribute],
    rightOutput: Seq[Attribute],
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    leftSortExprs: Seq[Expression],
    rightSortExprs: Seq[Expression],
    asOfCondition: Expression,
    joinType: JoinType,
    residualCondition: Option[Expression],
    numOutputRows: SQLMetric,
    spillSize: SQLMetric,
    direction: AsOfJoinDirection,
    inMemoryThreshold: Int,
    sizeInBytesSpillThreshold: Long,
    spillThreshold: Int,
    useCursorScan: Boolean,
    numAsOfCursorTasks: SQLMetric) {

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

  // Lazy, since only Nearest reads the keys.
  private lazy val nearestKeys: (Expression, Expression, Ordering[Any]) =
    (leftSortExprs, rightSortExprs) match {
      case (Seq(leftKey), Seq(rightKey))
          if AsOfJoin.supportsNearestKeys(leftKey.dataType, rightKey.dataType) =>
        (
          bindReference(leftKey, leftOutput),
          bindReference(rightKey, rightOutput),
          TypeUtils.getInterpretedOrdering(leftKey.dataType))
      case _ =>
        throw SparkException.internalError(
          s"Unexpected nearest keys: $leftSortExprs and $rightSortExprs")
    }

  // Lazy, since only the cursor scan reads the keys.
  private lazy val cursorKeys: (Expression, Expression) =
    (bindReference(leftSortExprs.head, leftOutput), bindReference(rightSortExprs.head, rightOutput))

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

  // Cursor scan state: the left equi-key of the current group, and its best right row so far.
  private var cursorGroupKey: UnsafeRow = _
  private var cursorBest: UnsafeRow = _

  // Projection to convert right rows to UnsafeRow for the buffer
  private val rightToUnsafe = UnsafeProjection.create(rightOutput, rightOutput)

  if (!rightDone) {
    rightPeek = rightToUnsafe(rightIter.next()).copy()
  }

  if (useCursorScan) {
    numAsOfCursorTasks += 1
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
          val bestMatch = if (useCursorScan) {
            findWithCursor(leftRow, leftKey)
          } else {
            advanceRightTo(leftKey)
            findBestInGroup(leftRow)
          }

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
            advanceRightPeek()
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
        advanceRightPeek()
      } else {
        return
      }
    }
  }

  /** Moves `rightPeek` to the next right row, or to null at the end of the right input. */
  private def advanceRightPeek(): Unit = {
    rightPeek = if (rightIter.hasNext) {
      rightToUnsafe(rightIter.next()).copy()
    } else {
      rightDone = true; null
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
   * Backward joins keep the last row that passes the as-of and residual conditions.
   * Once the as-of condition has been true, the scan stops at the first row that fails
   * it, as later sorted rows cannot satisfy it.
   */
  private def findLastBackward(leftRow: InternalRow): InternalRow = {
    var bestMatch: InternalRow = null
    val iter = rightGroupBuffer.generateIterator()
    val needsCopy = rightGroupBuffer.isSpillBacked
    var asOfSeen = false

    joinedRow.withLeft(leftRow)
    while (iter.hasNext) {
      val rightRow = iter.next()
      joinedRow.withRight(rightRow)

      if (holds(boundAsOfCond)) {
        asOfSeen = true
        if (residualHolds) {
          // Last match wins (closest right.t to left.t)
          bestMatch = retainMatch(rightRow, needsCopy)
        }
      } else if (asOfSeen) {
        // At or past left.t, so no later row passes the as-of condition.
        return bestMatch
      }
    }
    bestMatch
  }

  /**
   * Forward joins return the first row that passes the as-of and residual conditions.
   * With a tolerance, if the scan enters the allowed range but finds no match, it
   * stops at the first row beyond that range, as later sorted rows cannot qualify.
   */
  private def findFirstForward(leftRow: InternalRow): InternalRow = {
    val iter = rightGroupBuffer.generateIterator()
    val needsCopy = rightGroupBuffer.isSpillBacked
    var asOfSeen = false

    joinedRow.withLeft(leftRow)
    while (iter.hasNext) {
      val rightRow = iter.next()
      joinedRow.withRight(rightRow)

      if (holds(boundAsOfCond)) {
        asOfSeen = true
        if (residualHolds) {
          return retainMatch(rightRow, needsCopy)
        }
      } else if (asOfSeen) {
        // Past the tolerance bound, so no later row passes the as-of condition.
        return null
      }
    }
    null
  }

  /**
   * Finds the best right row for `leftRow` by moving a cursor over the right input, or null.
   *
   * Left rows of a group come in ascending as-of key order. So a right row that passes for one
   * left row also passes for the next (backward), and a row that fails stays failed (forward).
   * The cursor never steps back: backward stops at the first failing row and keeps it for the
   * next left row, and forward drops each failing row, since no later left row can match it.
   */
  private def findWithCursor(leftRow: InternalRow, leftKey: UnsafeRow): InternalRow = {
    equiKeyOrdering match {
      case Some(ordering)
          if cursorGroupKey == null || ordering.compare(leftKey, cursorGroupKey) != 0 =>
        cursorGroupKey = leftKey
        cursorBest = null
      case _ =>
    }
    val (leftAsOfKey, rightAsOfKey) = cursorKeys
    // A NULL left as-of key matches nothing. It must not move the cursor, or forward would drop
    // right rows that later left rows need.
    if (leftAsOfKey.eval(leftRow) == null) return null

    joinedRow.withLeft(leftRow)
    if (direction == Forward && cursorBest != null) {
      // The cursor is already past the best row, and every row before it failed.
      joinedRow.withRight(cursorBest)
      if (holds(boundAsOfCond)) return cursorBest
      cursorBest = null
    }

    while (rightPeek != null) {
      val cmp = equiKeyOrdering match {
        case Some(ordering) => ordering.compare(leftKey, rightKeyProj(rightPeek))
        case None => 0
      }
      if (cmp > 0 || (cmp == 0 && rightAsOfKey.eval(rightPeek) == null)) {
        // An earlier group, or a NULL right as-of key: no later left row can match it.
        advanceRightPeek()
      } else if (cmp < 0) {
        // A later group, so this group has no more right rows.
        return cursorBest
      } else {
        joinedRow.withRight(rightPeek)
        if (holds(boundAsOfCond)) {
          // Each peeked row is its own copy, so it can be kept as is.
          cursorBest = rightPeek
          advanceRightPeek()
          if (direction == Forward) return cursorBest
        } else if (direction == Backward) {
          return cursorBest
        } else {
          advanceRightPeek()
        }
      }
    }
    cursorBest
  }

  /** Finds the nearest row passing both conditions, preferring the predecessor on equal gaps. */
  private def findNearest(leftRow: InternalRow): InternalRow = {
    val (leftKeyExpr, rightKeyExpr, keyOrdering) = nearestKeys
    val leftKey = leftKeyExpr.eval(leftRow)
    if (leftKey == null) return null

    var bestBefore: UnsafeRow = null
    var bestBeforeKey: Any = null
    val iter = rightGroupBuffer.generateIterator()
    val needsCopy = rightGroupBuffer.isSpillBacked

    joinedRow.withLeft(leftRow)
    while (iter.hasNext) {
      val rightRow = iter.next()
      joinedRow.withRight(rightRow)

      if (holds(boundAsOfCond) && residualHolds) {
        val rightKey = rightKeyExpr.eval(rightRow)
        if (rightKey != null) {
          if (keyOrdering.lt(rightKey, leftKey)) {
            // Keep the first eligible row at each key so equal-distance ties stay stable.
            if (bestBefore == null || keyOrdering.lt(bestBeforeKey, rightKey)) {
              bestBefore = retainMatch(rightRow, needsCopy)
              bestBeforeKey = InternalRow.copyValue(rightKey)
            }
          } else if (keyOrdering.equiv(rightKey, leftKey)) {
            // An exact match is the nearest row, even for an infinite key (Inf - Inf is NaN).
            return retainMatch(rightRow, needsCopy)
          } else if (bestBefore == null ||
              isAfterCloser(bestBeforeKey, leftKey, rightKey, leftKeyExpr.dataType)) {
            return retainMatch(rightRow, needsCopy)
          } else {
            return bestBefore
          }
        }
      }
    }

    bestBefore
  }

  /** The exact value of a nearest key, or None for a NaN or infinite floating-point key. */
  private def exactNearestKeyValue(key: Any, dataType: DataType): Option[JavaBigDecimal] =
    dataType match {
      case _: IntegralType | DateType | _: YearMonthIntervalType | _: DayTimeIntervalType |
           TimestampType | TimestampNTZType =>
        Some(JavaBigDecimal.valueOf(key.asInstanceOf[java.lang.Number].longValue()))
      case _: AnyTimestampNanoType =>
        val timestamp = key.asInstanceOf[TimestampNanosVal]
        Some(JavaBigDecimal.valueOf(timestamp.epochMicros)
          .multiply(JavaBigDecimal.valueOf(1000))
          .add(JavaBigDecimal.valueOf(timestamp.nanosWithinMicro)))
      case _: DecimalType => Some(key.asInstanceOf[Decimal].toJavaBigDecimal)
      case FloatType | DoubleType =>
        val value = key.asInstanceOf[java.lang.Number].doubleValue()
        // The BigDecimal(double) constructor keeps the exact binary value; valueOf rounds it.
        if (value.isNaN || value.isInfinite) None else Some(new JavaBigDecimal(value))
      case _ => throw SparkException.internalError(s"Unsupported nearest key type: $dataType")
    }

  /** Whether the row after the left key is strictly closer than the row before it. */
  private def isAfterCloser(
      beforeKey: Any,
      leftKey: Any,
      afterKey: Any,
      dataType: DataType): Boolean = {
    (exactNearestKeyValue(beforeKey, dataType),
      exactNearestKeyValue(leftKey, dataType),
      exactNearestKeyValue(afterKey, dataType)) match {
      case (Some(before), Some(left), Some(after)) =>
        after.subtract(left).compareTo(left.subtract(before)) < 0
      case _ =>
        // A NaN or infinite key leaves at most one gap finite.
        nonFiniteGapRank(leftKey, afterKey) < nonFiniteGapRank(beforeKey, leftKey)
    }
  }

  /** 0 for a gap between finite keys, 1 for an infinite gap, and 2 for a gap from NaN. */
  private def nonFiniteGapRank(lowerKey: Any, upperKey: Any): Int = {
    val lower = lowerKey.asInstanceOf[java.lang.Number].doubleValue()
    val upper = upperKey.asInstanceOf[java.lang.Number].doubleValue()
    if (lower.isNaN || upper.isNaN) 2 else if (lower.isInfinite || upper.isInfinite) 1 else 0
  }
}

private[execution] object SortMergeAsOfJoinScanner {

  /**
   * Whether the scan can read the right side once with a cursor, instead of buffering each
   * right-side group and scanning it again for every left row.
   *
   * This needs a backward or forward join with no residual condition, whose as-of condition
   * only compares the two sort keys. Both sides are sorted on exactly those keys, so the right
   * rows that pass the condition move forward as the left key grows. Anything else, such as a
   * tolerance, a tuple, or an operand cast that the sort key does not have, keeps the buffer.
   */
  def supportsCursorScan(
      direction: AsOfJoinDirection,
      asOfCondition: Expression,
      residualCondition: Option[Expression],
      leftSortExprs: Seq[Expression],
      rightSortExprs: Seq[Expression]): Boolean = {
    // The operands as (lesser, greater).
    val ordered = asOfCondition match {
      case LessThan(lesser, greater) => Some((lesser, greater))
      case LessThanOrEqual(lesser, greater) => Some((lesser, greater))
      case GreaterThan(greater, lesser) => Some((lesser, greater))
      case GreaterThanOrEqual(greater, lesser) => Some((lesser, greater))
      case _ => None
    }
    // The operands as (left, right): backward needs left >= right, forward left <= right.
    val operands = (direction, ordered) match {
      case (Backward, Some((lesser, greater))) => Some((greater, lesser))
      case (Forward, Some((lesser, greater))) => Some((lesser, greater))
      case _ => None
    }
    (leftSortExprs, rightSortExprs, operands) match {
      case (Seq(leftKey), Seq(rightKey), Some((leftOperand, rightOperand))) =>
        residualCondition.isEmpty &&
          leftOperand.semanticEquals(leftKey) &&
          rightOperand.semanticEquals(rightKey) &&
          leftKey.dataType == rightKey.dataType &&
          supportsCursorKey(leftKey.dataType)
      case _ => false
    }
  }

  /** Scalar key types only for now. STRUCT and ARRAY keys keep the buffered scan. */
  private def supportsCursorKey(dataType: DataType): Boolean = dataType match {
    case _: NumericType | BooleanType | _: StringType | BinaryType | NullType => true
    case DateType | TimestampType | TimestampNTZType => true
    case _: YearMonthIntervalType | _: DayTimeIntervalType => true
    case _ => false
  }
}
