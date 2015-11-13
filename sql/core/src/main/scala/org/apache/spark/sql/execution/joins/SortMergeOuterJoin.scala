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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{BinaryNode, RowIterator, SparkPlan}
import org.apache.spark.util.collection.BitSet

/**
 * Performs an sort merge outer join of two child relations.
 */
case class SortMergeOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def output: Seq[Attribute] = {
    joinType match {
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        (left.output ++ right.output).map(_.withNullability(true))
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} should not take $x as the JoinType")
    }
  }

  override def outputPartitioning: Partitioning = joinType match {
    // For left and right outer joins, the output is partitioned by the streamed input's join keys.
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  override def outputOrdering: Seq[SortOrder] = joinType match {
    // For left and right outer joins, the output is ordered by the streamed input's join keys.
    case LeftOuter => requiredOrders(leftKeys)
    case RightOuter => requiredOrders(rightKeys)
    // there are null rows in both streams, so there is no order
    case FullOuter => Nil
    case x => throw new IllegalArgumentException(
      s"SortMergeOuterJoin should not take $x as the JoinType")
  }

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  override def outputsUnsafeRows: Boolean = true
  override def canProcessUnsafeRows: Boolean = true
  override def canProcessSafeRows: Boolean = false

  private def createLeftKeyGenerator(): Projection =
    UnsafeProjection.create(leftKeys, left.output)

  private def createRightKeyGenerator(): Projection =
    UnsafeProjection.create(rightKeys, right.output)

  override def doExecute(): RDD[InternalRow] = {
    val numLeftRows = longMetric("numLeftRows")
    val numRightRows = longMetric("numRightRows")
    val numOutputRows = longMetric("numOutputRows")

    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      // An ordering that can be used to compare keys from both sides.
      val keyOrdering = newNaturalAscendingOrdering(leftKeys.map(_.dataType))
      val boundCondition: (InternalRow) => Boolean = {
        condition.map { cond =>
          newPredicate(cond, left.output ++ right.output)
        }.getOrElse {
          (r: InternalRow) => true
        }
      }
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(schema)

      joinType match {
        case LeftOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createLeftKeyGenerator(),
            bufferedKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(leftIter),
            numLeftRows,
            bufferedIter = RowIterator.fromScala(rightIter),
            numRightRows
          )
          val rightNullRow = new GenericInternalRow(right.output.length)
          new LeftOuterIterator(
            smjScanner, rightNullRow, boundCondition, resultProj, numOutputRows).toScala

        case RightOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createRightKeyGenerator(),
            bufferedKeyGenerator = createLeftKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(rightIter),
            numRightRows,
            bufferedIter = RowIterator.fromScala(leftIter),
            numLeftRows
          )
          val leftNullRow = new GenericInternalRow(left.output.length)
          new RightOuterIterator(
            smjScanner, leftNullRow, boundCondition, resultProj, numOutputRows).toScala

        case FullOuter =>
          val leftNullRow = new GenericInternalRow(left.output.length)
          val rightNullRow = new GenericInternalRow(right.output.length)
          val smjScanner = new SortMergeFullOuterJoinScanner(
            leftKeyGenerator = createLeftKeyGenerator(),
            rightKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            leftIter = RowIterator.fromScala(leftIter),
            numLeftRows,
            rightIter = RowIterator.fromScala(rightIter),
            numRightRows,
            boundCondition,
            leftNullRow,
            rightNullRow)

          new FullOuterIterator(
            smjScanner,
            resultProj,
            numOutputRows).toScala

        case x =>
          throw new IllegalArgumentException(
            s"SortMergeOuterJoin should not take $x as the JoinType")
      }
    }
  }
}

/**
 * An iterator for outputting rows in left outer join.
 */
private class LeftOuterIterator(
    smjScanner: SortMergeJoinScanner,
    rightNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow,
    numOutputRows: LongSQLMetric)
  extends OneSideOuterIterator(
    smjScanner, rightNullRow, boundCondition, resultProj, numOutputRows) {

  protected override def setStreamSideOutput(row: InternalRow): Unit = joinedRow.withLeft(row)
  protected override def setBufferedSideOutput(row: InternalRow): Unit = joinedRow.withRight(row)
}

/**
 * An iterator for outputting rows in right outer join.
 */
private class RightOuterIterator(
    smjScanner: SortMergeJoinScanner,
    leftNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow,
    numOutputRows: LongSQLMetric)
  extends OneSideOuterIterator(
    smjScanner, leftNullRow, boundCondition, resultProj, numOutputRows) {

  protected override def setStreamSideOutput(row: InternalRow): Unit = joinedRow.withRight(row)
  protected override def setBufferedSideOutput(row: InternalRow): Unit = joinedRow.withLeft(row)
}

/**
 * An abstract iterator for sharing code between [[LeftOuterIterator]] and [[RightOuterIterator]].
 *
 * Each [[OneSideOuterIterator]] has a streamed side and a buffered side. Each row on the
 * streamed side will output 0 or many rows, one for each matching row on the buffered side.
 * If there are no matches, then the buffered side of the joined output will be a null row.
 *
 * In left outer join, the left is the streamed side and the right is the buffered side.
 * In right outer join, the right is the streamed side and the left is the buffered side.
 *
 * @param smjScanner a scanner that streams rows and buffers any matching rows
 * @param bufferedSideNullRow the default row to return when a streamed row has no matches
 * @param boundCondition an additional filter condition for buffered rows
 * @param resultProj how the output should be projected
 * @param numOutputRows an accumulator metric for the number of rows output
 */
private abstract class OneSideOuterIterator(
    smjScanner: SortMergeJoinScanner,
    bufferedSideNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow,
    numOutputRows: LongSQLMetric) extends RowIterator {

  // A row to store the joined result, reused many times
  protected[this] val joinedRow: JoinedRow = new JoinedRow()

  // Index of the buffered rows, reset to 0 whenever we advance to a new streamed row
  private[this] var bufferIndex: Int = 0

  // This iterator is initialized lazily so there should be no matches initially
  assert(smjScanner.getBufferedMatches.length == 0)

  // Set output methods to be overridden by subclasses
  protected def setStreamSideOutput(row: InternalRow): Unit
  protected def setBufferedSideOutput(row: InternalRow): Unit

  /**
   * Advance to the next row on the stream side and populate the buffer with matches.
   * @return whether there are more rows in the stream to consume.
   */
  private def advanceStream(): Boolean = {
    bufferIndex = 0
    if (smjScanner.findNextOuterJoinRows()) {
      setStreamSideOutput(smjScanner.getStreamedRow)
      if (smjScanner.getBufferedMatches.isEmpty) {
        // There are no matching rows in the buffer, so return the null row
        setBufferedSideOutput(bufferedSideNullRow)
      } else {
        // Find the next row in the buffer that satisfied the bound condition
        if (!advanceBufferUntilBoundConditionSatisfied()) {
          setBufferedSideOutput(bufferedSideNullRow)
        }
      }
      true
    } else {
      // Stream has been exhausted
      false
    }
  }

  /**
   * Advance to the next row in the buffer that satisfies the bound condition.
   * @return whether there is such a row in the current buffer.
   */
  private def advanceBufferUntilBoundConditionSatisfied(): Boolean = {
    var foundMatch: Boolean = false
    while (!foundMatch && bufferIndex < smjScanner.getBufferedMatches.length) {
      setBufferedSideOutput(smjScanner.getBufferedMatches(bufferIndex))
      foundMatch = boundCondition(joinedRow)
      bufferIndex += 1
    }
    foundMatch
  }

  override def advanceNext(): Boolean = {
    val r = advanceBufferUntilBoundConditionSatisfied() || advanceStream()
    if (r) numOutputRows += 1
    r
  }

  override def getRow: InternalRow = resultProj(joinedRow)
}

private class SortMergeFullOuterJoinScanner(
    leftKeyGenerator: Projection,
    rightKeyGenerator: Projection,
    keyOrdering: Ordering[InternalRow],
    leftIter: RowIterator,
    numLeftRows: LongSQLMetric,
    rightIter: RowIterator,
    numRightRows: LongSQLMetric,
    boundCondition: InternalRow => Boolean,
    leftNullRow: InternalRow,
    rightNullRow: InternalRow)  {
  private[this] val joinedRow: JoinedRow = new JoinedRow()
  private[this] var leftRow: InternalRow = _
  private[this] var leftRowKey: InternalRow = _
  private[this] var rightRow: InternalRow = _
  private[this] var rightRowKey: InternalRow = _

  private[this] var leftIndex: Int = 0
  private[this] var rightIndex: Int = 0
  private[this] val leftMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]
  private[this] val rightMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]
  private[this] var leftMatched: BitSet = new BitSet(1)
  private[this] var rightMatched: BitSet = new BitSet(1)

  advancedLeft()
  advancedRight()

  // --- Private methods --------------------------------------------------------------------------

  /**
   * Advance the left iterator and compute the new row's join key.
   * @return true if the left iterator returned a row and false otherwise.
   */
  private def advancedLeft(): Boolean = {
    if (leftIter.advanceNext()) {
      leftRow = leftIter.getRow
      leftRowKey = leftKeyGenerator(leftRow)
      numLeftRows += 1
      true
    } else {
      leftRow = null
      leftRowKey = null
      false
    }
  }

  /**
   * Advance the right iterator and compute the new row's join key.
   * @return true if the right iterator returned a row and false otherwise.
   */
  private def advancedRight(): Boolean = {
    if (rightIter.advanceNext()) {
      rightRow = rightIter.getRow
      rightRowKey = rightKeyGenerator(rightRow)
      numRightRows += 1
      true
    } else {
      rightRow = null
      rightRowKey = null
      false
    }
  }

  /**
   * Populate the left and right buffers with rows matching the provided key.
   * This consumes rows from both iterators until their keys are different from the matching key.
   */
  private def findMatchingRows(matchingKey: InternalRow): Unit = {
    leftMatches.clear()
    rightMatches.clear()
    leftIndex = 0
    rightIndex = 0

    while (leftRowKey != null && keyOrdering.compare(leftRowKey, matchingKey) == 0) {
      leftMatches += leftRow.copy()
      advancedLeft()
    }
    while (rightRowKey != null && keyOrdering.compare(rightRowKey, matchingKey) == 0) {
      rightMatches += rightRow.copy()
      advancedRight()
    }

    if (leftMatches.size <= leftMatched.capacity) {
      leftMatched.clear()
    } else {
      leftMatched = new BitSet(leftMatches.size)
    }
    if (rightMatches.size <= rightMatched.capacity) {
      rightMatched.clear()
    } else {
      rightMatched = new BitSet(rightMatches.size)
    }
  }

  /**
   * Scan the left and right buffers for the next valid match.
   *
   * Note: this method mutates `joinedRow` to point to the latest matching rows in the buffers.
   * If a left row has no valid matches on the right, or a right row has no valid matches on the
   * left, then the row is joined with the null row and the result is considered a valid match.
   *
   * @return true if a valid match is found, false otherwise.
   */
  private def scanNextInBuffered(): Boolean = {
    while (leftIndex < leftMatches.size) {
      while (rightIndex < rightMatches.size) {
        joinedRow(leftMatches(leftIndex), rightMatches(rightIndex))
        if (boundCondition(joinedRow)) {
          leftMatched.set(leftIndex)
          rightMatched.set(rightIndex)
          rightIndex += 1
          return true
        }
        rightIndex += 1
      }
      rightIndex = 0
      if (!leftMatched.get(leftIndex)) {
        // the left row has never matched any right row, join it with null row
        joinedRow(leftMatches(leftIndex), rightNullRow)
        leftIndex += 1
        return true
      }
      leftIndex += 1
    }

    while (rightIndex < rightMatches.size) {
      if (!rightMatched.get(rightIndex)) {
        // the right row has never matched any left row, join it with null row
        joinedRow(leftNullRow, rightMatches(rightIndex))
        rightIndex += 1
        return true
      }
      rightIndex += 1
    }

    // There are no more valid matches in the left and right buffers
    false
  }

  // --- Public methods --------------------------------------------------------------------------

  def getJoinedRow(): JoinedRow = joinedRow

  def advanceNext(): Boolean = {
    // If we already buffered some matching rows, use them directly
    if (leftIndex <= leftMatches.size || rightIndex <= rightMatches.size) {
      if (scanNextInBuffered()) {
        return true
      }
    }

    if (leftRow != null && (leftRowKey.anyNull || rightRow == null)) {
      joinedRow(leftRow.copy(), rightNullRow)
      advancedLeft()
      true
    } else if (rightRow != null && (rightRowKey.anyNull || leftRow == null)) {
      joinedRow(leftNullRow, rightRow.copy())
      advancedRight()
      true
    } else if (leftRow != null && rightRow != null) {
      // Both rows are present and neither have null values,
      // so we populate the buffers with rows matching the next key
      val comp = keyOrdering.compare(leftRowKey, rightRowKey)
      if (comp <= 0) {
        findMatchingRows(leftRowKey.copy())
      } else {
        findMatchingRows(rightRowKey.copy())
      }
      scanNextInBuffered()
      true
    } else {
      // Both iterators have been consumed
      false
    }
  }
}

private class FullOuterIterator(
    smjScanner: SortMergeFullOuterJoinScanner,
    resultProj: InternalRow => InternalRow,
    numRows: LongSQLMetric
  ) extends RowIterator {
  private[this] val joinedRow: JoinedRow = smjScanner.getJoinedRow()

  override def advanceNext(): Boolean = {
    val r = smjScanner.advanceNext()
    if (r) numRows += 1
    r
  }

  override def getRow: InternalRow = resultProj(joinedRow)
}
