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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetrics}

/**
 * :: DeveloperApi ::
 * Performs an sort merge outer join of two child relations.
 *
 * Note: this does not support full outer join yet; see SPARK-9730 for progress on this.
 */
@DeveloperApi
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
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} should not take $x as the JoinType")
    }
  }

  override def outputPartitioning: Partitioning = joinType match {
    // For left and right outer joins, the output is partitioned by the streamed input's join keys.
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  override def outputOrdering: Seq[SortOrder] = joinType match {
    // For left and right outer joins, the output is ordered by the streamed input's join keys.
    case LeftOuter => requiredOrders(leftKeys)
    case RightOuter => requiredOrders(rightKeys)
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

  private def isUnsafeMode: Boolean = {
    (codegenEnabled && unsafeEnabled
      && UnsafeProjection.canSupport(leftKeys)
      && UnsafeProjection.canSupport(rightKeys)
      && UnsafeProjection.canSupport(schema))
  }

  override def outputsUnsafeRows: Boolean = isUnsafeMode
  override def canProcessUnsafeRows: Boolean = isUnsafeMode
  override def canProcessSafeRows: Boolean = !isUnsafeMode

  private def createLeftKeyGenerator(): Projection = {
    if (isUnsafeMode) {
      UnsafeProjection.create(leftKeys, left.output)
    } else {
      newProjection(leftKeys, left.output)
    }
  }

  private def createRightKeyGenerator(): Projection = {
    if (isUnsafeMode) {
      UnsafeProjection.create(rightKeys, right.output)
    } else {
      newProjection(rightKeys, right.output)
    }
  }

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
      val resultProj: InternalRow => InternalRow = {
        if (isUnsafeMode) {
          UnsafeProjection.create(schema)
        } else {
          identity[InternalRow]
        }
      }

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
