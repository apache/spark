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


private class LeftOuterIterator(
    smjScanner: SortMergeJoinScanner,
    rightNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow,
    numRows: LongSQLMetric
  ) extends RowIterator {
  private[this] val joinedRow: JoinedRow = new JoinedRow()
  private[this] var rightIdx: Int = 0
  assert(smjScanner.getBufferedMatches.length == 0)

  private def advanceLeft(): Boolean = {
    rightIdx = 0
    if (smjScanner.findNextOuterJoinRows()) {
      joinedRow.withLeft(smjScanner.getStreamedRow)
      if (smjScanner.getBufferedMatches.isEmpty) {
        // There are no matching right rows, so return nulls for the right row
        joinedRow.withRight(rightNullRow)
      } else {
        // Find the next row from the right input that satisfied the bound condition
        if (!advanceRightUntilBoundConditionSatisfied()) {
          joinedRow.withRight(rightNullRow)
        }
      }
      true
    } else {
      // Left input has been exhausted
      false
    }
  }

  private def advanceRightUntilBoundConditionSatisfied(): Boolean = {
    var foundMatch: Boolean = false
    while (!foundMatch && rightIdx < smjScanner.getBufferedMatches.length) {
      foundMatch = boundCondition(joinedRow.withRight(smjScanner.getBufferedMatches(rightIdx)))
      rightIdx += 1
    }
    foundMatch
  }

  override def advanceNext(): Boolean = {
    val r = advanceRightUntilBoundConditionSatisfied() || advanceLeft()
    if (r) numRows += 1
    r
  }

  override def getRow: InternalRow = resultProj(joinedRow)
}

private class RightOuterIterator(
    smjScanner: SortMergeJoinScanner,
    leftNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow,
    numRows: LongSQLMetric
  ) extends RowIterator {
  private[this] val joinedRow: JoinedRow = new JoinedRow()
  private[this] var leftIdx: Int = 0
  assert(smjScanner.getBufferedMatches.length == 0)

  private def advanceRight(): Boolean = {
    leftIdx = 0
    if (smjScanner.findNextOuterJoinRows()) {
      joinedRow.withRight(smjScanner.getStreamedRow)
      if (smjScanner.getBufferedMatches.isEmpty) {
        // There are no matching left rows, so return nulls for the left row
        joinedRow.withLeft(leftNullRow)
      } else {
        // Find the next row from the left input that satisfied the bound condition
        if (!advanceLeftUntilBoundConditionSatisfied()) {
          joinedRow.withLeft(leftNullRow)
        }
      }
      true
    } else {
      // Right input has been exhausted
      false
    }
  }

  private def advanceLeftUntilBoundConditionSatisfied(): Boolean = {
    var foundMatch: Boolean = false
    while (!foundMatch && leftIdx < smjScanner.getBufferedMatches.length) {
      foundMatch = boundCondition(joinedRow.withLeft(smjScanner.getBufferedMatches(leftIdx)))
      leftIdx += 1
    }
    foundMatch
  }

  override def advanceNext(): Boolean = {
    val r = advanceLeftUntilBoundConditionSatisfied() || advanceRight()
    if (r) numRows += 1
    r
  }

  override def getRow: InternalRow = resultProj(joinedRow)
}
