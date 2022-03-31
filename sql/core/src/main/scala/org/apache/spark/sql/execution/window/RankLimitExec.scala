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


package org.apache.spark.sql.execution.window

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.collection.Utils


sealed trait RankLimitMode

case object Partial extends RankLimitMode

case object Final extends RankLimitMode


/**
 * This operator is designed to filter out unnecessary rows before WindowExec,
 * for top-k computation.
 * @param partitionSpec Should be the same as [[WindowExec#partitionSpec]]
 * @param orderSpec Should be the same as [[WindowExec#orderSpec]]
 * @param rankFunction The function to compute row rank, should be RowNumber/Rank/DenseRank.
 */
case class RankLimitExec(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankFunction: Expression,
    limit: Int,
    mode: RankLimitMode,
    child: SparkPlan) extends UnaryExecNode {
  assert(orderSpec.nonEmpty && limit > 0)

  private val shouldApplyTakeOrdered: Boolean = rankFunction match {
    case _: RowNumber => limit < conf.topKSortFallbackThreshold
    case _: Rank => false
    case _: DenseRank => false
    case f => throw new IllegalArgumentException(s"Unsupported rank function: $f")
  }

  override def output: Seq[Attribute] = child.output

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    if (shouldApplyTakeOrdered) {
      Seq(partitionSpec.map(SortOrder(_, Ascending)))
    } else {
      // Should be the same as [[WindowExec#requiredChildOrdering]]
      Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
    }
  }

  override def outputOrdering: Seq[SortOrder] = {
    partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec
  }

  override def requiredChildDistribution: Seq[Distribution] = mode match {
    case Partial => super.requiredChildDistribution
    case Final =>
      // Should be the same as [[WindowExec#requiredChildDistribution]]
      if (partitionSpec.isEmpty) {
        AllTuples :: Nil
      } else ClusteredDistribution(partitionSpec) :: Nil
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private lazy val ordering = GenerateOrdering.generate(orderSpec, output)

  private lazy val limitFunction = rankFunction match {
    case _: RowNumber if shouldApplyTakeOrdered =>
      (stream: Iterator[InternalRow]) =>
        Utils.takeOrdered(stream.map(_.copy()), limit)(ordering)

    case _: RowNumber =>
      (stream: Iterator[InternalRow]) =>
        stream.take(limit)

    case _: Rank =>
      (stream: Iterator[InternalRow]) =>
        var count = 0
        var rank = 0
        SimpleGroupedIterator.apply(stream, ordering)
          .flatMap { group =>
            rank = count + 1
            group.map { row => count += 1; row }
          }.takeWhile(_ => rank <= limit)

    case _: DenseRank =>
      (stream: Iterator[InternalRow]) =>
        SimpleGroupedIterator.apply(stream, ordering)
          .take(limit)
          .flatten

    case f => throw new IllegalArgumentException(s"Unsupported rank function: $f")
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsInternal { stream =>
      val filteredStream = if (stream.isEmpty) {
        Iterator.empty
      } else if (partitionSpec.isEmpty) {
        limitFunction(stream)
      } else {
        val partitionOrdering = GenerateOrdering.generate(
          partitionSpec.map(SortOrder(_, Ascending)), output)
        SimpleGroupedIterator.apply(stream, partitionOrdering)
          .flatMap(limitFunction)
      }

      filteredStream.map { row =>
        numOutputRows += 1
        row
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): RankLimitExec =
    copy(child = newChild)
}


object SimpleGroupedIterator {
  def apply(
      input: Iterator[InternalRow],
      ordering: BaseOrdering): Iterator[Iterator[InternalRow]] = {
    if (input.hasNext) {
      new SimpleGroupedIterator(input.buffered, ordering)
    } else {
      Iterator.empty
    }
  }
}


/**
 * A simplified version of [[GroupedIterator]], there are mainly two differences:
 * 1, does not need to perform key projection, since grouping key is not used,
 * 2, the ordering is passed in, so can be reused.
 *
 * Note, the class does not handle the case of an empty input for simplicity of implementation.
 * Use the factory to construct a new instance.
 *
 * @param input An iterator of rows.  This iterator must be ordered by the groupingExpressions or
 *              it is possible for the same group to appear more than once.
 * @param ordering Compares two input rows and returns 0 if they are in the same group.
 */
class SimpleGroupedIterator private(
    input: BufferedIterator[InternalRow],
    ordering: BaseOrdering)
  extends Iterator[Iterator[InternalRow]] {

  /**
   * Holds null or the row that will be returned on next call to `next()` in the inner iterator.
   */
  var currentRow = input.next()

  /** Holds a copy of an input row that is in the current group. */
  var currentGroup = currentRow.copy()

  assert(ordering.compare(currentGroup, currentRow) == 0)
  var currentIterator = createGroupValuesIterator()

  /**
   * Return true if we already have the next iterator or fetching a new iterator is successful.
   *
   * Note that, if we get the iterator by `next`, we should consume it before call `hasNext`,
   * because we will consume the input data to skip to next group while fetching a new iterator,
   * thus make the previous iterator empty.
   */
  def hasNext: Boolean = currentIterator != null || fetchNextGroupIterator

  def next(): Iterator[InternalRow] = {
    assert(hasNext) // Ensure we have fetched the next iterator.
    val ret = currentIterator
    currentIterator = null
    ret
  }

  private def fetchNextGroupIterator(): Boolean = {
    assert(currentIterator == null)

    if (currentRow == null && input.hasNext) {
      currentRow = input.next()
    }

    if (currentRow == null) {
      // These is no data left, return false.
      false
    } else {
      // Skip to next group.
      // currentRow may be overwritten by `hasNext`, so we should compare them first.
      while (ordering.compare(currentGroup, currentRow) == 0 && input.hasNext) {
        currentRow = input.next()
      }

      if (ordering.compare(currentGroup, currentRow) == 0) {
        // We are in the last group, there is no more groups, return false.
        false
      } else {
        // Now the `currentRow` is the first row of next group.
        currentGroup = currentRow.copy()
        currentIterator = createGroupValuesIterator()
        true
      }
    }
  }

  private def createGroupValuesIterator(): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      def hasNext: Boolean = currentRow != null || fetchNextRowInGroup()

      def next(): InternalRow = {
        assert(hasNext)
        val res = currentRow
        currentRow = null
        res
      }

      private def fetchNextRowInGroup(): Boolean = {
        assert(currentRow == null)

        if (input.hasNext) {
          // The inner iterator should NOT consume the input into next group, here we use `head` to
          // peek the next input, to see if we should continue to process it.
          if (ordering.compare(currentGroup, input.head) == 0) {
            // Next input is in the current group.  Continue the inner iterator.
            currentRow = input.next()
            true
          } else {
            // Next input is not in the right group.  End this inner iterator.
            false
          }
        } else {
          // There is no more data, return false.
          false
        }
      }
    }
  }
}
