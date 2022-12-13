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
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, DenseRank, Expression, Rank, RowNumber, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

sealed trait WindowGroupLimitMode

case object Partial extends WindowGroupLimitMode

case object Final extends WindowGroupLimitMode

/**
 * This operator is designed to filter out unnecessary rows before WindowExec
 * for top-k computation.
 * @param partitionSpec Should be the same as [[WindowExec#partitionSpec]]
 * @param orderSpec Should be the same as [[WindowExec#orderSpec]]
 * @param rankLikeFunction The function to compute row rank, should be RowNumber/Rank/DenseRank.
 */
case class WindowGroupLimitExec(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    mode: WindowGroupLimitMode,
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def requiredChildDistribution: Seq[Distribution] = mode match {
    case Partial => super.requiredChildDistribution
    case Final =>
      if (partitionSpec.isEmpty) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(partitionSpec) :: Nil
      }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected override def doExecute(): RDD[InternalRow] = rankLikeFunction match {
    case _: RowNumber =>
      child.execute().mapPartitions(SimpleGroupLimitIterator(partitionSpec, output, _, limit))
    case _: Rank =>
      child.execute().mapPartitions(
        RankGroupLimitIterator(partitionSpec, output, _, orderSpec, limit))
    case _: DenseRank =>
      child.execute().mapPartitions(
        DenseRankGroupLimitIterator(partitionSpec, output, _, orderSpec, limit))
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WindowGroupLimitExec =
    copy(child = newChild)
}

abstract class WindowIterator extends Iterator[InternalRow] {

  def partitionSpec: Seq[Expression]

  def output: Seq[Attribute]

  def input: Iterator[InternalRow]

  def limit: Int

  val grouping = UnsafeProjection.create(partitionSpec, output)

  // Manage the stream and the grouping.
  var nextRow: UnsafeRow = null
  var nextGroup: UnsafeRow = null
  var nextRowAvailable: Boolean = false
  protected[this] def fetchNextRow(): Unit = {
    nextRowAvailable = input.hasNext
    if (nextRowAvailable) {
      nextRow = input.next().asInstanceOf[UnsafeRow]
      nextGroup = grouping(nextRow)
    } else {
      nextRow = null
      nextGroup = null
    }
  }
  fetchNextRow()

  var rank = 0

  // Increase the rank value.
  def increaseRank(): Unit

  // Clear the rank value.
  def clearRank(): Unit

  var bufferIterator: Iterator[InternalRow] = _

  private[this] def fetchNextGroup(): Unit = {
    clearRank()
    bufferIterator = createGroupIterator()
  }

  override final def hasNext: Boolean =
    (bufferIterator != null && bufferIterator.hasNext) || nextRowAvailable

  override final def next(): InternalRow = {
    // Load the next partition if we need to.
    if ((bufferIterator == null || !bufferIterator.hasNext) && nextRowAvailable) {
      fetchNextGroup()
    }

    if (bufferIterator.hasNext) {
      bufferIterator.next()
    } else {
      throw new NoSuchElementException
    }
  }

  private def createGroupIterator(): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      // Before we start to fetch new input rows, make a copy of nextGroup.
      val currentGroup = nextGroup.copy()

      def hasNext: Boolean = {
        if (nextRowAvailable) {
          if (rank >= limit && nextGroup == currentGroup) {
            do {
              fetchNextRow()
            } while (nextRowAvailable && nextGroup == currentGroup)
          }
          nextRowAvailable && nextGroup == currentGroup
        } else {
          nextRowAvailable
        }
      }

      def next(): InternalRow = {
        val currentRow = nextRow.copy()
        increaseRank()
        fetchNextRow()
        currentRow
      }
    }
  }
}

case class SimpleGroupLimitIterator(
    partitionSpec: Seq[Expression],
    output: Seq[Attribute],
    input: Iterator[InternalRow],
    limit: Int) extends WindowIterator {

  override def increaseRank(): Unit = {
    rank += 1
  }

  override def clearRank(): Unit = {
    rank = 0
  }
}

case class RankGroupLimitIterator(
    partitionSpec: Seq[Expression],
    output: Seq[Attribute],
    input: Iterator[InternalRow],
    orderSpec: Seq[SortOrder],
    limit: Int) extends WindowIterator {
  val ordering = GenerateOrdering.generate(orderSpec, output)
  var count = 0
  var currentRank: UnsafeRow = null

  override def increaseRank(): Unit = {
    if (count == 0) {
      currentRank = nextRow.copy()
    } else {
      if (ordering.compare(currentRank, nextRow) != 0) {
        rank = count
        currentRank = nextRow.copy()
      }
    }
    count += 1
  }

  override def clearRank(): Unit = {
    count = 0
    rank = 0
    currentRank = null
  }
}

case class DenseRankGroupLimitIterator(
    partitionSpec: Seq[Expression],
    output: Seq[Attribute],
    input: Iterator[InternalRow],
    orderSpec: Seq[SortOrder],
    limit: Int) extends WindowIterator {
  val ordering = GenerateOrdering.generate(orderSpec, output)
  var currentRank: UnsafeRow = null

  override def increaseRank(): Unit = {
    if (currentRank == null) {
      currentRank = nextRow.copy()
    } else {
      if (ordering.compare(currentRank, nextRow) != 0) {
        rank += 1
        currentRank = nextRow.copy()
      }
    }
  }

  override def clearRank(): Unit = {
    rank = 0
    currentRank = null
  }
}
