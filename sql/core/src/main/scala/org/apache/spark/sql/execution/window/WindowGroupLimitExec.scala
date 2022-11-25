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
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, SparkPlan, UnaryExecNode}

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
        // Only show warning when the number of bytes is larger than 100 MiB?
        logWarning("No Partition Defined for Window operation! Moving all data to a single "
          + "partition, this can cause serious performance degradation.")
        AllTuples :: Nil
      } else {
        ClusteredDistribution(partitionSpec) :: Nil
      }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected override def doExecute(): RDD[InternalRow] = {
    val inMemoryThreshold = conf.windowExecBufferInMemoryThreshold
    val spillThreshold = conf.windowExecBufferSpillThreshold

    abstract class WindowIterator extends Iterator[InternalRow] {

      def stream: Iterator[InternalRow]

      val grouping = UnsafeProjection.create(partitionSpec, child.output)

      // Manage the stream and the grouping.
      var nextRow: UnsafeRow = null
      var nextGroup: UnsafeRow = null
      var nextRowAvailable: Boolean = false
      private[this] def fetchNextRow(): Unit = {
        nextRowAvailable = stream.hasNext
        if (nextRowAvailable) {
          nextRow = stream.next().asInstanceOf[UnsafeRow]
          nextGroup = grouping(nextRow)
        } else {
          nextRow = null
          nextGroup = null
        }
      }
      fetchNextRow()

      // Manage the current partition.
      val buffer: ExternalAppendOnlyUnsafeRowArray =
        new ExternalAppendOnlyUnsafeRowArray(inMemoryThreshold, spillThreshold)

      var bufferIterator: Iterator[UnsafeRow] = _

      def clearBuffer(): Unit
      def fillBuffer(): Unit

      private[this] def fetchNextPartition(): Unit = {
        // Collect all the rows in the current partition.
        // Before we start to fetch new input rows, make a copy of nextGroup.
        val currentGroup = nextGroup.copy()

        // clear last partition
        clearBuffer()

        while (nextRowAvailable && nextGroup == currentGroup) {
          fillBuffer()
          fetchNextRow()
        }

        // Setup iteration
        bufferIterator = buffer.generateIterator()
      }

      override final def hasNext: Boolean = {
        val found = (bufferIterator != null && bufferIterator.hasNext) || nextRowAvailable
        if (!found) {
          // clear final partition
          buffer.clear()
        }
        found
      }

      override final def next(): InternalRow = {
        // Load the next partition if we need to.
        if ((bufferIterator == null || !bufferIterator.hasNext) && nextRowAvailable) {
          fetchNextPartition()
        }

        if (bufferIterator.hasNext) {
          val current = bufferIterator.next()

          // Return the row.
          current
        } else {
          throw new NoSuchElementException
        }
      }
    }

    case class SimpleGroupLimitIterator(stream: Iterator[InternalRow]) extends WindowIterator {
      override def clearBuffer(): Unit = {
        buffer.clear()
      }

      override def fillBuffer(): Unit = {
        if (buffer.length < limit) {
          buffer.add(nextRow)
        }
      }
    }

    case class RankGroupLimitIterator(stream: Iterator[InternalRow]) extends WindowIterator {
      val ordering = GenerateOrdering.generate(orderSpec, output)
      var count = 0
      var rank = 0
      var currentRank: UnsafeRow = null

      override def clearBuffer(): Unit = {
        count = 0
        rank = 0
        currentRank = null
        buffer.clear()
      }

      override def fillBuffer(): Unit = {
        if (rank >= limit) {
          return
        }
        if (count == 0) {
          currentRank = nextRow.copy()
          buffer.add(nextRow)
        } else {
          if (ordering.compare(currentRank, nextRow) != 0) {
            rank = count
            currentRank = nextRow.copy()
          }
          if (rank < limit) {
            buffer.add(nextRow)
          }
        }
        count += 1
      }
    }

    case class DenseRankGroupLimitIterator(stream: Iterator[InternalRow]) extends WindowIterator {
      val ordering = GenerateOrdering.generate(orderSpec, output)
      var rank = 0
      var currentRank: UnsafeRow = null

      override def clearBuffer(): Unit = {
        rank = 0
        currentRank = null
        buffer.clear()
      }

      override def fillBuffer(): Unit = {
        if (rank >= limit) {
          return
        }
        if (currentRank == null) {
          currentRank = nextRow.copy()
          buffer.add(nextRow)
        } else {
          if (ordering.compare(currentRank, nextRow) != 0) {
            rank += 1
            currentRank = nextRow.copy()
          }
          if (rank < limit) {
            buffer.add(nextRow)
          }
        }
      }
    }

    // Start processing.
    rankLikeFunction match {
      case _: RowNumber =>
        child.execute().mapPartitions(SimpleGroupLimitIterator)
      case _: Rank =>
        child.execute().mapPartitions(RankGroupLimitIterator)
      case _: DenseRank =>
        child.execute().mapPartitions(DenseRankGroupLimitIterator)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WindowGroupLimitExec =
    copy(child = newChild)
}
