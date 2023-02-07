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

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences, DenseRank, Expression, Rank, RowNumber, RowOrdering, SortOrder, SortPrefix, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, SortPrefixUtils, SparkPlan, UnaryExecNode, UnsafeExternalRowSorter}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

sealed trait WindowGroupLimitMode

case object Partial extends WindowGroupLimitMode

case object Final extends WindowGroupLimitMode

/**
 * This operator is designed to filter out unnecessary rows before WindowExec
 * for top-k computation.
 * @param partitionSpec Should be the same as [[WindowExec#partitionSpec]].
 * @param orderSpec Should be the same as [[WindowExec#orderSpec]].
 * @param rankLikeFunction The function to compute row rank, should be RowNumber/Rank/DenseRank.
 * @param limit The limit for rank value.
 * @param mode The mode describes [[WindowGroupLimitExec]] before or after shuffle.
 * @param child The child spark plan.
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

  private val enableRadixSort = conf.enableRadixSort

  override lazy val metrics = Map(
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "sort time"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  private[sql] var rowSorter: UnsafeExternalRowSorter = _

  protected override def doExecute(): RDD[InternalRow] = mode match {
    case Partial =>
      rankLikeFunction match {
        case _: RowNumber if partitionSpec.isEmpty =>
          child.execute().mapPartitionsInternal(new SimpleIterator(output, _, limit))
        case _: RowNumber =>
          child.execute().mapPartitionsInternal(
            SimpleGroupLimitIterator(partitionSpec, output, _, limit))
        case _: Rank if partitionSpec.isEmpty =>
          child.execute().mapPartitionsInternal(new RankIterator(output, _, orderSpec, limit))
        case _: Rank =>
          val peakMemory = longMetric("peakMemory")
          val spillSize = longMetric("spillSize")
          val sortTime = longMetric("sortTime")
          child.execute().mapPartitionsInternal(
            createRankHashTableIterator(_, peakMemory, spillSize, sortTime))
        case _: DenseRank if partitionSpec.isEmpty =>
          child.execute().mapPartitionsInternal(new DenseRankIterator(output, _, orderSpec, limit))
        case _: DenseRank =>
          child.execute().mapPartitionsInternal(
            DenseRankGroupLimitIterator(partitionSpec, output, _, orderSpec, limit))
      }
    case Final =>
      rankLikeFunction match {
        case _: RowNumber =>
          child.execute().mapPartitionsInternal(
            SimpleGroupLimitIterator(partitionSpec, output, _, limit))
        case _: Rank =>
          child.execute().mapPartitionsInternal(
            RankGroupLimitIterator(partitionSpec, output, _, orderSpec, limit))
        case _: DenseRank =>
          child.execute().mapPartitionsInternal(
            DenseRankGroupLimitIterator(partitionSpec, output, _, orderSpec, limit))
      }
  }

  /**
   * This method gets invoked only once for each SortExec instance to initialize an
   * UnsafeExternalRowSorter, both `plan.execute` and code generation are using it.
   * In the code generation code path, we need to call this function outside the class so we
   * should make it public.
   */
  def createSorter(): UnsafeExternalRowSorter = {
    val ordering = RowOrdering.create(orderSpec, output)

    // The comparator for comparing prefix
    val boundSortExpression = BindReferences.bindReference(orderSpec.head, output)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

    val canUseRadixSort = enableRadixSort && orderSpec.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

    // The generator for prefix
    val prefixExpr = SortPrefix(boundSortExpression)
    val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))
    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
      override def computePrefix(row: InternalRow):
      UnsafeExternalRowSorter.PrefixComputer.Prefix = {
        val prefix = prefixProjection.apply(row)
        result.isNull = prefix.isNullAt(0)
        result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
        result
      }
    }

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    rowSorter = UnsafeExternalRowSorter.create(
      schema, ordering, prefixComparator, prefixComputer, pageSize, canUseRadixSort)

    rowSorter
  }

  private def createRankHashTableIterator(
      stream: Iterator[InternalRow],
      peakMemory: SQLMetric,
      spillSize: SQLMetric,
      sortTime: SQLMetric): Iterator[InternalRow] = {
    val inMemoryThreshold = conf.windowExecBufferInMemoryThreshold
    val spillThreshold = conf.windowExecBufferSpillThreshold
    val hashTableSize = conf.windowGroupLimitHashTableSize

    new Iterator[InternalRow] {

      val grouping = UnsafeProjection.create(partitionSpec, child.output)

      // Manage the stream and the grouping.
      var nextRow: UnsafeRow = null
      val groupToBuffer = mutable.HashMap.empty[Int, ExternalAppendOnlyUnsafeRowArray]

      while (stream.hasNext && groupToBuffer.size < hashTableSize) {
        nextRow = stream.next().asInstanceOf[UnsafeRow]
        val groupKey = grouping(nextRow).hashCode()
        val buffer = groupToBuffer.getOrElseUpdate(groupKey,
          new ExternalAppendOnlyUnsafeRowArray(inMemoryThreshold, spillThreshold))
        buffer.add(nextRow)
      }

      val buffers = groupToBuffer.valuesIterator
      var nextBuffer: ExternalAppendOnlyUnsafeRowArray = _
      var nextBufferAvailable: Boolean = false
      private[this] def fetchNextBuffer(): Unit = {
        nextBufferAvailable = buffers.hasNext
        if (nextBufferAvailable) {
          nextBuffer = buffers.next()
        } else {
          nextBuffer = null
        }
      }
      fetchNextBuffer()

      var bufferIterator: Iterator[InternalRow] = _
      val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, orderSpec)
      private[this] def fetchNextPartition(): Unit = {
        bufferIterator = if (groupToBuffer.size >= hashTableSize) {
          nextBuffer.generateIterator()
        } else if (orderingSatisfies) {
          new RankIterator(output, nextBuffer.generateIterator(), orderSpec, limit)
        } else {
          val sorter = createSorter()

          val metrics = TaskContext.get().taskMetrics()
          // Remember spill data size of this task before execute this operator so that we can
          // figure out how many bytes we spilled for this operator.
          val spillSizeBefore = metrics.memoryBytesSpilled
          val sortedIterator = sorter.sort(nextBuffer.generateIterator())
          sortTime += NANOSECONDS.toMillis(sorter.getSortTimeNanos)
          peakMemory += sorter.getPeakMemoryUsage
          spillSize += metrics.memoryBytesSpilled - spillSizeBefore
          metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)
          new RankIterator(output, sortedIterator, orderSpec, limit)
        }

        fetchNextBuffer()
      }

      override final def hasNext: Boolean = {
        val found = (bufferIterator != null && bufferIterator.hasNext) ||
          nextBufferAvailable || stream.hasNext
        if (!found) {
          buffers.foreach { buffer =>
            // clear final partition
            buffer.clear()
            spillSize += buffer.spillSize
          }
        }
        found
      }

      override final def next(): InternalRow = {
        // Load the next partition if we need to.
        if ((bufferIterator == null || !bufferIterator.hasNext) && nextBufferAvailable) {
          fetchNextPartition()
        }

        if (bufferIterator.hasNext) {
          bufferIterator.next()
        } else if (stream.hasNext) {
          stream.next()
        } else {
          throw new NoSuchElementException
        }
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WindowGroupLimitExec =
    copy(child = newChild)
}

trait BaseIterator extends Iterator[InternalRow] {

  def output: Seq[Attribute]

  def input: Iterator[InternalRow]

  def limit: Int

  var rank = 0

  var nextRow: UnsafeRow = null

  // Increase the rank value.
  def increaseRank(): Unit

  override def hasNext: Boolean =
    rank < limit && input.hasNext

  override def next(): InternalRow = {
    nextRow = input.next().asInstanceOf[UnsafeRow]
    increaseRank()
    nextRow
  }
}

class SimpleIterator(
    val output: Seq[Attribute],
    val input: Iterator[InternalRow],
    val limit: Int) extends BaseIterator {

  override def increaseRank(): Unit = {
    rank += 1
  }
}

trait OrderSpecProvider {
  def output: Seq[Attribute]
  def orderSpec: Seq[SortOrder]
  val ordering = GenerateOrdering.generate(orderSpec, output)
}

class RankIterator(
    val output: Seq[Attribute],
    val input: Iterator[InternalRow],
    val orderSpec: Seq[SortOrder],
    val limit: Int) extends BaseIterator with OrderSpecProvider {

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
}

class DenseRankIterator(
    val output: Seq[Attribute],
    val input: Iterator[InternalRow],
    val orderSpec: Seq[SortOrder],
    val limit: Int) extends BaseIterator with OrderSpecProvider {

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
            // Skip all the remaining rows in this group
            do {
              fetchNextRow()
            } while (nextRowAvailable && nextGroup == currentGroup)
            false
          } else {
            // Returns true if there are more rows in this group.
            nextGroup == currentGroup
          }
        } else {
          false
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
