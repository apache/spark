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

package org.apache.spark.sql.execution

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning}

/**
 * The physical plan for streaming query, merge session window after restore from state store.
 * Note: the end time of window that restore from statestore has already contain session windowGap
 *
 * @param windowExpressions
 * @param sessionSpec
 * @param child
 */
case class SessionWindowMergeExec(
    windowExpressions: NamedExpression,
    sessionSpec: Seq[Expression],
    child: SparkPlan)
  extends UnaryExecNode {

  override def requiredChildDistribution: Seq[Distribution] = {
    ClusteredDistribution(sessionSpec) :: Nil
  }

  // Data should be sorted, so we can merge session window directly.
  // TODO: use this requirement for simplicity, not necessary to sort the whole dataset,
  // try better way later.
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(sessionSpec.map(SortOrder(_, Ascending)) :+ SortOrder(
      windowExpressions.toAttribute, Ascending))

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  override protected def doExecute(): RDD[InternalRow] = {
    val inMemoryThreshold = sqlContext.conf.windowExecBufferInMemoryThreshold
    val spillThreshold = sqlContext.conf.windowExecBufferSpillThreshold

    // Get all relevant projections.
    // Get session window index.
    val sessionWindowIdx = output.zipWithIndex.find(_._1.name == windowExpressions.name).get._2

    // Start processing.
    child.execute().mapPartitions { stream =>
      new Iterator[InternalRow] {
        val grouping = UnsafeProjection.create(sessionSpec, child.output)

        // Manage the stream and the grouping.
        var nextRow: UnsafeRow = _
        var nextGroup: UnsafeRow = _
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
        // Init next row before fetchNextPartition so we can make split the group.
        fetchNextRow()

        var windowResultWithBoundary = mutable.ArrayBuffer.empty[(Int, (Long, Long))]
        var windowResultIndex = 0
        var windowStartTime: Long = _
        var lastEndTime: Long = _

        var bufferIterator: Iterator[UnsafeRow] = _
        val buffer: ExternalAppendOnlyUnsafeRowArray =
          new ExternalAppendOnlyUnsafeRowArray(inMemoryThreshold, spillThreshold)
        var rowIndexWithinPartition: Int = _

        private[this] def getWindowStartAndEnd(row: InternalRow): (Long, Long) = {
          val start = row.getStruct(sessionWindowIdx, 2).getLong(0)
          val end = row.getStruct(sessionWindowIdx, 2).getLong(1)
          (start, end)
        }

        private[this] def fetchNextPartition(): Unit = {
          // Collect all the rows in the current partition.
          // Before we start to fetch new input rows, make a copy of nextGroup.
          val currentGroup = nextGroup.copy()

          // clear last partition
          buffer.clear()

          // clear session window relevant var
          windowResultWithBoundary.clear()
          rowIndexWithinPartition = 0

          // Init windowStartTime and lastEndTime for this partition.
          val pair = getWindowStartAndEnd(nextRow)
          windowStartTime = pair._1
          lastEndTime = pair._2

          while (nextRowAvailable && nextGroup == currentGroup) {
            // Get start and end of the session window.
            val (start, end) = getWindowStartAndEnd(nextRow)
            if (start > lastEndTime) { // skip to new window
              windowResultWithBoundary.append(
                (rowIndexWithinPartition, (windowStartTime, lastEndTime)))
              windowStartTime = start
            }

            rowIndexWithinPartition += 1
            lastEndTime = end
            buffer.add(nextRow)
            fetchNextRow()
          }

          windowResultWithBoundary.append(
            (rowIndexWithinPartition, (windowStartTime, lastEndTime)))
          // Setup iteration
          bufferIterator = buffer.generateIterator()
        }

        // Iteration
        override final def hasNext: Boolean =
          (bufferIterator != null && bufferIterator.hasNext) || nextRowAvailable

        override final def next(): InternalRow = {
          // Load the next partition if we need to.
          if ((bufferIterator == null || !bufferIterator.hasNext) && nextRowAvailable) {
            fetchNextPartition()
            rowIndexWithinPartition = 0
            windowResultIndex = 0
          }

          if (bufferIterator.hasNext) {
            val current = bufferIterator.next()

            if (rowIndexWithinPartition >= windowResultWithBoundary(windowResultIndex)._1) {
              windowResultIndex += 1
            }
            rowIndexWithinPartition += 1

            // Update session window.
            val (start, end) = windowResultWithBoundary(windowResultIndex)._2
            current.getStruct(sessionWindowIdx, 2).setLong(0, start)
            current.getStruct(sessionWindowIdx, 2).setLong(1, end)
            current
          } else {
            throw new NoSuchElementException
          }
        }
      }
    }
  }
}
