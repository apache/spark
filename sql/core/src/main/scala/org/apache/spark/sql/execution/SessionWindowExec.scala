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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, TimestampType}

/**
 * Used for calculating the session window start and end for each row, so this plan requires
 * child distributed by sessionSpec and sorted by time column in each part. The value for
 * window start is time value of the first row in this window, the value for window end is
 * time value of the last row plus the windowGap.
 *
 * @param windowExpressions session window expression for the exec node.
 * @param sessionSpec the partition key of this session window, it is the rest column of
 *                    groupingExpr in parent aggregate node.
 * @param windowGap window gap in micro second.
 * @param child child plan for this node.
 */
case class SessionWindowExec(
    windowExpressions: NamedExpression,
    timeColumn: Expression,
    sessionSpec: Seq[Expression],
    windowGap: Long,
    child: SparkPlan)
  extends UnaryExecNode {

  private final val WINDOW_START = "start"
  private final val WINDOW_END = "end"

  override def requiredChildDistribution: Seq[Distribution] = {
    ClusteredDistribution(sessionSpec) :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(sessionSpec.map(SortOrder(_, Ascending)) :+ SortOrder(timeColumn, Ascending))

  override def output: Seq[Attribute] = child.output ++ Seq(windowExpressions.toAttribute)

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  override protected def doExecute(): RDD[InternalRow] = {
    val inMemoryThreshold = sqlContext.conf.windowExecBufferInMemoryThreshold
    val spillThreshold = sqlContext.conf.windowExecBufferSpillThreshold

    // Start processing.
    child.execute().mapPartitions { stream =>
      new Iterator[InternalRow] {

        // Get all relevant projections.
        val grouping = UnsafeProjection.create(sessionSpec, child.output)
        val getTime = GenerateUnsafeProjection.generate(timeColumn :: Nil, child.output)

        // Manage the stream and the grouping.
        var nextRow: UnsafeRow = null
        var nextGroup: UnsafeRow = null
        var nextRowAvailable: Boolean = false

        // Manage the time window.
        // Considering about windowGap = 3min, the windowResultWithBoundary for partition a in
        // below scenario is [(1, (start: 00:00, end: 00:03)), (3, (start: 00:05, end: 00:10))].
        // The partition b will use a new windowResultWithBoundary to keep tracking
        // IndexWithinPartition and corresponding session window start and end.
        // -------------------------------------------------------------------------------------
        // |   Partition    |   IndexWithinPartition    |              RowValue                |
        // |----------------|---------------------------|--------------|-----------------------|
        // |       -        |             -             |     time     |   session_spec_key    |
        // |----------------|---------------------------|--------------|-----------------------|
        // |        a       |             0             |    00:00     |          a            |
        // |                |---------------------------|--------------|-----------------------|
        // |                |             1             |    00:01     |          a            |
        // |                |---------------------------|--------------|-----------------------|
        // |                |             2             |    00:05     |          a            |
        // |                |---------------------------|--------------|-----------------------|
        // |                |             3             |    00:07     |          a            |
        // |----------------|---------------------------|--------------|-----------------------|
        // |        b       |             0             |    00:00     |          b            |
        //         ...                   ...                  ...                ...
        var rowIndexWithinPartition = 0
        var lastTime: Long = _
        var windowStartTime: Long = _
        var windowResultWithBoundary = mutable.ArrayBuffer.empty[(Int, CreateNamedStruct)]
        var windowResultIndex = 0

        private[this] def addWindowValueAndBoundary(rowIndex: Int) {
          val windowValue = CreateNamedStruct(
            Seq(Literal(WINDOW_START),
              PreciseTimestampConversion(Literal(windowStartTime), LongType, TimestampType),
              Literal(WINDOW_END),
              PreciseTimestampConversion(
                Literal(lastTime + windowGap), LongType, TimestampType)))
          windowResultWithBoundary.append((rowIndex, windowValue))
        }

        private[this] def getTimeFromRow(row: InternalRow) = getTime.apply(row).getLong(0)

        private[this] def fetchNextRow() {
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

        val buffer: ExternalAppendOnlyUnsafeRowArray =
          new ExternalAppendOnlyUnsafeRowArray(inMemoryThreshold, spillThreshold)

        var bufferIterator: Iterator[UnsafeRow] = _

        val windowFunctionResult = new SpecificInternalRow(windowExpressions.map(_.dataType))

        private[this] def fetchNextPartition() {
          // Collect all the rows in the current partition.
          // Before we start to fetch new input rows, make a copy of nextGroup and initialize
          // value of windowStartTime.
          val currentGroup = nextGroup.copy()
          windowStartTime = getTimeFromRow(nextRow)

          // clear last partition
          buffer.clear()

          // clear session window relevant var
          windowResultWithBoundary.clear()
          rowIndexWithinPartition = 0

          while (nextRowAvailable && nextGroup == currentGroup) {
            val nextTime = getTimeFromRow(nextRow)
            if (nextTime - lastTime > windowGap) {
              addWindowValueAndBoundary(rowIndexWithinPartition)
              windowStartTime = nextTime
            }
            buffer.add(nextRow)
            rowIndexWithinPartition += 1
            lastTime = nextTime
            fetchNextRow()
          }

          addWindowValueAndBoundary(rowIndexWithinPartition)
          // Setup iteration
          bufferIterator = buffer.generateIterator()
        }

        // Iteration
        override final def hasNext: Boolean =
          (bufferIterator != null && bufferIterator.hasNext) || nextRowAvailable

        val join = new JoinedRow

        override final def next(): InternalRow = {
          // Load the next partition if we need to.
          if ((bufferIterator == null || !bufferIterator.hasNext) && nextRowAvailable) {
            fetchNextPartition()
            rowIndexWithinPartition = 0
            windowResultIndex = 0
          }

          if (bufferIterator.hasNext) {
            val current = bufferIterator.next()

            rowIndexWithinPartition += 1

            if (rowIndexWithinPartition > windowResultWithBoundary(windowResultIndex)._1) {
              windowResultIndex += 1
            }

            // 'Merge' the input row with the session window struct
            join(current,
              UnsafeProjection.create(
                windowResultWithBoundary(windowResultIndex)._2).apply(InternalRow.empty))
          } else {
            throw new NoSuchElementException
          }
        }
      }
    }
  }
}
