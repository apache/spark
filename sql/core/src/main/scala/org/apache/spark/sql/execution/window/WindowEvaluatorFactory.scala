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

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, InterpretedOrdering, JoinedRow, NamedExpression, SortOrder, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf

class WindowEvaluatorFactory(
    val windowExpression: Seq[NamedExpression],
    val partitionSpec: Seq[Expression],
    val orderSpec: Seq[SortOrder],
    val childOutput: Seq[Attribute],
    val spillSize: SQLMetric)
  extends PartitionEvaluatorFactory[InternalRow, InternalRow] with WindowEvaluatorFactoryBase {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] = {
    new WindowPartitionEvaluator()
  }

  class WindowPartitionEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    private val conf: SQLConf = SQLConf.get

    // Unwrap the window expressions and window frame factories from the map.
    private val expressions = windowFrameExpressionFactoryPairs.flatMap(_._1)
    private val factories = windowFrameExpressionFactoryPairs.map(_._2).toArray
    private val inMemoryThreshold = conf.windowExecBufferInMemoryThreshold
    private val spillThreshold = conf.windowExecBufferSpillThreshold

    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      val stream = inputs.head
      new Iterator[InternalRow] {

        // Get all relevant projections.
        val result = createResultProjection(expressions)
        val grouping = UnsafeProjection.create(partitionSpec, childOutput)
        val groupEqualityCheck =
          if (partitionSpec.forall(e => UnsafeRowUtils.isBinaryStable(e.dataType))) {
            (key1: UnsafeRow, key2: UnsafeRow) => key1.equals(key2)
          } else {
            val types = partitionSpec.map(_.dataType)
            val ordering = InterpretedOrdering.forSchema(types)
            (key1: UnsafeRow, key2: UnsafeRow) => ordering.compare(key1, key2) == 0
        }

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

        val windowFunctionResult = new SpecificInternalRow(expressions.map(_.dataType))
        val frames = factories.map(_(windowFunctionResult))
        val numFrames = frames.length
        private[this] def fetchNextPartition(): Unit = {
          // Collect all the rows in the current partition.
          // Before we start to fetch new input rows, make a copy of nextGroup.
          val currentGroup = nextGroup.copy()

          // clear last partition
          buffer.clear()

          while (nextRowAvailable && groupEqualityCheck(nextGroup, currentGroup)) {
            buffer.add(nextRow)
            fetchNextRow()
          }

          // Setup the frames.
          var i = 0
          while (i < numFrames) {
            frames(i).prepare(buffer)
            i += 1
          }

          // Setup iteration
          rowIndex = 0
          bufferIterator = buffer.generateIterator()
        }

        // Iteration
        var rowIndex = 0

        override final def hasNext: Boolean = {
          val found = (bufferIterator != null && bufferIterator.hasNext) || nextRowAvailable
          if (!found) {
            // clear final partition
            buffer.clear()
            spillSize += buffer.spillSize
          }
          found
        }

        val join = new JoinedRow
        override final def next(): InternalRow = {
          // Load the next partition if we need to.
          if ((bufferIterator == null || !bufferIterator.hasNext) && nextRowAvailable) {
            fetchNextPartition()
          }

          if (bufferIterator.hasNext) {
            val current = bufferIterator.next()

            // Get the results for the window frames.
            var i = 0
            while (i < numFrames) {
              frames(i).write(rowIndex, current)
              i += 1
            }

            // 'Merge' the input row with the window function result
            join(current, windowFunctionResult)
            rowIndex += 1

            // Return the projection.
            result(join)
          } else {
            throw new NoSuchElementException
          }
        }
      }
    }
  }
}
