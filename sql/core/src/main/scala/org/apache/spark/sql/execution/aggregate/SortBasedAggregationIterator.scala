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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * An iterator used to evaluate [[AggregateFunction]]. It assumes the input rows have been
 * sorted by values of [[groupingExpressions]].
 */
class SortBasedAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    valueAttributes: Seq[Attribute],
    inputIterator: Iterator[InternalRow],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    numOutputRows: SQLMetric)
  extends AggregationIterator(
    groupingExpressions,
    valueAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) {

  /**
   * Creates a new aggregation buffer and initializes buffer values
   * for all aggregate functions.
   */
  private def newBuffer: InternalRow = {
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val bufferRowSize: Int = bufferSchema.length

    val genericMutableBuffer = new GenericInternalRow(bufferRowSize)
    val useUnsafeBuffer = bufferSchema.map(_.dataType).forall(UnsafeRow.isMutable)

    val buffer = if (useUnsafeBuffer) {
      val unsafeProjection =
        UnsafeProjection.create(bufferSchema.map(_.dataType))
      unsafeProjection.apply(genericMutableBuffer)
    } else {
      genericMutableBuffer
    }
    initializeBuffer(buffer)
    buffer
  }

  ///////////////////////////////////////////////////////////////////////////
  // Mutable states for sort based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // The partition key of the current partition.
  private[this] var currentGroupingKey: UnsafeRow = _

  // The partition key of next partition.
  private[this] var nextGroupingKey: UnsafeRow = _

  // The first row of next partition.
  private[this] var firstRowInNextGroup: InternalRow = _

  // Indicates if we has new group of rows from the sorted input iterator
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
  private[this] val sortBasedAggregationBuffer: InternalRow = newBuffer

  // This safe projection is used to turn the input row into safe row. This is necessary
  // because the input row may be produced by unsafe projection in child operator and all the
  // produced rows share one byte array. However, when we update the aggregate buffer according to
  // the input row, we may cache some values from input row, e.g. `Max` will keep the max value from
  // input row via MutableProjection, `CollectList` will keep all values in an array via
  // ImperativeAggregate framework. These values may get changed unexpectedly if the underlying
  // unsafe projection update the shared byte array. By applying a safe projection to the input row,
  // we can cut down the connection from input row to the shared byte array, and thus it's safe to
  // cache values from input row while updating the aggregation buffer.
  private[this] val safeProj: Projection = FromUnsafeProjection(valueAttributes.map(_.dataType))

  protected def initialize(): Unit = {
    if (inputIterator.hasNext) {
      initializeBuffer(sortBasedAggregationBuffer)
      val inputRow = inputIterator.next()
      nextGroupingKey = groupingProjection(inputRow).copy()
      firstRowInNextGroup = inputRow.copy()
      sortedInputHasNewGroup = true
    } else {
      // This inputIter is empty.
      sortedInputHasNewGroup = false
    }
  }

  initialize()

  /** Processes rows in the current group. It will stop when it find a new group. */
  protected def processCurrentSortedGroup(): Unit = {
    currentGroupingKey = nextGroupingKey
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    processRow(sortBasedAggregationBuffer, safeProj(firstRowInNextGroup))

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    while (!findNextPartition && inputIterator.hasNext) {
      // Get the grouping key.
      val currentRow = inputIterator.next()
      val groupingKey = groupingProjection(currentRow)

      // Check if the current row belongs the current input row.
      if (currentGroupingKey == groupingKey) {
        processRow(sortBasedAggregationBuffer, safeProj(currentRow))
      } else {
        // We find a new group.
        findNextPartition = true
        nextGroupingKey = groupingKey.copy()
        firstRowInNextGroup = currentRow.copy()
      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the iter.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Iterator's public methods
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = sortedInputHasNewGroup

  override final def next(): UnsafeRow = {
    if (hasNext) {
      // Process the current group.
      processCurrentSortedGroup()
      // Generate output row for the current group.
      val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
      // Initialize buffer values for the next group.
      initializeBuffer(sortBasedAggregationBuffer)
      numOutputRows += 1
      outputRow
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    initializeBuffer(sortBasedAggregationBuffer)
    generateOutput(UnsafeRow.createFromByteArray(0, 0), sortBasedAggregationBuffer)
  }
}
