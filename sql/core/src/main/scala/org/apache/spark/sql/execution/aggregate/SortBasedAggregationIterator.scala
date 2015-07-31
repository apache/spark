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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression2, AggregateFunction2}

/**
 * An iterator used to evaluate [[AggregateFunction2]]. It assumes the input rows are sorted
 * by values of [[groupingExpressions]].
 */
class SortBasedAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    nonCompleteAggregateExpressions: Seq[AggregateExpression2],
    nonCompleteAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    newProjection: (Seq[Expression], Seq[Attribute]) => Projection,
    newOrdering: (Seq[SortOrder], Seq[Attribute]) => Ordering[InternalRow],
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow])
  extends AggregationIterator(
    groupingExpressions,
    nonCompleteAggregateExpressions,
    nonCompleteAggregateAttributes,
    completeAggregateExpressions,
    completeAggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection,
    newProjection,
    newOrdering,
    inputAttributes,
    inputIter) {

  logInfo("Using SortBasedAggregationIterator.")

  override protected def newBuffer: MutableRow = {
    val bufferRowSize: Int = allAggregateFunctions.map(_.bufferSchema.length).sum
    // We use a mutable row and a mutable projection at here since we need to fill in
    // buffer values for those nonAlgebraicAggregateFunctions manually.
    val buffer = new GenericMutableRow(bufferRowSize)
    buffer
  }


  ///////////////////////////////////////////////////////////////////////////
  // Mutable states for sort based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // The partition key of the current partition.
  protected var currentGroupingKey: InternalRow = _

  // The partition key of next partition.
  protected var nextGroupingKey: InternalRow = _

  // The first row of next partition.
  protected var firstRowInNextGroup: InternalRow = _

  // Indicates if we has new group of rows from the sorted input iterator
  protected var sortedInputHasNewGroup: Boolean = false

  // The sorted input iterator.
  protected var sortBasedInputIter: Iterator[InternalRow] = inputIter

  // The aggregation buffer used by the sort-based aggregation.
  protected var sortBasedAggregationBuffer: MutableRow = newBuffer

  /** Processes rows in the current group. It will stop when it find a new group. */
  protected def processCurrentSortedGroup(): Unit = {
    currentGroupingKey = nextGroupingKey
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    processRow(sortBasedAggregationBuffer, firstRowInNextGroup)
    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    while (sortBasedInputIter.hasNext && !findNextPartition) {
      val currentRow = sortBasedInputIter.next()
      // Get the grouping key based on the grouping expressions.
      // For the below compare method, we do not need to make a copy of groupingKey.
      val groupingKey = groupGenerator(currentRow)
      // Check if the current row belongs the current input row.
      if (currentGroupingKey == groupingKey) {
        processRow(sortBasedAggregationBuffer, currentRow)
      } else {
        // We find a new group.
        findNextPartition = true
        nextGroupingKey = groupingKey
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

  override final def next(): InternalRow = {
    if (hasNext) {
      // Process the current group.
      processCurrentSortedGroup()
      // Generate output row for the current group.
      val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
      // Initialize buffer values for the next group.
      initializeBuffer(sortBasedAggregationBuffer)

      outputRow
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  protected def initialize(): Unit = {
    if (inputIter.hasNext) {
      initializeBuffer(sortBasedAggregationBuffer)
      val currentRow = inputIter.next()
      nextGroupingKey = groupGenerator(currentRow)
      firstRowInNextGroup = currentRow.copy()
      sortedInputHasNewGroup = true
    } else {
      // This inputIter is empty.
      sortedInputHasNewGroup = false
    }
  }

  initialize()

  def generateResultForEmptyInput(): InternalRow = {
    initializeBuffer(sortBasedAggregationBuffer)
    generateOutput(groupGenerator(new GenericInternalRow(inputAttributes.length)), sortBasedAggregationBuffer)
  }
}
