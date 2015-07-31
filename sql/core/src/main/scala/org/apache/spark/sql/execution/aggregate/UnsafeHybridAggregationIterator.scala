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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.StructType

/**
 * An iterator used to evaluate [[AggregateFunction2]].
 * It first tries to use in-memory hash-based aggregation. If we cannot allocate more
 * space for the hash map, we spill the sorted map entries, free the map, and then
 * switch to sort-based aggregation.
 */
class UnsafeHybridAggregationIterator(
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

  require(groupingExpressions.nonEmpty)

  logInfo("Using UnsafeHybridAggregationIterator.")

  ///////////////////////////////////////////////////////////////////////////
  // Unsafe Aggregation buffers
  ///////////////////////////////////////////////////////////////////////////

  // This is the Unsafe Aggregation Map used to store all buffers.
  private[this] val buffers = new UnsafeFixedWidthAggregationMap(
    newBuffer,
    StructType.fromAttributes(allAggregateFunctions.flatMap(_.bufferAttributes)),
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get.taskMemoryManager(),
    1024 * 16, // initial capacity
    SparkEnv.get.conf.getSizeAsBytes("spark.buffer.pageSize", "64m"),
    false // disable tracking of performance metrics
  )

  override protected def newBuffer: MutableRow = {
    val bufferRowSize: Int = allAggregateFunctions.map(_.bufferSchema.length).sum
    val projection =
      UnsafeProjection.create(allAggregateFunctions.flatMap(_.bufferAttributes).map(_.dataType))
    // We use a mutable row and a mutable projection at here since we need to fill in
    // buffer values for those nonAlgebraicAggregateFunctions manually.
    val buffer = new GenericMutableRow(bufferRowSize)
    initializeBuffer(buffer)
    projection.apply(buffer)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Iterator's public methods
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean =
    sortedInputHasNewGroup || aggregationBufferMapIterator.hasNext

  // This can be abstract
  override final def next(): InternalRow = {
    if (hasNext) {
      val result = if (sortedInputHasNewGroup) {
        // Process the current group.
        processCurrentSortedGroup()
        // Generate output row for the current group.
        val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
        // Initialize buffer values for the next group.
        initializeBuffer(sortBasedAggregationBuffer)

        outputRow
      } else {
        // We did not fall back to the sort-based aggregation.
        val currentGroup = aggregationBufferMapIterator.next()
        generateOutput(currentGroup.key, currentGroup.value)
      }

      if (hasNext) {
        result
      } else {
        val resultCopy = result.copy()
        // free is idempotent. So, we can just call it even if we have called it
        // once when we fall back to the sort-based aggregation.
        buffers.free()
        resultCopy
      }
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Methods used to initialize this iterator.
  ///////////////////////////////////////////////////////////////////////////

  // we find we cannot alloate memory when processing currentRow, we need to also put it into the sorter.
  private def switchToSortBasedAggregation(currentRow: InternalRow): Unit = {
    logInfo("falling back to sort based aggregation.")
    // TODO: Step 1: Sort the map entries.
    // val sortedAggregationBufferMapIterator = MapSorter.doSort(aggregationBufferMapIterator)

    // TODO: Step 2: Spill the sorted map entries.
    // val spillHandle = sortedAggregationBufferMapIterator.spill()

    // Step3: Free the memory used by the map.
    buffers.free()

    // TODO: Step 4: Create a external sorter
    // val externalSorter = create external sorter

    // TODO: Step 5: Add the spilled map to the sorter.
    // externalSorter.addSpill(spillHandle)

    // TODO: Step 6: Switch to sort-based aggregation
    aggregationMode match {
      case (Some(Partial), None) | (None, Some(Complete)) =>
        // We need to get aggregation buffer for those functions with mode partial and complete.
      case (Some(Final), Some(Complete)) =>
        // We need to get aggregation buffer for those functions with mode complete
      case _ =>
        // First, if necessary, create a UnsafeProjection to convert the input row to
        // unsafe.
        // val convertToUnsafe = UnsafeProjection.create(inputAttributes.map(_.dataType).toArray)
        // externalSorter.insert(currentRow)
        // externalSorter.insertAll(inputIter)
    }

    // TODO: Step 7: Get the iterator of the external sorter
    // sortBasedInputIter = externalSorter.iterator

    // Step 8: Initialize sort-based aggregation
    initializeBuffer(sortBasedAggregationBuffer)
    val currentRow = sortBasedInputIter.next()
    nextGroupingKey = groupGenerator(currentRow)
    firstRowInNextGroup = currentRow.copy()
    sortedInputHasNewGroup = true
  }

  /** Starts to read input rows and falls back to sort-based aggregation if necessary. */
  private def initialize(): Unit = {
    while (inputIter.hasNext && !sortedInputHasNewGroup) {
      val currentRow = inputIter.next()
      val groupingKey = groupGenerator(currentRow)
      val buffer = buffers.getAggregationBuffer(groupingKey)
      if (buffer == null) {
        // buffer == null means that we could not allocate more memory.
        // Now, we need to spill the map and switch to sort-based aggregation.
        switchToSortBasedAggregation(currentRow)
      } else {
        processRow(buffer, currentRow)
      }
    }
  }

  // This is the starting point of this iterator.
  initialize()

  // Creates the iterator for the Hash Aggregation Map after we have populated
  // contents of that map.
  private[this] val aggregationBufferMapIterator = buffers.iterator()
}
