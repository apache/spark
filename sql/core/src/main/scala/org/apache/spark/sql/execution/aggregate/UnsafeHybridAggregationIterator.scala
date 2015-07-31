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
    newProjection: (Seq[Expression], Seq[Attribute]) => Projection, // It's UnsafeProjection.
    newOrdering: (Seq[SortOrder], Seq[Attribute]) => Ordering[InternalRow],
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow],
    unsafeInput: Boolean)
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
  // Methods and variables related to switching to sort-based aggregation
  ///////////////////////////////////////////////////////////////////////////
  private[this] var sortBased = false

  private[this] var sortBasedAggregationIterator: SortBasedAggregationIterator = _

  private def processOriginalInput(firstRow: InternalRow): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private[this] var isFirstRow = true

      private[this] val buffer = newBuffer

      // TODO: We should use UnsafeConcat.
      private[this] val joinedRow = new JoinedRow()

      private[this] val convertToUnsafe =
        UnsafeProjection.create(resultExpressions.map(_.dataType).toArray)

      override def hasNext: Boolean = isFirstRow || inputIter.hasNext

      override def next(): InternalRow = {
        val rowToBeProcessed = if (isFirstRow) {
          isFirstRow = false
          firstRow.copy()
        } else if (inputIter.hasNext) {
          inputIter.next()
        } else {
          // no more result
          throw new NoSuchElementException
        }
        val groupingKey = groupGenerator(rowToBeProcessed)
        initializeBuffer(buffer)
        processRow(buffer, rowToBeProcessed)

        // TODO: Use unsafe concat at here to avoid use UnsafeProjection + JoinedRow.
        convertToUnsafe(joinedRow(groupingKey, buffer))
      }
    }
  }

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

    // TODO: Step 6: Get the iterator of the external sorter
    val sortBasedInputIter: Iterator[InternalRow] = null
    // sortBasedInputIter = externalSorter.iterator

    // TODO: Step 7: If we have aggregate function with mode Partial or Complete,
    // we need to process them to get aggregation buffer.
    // So, later in the sort-based aggregation iterator, we can do merge.
    // Also, when we do not need to process input rows but input rows are not
    // UnsafeRows, we need to add a conversion.
    val needsProcess = aggregationMode match {
      case (Some(Partial), None) => true
      case (None, Some(Complete)) => true
      case (Some(Final), Some(Complete)) => true
      case _ => false
    }

    val processedIterator = if (needsProcess) {
      // Rows returned by this
      processOriginalInput(currentRow)
    } else if (!unsafeInput) {
      new Iterator[InternalRow] {
        val convertToUnsafe = UnsafeProjection.create(inputAttributes.map(_.dataType).toArray)

        override def hasNext: Boolean = inputIter.hasNext

        override def next(): InternalRow = {
          if (hasNext) {
            convertToUnsafe(inputIter.next())
          } else {
            // no more result
            throw new NoSuchElementException
          }
        }
      }
    } else {
      inputIter
    }

    // TODO: Step 8: Direct processedIterator to sortBasedInputIter.

    // Step 9: sortBasedAggregationIterator.

    val newNonCompleteAggregateExpressions = allAggregateExpressions.map {
        case AggregateExpression2(func, Partial, isDistinct) =>
          AggregateExpression2(func, PartialMerge, isDistinct)
        case AggregateExpression2(func, Complete, isDistinct) =>
          AggregateExpression2(func, Final, isDistinct)
        case other => other
      }
    val newNonCompleteAggregateAttributes =
      nonCompleteAggregateAttributes ++ completeAggregateAttributes

    val newResultExpressions =
      groupingExpressions.map(_.toAttribute) ++
        allAggregateExpressions.flatMap(_.aggregateFunction.bufferAttributes)

    sortBased = true
    sortBasedAggregationIterator = new SortBasedAggregationIterator(
      groupingExpressions = groupingExpressions,
      nonCompleteAggregateExpressions = newNonCompleteAggregateExpressions,
      nonCompleteAggregateAttributes = newNonCompleteAggregateAttributes,
      completeAggregateExpressions = Nil,
      completeAggregateAttributes = Nil,
      initialInputBufferOffset = groupingExpressions.length,
      resultExpressions = newResultExpressions,
      newMutableProjection = newMutableProjection,
      newProjection = UnsafeProjection.create,
      newOrdering = newOrdering,
      inputAttributes = newResultExpressions,
      inputIter = sortBasedInputIter)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Methods used to initialize this iterator.
  ///////////////////////////////////////////////////////////////////////////

  /** Starts to read input rows and falls back to sort-based aggregation if necessary. */
  private def initialize(): Unit = {
    while (inputIter.hasNext && !sortBased) {
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


  ///////////////////////////////////////////////////////////////////////////
  // Iterator's public methods
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean =
    (sortBased && sortBasedAggregationIterator.hasNext) || (!sortBased && aggregationBufferMapIterator.hasNext)

  // This can be abstract
  override final def next(): InternalRow = {
    if (hasNext) {
      val result = if (sortBased) {
        sortBasedAggregationIterator.next()
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
}
