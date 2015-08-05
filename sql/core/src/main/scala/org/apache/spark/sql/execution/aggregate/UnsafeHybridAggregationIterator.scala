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

import org.apache.spark.unsafe.KVIterator
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.{UnsafeKVExternalSorter, UnsafeFixedWidthAggregationMap}
import org.apache.spark.sql.types.StructType

/**
 * An iterator used to evaluate [[AggregateFunction2]].
 * It first tries to use in-memory hash-based aggregation. If we cannot allocate more
 * space for the hash map, we spill the sorted map entries, free the map, and then
 * switch to sort-based aggregation.
 */
class UnsafeHybridAggregationIterator(
    groupingKeyAttributes: Seq[Attribute],
    valueAttributes: Seq[Attribute],
    inputKVIterator: KVIterator[UnsafeRow, InternalRow],
    nonCompleteAggregateExpressions: Seq[AggregateExpression2],
    nonCompleteAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    outputsUnsafeRows: Boolean)
  extends AggregationIterator(
    groupingKeyAttributes,
    valueAttributes,
    nonCompleteAggregateExpressions,
    nonCompleteAggregateAttributes,
    completeAggregateExpressions,
    completeAggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection,
    outputsUnsafeRows) {

  require(groupingKeyAttributes.nonEmpty)

  ///////////////////////////////////////////////////////////////////////////
  // Unsafe Aggregation buffers
  ///////////////////////////////////////////////////////////////////////////

  // This is the Unsafe Aggregation Map used to store all buffers.
  private[this] val buffers = new UnsafeFixedWidthAggregationMap(
    newBuffer,
    StructType.fromAttributes(allAggregateFunctions.flatMap(_.bufferAttributes)),
    StructType.fromAttributes(groupingKeyAttributes),
    TaskContext.get.taskMemoryManager(),
    SparkEnv.get.shuffleMemoryManager,
    1024 * 16, // initial capacity
    SparkEnv.get.conf.getSizeAsBytes("spark.buffer.pageSize", "64m"),
    false // disable tracking of performance metrics
  )

  override protected def newBuffer: UnsafeRow = {
    val bufferSchema = allAggregateFunctions.flatMap(_.bufferAttributes)
    val bufferRowSize: Int = bufferSchema.length

    val genericMutableBuffer = new GenericMutableRow(bufferRowSize)
    val unsafeProjection =
      UnsafeProjection.create(bufferSchema.map(_.dataType))
    val buffer = unsafeProjection.apply(genericMutableBuffer)
    initializeBuffer(buffer)
    buffer
  }

  ///////////////////////////////////////////////////////////////////////////
  // Methods and variables related to switching to sort-based aggregation
  ///////////////////////////////////////////////////////////////////////////
  private[this] var sortBased = false

  private[this] var sortBasedAggregationIterator: SortBasedAggregationIterator = _

  // The value part of the input KV iterator is used to store original input values of
  // aggregate functions, we need to convert them to aggregation buffers.
  private def processOriginalInput(
      firstKey: UnsafeRow,
      firstValue: InternalRow): KVIterator[UnsafeRow, UnsafeRow] = {
    new KVIterator[UnsafeRow, UnsafeRow] {
      private[this] var isFirstRow = true

      private[this] var groupingKey: UnsafeRow = _

      private[this] val buffer: UnsafeRow = newBuffer

      override def next(): Boolean = {
        initializeBuffer(buffer)
        if (isFirstRow) {
          isFirstRow = false
          groupingKey = firstKey
          processRow(buffer, firstValue)

          true
        } else if (inputKVIterator.next()) {
          groupingKey = inputKVIterator.getKey()
          val value = inputKVIterator.getValue()
          processRow(buffer, value)

          true
        } else {
          false
        }
      }

      override def getKey(): UnsafeRow = {
        groupingKey
      }

      override def getValue(): UnsafeRow = {
        buffer
      }

      override def close(): Unit = {
        // Do nothing.
      }
    }
  }

  // The value of the input KV Iterator has the format of groupingExprs + aggregation buffer.
  // We need to project the aggregation buffer out.
  private def projectInputBufferToUnsafe(
      firstKey: UnsafeRow,
      firstValue: InternalRow): KVIterator[UnsafeRow, UnsafeRow] = {
    new KVIterator[UnsafeRow, UnsafeRow] {
      private[this] var isFirstRow = true

      private[this] var groupingKey: UnsafeRow = _

      private[this] val bufferSchema = allAggregateFunctions.flatMap(_.bufferAttributes)

      private[this] val value: UnsafeRow = {
        val genericMutableRow = new GenericMutableRow(bufferSchema.length)
        UnsafeProjection.create(bufferSchema.map(_.dataType)).apply(genericMutableRow)
      }

      private[this] val projectInputBuffer = {
        newMutableProjection(bufferSchema, valueAttributes)().target(value)
      }

      override def next(): Boolean = {
        if (isFirstRow) {
          isFirstRow = false
          groupingKey = firstKey
          projectInputBuffer(firstValue)

          true
        } else if (inputKVIterator.next()) {
          groupingKey = inputKVIterator.getKey()
          projectInputBuffer(inputKVIterator.getValue())

          true
        } else {
          false
        }
      }

      override def getKey(): UnsafeRow = {
        groupingKey
      }

      override def getValue(): UnsafeRow = {
        value
      }

      override def close(): Unit = {
        // Do nothing.
      }
    }
  }

  /**
   * We need to fall back to sort based aggregation because we do not have enough memory
   * for our in-memory hash map (i.e. `buffers`).
   */
  private def switchToSortBasedAggregation(
      currentGroupingKey: UnsafeRow,
      currentRow: InternalRow): Unit = {
    logInfo("falling back to sort based aggregation.")

    // Step 1: Get the ExternalSorter containing entries of the map.
    val externalSorter = buffers.destructAndCreateExternalSorter()

    // Step 2: Free the memory used by the map.
    buffers.free()

    // Step 3: If we have aggregate function with mode Partial or Complete,
    // we need to process them to get aggregation buffer.
    // So, later in the sort-based aggregation iterator, we can do merge.
    // If aggregate functions are with mode Final and PartialMerge,
    // we just need to project the aggregation buffer from the input.
    val needsProcess = aggregationMode match {
      case (Some(Partial), None) => true
      case (None, Some(Complete)) => true
      case (Some(Final), Some(Complete)) => true
      case _ => false
    }

    val processedIterator = if (needsProcess) {
      processOriginalInput(currentGroupingKey, currentRow)
    } else {
      // The input value's format is groupingExprs + buffer.
      // We need to project the buffer part out.
      projectInputBufferToUnsafe(currentGroupingKey, currentRow)
    }

    // Step 4: Redirect processedIterator to externalSorter.
    while (processedIterator.next()) {
      externalSorter.insertKV(processedIterator.getKey(), processedIterator.getValue())
    }

    // Step 5: Get the sorted iterator from the externalSorter.
    val sortedKVIterator: UnsafeKVExternalSorter#KVSorterIterator = externalSorter.sortedIterator()

    // Step 6: We now create a SortBasedAggregationIterator based on sortedKVIterator.
    // For a aggregate function with mode Partial, its mode in the SortBasedAggregationIterator
    // will be PartialMerge. For a aggregate function with mode Complete,
    // its mode in the SortBasedAggregationIterator will be Final.
    val newNonCompleteAggregateExpressions = allAggregateExpressions.map {
        case AggregateExpression2(func, Partial, isDistinct) =>
          AggregateExpression2(func, PartialMerge, isDistinct)
        case AggregateExpression2(func, Complete, isDistinct) =>
          AggregateExpression2(func, Final, isDistinct)
        case other => other
      }
    val newNonCompleteAggregateAttributes =
      nonCompleteAggregateAttributes ++ completeAggregateAttributes

    val newValueAttributes =
      allAggregateExpressions.flatMap(_.aggregateFunction.cloneBufferAttributes)

    sortBasedAggregationIterator = SortBasedAggregationIterator.createFromKVIterator(
      groupingKeyAttributes = groupingKeyAttributes,
      valueAttributes = newValueAttributes,
      inputKVIterator = sortedKVIterator.asInstanceOf[KVIterator[InternalRow, InternalRow]],
      nonCompleteAggregateExpressions = newNonCompleteAggregateExpressions,
      nonCompleteAggregateAttributes = newNonCompleteAggregateAttributes,
      completeAggregateExpressions = Nil,
      completeAggregateAttributes = Nil,
      initialInputBufferOffset = 0,
      resultExpressions = resultExpressions,
      newMutableProjection = newMutableProjection,
      outputsUnsafeRows = outputsUnsafeRows)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Methods used to initialize this iterator.
  ///////////////////////////////////////////////////////////////////////////

  /** Starts to read input rows and falls back to sort-based aggregation if necessary. */
  protected def initialize(): Unit = {
    var hasNext = inputKVIterator.next()
    while (!sortBased && hasNext) {
      val groupingKey = inputKVIterator.getKey()
      val currentRow = inputKVIterator.getValue()
      val buffer = buffers.getAggregationBuffer(groupingKey)
      if (buffer == null) {
        // buffer == null means that we could not allocate more memory.
        // Now, we need to spill the map and switch to sort-based aggregation.
        switchToSortBasedAggregation(groupingKey, currentRow)
        sortBased = true
      } else {
        processRow(buffer, currentRow)
        hasNext = inputKVIterator.next()
      }
    }
  }

  // This is the starting point of this iterator.
  initialize()

  // Creates the iterator for the Hash Aggregation Map after we have populated
  // contents of that map.
  private[this] val aggregationBufferMapIterator = buffers.iterator()

  private[this] var _mapIteratorHasNext = false

  // Pre-load the first key-value pair from the map to make hasNext idempotent.
  if (!sortBased) {
    _mapIteratorHasNext = aggregationBufferMapIterator.next()
    // If the map is empty, we just free it.
    if (!_mapIteratorHasNext) {
      buffers.free()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Iterator's public methods
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = {
    (sortBased && sortBasedAggregationIterator.hasNext) || (!sortBased && _mapIteratorHasNext)
  }


  override final def next(): InternalRow = {
    if (hasNext) {
      if (sortBased) {
        sortBasedAggregationIterator.next()
      } else {
        // We did not fall back to the sort-based aggregation.
        val result =
          generateOutput(
            aggregationBufferMapIterator.getKey,
            aggregationBufferMapIterator.getValue)
        // Pre-load next key-value pair form aggregationBufferMapIterator.
        _mapIteratorHasNext = aggregationBufferMapIterator.next()

        if (!_mapIteratorHasNext) {
          val resultCopy = result.copy()
          buffers.free()
          resultCopy
        } else {
          result
        }
      }
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }
}

object UnsafeHybridAggregationIterator {
  // scalastyle:off
  def createFromInputIterator(
      groupingExprs: Seq[NamedExpression],
      nonCompleteAggregateExpressions: Seq[AggregateExpression2],
      nonCompleteAggregateAttributes: Seq[Attribute],
      completeAggregateExpressions: Seq[AggregateExpression2],
      completeAggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
      inputAttributes: Seq[Attribute],
      inputIter: Iterator[InternalRow],
      outputsUnsafeRows: Boolean): UnsafeHybridAggregationIterator = {
    new UnsafeHybridAggregationIterator(
      groupingExprs.map(_.toAttribute),
      inputAttributes,
      AggregationIterator.unsafeKVIterator(groupingExprs, inputAttributes, inputIter),
      nonCompleteAggregateExpressions,
      nonCompleteAggregateAttributes,
      completeAggregateExpressions,
      completeAggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      newMutableProjection,
      outputsUnsafeRows)
  }
  // scalastyle:on
}
