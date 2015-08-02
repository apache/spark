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

import org.apache.spark.sql.execution.{UnsafeKeyValueSorter, UnsafeFixedWidthAggregationMap}
import org.apache.spark.unsafe.KVIterator
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
    unsafeInputRows: Boolean)
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
    outputsUnsafeRows = true) {

  require(groupingKeyAttributes.nonEmpty)

  logInfo(s"Using UnsafeHybridAggregationIterator (output UnsafeRow: true).")

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

  private def processOriginalInput(firstKey: InternalRow, firstValue: InternalRow): KVIterator[InternalRow, InternalRow] = {
    new KVIterator[InternalRow, InternalRow] {
      private[this] var isFirstRow = true

      private[this] var groupingKey: InternalRow = _

      private[this] val buffer = newBuffer

      override def next(): Boolean = {
        initializeBuffer(buffer)
        if (isFirstRow) {
          isFirstRow = false
          groupingKey = firstKey.copy()
          processRow(buffer, firstValue)

          true
        } else if (inputKVIterator.next()) {
          groupingKey = inputKVIterator.getKey()
          processRow(buffer, inputKVIterator.getValue())

          true
        } else {
          false
        }
      }

      override def getKey(): InternalRow = {
        groupingKey
      }

      override def getValue(): InternalRow = {
        buffer
      }

      override def close(): Unit = {
        // Do nothing.
      }
    }
  }

  // we find we cannot alloate memory when processing currentRow, we need to also put it into the sorter.
  private def switchToSortBasedAggregation(groupingKey: UnsafeRow, currentRow: InternalRow): Unit = {
    logInfo("falling back to sort based aggregation.")
    // TODO: Step 1: Sort the map entries.
    val externalSorter: UnsafeKeyValueSorter = null // buffers.externalSorter

    // TODO: Step 2: Spill the sorted map entries.
    // val spillHandle = sortedAggregationBufferMapIterator.spill()

    // Step3: Free the memory used by the map.
    buffers.free()

    // TODO: Step 4: If we have aggregate function with mode Partial or Complete,
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
      processOriginalInput(groupingKey, currentRow)
    } else {
      inputKVIterator
    }

    // TODO: Step 8: Direct processedIterator to sortBasedInputIter.
    while (processedIterator.next()) {
      // externalSorter.insert(processedIterator.getKey(), processedIterator.getValue())
    }

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
      groupingKeyAttributes ++
        allAggregateExpressions.flatMap(_.aggregateFunction.bufferAttributes)

    sortBased = true

    // externalSorter.kvIterator
    val sortedKVIterator: KVIterator[UnsafeRow, UnsafeRow] = null
    sortBasedAggregationIterator = SortBasedAggregationIterator.createFromKVIterator(
      groupingKeyAttributes = groupingKeyAttributes,
      valueAttributes = newResultExpressions,
      inputKVIterator = null,
      nonCompleteAggregateExpressions = newNonCompleteAggregateExpressions,
      nonCompleteAggregateAttributes = newNonCompleteAggregateAttributes,
      completeAggregateExpressions = Nil,
      completeAggregateAttributes = Nil,
      initialInputBufferOffset = 0,
      resultExpressions = newResultExpressions,
      newMutableProjection,
      UnsafeProjection.create(_, _),
      true)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Methods used to initialize this iterator.
  ///////////////////////////////////////////////////////////////////////////

  /** Starts to read input rows and falls back to sort-based aggregation if necessary. */
  private def initialize(): Unit = {
    while (inputKVIterator.next() && !sortBased) {
      val groupingKey = inputKVIterator.getKey()
      val currentRow = inputKVIterator.getValue()
      val buffer = buffers.getAggregationBuffer(groupingKey)
      if (buffer == null) {
        // buffer == null means that we could not allocate more memory.
        // Now, we need to spill the map and switch to sort-based aggregation.
        switchToSortBasedAggregation(groupingKey, currentRow)
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
      unsafeInputRows: Boolean): UnsafeHybridAggregationIterator = {
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
      unsafeInputRows)
  }

  def createFromKVIterator(
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
      unsafeInputRows: Boolean): UnsafeHybridAggregationIterator = {
    new UnsafeHybridAggregationIterator(
      groupingKeyAttributes,
      valueAttributes,
      inputKVIterator,
      nonCompleteAggregateExpressions,
      nonCompleteAggregateAttributes,
      completeAggregateExpressions,
      completeAggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      newMutableProjection,
      unsafeInputRows)
  }
}
