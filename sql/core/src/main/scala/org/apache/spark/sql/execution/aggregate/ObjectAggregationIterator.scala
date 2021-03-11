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
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.KVIterator

class ObjectAggregationIterator(
    partIndex: Int,
    outputAttributes: Seq[Attribute],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    originalInputAttributes: Seq[Attribute],
    inputRows: Iterator[InternalRow],
    fallbackCountThreshold: Int,
    numOutputRows: SQLMetric)
  extends AggregationIterator(
    partIndex,
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) with Logging {

  // Indicates whether we have fallen back to sort-based aggregation or not.
  private[this] var sortBased: Boolean = false

  private[this] var aggBufferIterator: Iterator[AggregationBufferEntry] = _

  // Hacking the aggregation mode to call AggregateFunction.merge to merge two aggregation buffers
  private val mergeAggregationBuffers: (InternalRow, InternalRow) => Unit = {
    val newExpressions = aggregateExpressions.map {
      case agg @ AggregateExpression(_, Partial, _, _, _) =>
        agg.copy(mode = PartialMerge)
      case agg @ AggregateExpression(_, Complete, _, _, _) =>
        agg.copy(mode = Final)
      case other => other
    }
    val newFunctions = initializeAggregateFunctions(newExpressions, 0)
    val newInputAttributes = newFunctions.flatMap(_.inputAggBufferAttributes)
    generateProcessRow(newExpressions, newFunctions, newInputAttributes)
  }

  /**
   * Start processing input rows.
   */
  processInputs()

  override final def hasNext: Boolean = {
    aggBufferIterator.hasNext
  }

  override final def next(): UnsafeRow = {
    val entry = aggBufferIterator.next()
    val res = generateOutput(entry.groupingKey, entry.aggregationBuffer)
    numOutputRows += 1
    res
  }

  /**
   * Generate an output row when there is no input and there is no grouping expression.
   */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    if (groupingExpressions.isEmpty) {
      val defaultAggregationBuffer = createNewAggregationBuffer()
      generateOutput(UnsafeRow.createFromByteArray(0, 0), defaultAggregationBuffer)
    } else {
      throw new IllegalStateException(
        "This method should not be called when groupingExpressions is not empty.")
    }
  }

  // Creates a new aggregation buffer and initializes buffer values. This function should only be
  // called under two cases:
  //
  //  - when creating aggregation buffer for a new group in the hash map, and
  //  - when creating the re-used buffer for sort-based aggregation
  private def createNewAggregationBuffer(): SpecificInternalRow = {
    val bufferFieldTypes = aggregateFunctions.flatMap(_.aggBufferAttributes.map(_.dataType))
    val buffer = new SpecificInternalRow(bufferFieldTypes)
    initAggregationBuffer(buffer)
    buffer
  }

  private def initAggregationBuffer(buffer: SpecificInternalRow): Unit = {
    // Initializes declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initializes imperative aggregates' buffer values
    aggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
  }

  private def getAggregationBufferByKey(
    hashMap: ObjectAggregationMap, groupingKey: UnsafeRow): InternalRow = {
    var aggBuffer = hashMap.getAggregationBuffer(groupingKey)

    if (aggBuffer == null) {
      aggBuffer = createNewAggregationBuffer()
      hashMap.putAggregationBuffer(groupingKey.copy(), aggBuffer)
    }

    aggBuffer
  }

  // This function is used to read and process input rows. When processing input rows, it first uses
  // hash-based aggregation by putting groups and their buffers in `hashMap`. If `hashMap` grows too
  // large, it sorts the contents, spills them to disk, and creates a new map. At last, all sorted
  // spills are merged together for sort-based aggregation.
  private def processInputs(): Unit = {
    // In-memory map to store aggregation buffer for hash-based aggregation.
    val hashMap = new ObjectAggregationMap()

    // If in-memory map is unable to stores all aggregation buffer, fallback to sort-based
    // aggregation backed by sorted physical storage.
    var sortBasedAggregationStore: SortBasedAggregator = null

    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      val groupingKey = groupingProjection.apply(null)
      val buffer: InternalRow = getAggregationBufferByKey(hashMap, groupingKey)
      while (inputRows.hasNext) {
        processRow(buffer, inputRows.next())
      }
    } else {
      while (inputRows.hasNext && !sortBased) {
        val newInput = inputRows.next()
        val groupingKey = groupingProjection.apply(newInput)
        val buffer: InternalRow = getAggregationBufferByKey(hashMap, groupingKey)
        processRow(buffer, newInput)

        // The hash map gets too large, makes a sorted spill and clear the map.
        if (hashMap.size >= fallbackCountThreshold) {
          logInfo(
            s"Aggregation hash map size ${hashMap.size} reaches threshold " +
              s"capacity ($fallbackCountThreshold entries), spilling and falling back to sort" +
              " based aggregation. You may change the threshold by adjust option " +
              SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key
          )

          // Falls back to sort-based aggregation
          sortBased = true

        }
      }

      if (sortBased) {
        val sortIteratorFromHashMap = hashMap
          .dumpToExternalSorter(groupingAttributes, aggregateFunctions)
          .sortedIterator()
        sortBasedAggregationStore = new SortBasedAggregator(
          sortIteratorFromHashMap,
          StructType.fromAttributes(originalInputAttributes),
          StructType.fromAttributes(groupingAttributes),
          processRow,
          mergeAggregationBuffers,
          createNewAggregationBuffer())

        while (inputRows.hasNext) {
          // NOTE: The input row is always UnsafeRow
          val unsafeInputRow = inputRows.next().asInstanceOf[UnsafeRow]
          val groupingKey = groupingProjection.apply(unsafeInputRow)
          sortBasedAggregationStore.addInput(groupingKey, unsafeInputRow)
        }
      }
    }

    if (sortBased) {
      aggBufferIterator = sortBasedAggregationStore.destructiveIterator()
    } else {
      aggBufferIterator = hashMap.iterator
    }
  }
}

/**
 * A class used to handle sort-based aggregation, used together with [[ObjectHashAggregateExec]].
 *
 * @param initialAggBufferIterator iterator that points to sorted input aggregation buffers
 * @param inputSchema  The schema of input row
 * @param groupingSchema The schema of grouping key
 * @param processRow  Function to update the aggregation buffer with input rows
 * @param mergeAggregationBuffers Function used to merge the input aggregation buffers into existing
 *                                aggregation buffers
 * @param makeEmptyAggregationBuffer Creates an empty aggregation buffer
 *
 * @todo Try to eliminate this class by refactor and reuse code paths in [[SortAggregateExec]].
 */
class SortBasedAggregator(
    initialAggBufferIterator: KVIterator[UnsafeRow, UnsafeRow],
    inputSchema: StructType,
    groupingSchema: StructType,
    processRow: (InternalRow, InternalRow) => Unit,
    mergeAggregationBuffers: (InternalRow, InternalRow) => Unit,
    makeEmptyAggregationBuffer: => InternalRow) {

  // external sorter to sort the input (grouping key + input row) with grouping key.
  private val inputSorter = createExternalSorterForInput()
  private val groupingKeyOrdering: BaseOrdering = GenerateOrdering.create(groupingSchema)

  def addInput(groupingKey: UnsafeRow, inputRow: UnsafeRow): Unit = {
    inputSorter.insertKV(groupingKey, inputRow)
  }

  /**
   * Returns a destructive iterator of AggregationBufferEntry.
   * Notice: it is illegal to call any method after `destructiveIterator()` has been called.
   */
  def destructiveIterator(): Iterator[AggregationBufferEntry] = {
    new Iterator[AggregationBufferEntry] {
      val inputIterator = inputSorter.sortedIterator()
      var hasNextInput: Boolean = inputIterator.next()
      var hasNextAggBuffer: Boolean = initialAggBufferIterator.next()
      private var result: AggregationBufferEntry = _
      private var groupingKey: UnsafeRow = _

      override def hasNext(): Boolean = {
        result != null || findNextSortedGroup()
      }

      override def next(): AggregationBufferEntry = {
        val returnResult = result
        result = null
        returnResult
      }

      // Two-way merges initialAggBufferIterator and inputIterator
      private def findNextSortedGroup(): Boolean = {
        if (hasNextInput || hasNextAggBuffer) {
          // Find smaller key of the initialAggBufferIterator and initialAggBufferIterator
          groupingKey = findGroupingKey()
          result = new AggregationBufferEntry(groupingKey, makeEmptyAggregationBuffer)

          // Firstly, update the aggregation buffer with input rows.
          while (hasNextInput &&
            groupingKeyOrdering.compare(inputIterator.getKey, groupingKey) == 0) {
            processRow(result.aggregationBuffer, inputIterator.getValue)
            hasNextInput = inputIterator.next()
          }

          // Secondly, merge the aggregation buffer with existing aggregation buffers.
          // NOTE: the ordering of these two while-block matter, mergeAggregationBuffer() should
          // be called after calling processRow.
          while (hasNextAggBuffer &&
            groupingKeyOrdering.compare(initialAggBufferIterator.getKey, groupingKey) == 0) {
            mergeAggregationBuffers(result.aggregationBuffer, initialAggBufferIterator.getValue)
            hasNextAggBuffer = initialAggBufferIterator.next()
          }

          true
        } else {
          false
        }
      }

      private def findGroupingKey(): UnsafeRow = {
        var newGroupingKey: UnsafeRow = null
        if (!hasNextInput) {
          newGroupingKey = initialAggBufferIterator.getKey
        } else if (!hasNextAggBuffer) {
          newGroupingKey = inputIterator.getKey
        } else {
          val compareResult =
            groupingKeyOrdering.compare(inputIterator.getKey, initialAggBufferIterator.getKey)
          if (compareResult <= 0) {
            newGroupingKey = inputIterator.getKey
          } else {
            newGroupingKey = initialAggBufferIterator.getKey
          }
        }

        if (groupingKey == null) {
          groupingKey = newGroupingKey.copy()
        } else {
          groupingKey.copyFrom(newGroupingKey)
        }
        groupingKey
      }
    }
  }

  private def createExternalSorterForInput(): UnsafeKVExternalSorter = {
    new UnsafeKVExternalSorter(
      groupingSchema,
      inputSchema,
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      TaskContext.get().taskMemoryManager().pageSizeBytes,
      SparkEnv.get.conf.get(config.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD),
      null
    )
  }
}
