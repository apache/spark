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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.unsafe.KVIterator
import org.apache.spark.{InternalAccumulator, Logging, TaskContext}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{UnsafeKVExternalSorter, UnsafeFixedWidthAggregationMap}
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.sql.types.StructType

/**
 * An iterator used to evaluate aggregate functions. It operates on [[UnsafeRow]]s.
 *
 * This iterator first uses hash-based aggregation to process input rows. It uses
 * a hash map to store groups and their corresponding aggregation buffers. If we
 * this map cannot allocate memory from memory manager, it spill the map into disk
 * and create a new one. After processed all the input, then merge all the spills
 * together using external sorter, and do sort-based aggregation.
 *
 * The process has the following step:
 *  - Step 0: Do hash-based aggregation.
 *  - Step 1: Sort all entries of the hash map based on values of grouping expressions and
 *            spill them to disk.
 *  - Step 2: Create a external sorter based on the spilled sorted map entries and reset the map.
 *  - Step 3: Get a sorted [[KVIterator]] from the external sorter.
 *  - Step 4: Repeat step 0 until no more input.
 *  - Step 5: Initialize sort-based aggregation on the sorted iterator.
 * Then, this iterator works in the way of sort-based aggregation.
 *
 * The code of this class is organized as follows:
 *  - Part 1: Initializing aggregate functions.
 *  - Part 2: Methods and fields used by setting aggregation buffer values,
 *            processing input rows from inputIter, and generating output
 *            rows.
 *  - Part 3: Methods and fields used by hash-based aggregation.
 *  - Part 4: Methods and fields used when we switch to sort-based aggregation.
 *  - Part 5: Methods and fields used by sort-based aggregation.
 *  - Part 6: Loads input and process input rows.
 *  - Part 7: Public methods of this iterator.
 *  - Part 8: A utility function used to generate a result when there is no
 *            input and there is no grouping expression.
 *
 * @param groupingExpressions
 *   expressions for grouping keys
 * @param aggregateExpressions
 * [[AggregateExpression]] containing [[AggregateFunction]]s with mode [[Partial]],
 * [[PartialMerge]], or [[Final]].
 * @param aggregateAttributes the attributes of the aggregateExpressions'
 *   outputs when they are stored in the final aggregation buffer.
 * @param resultExpressions
 *   expressions for generating output rows.
 * @param newMutableProjection
 *   the function used to create mutable projections.
 * @param originalInputAttributes
 *   attributes of representing input rows from `inputIter`.
 * @param inputIter
 *   the iterator containing input [[UnsafeRow]]s.
 */
class TungstenAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    originalInputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow],
    testFallbackStartsAt: Option[Int],
    numInputRows: LongSQLMetric,
    numOutputRows: LongSQLMetric,
    dataSize: LongSQLMetric,
    spillSize: LongSQLMetric)
  extends Iterator[UnsafeRow] with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Part 1: Initializing aggregate functions.
  ///////////////////////////////////////////////////////////////////////////

  // Check to make sure we do not have more than three modes in our AggregateExpressions.
  // If we have, users are hitting a bug and we throw an IllegalStateException.
  if (aggregateExpressions.map(_.mode).distinct.length > 2) {
    throw new IllegalStateException(
      s"$aggregateExpressions should have no more than 2 kinds of modes.")
  }

  // Remember spill data size of this task before execute this operator so that we can
  // figure out how many bytes we spilled for this operator.
  private val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

  // Initialize all AggregateFunctions by binding references, if necessary,
  // and setting inputBufferOffset and mutableBufferOffset.
  private def initializeAllAggregateFunctions(
      startingInputBufferOffset: Int,
      sortBased: Boolean): Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = startingInputBufferOffset
    val functions = new Array[AggregateFunction](aggregateExpressions.length)
    var i = 0
    while (i < aggregateExpressions.length) {
      val func = aggregateExpressions(i).aggregateFunction
      val funcWithBoundReferences = aggregateExpressions(i).mode match {
        case Partial | Complete if func.isInstanceOf[ImperativeAggregate] && !sortBased =>
          // We need to create BoundReferences if the function is not an
          // expression-based aggregate function (it does not support code-gen) and the mode of
          // this function is Partial or Complete because we will call eval of this
          // function's children in the update method of this aggregate function.
          // Those eval calls require BoundReferences to work.
          BindReferences.bindReference(func, originalInputAttributes)
        case _ =>
          // We only need to set inputBufferOffset for aggregate functions with mode
          // PartialMerge and Final.
          val updatedFunc = func match {
            case function: ImperativeAggregate =>
              function.withNewInputAggBufferOffset(inputBufferOffset)
            case function => function
          }
          inputBufferOffset += func.aggBufferSchema.length
          updatedFunc
      }
      val funcWithUpdatedAggBufferOffset = funcWithBoundReferences match {
        case function: ImperativeAggregate =>
          // Set mutableBufferOffset for this function. It is important that setting
          // mutableBufferOffset happens after all potential bindReference operations
          // because bindReference will create a new instance of the function.
          function.withNewMutableAggBufferOffset(mutableBufferOffset)
        case function => function
      }
      mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
      functions(i) = funcWithUpdatedAggBufferOffset
      i += 1
    }
    functions
  }

  private[this] val allAggregateFunctions: Array[AggregateFunction] =
    initializeAllAggregateFunctions(initialInputBufferOffset, false)

  // Positions of those imperative aggregate functions in allAggregateFunctions.
  // For example, say that we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are imperative aggregate functions. Then
  // allImperativeAggregateFunctionPositions will be [1, 2]. Note that this does not need to be
  // updated when falling back to sort-based aggregation because the positions of the aggregate
  // functions do not change in that case.
  private[this] val allImperativeAggregateFunctionPositions: Array[Int] = {
    val positions = new ArrayBuffer[Int]()
    var i = 0
    while (i < allAggregateFunctions.length) {
      allAggregateFunctions(i) match {
        case agg: DeclarativeAggregate =>
        case _ => positions += i
      }
      i += 1
    }
    positions.toArray
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 2: Methods and fields used by setting aggregation buffer values,
  //         processing input rows from inputIter, and generating output
  //         rows.
  ///////////////////////////////////////////////////////////////////////////

  // The projection used to initialize buffer values for all expression-based aggregates.
  // Note that this projection does not need to be updated when switching to sort-based aggregation
  // because the schema of empty aggregation buffers does not change in that case.
  private[this] val expressionAggInitialProjection: MutableProjection = {
    val initExpressions = allAggregateFunctions.flatMap {
      case ae: DeclarativeAggregate => ae.initialValues
      // For the positions corresponding to imperative aggregate functions, we'll use special
      // no-op expressions which are ignored during projection code-generation.
      case i: ImperativeAggregate => Seq.fill(i.aggBufferAttributes.length)(NoOp)
    }
    newMutableProjection(initExpressions, Nil)()
  }

  // Creates a new aggregation buffer and initializes buffer values.
  // This function should be only called at most three times (when we create the hash map,
  // when we switch to sort-based aggregation, and when we create the re-used buffer for
  // sort-based aggregation).
  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = allAggregateFunctions.flatMap(_.aggBufferAttributes)
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema.map(_.dataType))
      .apply(new GenericMutableRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initialize imperative aggregates' buffer values
    allAggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
    buffer
  }

  // Creates a function used to process a row based on the given inputAttributes.
  private def generateProcessRow(
      allAggregateFunctions: Array[AggregateFunction],
      inputAttributes: Seq[Attribute],
      sortBased: Boolean): (UnsafeRow, InternalRow) => Unit = {
    if (allAggregateFunctions.isEmpty) {
      return (currentBuffer: UnsafeRow, row: InternalRow) => {}
    }

    val aggregationBufferAttributes = allAggregateFunctions.flatMap(_.aggBufferAttributes)
    val joinedRow = new JoinedRow()

    val updateExpressions = allAggregateFunctions.zip(aggregateExpressions).flatMap {
      case (ae: DeclarativeAggregate, a) =>
        if (!sortBased && (a.mode == Partial || a.mode == Complete)) {
          ae.updateExpressions
        } else {
          ae.mergeExpressions
        }
      case (agg: AggregateFunction, a) => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
    }
    val expressionAggUpdateProjection =
      newMutableProjection(updateExpressions, aggregationBufferAttributes ++ inputAttributes)()

    val imperativeAggregateFunctions: Array[(MutableRow, InternalRow) => Unit] =
      allAggregateFunctions.zip(aggregateExpressions).collect {
        case (func: ImperativeAggregate, a) =>
          if (!sortBased && (a.mode == Partial || a.mode == Complete)) {
            (cur: MutableRow, row: InternalRow) => func.update(cur, row)
          } else {
            (cur: MutableRow, row: InternalRow) => func.merge(cur, row)
          }
      }

    (currentBuffer: UnsafeRow, row: InternalRow) => {
      expressionAggUpdateProjection.target(currentBuffer)
      // Process all expression-based aggregate functions.
      expressionAggUpdateProjection(joinedRow(currentBuffer, row))
      // Process all imperative aggregate functions
      var i = 0
      while (i < imperativeAggregateFunctions.length) {
        imperativeAggregateFunctions(i)(currentBuffer, row)
        i += 1
      }
    }
  }

  // Creates a function used to generate output rows.
  private def generateResultProjection(): (UnsafeRow, UnsafeRow) => UnsafeRow = {

    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val bufferAttributes = allAggregateFunctions.flatMap(_.aggBufferAttributes)

    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.contains(Final) || modes.contains(Complete)) {
      val joinedRow = new JoinedRow()
      val evalExpressions = allAggregateFunctions.map {
        case ae: DeclarativeAggregate => ae.evaluateExpression
        case agg: AggregateFunction => NoOp
      }
      val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferAttributes)()
      // These are the attributes of the row produced by `expressionAggEvalProjection`
      val aggregateResultSchema = aggregateAttributes
      val aggregateResult = new SpecificMutableRow(aggregateResultSchema.map(_.dataType))
      expressionAggEvalProjection.target(aggregateResult)
      val resultProjection =
        UnsafeProjection.create(resultExpressions, groupingAttributes ++ aggregateResultSchema)

      val allImperativeAggregateFunctions: Array[ImperativeAggregate] =
        allAggregateFunctions.collect { case func: ImperativeAggregate => func}

      (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
        // Generate results for all expression-based aggregate functions.
        expressionAggEvalProjection(currentBuffer)
        // Generate results for all imperative aggregate functions.
        var i = 0
        while (i < allImperativeAggregateFunctions.length) {
          aggregateResult.update(
            allImperativeAggregateFunctionPositions(i),
            allImperativeAggregateFunctions(i).eval(currentBuffer))
          i += 1
        }
        resultProjection(joinedRow(currentGroupingKey, aggregateResult))
      }

    } else if (aggregateExpressions.nonEmpty) {
      // Fast path for partial aggregation
      val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
      val bufferSchema = StructType.fromAttributes(bufferAttributes)
      val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

      (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
        unsafeRowJoiner.join(currentGroupingKey, currentBuffer)
      }
    } else {
      val resultProjection = UnsafeProjection.create(resultExpressions, groupingAttributes)

      (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
        resultProjection(currentGroupingKey)
      }
    }
  }

  // An UnsafeProjection used to extract grouping keys from the input rows.
  private[this] val groupProjection =
    UnsafeProjection.create(groupingExpressions, originalInputAttributes)

  // A function used to process a input row. Its first argument is the aggregation buffer
  // and the second argument is the input row.
  private[this] var processRow: (UnsafeRow, InternalRow) => Unit =
    generateProcessRow(allAggregateFunctions, originalInputAttributes, false)

  // A function used to generate output rows based on the grouping keys (first argument)
  // and the corresponding aggregation buffer (second argument).
  private[this] var generateOutput: (UnsafeRow, UnsafeRow) => UnsafeRow =
    generateResultProjection()

  // An aggregation buffer containing initial buffer values. It is used to
  // initialize other aggregation buffers.
  private[this] val initialAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  ///////////////////////////////////////////////////////////////////////////
  // Part 3: Methods and fields used by hash-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // This is the hash map used for hash-based aggregation. It is backed by an
  // UnsafeFixedWidthAggregationMap and it is used to store
  // all groups and their corresponding aggregation buffers for hash-based aggregation.
  private[this] val hashMap = new UnsafeFixedWidthAggregationMap(
    initialAggregationBuffer,
    StructType.fromAttributes(allAggregateFunctions.flatMap(_.aggBufferAttributes)),
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get().taskMemoryManager(),
    1024 * 16, // initial capacity
    TaskContext.get().taskMemoryManager().pageSizeBytes,
    false // disable tracking of performance metrics
  )

  // The function used to read and process input rows. When processing input rows,
  // it first uses hash-based aggregation by putting groups and their buffers in
  // hashMap. If there is not enough memory, it will multiple hash-maps, spilling
  // after each becomes full then using sort to merge these spills, finally do sort
  // based aggregation.
  private def processInputs(fallbackStartsAt: Int): Unit = {
    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      // Note that it would be better to eliminate the hash map entirely in the future.
      val groupingKey = groupProjection.apply(null)
      val buffer: UnsafeRow = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        processRow(buffer, newInput)
      }
    } else {
      var i = 0
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        val groupingKey = groupProjection.apply(newInput)
        var buffer: UnsafeRow = null
        if (i < fallbackStartsAt) {
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
        }
        if (buffer == null) {
          val sorter = hashMap.destructAndCreateExternalSorter()
          if (externalSorter == null) {
            externalSorter = sorter
          } else {
            externalSorter.merge(sorter)
          }
          i = 0
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
          if (buffer == null) {
            // failed to allocate the first page
            throw new OutOfMemoryError("No enough memory for aggregation")
          }
        }
        processRow(buffer, newInput)
        i += 1
      }

      if (externalSorter != null) {
        val sorter = hashMap.destructAndCreateExternalSorter()
        externalSorter.merge(sorter)
        hashMap.free()

        switchToSortBasedAggregation()
      }
    }
  }

  // The iterator created from hashMap. It is used to generate output rows when we
  // are using hash-based aggregation.
  private[this] var aggregationBufferMapIterator: KVIterator[UnsafeRow, UnsafeRow] = null

  // Indicates if aggregationBufferMapIterator still has key-value pairs.
  private[this] var mapIteratorHasNext: Boolean = false

  ///////////////////////////////////////////////////////////////////////////
  // Part 4: Methods and fields used when we switch to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // This sorter is used for sort-based aggregation. It is initialized as soon as
  // we switch from hash-based to sort-based aggregation. Otherwise, it is not used.
  private[this] var externalSorter: UnsafeKVExternalSorter = null

  /**
   * Switch to sort-based aggregation when the hash-based approach is unable to acquire memory.
   */
  private def switchToSortBasedAggregation(): Unit = {
    logInfo("falling back to sort based aggregation.")

    val allAggregateFunctions = initializeAllAggregateFunctions(startingInputBufferOffset = 0, true)

    // Basically the value of the KVIterator returned by externalSorter
    // will just aggregation buffer. At here, we use inputAggBufferAttributes.
    val newInputAttributes: Seq[Attribute] =
      allAggregateFunctions.flatMap(_.inputAggBufferAttributes)

    // Set up new processRow and generateOutput.
    processRow = generateProcessRow(allAggregateFunctions, newInputAttributes, true)
    generateOutput = generateResultProjection()

    // Step 5: Get the sorted iterator from the externalSorter.
    sortedKVIterator = externalSorter.sortedIterator()

    // Step 6: Pre-load the first key-value pair from the sorted iterator to make
    // hasNext idempotent.
    sortedInputHasNewGroup = sortedKVIterator.next()

    // Copy the first key and value (aggregation buffer).
    if (sortedInputHasNewGroup) {
      val key = sortedKVIterator.getKey
      val value = sortedKVIterator.getValue
      nextGroupingKey = key.copy()
      currentGroupingKey = key.copy()
      firstRowInNextGroup = value.copy()
    }

    // Step 7: set sortBased to true.
    sortBased = true
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 5: Methods and fields used by sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // Indicates if we are using sort-based aggregation. Because we first try to use
  // hash-based aggregation, its initial value is false.
  private[this] var sortBased: Boolean = false

  // The KVIterator containing input rows for the sort-based aggregation. It will be
  // set in switchToSortBasedAggregation when we switch to sort-based aggregation.
  private[this] var sortedKVIterator: UnsafeKVExternalSorter#KVSorterIterator = null

  // The grouping key of the current group.
  private[this] var currentGroupingKey: UnsafeRow = null

  // The grouping key of next group.
  private[this] var nextGroupingKey: UnsafeRow = null

  // The first row of next group.
  private[this] var firstRowInNextGroup: UnsafeRow = null

  // Indicates if we has new group of rows from the sorted input iterator.
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
  private[this] val sortBasedAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  // Processes rows in the current group. It will stop when it find a new group.
  private def processCurrentSortedGroup(): Unit = {
    // First, we need to copy nextGroupingKey to currentGroupingKey.
    currentGroupingKey.copyFrom(nextGroupingKey)
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    processRow(sortBasedAggregationBuffer, firstRowInNextGroup)

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    // Pre-load the first key-value pair to make the condition of the while loop
    // has no action (we do not trigger loading a new key-value pair
    // when we evaluate the condition).
    var hasNext = sortedKVIterator.next()
    while (!findNextPartition && hasNext) {
      // Get the grouping key and value (aggregation buffer).
      val groupingKey = sortedKVIterator.getKey
      val inputAggregationBuffer = sortedKVIterator.getValue

      // Check if the current row belongs the current input row.
      if (currentGroupingKey.equals(groupingKey)) {
        processRow(sortBasedAggregationBuffer, inputAggregationBuffer)

        hasNext = sortedKVIterator.next()
      } else {
        // We find a new group.
        findNextPartition = true
        // copyFrom will fail when
        nextGroupingKey.copyFrom(groupingKey) // = groupingKey.copy()
        firstRowInNextGroup.copyFrom(inputAggregationBuffer) // = inputAggregationBuffer.copy()

      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the sortedKVIterator.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
      sortedKVIterator.close()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 6: Loads input rows and setup aggregationBufferMapIterator if we
  //         have not switched to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Start processing input rows.
   */
  processInputs(testFallbackStartsAt.getOrElse(Int.MaxValue))

  // If we did not switch to sort-based aggregation in processInputs,
  // we pre-load the first key-value pair from the map (to make hasNext idempotent).
  if (!sortBased) {
    // First, set aggregationBufferMapIterator.
    aggregationBufferMapIterator = hashMap.iterator()
    // Pre-load the first key-value pair from the aggregationBufferMapIterator.
    mapIteratorHasNext = aggregationBufferMapIterator.next()
    // If the map is empty, we just free it.
    if (!mapIteratorHasNext) {
      hashMap.free()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = {
    (sortBased && sortedInputHasNewGroup) || (!sortBased && mapIteratorHasNext)
  }

  override final def next(): UnsafeRow = {
    if (hasNext) {
      val res = if (sortBased) {
        // Process the current group.
        processCurrentSortedGroup()
        // Generate output row for the current group.
        val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
        // Initialize buffer values for the next group.
        sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)

        outputRow
      } else {
        // We did not fall back to sort-based aggregation.
        val result =
          generateOutput(
            aggregationBufferMapIterator.getKey,
            aggregationBufferMapIterator.getValue)

        // Pre-load next key-value pair form aggregationBufferMapIterator to make hasNext
        // idempotent.
        mapIteratorHasNext = aggregationBufferMapIterator.next()

        if (!mapIteratorHasNext) {
          // If there is no input from aggregationBufferMapIterator, we copy current result.
          val resultCopy = result.copy()
          // Then, we free the map.
          hashMap.free()

          resultCopy
        } else {
          result
        }
      }

      // If this is the last record, update the task's peak memory usage. Since we destroy
      // the map to create the sorter, their memory usages should not overlap, so it is safe
      // to just use the max of the two.
      if (!hasNext) {
        val mapMemory = hashMap.getPeakMemoryUsedBytes
        val sorterMemory = Option(externalSorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
        val peakMemory = Math.max(mapMemory, sorterMemory)
        dataSize += peakMemory
        spillSize += TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore
        TaskContext.get().internalMetricsToAccumulators(
          InternalAccumulator.PEAK_EXECUTION_MEMORY).add(peakMemory)
      }
      numOutputRows += 1
      res
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 8: Utility functions
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Generate a output row when there is no input and there is no grouping expression.
   */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    if (groupingExpressions.isEmpty) {
      sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)
      // We create a output row and copy it. So, we can free the map.
      val resultCopy =
        generateOutput(UnsafeRow.createFromByteArray(0, 0), sortBasedAggregationBuffer).copy()
      hashMap.free()
      resultCopy
    } else {
      throw new IllegalStateException(
        "This method should not be called when groupingExpressions is not empty.")
    }
  }
}
