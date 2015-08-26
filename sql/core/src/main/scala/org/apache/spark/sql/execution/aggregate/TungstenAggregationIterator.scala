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
import org.apache.spark.{InternalAccumulator, Logging, SparkEnv, TaskContext}
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
 * this map cannot allocate memory from [[org.apache.spark.shuffle.ShuffleMemoryManager]],
 * it switches to sort-based aggregation. The process of the switch has the following step:
 *  - Step 1: Sort all entries of the hash map based on values of grouping expressions and
 *            spill them to disk.
 *  - Step 2: Create a external sorter based on the spilled sorted map entries.
 *  - Step 3: Redirect all input rows to the external sorter.
 *  - Step 4: Get a sorted [[KVIterator]] from the external sorter.
 *  - Step 5: Initialize sort-based aggregation.
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
 * @param nonCompleteAggregateExpressions
 *   [[AggregateExpression2]] containing [[AggregateFunction2]]s with mode [[Partial]],
 *   [[PartialMerge]], or [[Final]].
 * @param completeAggregateExpressions
 *   [[AggregateExpression2]] containing [[AggregateFunction2]]s with mode [[Complete]].
 * @param initialInputBufferOffset
 *   If this iterator is used to handle functions with mode [[PartialMerge]] or [[Final]].
 *   The input rows have the format of `grouping keys + aggregation buffer`.
 *   This offset indicates the starting position of aggregation buffer in a input row.
 * @param resultExpressions
 *   expressions for generating output rows.
 * @param newMutableProjection
 *   the function used to create mutable projections.
 * @param originalInputAttributes
 *   attributes of representing input rows from `inputIter`.
 */
class TungstenAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    nonCompleteAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateExpressions: Seq[AggregateExpression2],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    originalInputAttributes: Seq[Attribute],
    testFallbackStartsAt: Option[Int],
    numInputRows: LongSQLMetric,
    numOutputRows: LongSQLMetric)
  extends Iterator[UnsafeRow] with Logging {

  // The parent partition iterator, to be initialized later in `start`
  private[this] var inputIter: Iterator[InternalRow] = null

  ///////////////////////////////////////////////////////////////////////////
  // Part 1: Initializing aggregate functions.
  ///////////////////////////////////////////////////////////////////////////

  // A Seq containing all AggregateExpressions.
  // It is important that all AggregateExpressions with the mode Partial, PartialMerge or Final
  // are at the beginning of the allAggregateExpressions.
  private[this] val allAggregateExpressions: Seq[AggregateExpression2] =
    nonCompleteAggregateExpressions ++ completeAggregateExpressions

  // Check to make sure we do not have more than three modes in our AggregateExpressions.
  // If we have, users are hitting a bug and we throw an IllegalStateException.
  if (allAggregateExpressions.map(_.mode).distinct.length > 2) {
    throw new IllegalStateException(
      s"$allAggregateExpressions should have no more than 2 kinds of modes.")
  }

  //
  // The modes of AggregateExpressions. Right now, we can handle the following mode:
  //  - Partial-only:
  //      All AggregateExpressions have the mode of Partial.
  //      For this case, aggregationMode is (Some(Partial), None).
  //  - PartialMerge-only:
  //      All AggregateExpressions have the mode of PartialMerge).
  //      For this case, aggregationMode is (Some(PartialMerge), None).
  //  - Final-only:
  //      All AggregateExpressions have the mode of Final.
  //      For this case, aggregationMode is (Some(Final), None).
  //  - Final-Complete:
  //      Some AggregateExpressions have the mode of Final and
  //      others have the mode of Complete. For this case,
  //      aggregationMode is (Some(Final), Some(Complete)).
  //  - Complete-only:
  //      nonCompleteAggregateExpressions is empty and we have AggregateExpressions
  //      with mode Complete in completeAggregateExpressions. For this case,
  //      aggregationMode is (None, Some(Complete)).
  //  - Grouping-only:
  //      There is no AggregateExpression. For this case, AggregationMode is (None,None).
  //
  private[this] var aggregationMode: (Option[AggregateMode], Option[AggregateMode]) = {
    nonCompleteAggregateExpressions.map(_.mode).distinct.headOption ->
      completeAggregateExpressions.map(_.mode).distinct.headOption
  }

  // All aggregate functions. TungstenAggregationIterator only handles AlgebraicAggregates.
  // If there is any functions that is not an AlgebraicAggregate, we throw an
  // IllegalStateException.
  private[this] val allAggregateFunctions: Array[AlgebraicAggregate] = {
    if (!allAggregateExpressions.forall(_.aggregateFunction.isInstanceOf[AlgebraicAggregate])) {
      throw new IllegalStateException(
        "Only AlgebraicAggregates should be passed in TungstenAggregationIterator.")
    }

    allAggregateExpressions
      .map(_.aggregateFunction.asInstanceOf[AlgebraicAggregate])
      .toArray
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 2: Methods and fields used by setting aggregation buffer values,
  //         processing input rows from inputIter, and generating output
  //         rows.
  ///////////////////////////////////////////////////////////////////////////

  // The projection used to initialize buffer values.
  private[this] val algebraicInitialProjection: MutableProjection = {
    val initExpressions = allAggregateFunctions.flatMap(_.initialValues)
    newMutableProjection(initExpressions, Nil)()
  }

  // Creates a new aggregation buffer and initializes buffer values.
  // This functions should be only called at most three times (when we create the hash map,
  // when we switch to sort-based aggregation, and when we create the re-used buffer for
  // sort-based aggregation).
  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = allAggregateFunctions.flatMap(_.bufferAttributes)
    val bufferRowSize: Int = bufferSchema.length

    val genericMutableBuffer = new GenericMutableRow(bufferRowSize)
    val unsafeProjection =
      UnsafeProjection.create(bufferSchema.map(_.dataType))
    val buffer = unsafeProjection.apply(genericMutableBuffer)
    algebraicInitialProjection.target(buffer)(EmptyRow)
    buffer
  }

  // Creates a function used to process a row based on the given inputAttributes.
  private def generateProcessRow(
      inputAttributes: Seq[Attribute]): (UnsafeRow, InternalRow) => Unit = {

    val aggregationBufferAttributes = allAggregateFunctions.flatMap(_.bufferAttributes)
    val joinedRow = new JoinedRow()

    aggregationMode match {
      // Partial-only
      case (Some(Partial), None) =>
        val updateExpressions = allAggregateFunctions.flatMap(_.updateExpressions)
        val algebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferAttributes ++ inputAttributes)()

        (currentBuffer: UnsafeRow, row: InternalRow) => {
          algebraicUpdateProjection.target(currentBuffer)
          algebraicUpdateProjection(joinedRow(currentBuffer, row))
        }

      // PartialMerge-only or Final-only
      case (Some(PartialMerge), None) | (Some(Final), None) =>
        val mergeExpressions = allAggregateFunctions.flatMap(_.mergeExpressions)
        // This projection is used to merge buffer values for all AlgebraicAggregates.
        val algebraicMergeProjection =
          newMutableProjection(
            mergeExpressions,
            aggregationBufferAttributes ++ inputAttributes)()

        (currentBuffer: UnsafeRow, row: InternalRow) => {
          // Process all algebraic aggregate functions.
          algebraicMergeProjection.target(currentBuffer)
          algebraicMergeProjection(joinedRow(currentBuffer, row))
        }

      // Final-Complete
      case (Some(Final), Some(Complete)) =>
        val nonCompleteAggregateFunctions: Array[AlgebraicAggregate] =
          allAggregateFunctions.take(nonCompleteAggregateExpressions.length)
        val completeAggregateFunctions: Array[AlgebraicAggregate] =
          allAggregateFunctions.takeRight(completeAggregateExpressions.length)

        val completeOffsetExpressions =
          Seq.fill(completeAggregateFunctions.map(_.bufferAttributes.length).sum)(NoOp)
        val mergeExpressions =
          nonCompleteAggregateFunctions.flatMap(_.mergeExpressions) ++ completeOffsetExpressions
        val finalAlgebraicMergeProjection =
          newMutableProjection(
            mergeExpressions,
            aggregationBufferAttributes ++ inputAttributes)()

        // We do not touch buffer values of aggregate functions with the Final mode.
        val finalOffsetExpressions =
          Seq.fill(nonCompleteAggregateFunctions.map(_.bufferAttributes.length).sum)(NoOp)
        val updateExpressions =
          finalOffsetExpressions ++ completeAggregateFunctions.flatMap(_.updateExpressions)
        val completeAlgebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferAttributes ++ inputAttributes)()

        (currentBuffer: UnsafeRow, row: InternalRow) => {
          val input = joinedRow(currentBuffer, row)
          // For all aggregate functions with mode Complete, update the given currentBuffer.
          completeAlgebraicUpdateProjection.target(currentBuffer)(input)

          // For all aggregate functions with mode Final, merge buffer values in row to
          // currentBuffer.
          finalAlgebraicMergeProjection.target(currentBuffer)(input)
        }

      // Complete-only
      case (None, Some(Complete)) =>
        val completeAggregateFunctions: Array[AlgebraicAggregate] =
          allAggregateFunctions.takeRight(completeAggregateExpressions.length)

        val updateExpressions =
          completeAggregateFunctions.flatMap(_.updateExpressions)
        val completeAlgebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferAttributes ++ inputAttributes)()

        (currentBuffer: UnsafeRow, row: InternalRow) => {
          completeAlgebraicUpdateProjection.target(currentBuffer)
          // For all aggregate functions with mode Complete, update the given currentBuffer.
          completeAlgebraicUpdateProjection(joinedRow(currentBuffer, row))
        }

      // Grouping only.
      case (None, None) => (currentBuffer: UnsafeRow, row: InternalRow) => {}

      case other =>
        throw new IllegalStateException(
          s"${aggregationMode} should not be passed into TungstenAggregationIterator.")
    }
  }

  // Creates a function used to generate output rows.
  private def generateResultProjection(): (UnsafeRow, UnsafeRow) => UnsafeRow = {

    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val bufferAttributes = allAggregateFunctions.flatMap(_.bufferAttributes)

    aggregationMode match {
      // Partial-only or PartialMerge-only: every output row is basically the values of
      // the grouping expressions and the corresponding aggregation buffer.
      case (Some(Partial), None) | (Some(PartialMerge), None) =>
        val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
        val bufferSchema = StructType.fromAttributes(bufferAttributes)
        val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

        (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
          unsafeRowJoiner.join(currentGroupingKey, currentBuffer)
        }

      // Final-only, Complete-only and Final-Complete: a output row is generated based on
      // resultExpressions.
      case (Some(Final), None) | (Some(Final) | None, Some(Complete)) =>
        val joinedRow = new JoinedRow()
        val resultProjection =
          UnsafeProjection.create(resultExpressions, groupingAttributes ++ bufferAttributes)

        (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
          resultProjection(joinedRow(currentGroupingKey, currentBuffer))
        }

      // Grouping-only: a output row is generated from values of grouping expressions.
      case (None, None) =>
        val resultProjection =
          UnsafeProjection.create(resultExpressions, groupingAttributes)

        (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
          resultProjection(currentGroupingKey)
        }

      case other =>
        throw new IllegalStateException(
          s"${aggregationMode} should not be passed into TungstenAggregationIterator.")
    }
  }

  // An UnsafeProjection used to extract grouping keys from the input rows.
  private[this] val groupProjection =
    UnsafeProjection.create(groupingExpressions, originalInputAttributes)

  // A function used to process a input row. Its first argument is the aggregation buffer
  // and the second argument is the input row.
  private[this] var processRow: (UnsafeRow, InternalRow) => Unit =
    generateProcessRow(originalInputAttributes)

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
    StructType.fromAttributes(allAggregateFunctions.flatMap(_.bufferAttributes)),
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get.taskMemoryManager(),
    SparkEnv.get.shuffleMemoryManager,
    1024 * 16, // initial capacity
    SparkEnv.get.shuffleMemoryManager.pageSizeBytes,
    false // disable tracking of performance metrics
  )

  // Exposed for testing
  private[aggregate] def getHashMap: UnsafeFixedWidthAggregationMap = hashMap

  // The function used to read and process input rows. When processing input rows,
  // it first uses hash-based aggregation by putting groups and their buffers in
  // hashMap. If we could not allocate more memory for the map, we switch to
  // sort-based aggregation (by calling switchToSortBasedAggregation).
  private def processInputs(): Unit = {
    assert(inputIter != null, "attempted to process input when iterator was null")
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
      while (!sortBased && inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        val groupingKey = groupProjection.apply(newInput)
        val buffer: UnsafeRow = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
        if (buffer == null) {
          // buffer == null means that we could not allocate more memory.
          // Now, we need to spill the map and switch to sort-based aggregation.
          switchToSortBasedAggregation(groupingKey, newInput)
        } else {
          processRow(buffer, newInput)
        }
      }
    }
  }

  // This function is only used for testing. It basically the same as processInputs except
  // that it switch to sort-based aggregation after `fallbackStartsAt` input rows have
  // been processed.
  private def processInputsWithControlledFallback(fallbackStartsAt: Int): Unit = {
    assert(inputIter != null, "attempted to process input when iterator was null")
    var i = 0
    while (!sortBased && inputIter.hasNext) {
      val newInput = inputIter.next()
      numInputRows += 1
      val groupingKey = groupProjection.apply(newInput)
      val buffer: UnsafeRow = if (i < fallbackStartsAt) {
        hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
      } else {
        null
      }
      if (buffer == null) {
        // buffer == null means that we could not allocate more memory.
        // Now, we need to spill the map and switch to sort-based aggregation.
        switchToSortBasedAggregation(groupingKey, newInput)
      } else {
        processRow(buffer, newInput)
      }
      i += 1
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
  private def switchToSortBasedAggregation(firstKey: UnsafeRow, firstInput: InternalRow): Unit = {
    assert(inputIter != null, "attempted to process input when iterator was null")
    logInfo("falling back to sort based aggregation.")
    // Step 1: Get the ExternalSorter containing sorted entries of the map.
    externalSorter = hashMap.destructAndCreateExternalSorter()

    // Step 2: Free the memory used by the map.
    hashMap.free()

    // Step 3: If we have aggregate function with mode Partial or Complete,
    // we need to process input rows to get aggregation buffer.
    // So, later in the sort-based aggregation iterator, we can do merge.
    // If aggregate functions are with mode Final and PartialMerge,
    // we just need to project the aggregation buffer from an input row.
    val needsProcess = aggregationMode match {
      case (Some(Partial), None) => true
      case (None, Some(Complete)) => true
      case (Some(Final), Some(Complete)) => true
      case _ => false
    }

    // Note: Since we spill the sorter's contents immediately after creating it, we must insert
    // something into the sorter here to ensure that we acquire at least a page of memory.
    // This is done through `externalSorter.insertKV`, which will trigger the page allocation.
    // Otherwise, children operators may steal the window of opportunity and starve our sorter.

    if (needsProcess) {
      // First, we create a buffer.
      val buffer = createNewAggregationBuffer()

      // Process firstKey and firstInput.
      // Initialize buffer.
      buffer.copyFrom(initialAggregationBuffer)
      processRow(buffer, firstInput)
      externalSorter.insertKV(firstKey, buffer)

      // Process the rest of input rows.
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        val groupingKey = groupProjection.apply(newInput)
        buffer.copyFrom(initialAggregationBuffer)
        processRow(buffer, newInput)
        externalSorter.insertKV(groupingKey, buffer)
      }
    } else {
      // When needsProcess is false, the format of input rows is groupingKey + aggregation buffer.
      // We need to project the aggregation buffer part from an input row.
      val buffer = createNewAggregationBuffer()
      // The originalInputAttributes are using cloneBufferAttributes. So, we need to use
      // allAggregateFunctions.flatMap(_.cloneBufferAttributes).
      val bufferExtractor = newMutableProjection(
        allAggregateFunctions.flatMap(_.cloneBufferAttributes),
        originalInputAttributes)()
      bufferExtractor.target(buffer)

      // Insert firstKey and its buffer.
      bufferExtractor(firstInput)
      externalSorter.insertKV(firstKey, buffer)

      // Insert the rest of input rows.
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        val groupingKey = groupProjection.apply(newInput)
        bufferExtractor(newInput)
        externalSorter.insertKV(groupingKey, buffer)
      }
    }

    // Set aggregationMode, processRow, and generateOutput for sort-based aggregation.
    val newAggregationMode = aggregationMode match {
      case (Some(Partial), None) => (Some(PartialMerge), None)
      case (None, Some(Complete)) => (Some(Final), None)
      case (Some(Final), Some(Complete)) => (Some(Final), None)
      case other => other
    }
    aggregationMode = newAggregationMode

    // Basically the value of the KVIterator returned by externalSorter
    // will just aggregation buffer. At here, we use cloneBufferAttributes.
    val newInputAttributes: Seq[Attribute] =
      allAggregateFunctions.flatMap(_.cloneBufferAttributes)

    // Set up new processRow and generateOutput.
    processRow = generateProcessRow(newInputAttributes)
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
   * Only after this method is called will this iterator be non-empty.
   */
  def start(parentIter: Iterator[InternalRow]): Unit = {
    inputIter = parentIter
    testFallbackStartsAt match {
      case None =>
        processInputs()
      case Some(fallbackStartsAt) =>
        // This is the testing path. processInputsWithControlledFallback is same as processInputs
        // except that it switches to sort-based aggregation after `fallbackStartsAt` input rows
        // have been processed.
        processInputsWithControlledFallback(fallbackStartsAt)
    }

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
    assert(groupingExpressions.isEmpty)
    assert(inputIter == null)
    generateOutput(UnsafeRow.createFromByteArray(0, 0), initialAggregationBuffer)
  }

  /** Free memory used in the underlying map. */
  def free(): Unit = {
    hashMap.free()
  }
}
