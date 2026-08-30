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

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{UnsafeFixedWidthAggregationMap, UnsafeKVExternalSorter}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.unsafe.KVIterator
import org.apache.spark.util.ArrayImplicits._

/**
 * An iterator used to evaluate aggregate functions. It operates on [[UnsafeRow]]s.
 *
 * This iterator first uses hash-based aggregation to process input rows. It uses
 * a hash map to store groups and their corresponding aggregation buffers. If
 * this map cannot allocate memory from memory manager, it spills the map into disk
 * and creates a new one. After processed all the input, then merge all the spills
 * together using external sorter, and do sort-based aggregation.
 *
 * The process has the following step:
 *  - Step 0: Do hash-based aggregation.
 *  - Step 1: Sort all entries of the hash map based on values of grouping expressions and
 *            spill them to disk.
 *  - Step 2: Create an external sorter based on the spilled sorted map entries and reset the map.
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
 *  - Part 6: Methods and fields used by adaptive partial aggregation pass-through.
 *  - Part 7: Loads input and processes input rows.
 *  - Part 8: Public methods of this iterator.
 *  - Part 9: A utility function used to generate a result when there is no
 *            input and there is no grouping expression.
 *
 * @param partIndex
 *   index of the partition
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
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    originalInputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow],
    testFallbackStartsAt: Option[(Int, Int)],
    numOutputRows: SQLMetric,
    peakMemory: SQLMetric,
    spillSize: SQLMetric,
    avgHashProbe: SQLMetric,
    numTasksFallBacked: SQLMetric,
    numBypassingRows: SQLMetric,
    aggTime: SQLMetric,
    adaptivePartialAggEnabled: Boolean,
    adaptiveMinRows: Long,
    adaptiveMinCompaction: Double)
  extends AggregationIterator(
    partIndex,
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Part 1: Initializing aggregate functions.
  ///////////////////////////////////////////////////////////////////////////

  // Remember spill data size of this task before execute this operator so that we can
  // figure out how many bytes we spilled for this operator.
  private val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

  ///////////////////////////////////////////////////////////////////////////
  // Part 2: Methods and fields used by setting aggregation buffer values,
  //         processing input rows from inputIter, and generating output
  //         rows.
  ///////////////////////////////////////////////////////////////////////////

  // Creates a new aggregation buffer and initializes buffer values.
  // This function should be called at most three times: when we create the hash map, when we
  // create the re-used buffer for sort-based aggregation, and when we create the buffer for
  // pass-through rows.
  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema.map(_.dataType))
      .apply(new GenericInternalRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initialize imperative aggregates' buffer values
    aggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
    buffer
  }

  // Creates a function used to generate output rows.
  override protected def generateResultProjection(): (UnsafeRow, InternalRow) => UnsafeRow = {
    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.nonEmpty && !modes.contains(Final) && !modes.contains(Complete)) {
      // Fast path for partial aggregation, UnsafeRowJoiner is usually faster than projection
      val groupingAttributes = groupingExpressions.map(_.toAttribute)
      val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
      val groupingKeySchema = DataTypeUtils.fromAttributes(groupingAttributes)
      val bufferSchema = DataTypeUtils.fromAttributes(bufferAttributes.toImmutableArraySeq)
      val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

      (currentGroupingKey: UnsafeRow, currentBuffer: InternalRow) => {
        unsafeRowJoiner.join(currentGroupingKey, currentBuffer.asInstanceOf[UnsafeRow])
      }
    } else {
      super.generateResultProjection()
    }
  }

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
    DataTypeUtils.fromAttributes(
      aggregateFunctions.flatMap(_.aggBufferAttributes).toImmutableArraySeq),
    DataTypeUtils.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get(),
    1024 * 16, // initial capacity
    TaskContext.get().taskMemoryManager().pageSizeBytes
  )

  // The function used to read and process input rows. When processing input rows,
  // it first uses hash-based aggregation by putting groups and their buffers in
  // hashMap. If there is not enough memory, it will multiple hash-maps, spilling
  // after each becomes full then using sort to merge these spills, finally do sort
  // based aggregation.
  //
  // When adaptive partial aggregation is enabled (`adaptivePartialAggEnabled`), the processing may
  // stop early and switch to pass-through mode: the remaining input rows are not added to the map
  // but are instead emitted as single-row partial buffers by the output stage (see
  // `nextPassThroughOutput`). One predicate decides both check points -- the aggregation is
  // ineffective when `processedRows < distinctKeys * minCompaction`, i.e. it does not collapse
  // `minCompaction` rows into one key:
  //   - periodically, every `minRows` processed rows;
  //   - when the map is about to spill, in which case the spill is skipped entirely and the row
  //     that could not be inserted becomes the first pass-through row.
  // A spill starts a new in-memory map epoch: the row counters restart so that epoch is judged on
  // its own rows, which lets an input whose cardinality only turns unfavorable later still be
  // passed through. Pass-through may therefore coexist with earlier spills. The output order
  // matches the generated path: the frozen map (or sort-based) output comes first, and the
  // pass-through rows stream afterwards.
  private def processInputs(fallbackStartsAt: (Int, Int)): Unit = {
    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      // Note that it would be better to eliminate the hash map entirely in the future.
      val groupingKey = groupingProjection.apply(null)
      val buffer: UnsafeRow = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        processRow(buffer, newInput)
      }
    } else {
      var i = 0
      var processedRows = 0L
      val minRows = adaptiveMinRows
      // The processed-row count at which the compaction ratio is evaluated next. It advances by
      // `minRows` after every check, and restarts after a spill so the new in-memory map epoch is
      // judged on its own rows. `minRows = 0` disables the periodic check: the count is only ever
      // compared after being incremented past 0, so it never matches and only the spill check
      // below remains.
      var nextCheckRow = minRows
      // The partial aggregation is ineffective when it does not collapse `minCompaction` rows into
      // one key. There is no fast map on this path, so the map's keys are all the operator holds.
      def ineffective(): Boolean =
        processedRows < hashMap.getNumKeys().toDouble * adaptiveMinCompaction
      while (inputIter.hasNext && !passThrough) {
        val newInput = inputIter.next()
        val groupingKey = groupingProjection.apply(newInput)
        var buffer: UnsafeRow = null
        if (i < fallbackStartsAt._2) {
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
        }
        if (buffer == null) {
          // The map is full and would normally spill. Adaptive partial aggregation may instead
          // bypass: keep the in-memory map as-is, pass this row and all remaining rows through,
          // and skip the spill entirely.
          if (adaptivePartialAggEnabled && processedRows > 0 && ineffective()) {
            passThrough = true
            // `newInput` could not be inserted; stash a copy as the first pass-through row so it
            // is not lost when we drain the rest of `inputIter`.
            pendingPassThroughRow = newInput.copy()
          } else {
            val sorter = hashMap.destructAndCreateExternalSorter()
            if (externalSorter == null) {
              externalSorter = sorter
            } else {
              externalSorter.merge(sorter)
            }
            i = 0
            // The map starts a new in-memory epoch, so the counters restart and the ratio of that
            // epoch alone decides whether the remaining rows are passed through.
            processedRows = 0L
            nextCheckRow = minRows
            buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
            if (buffer == null) {
              // failed to allocate the first page
              throw QueryExecutionErrors.aggregateOutOfMemoryError()
            }
          }
        }
        if (!passThrough) {
          processRow(buffer, newInput)
          i += 1
          processedRows += 1
          // Periodic check: if the aggregation is not collapsing enough rows, bypass it for the
          // rest of the input.
          if (adaptivePartialAggEnabled && processedRows == nextCheckRow) {
            if (ineffective()) {
              passThrough = true
            } else {
              nextCheckRow += minRows
            }
          }
        }
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

    // Basically the value of the KVIterator returned by externalSorter
    // will be just aggregation buffer, so we rewrite the aggregateExpressions to reflect it.
    val newExpressions = aggregateExpressions.map {
      case agg @ AggregateExpression(_, Partial, _, _, _) =>
        agg.copy(mode = PartialMerge)
      case agg @ AggregateExpression(_, Complete, _, _, _) =>
        agg.copy(mode = Final)
      case other => other
    }
    val newFunctions = initializeAggregateFunctions(newExpressions, 0)
    val newInputAttributes = newFunctions.flatMap(_.inputAggBufferAttributes)
    sortBasedProcessRow = generateProcessRow(
      newExpressions, newFunctions.toImmutableArraySeq, newInputAttributes.toImmutableArraySeq)

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
    numTasksFallBacked += 1
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

  // The function used to process rows in a group
  private[this] var sortBasedProcessRow: (InternalRow, InternalRow) => Unit = null

  // Processes rows in the current group. It will stop when it find a new group.
  private def processCurrentSortedGroup(): Unit = {
    // First, we need to copy nextGroupingKey to currentGroupingKey.
    currentGroupingKey.copyFrom(nextGroupingKey)
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    sortBasedProcessRow(sortBasedAggregationBuffer, firstRowInNextGroup)

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
        sortBasedProcessRow(sortBasedAggregationBuffer, inputAggregationBuffer)

        hasNext = sortedKVIterator.next()
      } else {
        // We find a new group.
        findNextPartition = true
        // copyFrom will fail when
        nextGroupingKey.copyFrom(groupingKey)
        firstRowInNextGroup.copyFrom(inputAggregationBuffer)
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
  // Part 6: Methods and fields used by adaptive partial aggregation pass-through.
  ///////////////////////////////////////////////////////////////////////////

  // Indicates that partial aggregation has been bypassed and the remaining input rows should be
  // passed through as single-row partial buffers. Set in `processInputs` by either check point.
  // It may coexist with earlier spills. The output order matches the generated path: the frozen
  // map (or sort-based) output comes first, and the pass-through rows stream afterwards.
  private[this] var passThrough: Boolean = false

  // The row that could not be inserted at the spill check. It is stashed here (as
  // a copy) so it becomes the first pass-through row rather than being lost.
  private[this] var pendingPassThroughRow: InternalRow = null

  // A reused aggregation buffer for building single-row partial buffers during pass-through. It is
  // re-initialized from `initialAggregationBuffer` for every passed-through row.
  private[this] lazy val passThroughAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  // Whether there are remaining pass-through rows to emit.
  private def passThroughHasNext: Boolean =
    passThrough && (pendingPassThroughRow != null || inputIter.hasNext)

  // `aggTime` only covers the build, which runs to completion in the constructor before
  // pass-through is known; the interpreted path consumes the rest of the input lazily through
  // `next()`. The drain is therefore timed there, bracketed once per drain (not per row) so the
  // timing call does not distort the per-row work, mirroring the generated path which times the
  // drain around each resume of the build.
  private var passThroughDrainStart: Long = -1L

  // Emits the next input row as a single-row partial aggregation buffer, i.e. a group of size one.
  // The output (grouping key ++ buffer) is a valid partial buffer that the downstream Final
  // aggregation merges, so the result is identical to running partial aggregation on this row.
  private def nextPassThroughOutput(): UnsafeRow = {
    val row = if (pendingPassThroughRow != null) {
      val stashed = pendingPassThroughRow
      pendingPassThroughRow = null
      stashed
    } else {
      inputIter.next()
    }
    val groupingKey = groupingProjection.apply(row)
    // Reset the buffer to initial values, then update it with this single row.
    passThroughAggregationBuffer.copyFrom(initialAggregationBuffer)
    processRow(passThroughAggregationBuffer, row)
    numBypassingRows += 1
    generateOutput(groupingKey, passThroughAggregationBuffer)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Loads input rows and sets up aggregationBufferMapIterator if we
  //         have not switched to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Start processing input rows.
   */
  processInputs(testFallbackStartsAt.getOrElse((Int.MaxValue, Int.MaxValue)))

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

  TaskContext.get().addTaskCompletionListener[Unit](_ => {
    // At the end of the task, update the task's peak memory usage. Since we destroy
    // the map to create the sorter, their memory usages should not overlap, so it is safe
    // to just use the max of the two.
    val mapMemory = hashMap.getPeakMemoryUsedBytes
    val sorterMemory = Option(externalSorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
    val maxMemory = Math.max(mapMemory, sorterMemory)
    val metrics = TaskContext.get().taskMetrics()
    peakMemory.set(maxMemory)
    spillSize.set(metrics.memoryBytesSpilled - spillSizeBefore)
    metrics.incPeakExecutionMemory(maxMemory)

    // Updating average hashmap probe
    avgHashProbe.set(hashMap.getAvgHashProbesPerKey)
  })

  ///////////////////////////////////////////////////////////////////////////
  // Part 8: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = {
    (sortBased && sortedInputHasNewGroup) || (!sortBased && mapIteratorHasNext) ||
      passThroughHasNext
  }

  override final def next(): UnsafeRow = {
    if (hasNext) {
      val res = if (sortBased && sortedInputHasNewGroup) {
        // Process the current group.
        processCurrentSortedGroup()
        // Generate output row for the current group.
        val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
        // Initialize buffer values for the next group.
        sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)

        outputRow
      } else if (mapIteratorHasNext) {
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
          // Then, we free the map. Pass-through (if any) does not use the map.
          hashMap.free()

          resultCopy
        } else {
          result
        }
      } else {
        // Adaptive partial aggregation bypassed partial aggregation: emit the remaining input
        // rows as single-row partial buffers, after the frozen map output above.
        if (passThroughDrainStart == -1L) {
          passThroughDrainStart = System.nanoTime()
        }
        val out = nextPassThroughOutput()
        if (!passThroughHasNext) {
          aggTime += NANOSECONDS.toMillis(System.nanoTime() - passThroughDrainStart)
          passThroughDrainStart = -1L
        }
        out
      }

      numOutputRows += 1
      res
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 9: Utility functions
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Generate an output row when there is no input and there is no grouping expression.
   */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    if (groupingExpressions.isEmpty) {
      sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)
      // We create an output row and copy it. So, we can free the map.
      val resultCopy =
        generateOutput(UnsafeRow.createFromByteArray(0, 0), sortBasedAggregationBuffer).copy()
      hashMap.free()
      resultCopy
    } else {
      throw SparkException.internalError(
        "This method should not be called when groupingExpressions is not empty.")
    }
  }
}
