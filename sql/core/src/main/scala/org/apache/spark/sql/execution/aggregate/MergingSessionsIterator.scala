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
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, CreateNamedStruct, Expression, GenericInternalRow, Literal, MutableProjection, NamedExpression, PreciseTimestampConversion, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{LongType, TimestampType}

// FIXME: javadoc!
// FIXME: groupingExpressions should contain sessionExpression
class MergingSessionsIterator(
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    sessionExpression: NamedExpression,
    valueAttributes: Seq[Attribute],
    inputIterator: Iterator[InternalRow],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    numOutputRows: SQLMetric)
  extends AggregationIterator(
    partIndex,
    groupingExpressions,
    valueAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) {

  val groupingWithoutSession: Seq[NamedExpression] =
    groupingExpressions.diff(Seq(sessionExpression))
  val groupingWithoutSessionAttributes: Seq[Attribute] = groupingWithoutSession.map(_.toAttribute)


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

  private[this] var currentSessionStart: Long = Long.MaxValue

  private[this] var currentSessionEnd: Long = Long.MinValue

  // The partition key of next partition.
  private[this] var nextGroupingKey: UnsafeRow = _

  private[this] var nextGroupingSessionStart: Long = Long.MaxValue

  private[this] var nextGroupingSessionEnd: Long = Long.MinValue

  // The first row of next partition.
  private[this] var firstRowInNextGroup: InternalRow = _

  // Indicates if we has new group of rows from the sorted input iterator
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
  private[this] val sortBasedAggregationBuffer: InternalRow = newBuffer

  private[this] val groupingWithoutSessionProjection: UnsafeProjection =
    UnsafeProjection.create(groupingWithoutSession, valueAttributes)

  private[this] val sessionIndex = resultExpressions.indexOf(sessionExpression)

  private[this] val sessionProjection: UnsafeProjection =
    UnsafeProjection.create(Seq(sessionExpression), valueAttributes)

  protected def initialize(): Unit = {
    if (inputIterator.hasNext) {
      initializeBuffer(sortBasedAggregationBuffer)
      val inputRow = inputIterator.next()
      nextGroupingKey = groupingWithoutSessionProjection(inputRow).copy()
      val session = sessionProjection(inputRow)
      val sessionRow = session.getStruct(0, 2)
      nextGroupingSessionStart = sessionRow.getLong(0)
      nextGroupingSessionEnd = sessionRow.getLong(1)
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
    currentSessionStart = nextGroupingSessionStart
    currentSessionEnd = nextGroupingSessionEnd

    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    processRow(sortBasedAggregationBuffer, firstRowInNextGroup)

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    while (!findNextPartition && inputIterator.hasNext) {
      // Get the grouping key.
      val currentRow = inputIterator.next()
      val groupingKey = groupingWithoutSessionProjection(currentRow)

      val session = sessionProjection(currentRow)
      val sessionRow = session.getStruct(0, 2)
      val sessionStart = sessionRow.getLong(0)
      val sessionEnd = sessionRow.getLong(1)

      // Check if the current row belongs the current input row.
      if (currentGroupingKey == groupingKey) {
        if (sessionStart < currentSessionStart) {
          throw new IllegalArgumentException("Input iterator is not sorted based on session!")
        } else if (sessionStart <= currentSessionEnd) {
          // expanding session length if needed
          expandEndOfCurrentSession(sessionEnd)
          processRow(sortBasedAggregationBuffer, currentRow)
        } else {
          // We find a new group.
          findNextPartition = true
          startNewSession(currentRow, groupingKey, sessionStart, sessionEnd)
        }
      } else {
        // We find a new group.
        findNextPartition = true
        startNewSession(currentRow, groupingKey, sessionStart, sessionEnd)
      }
    }

    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the iter.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
    }
  }

  private def startNewSession(currentRow: InternalRow, groupingKey: UnsafeRow, sessionStart: Long,
                              sessionEnd: Long): Unit = {
    nextGroupingKey = groupingKey.copy()
    nextGroupingSessionStart = sessionStart
    nextGroupingSessionEnd = sessionEnd
    firstRowInNextGroup = currentRow.copy()
  }

  private def expandEndOfCurrentSession(sessionEnd: Long): Unit = {
    if (sessionEnd > currentSessionEnd) {
      currentSessionEnd = sessionEnd
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

      val groupingKey = generateGroupingKey()

      val outputRow = generateOutput(groupingKey, sortBasedAggregationBuffer)
      // Initialize buffer values for the next group.
      initializeBuffer(sortBasedAggregationBuffer)
      numOutputRows += 1
      outputRow
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  private def generateGroupingKey(): UnsafeRow = {
    val sessionStruct = CreateNamedStruct(
      Literal("start") ::
        PreciseTimestampConversion(
          Literal(currentSessionStart, LongType), LongType, TimestampType) ::
        Literal("end") ::
        PreciseTimestampConversion(
          Literal(currentSessionEnd, LongType), LongType, TimestampType) ::
        Nil)

    val convertedGroupingExpressions = groupingExpressions.map { x =>
      if (x.semanticEquals(sessionExpression)) {
        sessionStruct
      } else {
        BindReferences.bindReference[Expression](x, groupingWithoutSessionAttributes)
      }
    }

    val proj = GenerateUnsafeProjection.generate(convertedGroupingExpressions,
      groupingWithoutSessionAttributes)
    proj(currentGroupingKey)
  }

  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    initializeBuffer(sortBasedAggregationBuffer)
    generateOutput(UnsafeRow.createFromByteArray(0, 0), sortBasedAggregationBuffer)
  }
}
