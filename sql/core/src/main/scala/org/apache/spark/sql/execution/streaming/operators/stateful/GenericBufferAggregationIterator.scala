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
package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, MutableProjection, NamedExpression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.aggregate.AggregationIterator

/**
 * This base class extends [[AggregationIterator]] and produces a new aggregation buffer on demand.
 * The instance of aggregation buffer is optimal for given aggregate functions.
 *
 * This base class is useful for cases where aggregation buffer needs to be initialized frequently,
 * e.g. more than the cardinality of grouping keys.
 */
abstract class GenericBufferAggregationIterator(
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    originalInputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection)
  extends AggregationIterator(
    partIndex,
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) {

  protected val useUnsafeBuffer = aggregateFunctions.flatMap(_.aggBufferAttributes)
    .map(_.dataType).forall(UnsafeRow.isMutable)

  /**
   * Returns an aggregation buffer containing initial buffer values. Each call will produce the
   * different buffer instance.
   */
  protected def newAggregationBuffer(): InternalRow = {
    val buffer = initialAggregationBuffer.copy()
    // if we are using a GenericInternalRow which
    // is just a wrapper for an underlying data structured
    // we need to re-initialize the buffer since
    // copy does not actually create a new copy
    // of the underlying data structure
    if (!useUnsafeBuffer) {
      initializeBuffer(buffer)
    }
    buffer
  }

  // An aggregation buffer containing initial buffer values. It is used to
  // initialize other aggregation buffers.
  private val initialAggregationBuffer: InternalRow = createNewAggregationBuffer()

  private def createNewAggregationBuffer(): InternalRow = {
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val bufferRowSize: Int = bufferSchema.length
    val genericMutableBuffer = new GenericInternalRow(bufferRowSize)

    val buffer = if (useUnsafeBuffer) {
      val unsafeProjection =
        UnsafeProjection.create(bufferSchema.map(_.dataType))
      val buf = unsafeProjection.apply(genericMutableBuffer)
      initializeBuffer(buf)
      buf
    } else {
      genericMutableBuffer
    }
    buffer
  }
}
