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

package org.apache.spark.sql.execution.window

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._


/**
 * This class prepares and manages the processing of a number of [[AggregateFunction]]s within a
 * single frame. The [[WindowFunctionFrame]] takes care of processing the frame in the correct way
 * that reduces the processing of a [[AggregateWindowFunction]] to processing the underlying
 * [[AggregateFunction]]. All [[AggregateFunction]]s are processed in [[Complete]] mode.
 *
 * [[SizeBasedWindowFunction]]s are initialized in a slightly different way. These functions
 * require the size of the partition processed and this value is exposed to them when
 * the processor is constructed.
 *
 * Processing of distinct aggregates is currently not supported.
 *
 * The implementation is split into an object which takes care of construction, and the actual
 * processor class.
 */
private[window] object AggregateProcessor {
  def apply(
      functions: Array[Expression],
      ordinal: Int,
      inputAttributes: Seq[Attribute],
      newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection)
    : AggregateProcessor = {
    val aggBufferAttributes = mutable.Buffer.empty[AttributeReference]
    val initialValues = mutable.Buffer.empty[Expression]
    val updateExpressions = mutable.Buffer.empty[Expression]
    val evaluateExpressions = mutable.Buffer.fill[Expression](ordinal)(NoOp)
    val imperatives = mutable.Buffer.empty[ImperativeAggregate]

    // SPARK-14244: `SizeBasedWindowFunction`s are firstly created on driver side and then
    // serialized to executor side. These functions all reference a global singleton window
    // partition size attribute reference, i.e., `SizeBasedWindowFunction.n`. Here we must collect
    // the singleton instance created on driver side instead of using executor side
    // `SizeBasedWindowFunction.n` to avoid binding failure caused by mismatching expression ID.
    val partitionSize: Option[AttributeReference] = {
      val aggs = functions.flatMap(_.collectFirst { case f: SizeBasedWindowFunction => f })
      aggs.headOption.map(_.n)
    }

    // Check if there are any SizeBasedWindowFunctions. If there are, we add the partition size to
    // the aggregation buffer. Note that the ordinal of the partition size value will always be 0.
    partitionSize.foreach { n =>
      aggBufferAttributes += n
      initialValues += NoOp
      updateExpressions += NoOp
    }

    // Add an AggregateFunction to the AggregateProcessor.
    functions.foreach {
      case agg: DeclarativeAggregate =>
        aggBufferAttributes ++= agg.aggBufferAttributes
        initialValues ++= agg.initialValues
        updateExpressions ++= agg.updateExpressions
        evaluateExpressions += agg.evaluateExpression
      case agg: ImperativeAggregate =>
        val offset = aggBufferAttributes.size
        val imperative = BindReferences.bindReference(agg
          .withNewInputAggBufferOffset(offset)
          .withNewMutableAggBufferOffset(offset),
          inputAttributes)
        imperatives += imperative
        aggBufferAttributes ++= imperative.aggBufferAttributes
        val noOps = Seq.fill(imperative.aggBufferAttributes.size)(NoOp)
        initialValues ++= noOps
        updateExpressions ++= noOps
        evaluateExpressions += imperative
      case other =>
        throw new IllegalStateException(s"Unsupported aggregate function: $other")
    }

    // Create the projections.
    val initialProj = newMutableProjection(initialValues.toSeq, partitionSize.toSeq)
    val updateProj =
      newMutableProjection(updateExpressions.toSeq, (aggBufferAttributes ++ inputAttributes).toSeq)
    val evalProj = newMutableProjection(evaluateExpressions.toSeq, aggBufferAttributes.toSeq)

    // Create the processor
    new AggregateProcessor(
      aggBufferAttributes.toArray,
      initialProj,
      updateProj,
      evalProj,
      imperatives.toArray,
      partitionSize.isDefined)
  }
}

/**
 * This class manages the processing of a number of aggregate functions. See the documentation of
 * the object for more information.
 */
private[window] final class AggregateProcessor(
    private[this] val bufferSchema: Array[AttributeReference],
    private[this] val initialProjection: MutableProjection,
    private[this] val updateProjection: MutableProjection,
    private[this] val evaluateProjection: MutableProjection,
    private[this] val imperatives: Array[ImperativeAggregate],
    private[this] val trackPartitionSize: Boolean) {

  private[this] val join = new JoinedRow
  private[this] val numImperatives = imperatives.length
  private[this] val buffer = new SpecificInternalRow(bufferSchema.toSeq.map(_.dataType))
  initialProjection.target(buffer)
  updateProjection.target(buffer)

  /** Create the initial state. */
  def initialize(size: Int): Unit = {
    // Some initialization expressions are dependent on the partition size so we have to
    // initialize the size before initializing all other fields, and we have to pass the buffer to
    // the initialization projection.
    if (trackPartitionSize) {
      buffer.setInt(0, size)
    }
    initialProjection(buffer)
    var i = 0
    while (i < numImperatives) {
      imperatives(i).initialize(buffer)
      i += 1
    }
  }

  /** Update the buffer. */
  def update(input: InternalRow): Unit = {
    updateProjection(join(buffer, input))
    var i = 0
    while (i < numImperatives) {
      imperatives(i).update(buffer, input)
      i += 1
    }
  }

  /** Evaluate buffer. */
  def evaluate(target: InternalRow): Unit = {
    evaluateProjection.target(target)(buffer)
  }
}
