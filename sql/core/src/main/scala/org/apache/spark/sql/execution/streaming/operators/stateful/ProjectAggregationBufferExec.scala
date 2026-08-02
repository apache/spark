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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, MutableProjection, NamedExpression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * This class handles the part of aggregation functions in the input rows, based on the function's
 * mode. This class intends to either initialize the aggregation buffer or complete the aggregate
 * buffer and produce the result, so it is expected to be used for two aggregate modes:
 * 1) partial merge 2) final. This class is pass-through and does not perform the actual
 * aggregation.
 */
case class ProjectAggregationBufferExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]] = None,
    numShufflePartitions: Option[Int],
    groupingExpressions: Seq[NamedExpression] = Nil,
    aggregateExpressions: Seq[AggregateExpression] = Nil,
    aggregateAttributes: Seq[Attribute] = Nil,
    initialInputBufferOffset: Int = 0,
    resultExpressions: Seq[NamedExpression] = Nil,
    isFinalAggregate: Boolean,
    child: SparkPlan)
  extends BaseAggregateExec {

  override val isStreaming: Boolean = true

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy initialization at driver

    val numOutputRows = longMetric("numOutputRows")

    child.execute().mapPartitionsWithIndex { case (partIdx, iter) =>
      val aggProcessor = new ProjectAggregationBufferProcessor(
        partIdx,
        groupingExpressions,
        inputAttributes,
        aggregateExpressions,
        aggregateAttributes,
        initialInputBufferOffset,
        resultExpressions,
        (expressions, inputSchema) =>
          MutableProjection.create(expressions, inputSchema))

      iter.map { row =>
        numOutputRows += 1
        aggProcessor.process(row)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def toSortAggregate: SortAggregateExec = {
    throw new IllegalStateException("This class cannot be replaced with SortAggregate!")
  }
}

/**
 * This class is an implementation of GenericBufferAggregationIterator which only handles the
 * aggregation buffer of input, depending on the aggregate mode. This class is pass-through
 * and does not perform the actual aggregation.
 */
class ProjectAggregationBufferProcessor(
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    originalInputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection)
  extends GenericBufferAggregationIterator(
    partIndex,
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) {

  def hasNext: Boolean =
    throw new UnsupportedOperationException(
      "hasNext is not supported in ProjectAggregationBufferProcessor")

  def next(): UnsafeRow =
    throw new UnsupportedOperationException(
      "next is not supported in ProjectAggregationBufferProcessor")

  def process(newInput: InternalRow): UnsafeRow = {
    val groupingKey = groupingProjection.apply(newInput)
    val buffer = newAggregationBuffer()
    processRow(buffer, newInput)
    generateOutput(groupingKey, buffer)
  }
}
