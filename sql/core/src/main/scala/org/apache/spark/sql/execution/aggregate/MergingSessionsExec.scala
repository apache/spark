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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, MutableProjection, NamedExpression, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * This node is a variant of SortAggregateExec which merges the session windows based on the fact
 * child node will provide inputs as sorted by group keys + the start time of session window.
 *
 * When merging windows, it also applies aggregations on merged window, which eliminates the
 * necessity on buffering inputs (which requires copying rows) and update the session spec
 * for each input.
 *
 * This class receives requiredChildDistribution from caller, to enable merging session in
 * local partition before shuffling. Specifying both parameters to None won't trigger shuffle,
 * but sort would still happen per local partition.
 *
 * Refer [[MergingSessionsIterator]] for more details.
 */
case class MergingSessionsExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    isStreaming: Boolean,
    numShufflePartitions: Option[Int],
    groupingExpressions: Seq[NamedExpression],
    sessionExpression: NamedExpression,
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan) extends BaseAggregateExec {

  private val keyWithoutSessionExpressions = groupingExpressions.diff(Seq(sessionExpression))

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    Seq((keyWithoutSessionExpressions ++ Seq(sessionExpression)).map(SortOrder(_, Ascending)))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsWithIndexInternal { (partIndex, iter) =>
      // Because the constructor of an aggregation iterator will read at least the first row,
      // we need to get the value of iter.hasNext first.
      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator[UnsafeRow]()
      } else {
        val outputIter = new MergingSessionsIterator(
          partIndex,
          groupingExpressions,
          sessionExpression,
          child.output,
          iter,
          aggregateExpressions,
          aggregateAttributes,
          initialInputBufferOffset,
          resultExpressions,
          (expressions, inputSchema) =>
            MutableProjection.create(expressions, inputSchema),
          numOutputRows)
        if (!hasInput && groupingExpressions.isEmpty) {
          // There is no input and there is no grouping expressions.
          // We need to output a single row as the output.
          numOutputRows += 1
          Iterator[UnsafeRow](outputIter.outputForEmptyGroupingKeyWithoutInput())
        } else {
          outputIter
        }
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): MergingSessionsExec =
    copy(child = newChild)
}
