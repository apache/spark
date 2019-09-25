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

import java.util.concurrent.TimeUnit._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * A hash-based aggregate operator that supports [[TypedImperativeAggregate]] functions that may
 * use arbitrary JVM objects as aggregation states.
 *
 * Similar to [[HashAggregateExec]], this operator also falls back to sort-based aggregation when
 * the size of the internal hash map exceeds the threshold. The differences are:
 *
 *  - It uses safe rows as aggregation buffer since it must support JVM objects as aggregation
 *    states.
 *
 *  - It tracks entry count of the hash map instead of byte size to decide when we should fall back.
 *    This is because it's hard to estimate the accurate size of arbitrary JVM objects in a
 *    lightweight way.
 *
 *  - Whenever fallen back to sort-based aggregation, this operator feeds all of the rest input rows
 *    into external sorters instead of building more hash map(s) as what [[HashAggregateExec]] does.
 *    This is because having too many JVM object aggregation states floating there can be dangerous
 *    for GC.
 *
 *  - CodeGen is not supported yet.
 *
 * This operator may be turned off by setting the following SQL configuration to `false`:
 * {{{
 *   spark.sql.execution.useObjectHashAggregateExec
 * }}}
 * The fallback threshold can be configured by tuning:
 * {{{
 *   spark.sql.objectHashAggregate.sortBased.fallbackThreshold
 * }}}
 */
case class ObjectHashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode {

  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in aggregation build")
  )

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
    AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
    AttributeSet(aggregateBufferAttributes)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) if exprs.nonEmpty => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numOutputRows = longMetric("numOutputRows")
    val aggTime = longMetric("aggTime")
    val fallbackCountThreshold = sqlContext.conf.objectAggSortBasedFallbackThreshold

    child.execute().mapPartitionsWithIndexInternal { (partIndex, iter) =>
      val beforeAgg = System.nanoTime()
      val hasInput = iter.hasNext
      val res = if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input kvIterator is empty,
        // so return an empty kvIterator.
        Iterator.empty
      } else {
        val aggregationIterator =
          new ObjectAggregationIterator(
            partIndex,
            child.output,
            groupingExpressions,
            aggregateExpressions,
            aggregateAttributes,
            initialInputBufferOffset,
            resultExpressions,
            (expressions, inputSchema) =>
              newMutableProjection(expressions, inputSchema, subexpressionEliminationEnabled),
            child.output,
            iter,
            fallbackCountThreshold,
            numOutputRows)
        if (!hasInput && groupingExpressions.isEmpty) {
          numOutputRows += 1
          Iterator.single[UnsafeRow](aggregationIterator.outputForEmptyGroupingKeyWithoutInput())
        } else {
          aggregationIterator
        }
      }
      aggTime += NANOSECONDS.toMillis(System.nanoTime() - beforeAgg)
      res
    }
  }

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions
    val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
    val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    if (verbose) {
      s"ObjectHashAggregate(keys=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"ObjectHashAggregate(keys=$keyString, functions=$functionString)"
    }
  }
}

object ObjectHashAggregateExec {
  def supportsAggregate(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    aggregateExpressions.map(_.aggregateFunction).exists {
      case _: TypedImperativeAggregate[_] => true
      case _ => false
    }
  }
}
