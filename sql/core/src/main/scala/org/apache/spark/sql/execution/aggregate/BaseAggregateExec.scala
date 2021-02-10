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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Final, PartialMerge}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{AliasAwareOutputPartitioning, ExplainUtils, UnaryExecNode}

/**
 * Holds common logic for aggregate operators
 */
trait BaseAggregateExec extends UnaryExecNode with AliasAwareOutputPartitioning {
  def requiredChildDistributionExpressions: Option[Seq[Expression]]
  def groupingExpressions: Seq[NamedExpression]
  def aggregateExpressions: Seq[AggregateExpression]
  def aggregateAttributes: Seq[Attribute]
  def resultExpressions: Seq[NamedExpression]

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("Keys", groupingExpressions)}
       |${ExplainUtils.generateFieldString("Functions", aggregateExpressions)}
       |${ExplainUtils.generateFieldString("Aggregate Attributes", aggregateAttributes)}
       |${ExplainUtils.generateFieldString("Results", resultExpressions)}
       |""".stripMargin
  }

  protected def inputAttributes: Seq[Attribute] = {
    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.contains(Final) || modes.contains(PartialMerge)) {
      // SPARK-31620: when planning aggregates, the partial aggregate uses aggregate function's
      // `inputAggBufferAttributes` as its output. And Final and PartialMerge aggregate rely on the
      // output to bind references for `DeclarativeAggregate.mergeExpressions`. But if we copy the
      // aggregate function somehow after aggregate planning, like `PlanSubqueries`, the
      // `DeclarativeAggregate` will be replaced by a new instance with new
      // `inputAggBufferAttributes` and `mergeExpressions`. Then Final and PartialMerge aggregate
      // can't bind the `mergeExpressions` with the output of the partial aggregate, as they use
      // the `inputAggBufferAttributes` of the original `DeclarativeAggregate` before copy. Instead,
      // we shall use `inputAggBufferAttributes` after copy to match the new `mergeExpressions`.
      val aggAttrs = inputAggBufferAttributes
      child.output.dropRight(aggAttrs.length) ++ aggAttrs
    } else {
      child.output
    }
  }

  private val inputAggBufferAttributes: Seq[Attribute] = {
    aggregateExpressions
      // there're exactly four cases needs `inputAggBufferAttributes` from child according to the
      // agg planning in `AggUtils`: Partial -> Final, PartialMerge -> Final,
      // Partial -> PartialMerge, PartialMerge -> PartialMerge.
      .filter(a => a.mode == Final || a.mode == PartialMerge)
      .flatMap(_.aggregateFunction.inputAggBufferAttributes)
  }

  protected val aggregateBufferAttributes: Seq[AttributeReference] = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
    AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
    AttributeSet(aggregateBufferAttributes) ++
    // it's not empty when the inputAggBufferAttributes is not equal to the aggregate buffer
    // attributes of the child Aggregate, when the child Aggregate contains the subquery in
    // AggregateFunction. See SPARK-31620 for more details.
    AttributeSet(inputAggBufferAttributes.filterNot(child.output.contains))

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }
}
