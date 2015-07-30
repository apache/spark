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
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

case class Aggregate2Sort(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression2],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {

  override def canProcessUnsafeRows: Boolean = true

  override def references: AttributeSet = {
    val referencesInResults =
      AttributeSet(resultExpressions.flatMap(_.references)) -- AttributeSet(aggregateAttributes)

    AttributeSet(
      groupingExpressions.flatMap(_.references) ++
      aggregateExpressions.flatMap(_.references) ++
      referencesInResults)
  }

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.length == 0 => AllTuples :: Nil
      case Some(exprs) if exprs.length > 0 => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    // TODO: We should not sort the input rows if they are just in reversed order.
    groupingExpressions.map(SortOrder(_, Ascending)) :: Nil
  }

  override def outputOrdering: Seq[SortOrder] = {
    // It is possible that the child.outputOrdering starts with the required
    // ordering expressions (e.g. we require [a] as the sort expression and the
    // child's outputOrdering is [a, b]). We can only guarantee the output rows
    // are sorted by values of groupingExpressions.
    groupingExpressions.map(SortOrder(_, Ascending))
  }

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      if (aggregateExpressions.length == 0) {
        new FinalSortAggregationIterator(
          groupingExpressions,
          Nil,
          Nil,
          resultExpressions,
          newMutableProjection,
          child.output,
          iter)
      } else {
        val aggregationIterator: SortAggregationIterator = {
          aggregateExpressions.map(_.mode).distinct.toList match {
            case Partial :: Nil =>
              new PartialSortAggregationIterator(
                groupingExpressions,
                aggregateExpressions,
                newMutableProjection,
                child.output,
                iter)
            case PartialMerge :: Nil =>
              new PartialMergeSortAggregationIterator(
                groupingExpressions,
                aggregateExpressions,
                newMutableProjection,
                child.output,
                iter)
            case Final :: Nil =>
              new FinalSortAggregationIterator(
                groupingExpressions,
                aggregateExpressions,
                aggregateAttributes,
                resultExpressions,
                newMutableProjection,
                child.output,
                iter)
            case other =>
              sys.error(
                s"Could not evaluate ${aggregateExpressions} because we do not support evaluate " +
                  s"modes $other in this operator.")
          }
        }

        aggregationIterator
      }
    }
  }
}

case class FinalAndCompleteAggregate2Sort(
    previousGroupingExpressions: Seq[NamedExpression],
    groupingExpressions: Seq[NamedExpression],
    finalAggregateExpressions: Seq[AggregateExpression2],
    finalAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {
  override def references: AttributeSet = {
    val referencesInResults =
      AttributeSet(resultExpressions.flatMap(_.references)) --
        AttributeSet(finalAggregateExpressions) --
        AttributeSet(completeAggregateExpressions)

    AttributeSet(
      groupingExpressions.flatMap(_.references) ++
        finalAggregateExpressions.flatMap(_.references) ++
        completeAggregateExpressions.flatMap(_.references) ++
        referencesInResults)
  }

  override def requiredChildDistribution: List[Distribution] = {
    if (groupingExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingExpressions) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    groupingExpressions.map(SortOrder(_, Ascending)) :: Nil

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>

      new FinalAndCompleteSortAggregationIterator(
        previousGroupingExpressions.length,
        groupingExpressions,
        finalAggregateExpressions,
        finalAggregateAttributes,
        completeAggregateExpressions,
        completeAggregateAttributes,
        resultExpressions,
        newMutableProjection,
        child.output,
        iter)
    }
  }

}
