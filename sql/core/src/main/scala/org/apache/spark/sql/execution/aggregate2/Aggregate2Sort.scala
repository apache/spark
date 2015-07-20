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

package org.apache.spark.sql.execution.aggregate2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate2._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

case class Aggregate2Sort(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression2],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {

  /** Indicates if this operator is for partial aggregations. */
  val partialAggregation: Boolean = {
    aggregateExpressions.map(_.mode).distinct.toList match {
      case Partial :: Nil => true
      case Final :: Nil => false
      case other =>
        sys.error(
          s"Could not evaluate ${aggregateExpressions} because we do not support evaluate " +
          s"modes $other in this operator.")
    }
  }

  override def references: AttributeSet = {
    val referencesInResults =
      AttributeSet(resultExpressions.flatMap(_.references)) -- AttributeSet(aggregateAttributes)

    AttributeSet(
      groupingExpressions.flatMap(_.references) ++
      aggregateExpressions.flatMap(_.references) ++
      referencesInResults)
  }

  override def requiredChildDistribution: List[Distribution] = {
    if (partialAggregation) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    groupingExpressions.map(SortOrder(_, Ascending)) :: Nil

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val aggregationIterator =
        if (partialAggregation) {
          new PartialSortAggregationIterator(
            groupingExpressions,
            aggregateExpressions,
            newMutableProjection,
            child.output,
            iter)
        } else {
          new FinalSortAggregationIterator(
            groupingExpressions,
            aggregateExpressions,
            aggregateAttributes,
            resultExpressions,
            newMutableProjection,
            child.output,
            iter)
        }

      aggregationIterator
    }
  }
}
