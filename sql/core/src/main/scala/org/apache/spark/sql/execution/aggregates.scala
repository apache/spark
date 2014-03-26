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

package org.apache.spark.sql.execution

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._

/* Implicit conversions */
import org.apache.spark.rdd.PartitionLocalRDDFunctions._

/**
 * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
 *
 * @param partial if true then aggregation is done partially on local data without shuffling to
 *                ensure all values where `groupingExpressions` are equal are present.
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child the input data source.
 */
case class Aggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)(@transient sc: SparkContext)
  extends UnaryNode {

  override def requiredChildDistribution =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def otherCopyArgs = sc :: Nil

  def output = aggregateExpressions.map(_.toAttribute)

  /* Replace all aggregate expressions with spark functions that will compute the result. */
  def createAggregateImplementations() = aggregateExpressions.map { agg =>
    val impl = agg transform {
      case a: AggregateExpression => a.newInstance
    }

    val remainingAttributes = impl.collect { case a: Attribute => a }
    // If any references exist that are not inside agg functions then the must be grouping exprs
    // in this case we must rebind them to the grouping tuple.
    if (remainingAttributes.nonEmpty) {
      val unaliasedAggregateExpr = agg transform { case Alias(c, _) => c }

      // An exact match with a grouping expression
      val exactGroupingExpr = groupingExpressions.indexOf(unaliasedAggregateExpr) match {
        case -1 => None
        case ordinal => Some(BoundReference(ordinal, Alias(impl, "AGGEXPR")().toAttribute))
      }

      exactGroupingExpr.getOrElse(
        sys.error(s"$agg is not in grouping expressions: $groupingExpressions"))
    } else {
      impl
    }
  }

  def execute() = attachTree(this, "execute") {
    // TODO: If the child of it is an [[catalyst.execution.Exchange]],
    // do not evaluate the groupingExpressions again since we have evaluated it
    // in the [[catalyst.execution.Exchange]].
    val grouped = child.execute().mapPartitions { iter =>
      val buildGrouping = new Projection(groupingExpressions)
      iter.map(row => (buildGrouping(row), row.copy()))
    }.groupByKeyLocally()

    val result = grouped.map { case (group, rows) =>
      val aggImplementations = createAggregateImplementations()

      // Pull out all the functions so we can feed each row into them.
      val aggFunctions = aggImplementations.flatMap(_ collect { case f: AggregateFunction => f })

      rows.foreach { row =>
        aggFunctions.foreach(_.update(row))
      }
      buildRow(aggImplementations.map(_.apply(group)))
    }

    // TODO: THIS BREAKS PIPELINING, DOUBLE COMPUTES THE ANSWER, AND USES TOO MUCH MEMORY...
    if (groupingExpressions.isEmpty && result.count == 0) {
      // When there there is no output to the Aggregate operator, we still output an empty row.
      val aggImplementations = createAggregateImplementations()
      sc.makeRDD(buildRow(aggImplementations.map(_.apply(null))) :: Nil)
    } else {
      result
    }
  }
}
