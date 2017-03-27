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

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan


/**
 * A pattern that finds a aggregate pair: a partial aggregate and its parent.
 */
object AggregatePair {

  def unapply(plan: SparkPlan): Option[(AggregateExec, AggregateExec)] = plan match {
    case outer: AggregateExec if outer.child.isInstanceOf[AggregateExec] =>
      val inner = outer.child.asInstanceOf[AggregateExec]

      // Check if classes and grouping keys are the same with each other to make sure the two
      // aggregates are for the same group by a GROUP-BY clause.
      if (outer.getClass == inner.getClass &&
          outer.groupingExpressions.map(_.toAttribute) ==
            inner.groupingExpressions.map(_.toAttribute)) {
        Some(outer, inner)
      } else {
        None
      }

    case _ =>
      None
  }
}

/**
 * Merge partial (map-side) aggregates into their parent aggregates if the parent aggregates
 * directly have the partial aggregates as children.
 */
object MergePartialAggregate extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = plan transform {
    // Normal pair: a partial aggregate and its parent
    case AggregatePair(outer, inner)
        if outer.aggregateExpressions.forall(_.mode == Final) &&
          inner.aggregateExpressions.forall(expr => expr.mode == Partial && !expr.isDistinct) =>
      inner.copy(
        aggregateExpressions = inner.aggregateExpressions.map(_.copy(mode = Complete)),
        aggregateAttributes = inner.aggregateExpressions.map(_.resultAttribute),
        resultExpressions = outer.resultExpressions)

    // First partial aggregate pair for aggregation with distinct
    case AggregatePair(outer, inner)
        if (outer.aggregateExpressions.forall(_.mode == PartialMerge) &&
            inner.aggregateExpressions.forall(_.mode == Partial)) ||
          // If a query has a single distinct aggregate (that is, it has no non-distinct aggregate),
          // the first aggregate pair has empty aggregate functions.
          Seq(outer, inner).forall(_.aggregateExpressions.isEmpty) =>
      inner

    // Second partial aggregate pair for aggregation with distinct. If input data are already
    // partitioned and the same columns are used in grouping keys and aggregation values,
    // a distribution requirement of a second partial aggregate is satisfied and then we can safely
    // merge the second aggregate into its parent.
    // A query example of this case is;
    //
    //  SELECT t.value, SUM(DISTINCT t.value)
    //    FROM (SELECT * FROM inputTable ORDER BY value) t
    //    GROUP BY t.value
    case AggregatePair(outer, inner)
        if outer.aggregateExpressions.forall(_.mode == Final) &&
          inner.aggregateExpressions.exists(_.isDistinct) =>
      outer.copy(
        aggregateExpressions = outer.aggregateExpressions.map {
          case funcWithDistinct if funcWithDistinct.isDistinct =>
            funcWithDistinct.copy(mode = Complete)
          case otherFunc =>
            otherFunc
        },
        child = inner.child
      )
  }
}
