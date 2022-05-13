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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{And, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, First}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, Limit, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.FIRST
import org.apache.spark.sql.types.IntegerType

/**
 * Rewrite aggregate plan with only [[First]] functions when grouping is absent. In such a case
 * we push a limit 1 with filter and projection to a sub-query.
 *
 * Input Pseudo-Query:
 * {{{
 *   SELECT FIRST(col1), FIRST(col2) FROM table
 * }}}
 *
 * Rewritten Query:
 * {{{
 *   SELECT FIRST(col1), FIRST(col2) FROM (SELECT col1, col2 FROM table LIMIT 1)
 * }}}
 *
 * Note that using IGNORE NULLS with [[First]] blocks rewrite logic since projection with NOT NULL
 * filter might return different result than [[First]] if all values are NULL.
 */
object RewriteNonAggregateFirst extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformWithPruning(_.containsPattern(FIRST), ruleId) {
      case agg@Aggregate(_, _, GlobalLimit(Literal(1, IntegerType), _)) => agg
      case agg@Aggregate(_, _, PhysicalOperation(projectList, filters, child))
        if isNonAggregateFirst(agg) =>
        agg.withNewChildren(Seq(
          Limit(Literal(1, IntegerType),
            if (filters.isEmpty) {
              Project(projectList, child = child)
            } else {
              Filter(filters.reduceLeft(And), Project(projectList, child = child))
            })))
    }
  }

  private def isNonAggregateFirst(agg: Aggregate): Boolean = {
    // We check that no grouping expression is present, aggregate expressions contain only First
    // function and IGNORE NULLS is not used in any First functions.
    !conf.optimizerMetadataOnly && agg.groupingExpressions.isEmpty &&
      agg.aggregateExpressions.flatMap {
        _.collect {
          case AggregateExpression(First(_, false), _, _, _, _) => 0
          case _: AggregateExpression => 1
          case _ => 0
        }
      }.sum == 0
  }
}
