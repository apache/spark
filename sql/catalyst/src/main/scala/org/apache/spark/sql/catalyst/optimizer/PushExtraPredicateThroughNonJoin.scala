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

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, FILTER, GENERATE, WINDOW}

/**
 * Try partially pushing down disjunctive filter condition through a non-join child
 * that produces new columns.
 * To avoid expanding the filter condition,
 * the filter condition will be kept in the original form even when predicate pushdown happens.
 */
object PushExtraPredicateThroughNonJoin extends Rule[LogicalPlan] with PredicateHelper {

  private def findExtraConditions(
    condition: Expression, outputSet: AttributeSet): Seq[Expression] = {
    splitConjunctivePredicates(condition).filter { f =>
      f.deterministic && f.references.nonEmpty && !f.references.subsetOf(outputSet)
    }.flatMap(extractPredicatesWithinOutputSet(_, outputSet))
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    t => t.containsPattern(FILTER) && t.containsAnyPattern(WINDOW, GENERATE, AGGREGATE), ruleId) {
    case filter @ Filter(condition, aggregate: Aggregate)
      if aggregate.aggregateExpressions.forall(_.deterministic)
        && aggregate.groupingExpressions.nonEmpty =>
      val aliasMap = getAliasMap(aggregate)
      val replaced = replaceAlias(condition, aliasMap)
      val extraConditions = findExtraConditions(replaced, aggregate.child.outputSet)

      if (extraConditions.nonEmpty) {
        val pushDownPredicate = extraConditions.reduce(And)
        filter.copy(child = aggregate.copy(child = Filter(pushDownPredicate, aggregate.child)))
      } else {
        filter
      }

    case filter @ Filter(condition, g: Generate) if g.expressions.forall(_.deterministic) =>
      val extraConditions = findExtraConditions(condition, g.child.outputSet)

      if (extraConditions.nonEmpty) {
        val pushDownPredicate = extraConditions.reduce(And)
        filter.copy(child = g.copy(child = Filter(pushDownPredicate, g.child)))
      } else {
        filter
      }

    case filter @ Filter(condition, w: Window)
        if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references))
      val extraConditions = findExtraConditions(condition, partitionAttrs)

      if (extraConditions.nonEmpty) {
        val pushDownPredicate = extraConditions.reduce(And)
        filter.copy(child = w.copy(child = Filter(pushDownPredicate, w.child)))
      } else {
        filter
      }
  }
}
