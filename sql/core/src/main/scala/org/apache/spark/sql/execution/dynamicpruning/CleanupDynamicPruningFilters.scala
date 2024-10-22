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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{DynamicPruning, DynamicPruningSubquery, EqualNullSafe, EqualTo, Expression, ExpressionSet, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.planning.NodeWithOnlyDeterministicProjectAndFilter
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, RelationAndCatalogTable}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 *  Removes the filter nodes with dynamic pruning that were not pushed down to the scan.
 *  These nodes will not be pushed through projects and aggregates with non-deterministic
 *  expressions.
 */
object CleanupDynamicPruningFilters extends Rule[LogicalPlan] with PredicateHelper {

  private def collectEqualityConditionExpressions(condition: Expression): Seq[Expression] = {
    splitConjunctivePredicates(condition).flatMap(_.collect {
      case EqualTo(l, r) if l.deterministic && r.foldable => l
      case EqualTo(l, r) if r.deterministic && l.foldable => r
      case EqualNullSafe(l, r) if l.deterministic && r.foldable => l
      case EqualNullSafe(l, r) if r.deterministic && l.foldable => r
    })
  }

  /**
   * If a partition key already has equality conditions, then its DPP filter is useless and
   * can't prune anything. So we should remove it.
   */
  private def removeUnnecessaryDynamicPruningSubquery(plan: LogicalPlan): LogicalPlan = {
    plan.transformWithPruning(_.containsPattern(DYNAMIC_PRUNING_SUBQUERY)) {
      case f @ Filter(condition, _) =>
        lazy val unnecessaryPruningKeys =
          ExpressionSet(collectEqualityConditionExpressions(condition))
        val newCondition = condition.transformWithPruning(
          _.containsPattern(DYNAMIC_PRUNING_SUBQUERY)) {
          case dynamicPruning: DynamicPruningSubquery
              if dynamicPruning.pruningKey.references.isEmpty ||
                unnecessaryPruningKeys.contains(dynamicPruning.pruningKey) =>
            TrueLiteral
        }
        f.copy(condition = newCondition)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan.transformWithPruning(
      // No-op for trees that do not contain dynamic pruning.
      _.containsAnyPattern(DYNAMIC_PRUNING_EXPRESSION, DYNAMIC_PRUNING_SUBQUERY)) {
      // pass through anything that is pushed down into PhysicalOperation
      case p @ NodeWithOnlyDeterministicProjectAndFilter(
          RelationAndCatalogTable(_, _: HadoopFsRelation, _)) =>
        removeUnnecessaryDynamicPruningSubquery(p)
      // pass through anything that is pushed down into PhysicalOperation
      case p @ NodeWithOnlyDeterministicProjectAndFilter(
          HiveTableRelation(_, _, _, _, _)) =>
        removeUnnecessaryDynamicPruningSubquery(p)
      case p @ NodeWithOnlyDeterministicProjectAndFilter(
          _: DataSourceV2ScanRelation) =>
        removeUnnecessaryDynamicPruningSubquery(p)
      // remove any Filters with DynamicPruning that didn't get pushed down to PhysicalOperation.
      case f @ Filter(condition, _) =>
        val newCondition = condition.transformWithPruning(
          _.containsAnyPattern(DYNAMIC_PRUNING_EXPRESSION, DYNAMIC_PRUNING_SUBQUERY)) {
          case _: DynamicPruning => TrueLiteral
        }
        f.copy(condition = newCondition)
    }
  }
}
