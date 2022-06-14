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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{JOIN, PROJECT}

object ConvertInnerJoinToSemiJoin
  extends Rule[LogicalPlan]
  with PredicateHelper
  with SQLConfHelper
  with JoinSelectionHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAllPatterns(PROJECT, JOIN)) {
    case p @ Project(_, j @ ExtractEquiJoinKeys(_, _, _, nonEquiCond, _, _, agg: Aggregate, _))
        if innerEqualJoin(j, nonEquiCond) &&
          !canBroadcastBySize(agg, conf) &&
          canTransform(agg, j, p) =>
      // eliminate the agg, replace it with a project with all its expressions
      p.copy(child = j.copy(
        joinType = LeftSemi, right = Project(agg.aggregateExpressions, agg.child)))
    case p @ Project(_, j @ ExtractEquiJoinKeys(_, _, _, nonEquiCond, _, agg: Aggregate, _, _))
      if innerEqualJoin(j, nonEquiCond) &&
        !canBroadcastBySize(agg, conf) &&
        canTransform(agg, j, p) =>
      // eliminate the agg, replace it with a project with all its expressions
      // it will swap the join order from inner join(left, right) to semi join(right, left)
      p.copy(child = j.copy(
        joinType = LeftSemi, left = j.right, right = Project(agg.aggregateExpressions, agg.child)))
  }

  def innerEqualJoin(join: Join, nonEquiCond: Option[Expression]): Boolean = {
    join.joinType == Inner && join.condition.nonEmpty && nonEquiCond.isEmpty
  }

  def canTransform(agg: Aggregate, join: Join, project: Project): Boolean = {
    // 1. grouping only - the agg output and only output all the grouping cols
    // 2. all grouping cols should be in join condition
    // 3. all grouping cols should not appear in the project above
    exactGroupingOnly(agg) &&
      agg.aggregateExpressions.forall(join.condition.get.references.contains(_)) &&
      agg.aggregateExpressions.forall(!project.references.contains(_))
  }

  // test if the aggregate exprs and grouping exprs are exactly the same
  def exactGroupingOnly(agg: Aggregate): Boolean = {
    if (agg.aggregateExpressions.size != agg.groupingExpressions.size) {
      return false
    }

    val foundInGroupings = agg.aggregateExpressions.map {
      case Alias(child, _) =>
        agg.groupingExpressions.indexWhere(_.semanticEquals(child))
      case other =>
        agg.groupingExpressions.indexWhere(_.semanticEquals(other))
    }.toSet
    // all agg expr should be found in grouping expr
    // and the number should match
    !foundInGroupings.contains(-1) && foundInGroupings.size == agg.groupingExpressions.size
  }
}
