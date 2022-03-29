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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._

/**
 * Push down the partial aggregation through join if it cannot be planned as broadcast hash join.
 */
object PushPartialAggregationThroughJoin extends Rule[LogicalPlan]
  with PredicateHelper
  with JoinSelectionHelper {

  private def isSupportPushdown(aggregateExpressions: Seq[NamedExpression]): Boolean = {
    aggregateExpressions.forall {
      case _: Attribute => true
      case Alias(_: Attribute, _) => true
      case Alias(_: Literal, _) => true
      case _ => false
    }
  }

  private def split(expressions: Seq[NamedExpression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftExpressions, rest) =
      expressions.partition(e => e.references.nonEmpty && e.references.subsetOf(left.outputSet))
    val (rightExpressions, remainingExpressions) =
      rest.partition(e => e.references.nonEmpty && e.references.subsetOf(right.outputSet))

    (leftExpressions, rightExpressions, remainingExpressions)
  }

  private def pushdown(
      agg: Aggregate,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      join: Join): LogicalPlan = {
    val (leftGroupExps, rightGroupExps, remainingGroupExps) =
      split(agg.groupingExpressions.map(_.asInstanceOf[NamedExpression]), join.left, join.right)

    val (leftAggExps, rightAggExps, remainingAggExps) =
      split(agg.aggregateExpressions, join.left, join.right)

    val newLeftGroup = leftGroupExps ++ leftKeys.flatMap(_.references)
    val newLeftAgg: Seq[NamedExpression] = newLeftGroup ++ leftAggExps.flatMap(_.references)

    val newRightGroup = rightGroupExps ++ rightKeys.flatMap(_.references)
    val newRightAgg: Seq[NamedExpression] = newRightGroup ++ rightAggExps.flatMap(_.references)

    val newJoin = join.copy(
      left = Aggregate(newLeftGroup.distinct, newLeftAgg.distinct, true, join.left),
      right = Aggregate(newRightGroup.distinct, newRightAgg.distinct, true, join.right))

    if (agg.isPartialOnly) {
      Project(agg.aggregateExpressions, newJoin)
    } else {
      agg.copy(child = newJoin)
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAllPatterns(AGGREGATE, JOIN), ruleId) {
    case agg @ Aggregate(_, _, _, join: Join)
        if join.left.isInstanceOf[Aggregate] || join.right.isInstanceOf[Aggregate] =>
      agg
    case agg @ Aggregate(_, _, _, Project(_, join: Join))
        if join.left.isInstanceOf[Aggregate] || join.right.isInstanceOf[Aggregate] =>
      agg

    case agg @ Aggregate(_, aggExprs, _,
          join @ ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, None, _, left, right, _))
        if isSupportPushdown(aggExprs) && (leftKeys ++ rightKeys).nonEmpty &&
          !canPlanAsBroadcastHashJoin(join, conf) =>
      pushdown(agg, leftKeys, rightKeys, join)

    case agg @ Aggregate(_, aggExprs, _, Project(projectList,
          join @ ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, None, _, left, right, _)))
        if isSupportPushdown(aggExprs) && (leftKeys ++ rightKeys).nonEmpty &&
          projectList.forall(_.isInstanceOf[Attribute]) &&
          !canPlanAsBroadcastHashJoin(join, conf) =>
      pushdown(agg, leftKeys, rightKeys, join)
  }
}
