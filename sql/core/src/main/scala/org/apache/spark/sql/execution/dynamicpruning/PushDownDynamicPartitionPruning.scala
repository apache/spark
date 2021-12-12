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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{DYNAMIC_PRUNING_SUBQUERY, JOIN}

/**
 * This rule can handle pushing down a dynamic partition pruning from a child join to
 * its parent join, when the following conditions are met:
 * (1) the pruning side of the parent join is a partition table
 * (2) the table to prune is filterable by the JOIN key
 * (3) the parent join operation is one of the following types: INNER, LEFT SEMI,
 *  LEFT OUTER (partitioned on right), or RIGHT OUTER (partitioned on left)
 *
 * Use the dynamic partition pruning of the child join to create a new dynamic partition pruning for
 * the parent join with the onlyInBroadcast is true, to make sure it will use the filter only if it
 * can reuse the results of the broadcast through ReuseExchange.
 */
object PushDownDynamicPartitionPruning extends Rule[LogicalPlan] with DynamicPruningHelper {

  /**
   * Searches for the head dynamic partition pruning in children joins, and use it to create a new
   * dynamic partition pruning for their parent join.
   */
  private def getDynamicPartitionPruning(
    exp: Expression,
    plan: LogicalPlan): Option[DynamicPruningSubquery] = plan match {
    case p @ Project(_, child: Join) =>
      if (canPushThrough(child.joinType)) {
        getDynamicPartitionPruning(replaceAlias(exp, getAliasMap(p)), child)
      } else {
        None
      }
    // we can unwrap only if there are row projections, and no aggregation operation
    case a @ Aggregate(_, _, child: Join) =>
      if (canPushThrough(child.joinType)) {
        getDynamicPartitionPruning(replaceAlias(exp, getAliasMap(a)), child)
      } else {
        None
      }
    case f @ Filter(d: DynamicPruningSubquery, l)
        if exp.references.subsetOf(f.outputSet) && exp.references.subsetOf(l.outputSet) =>
      Some(d)
    case other =>
      other.children.flatMap {
        child => if (exp.references.subsetOf(child.outputSet)) {
          getDynamicPartitionPruning(exp, child)
        } else {
          None
        }
      }.headOption
  }

  private def canPushThrough(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | RightOuter | LeftOuter => true
    case _ => false
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.dynamicPartitionPruningEnabled || !conf.dynamicPartitionPruningPushdownEnabled) {
      plan
    } else {
      plan.transformWithPruning(
        _.containsAllPatterns(DYNAMIC_PRUNING_SUBQUERY, JOIN)) {
        // push down the dynamic partition pruning from the a join to its parent join when the
        // exchange can reuse.
        case Join(left, right, joinType, Some(condition), hint) if conf.exchangeReuseEnabled &&
            canPushThrough(joinType) =>
          var newLeft = left
          var newRight = right
          splitConjunctivePredicates(condition).foreach {
            case EqualTo(a: Expression, b: Expression)
                if fromDifferentSides(left, right, a, b) =>
              val (l, r) = if (a.references.subsetOf(left.outputSet) &&
                b.references.subsetOf(right.outputSet)) {
                a -> b
              } else {
                b -> a
              }
              // currently only supports reusing the results of the broadcast from the child
              // join through ReuseExchange.
              var filterableScan = getFilterableTableScan(l, left)
              if (filterableScan.isDefined && canPruneLeft(joinType) &&
                hasPartitionPruningFilter(right)) {
                val rightChildDpp = getDynamicPartitionPruning(r, right)
                rightChildDpp.foreach {
                  case d: DynamicPruningSubquery
                      if l.dataType == d.buildKeys(d.broadcastKeyIndex).dataType =>
                    newLeft = Filter(DynamicPruningSubquery(l, d.buildQuery, d.buildKeys,
                      d.broadcastKeyIndex, true), newLeft)
                  case _ =>
                }
              } else {
                filterableScan = getFilterableTableScan(r, right)
                if (filterableScan.isDefined && canPruneRight(joinType) &&
                  hasPartitionPruningFilter(left)) {
                  val leftChildDpp = getDynamicPartitionPruning(l, left)
                  leftChildDpp.foreach {
                    case d: DynamicPruningSubquery
                        if r.dataType == d.buildKeys(d.broadcastKeyIndex).dataType =>
                      newRight = Filter(DynamicPruningSubquery(r, d.buildQuery, d.buildKeys,
                        d.broadcastKeyIndex, true), newRight)
                    case _ =>
                  }
                }
              }
            case _ =>
          }
          Join(newLeft, newRight, joinType, Some(condition), hint)
        case j => j
      }
    }
  }
}
