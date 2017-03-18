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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{Inner, InnerLike}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule


/**
 * Cost-based join reorder.
 * We may have several join reorder algorithms in the future. This class is the entry of these
 * algorithms, and chooses which one to use.
 */
case class CostBasedJoinReorder(conf: CatalystConf) extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.cboEnabled || !conf.joinReorderEnabled) {
      plan
    } else {
      val result = plan transformDown {
        case j @ Join(_, _, _: InnerLike, _) => reorder(j)
      }
      // After reordering is finished, convert OrderedJoin back to Join
      result transformDown {
        case oj: OrderedJoin => oj.join
      }
    }
  }

  def reorder(plan: LogicalPlan): LogicalPlan = {
    val (items, conditions) = extractInnerJoins(plan)
    val result =
      // Do reordering if the number of items is appropriate and join conditions exist.
      // We also need to check if costs of all items can be evaluated.
      if (items.size > 2 && items.size <= conf.joinReorderDPThreshold && conditions.nonEmpty &&
          items.forall(_.stats(conf).rowCount.isDefined)) {
        JoinReorderDP.search(conf, items, conditions).getOrElse(plan)
      } else {
        plan
      }
    // Set consecutive join nodes ordered.
    replaceWithOrderedJoin(result)
  }

  /**
   * Extract consecutive inner joinable items and join conditions.
   * This method works for bushy trees and left/right deep trees.
   */
  private def extractInnerJoins(plan: LogicalPlan): (Seq[LogicalPlan], Set[Expression]) = {
    plan match {
      case Join(left, right, _: InnerLike, cond) =>
        val (leftPlans, leftConditions) = extractInnerJoins(left)
        val (rightPlans, rightConditions) = extractInnerJoins(right)
        (leftPlans ++ rightPlans, cond.toSet.flatMap(splitConjunctivePredicates) ++
          leftConditions ++ rightConditions)
      case Project(projectList, j: Join) if projectList.forall(_.isInstanceOf[Attribute]) =>
        extractInnerJoins(j)
      case _ =>
        (Seq(plan), Set())
    }
  }

  private def replaceWithOrderedJoin(plan: LogicalPlan): LogicalPlan = plan match {
    case j @ Join(left, right, _: InnerLike, cond) =>
      val replacedLeft = replaceWithOrderedJoin(left)
      val replacedRight = replaceWithOrderedJoin(right)
      OrderedJoin(j.copy(left = replacedLeft, right = replacedRight))
    case p @ Project(projectList, j: Join) =>
      p.copy(child = replaceWithOrderedJoin(j))
    case _ =>
      plan
  }

  /** This is a wrapper class for a join node that has been ordered. */
  private case class OrderedJoin(join: Join) extends BinaryNode {
    override def left: LogicalPlan = join.left
    override def right: LogicalPlan = join.right
    override def output: Seq[Attribute] = join.output
  }
}

/**
 * Reorder the joins using a dynamic programming algorithm. This implementation is based on the
 * paper: Access Path Selection in a Relational Database Management System.
 * http://www.inf.ed.ac.uk/teaching/courses/adbs/AccessPath.pdf
 *
 * First we put all items (basic joined nodes) into level 0, then we build all two-way joins
 * at level 1 from plans at level 0 (single items), then build all 3-way joins from plans
 * at previous levels (two-way joins and single items), then 4-way joins ... etc, until we
 * build all n-way joins and pick the best plan among them.
 *
 * When building m-way joins, we only keep the best plan (with the lowest cost) for the same set
 * of m items. E.g., for 3-way joins, we keep only the best plan for items {A, B, C} among
 * plans (A J B) J C, (A J C) J B and (B J C) J A.
 *
 * Thus the plans maintained for each level when reordering four items A, B, C, D are as follows:
 * level 0: p({A}), p({B}), p({C}), p({D})
 * level 1: p({A, B}), p({A, C}), p({A, D}), p({B, C}), p({B, D}), p({C, D})
 * level 2: p({A, B, C}), p({A, B, D}), p({A, C, D}), p({B, C, D})
 * level 3: p({A, B, C, D})
 * where p({A, B, C, D}) is the final output plan.
 *
 * To evaluate cost for a given plan, we calculate the sum of cardinalities for all intermediate
 * joins in the plan.
 */
object JoinReorderDP extends PredicateHelper {

  def search(
      conf: CatalystConf,
      items: Seq[LogicalPlan],
      conditions: Set[Expression]): Option[LogicalPlan] = {

    // Level i maintains all found plans for i + 1 items.
    // Create the initial plans: each plan is a single item with zero cost.
    val itemIndex = items.zipWithIndex.map(_.swap).toMap
    val foundPlans = mutable.Buffer[JoinPlanMap](itemIndex.map {
      case (id, item) => Set(id) -> JoinPlan(Set(id), item, cost = 0)
    })

    while (foundPlans.size < items.length && foundPlans.last.size > 1) {
      // Build plans for the next level.
      foundPlans += searchLevel(foundPlans, conf, conditions)
    }

    // Find the best plan
    assert(foundPlans.last.size <= 1)
    val bestJoinPlan = foundPlans.last.headOption

    // Put cartesian products (inner join without join condition) at the end of the plan.
    bestJoinPlan.map { case (itemIds, joinPlan) =>
      val itemsNotJoined = itemIndex.keySet -- itemIds
      var completePlan = joinPlan.plan
      itemsNotJoined.foreach { id =>
        completePlan = Join(completePlan, itemIndex(id), Inner, None)
      }
      completePlan
    }
  }

  /** Find all possible plans at the next level, based on existing levels. */
  private def searchLevel(
      existingLevels: Seq[JoinPlanMap],
      conf: CatalystConf,
      conditions: Set[Expression]): JoinPlanMap = {

    val nextLevel = mutable.Map.empty[Set[Int], JoinPlan]
    var k = 0
    val lev = existingLevels.length - 1
    // Build plans for the next level from plans at level k (one side of the join) and level
    // lev - k (the other side of the join).
    // For the lower level k, we only need to search from 0 to lev - k, because when building
    // a join from A and B, both A J B and B J A are handled.
    while (k <= lev - k) {
      val oneSideCandidates = existingLevels(k).values.toSeq
      for (i <- oneSideCandidates.indices) {
        val oneSidePlan = oneSideCandidates(i)
        val otherSideCandidates = if (k == lev - k) {
          // Both sides of a join are at the same level, no need to repeat for previous ones.
          oneSideCandidates.drop(i)
        } else {
          existingLevels(lev - k).values.toSeq
        }

        otherSideCandidates.foreach { otherSidePlan =>
          // Should not join two overlapping item sets.
          if (oneSidePlan.itemIds.intersect(otherSidePlan.itemIds).isEmpty) {
            val joinPlan = buildJoin(oneSidePlan, otherSidePlan, conf, conditions)
            if (joinPlan.isDefined) {
              val newJoinPlan = joinPlan.get
              // Check if it's the first plan for the item set, or it's a better plan than
              // the existing one due to lower cost.
              val existingPlan = nextLevel.get(newJoinPlan.itemIds)
              if (existingPlan.isEmpty || newJoinPlan.cost < existingPlan.get.cost) {
                nextLevel.update(newJoinPlan.itemIds, newJoinPlan)
              }
            }
          }
        }
      }
      k += 1
    }
    nextLevel.toMap
  }

  /** Build a new join node. */
  private def buildJoin(
      oneJoinPlan: JoinPlan,
      otherJoinPlan: JoinPlan,
      conf: CatalystConf,
      conditions: Set[Expression]): Option[JoinPlan] = {

    val onePlan = oneJoinPlan.plan
    val otherPlan = otherJoinPlan.plan
    val joinConds = conditions
      .filterNot(l => canEvaluate(l, onePlan))
      .filterNot(r => canEvaluate(r, otherPlan))
      .filter(e => e.references.subsetOf(onePlan.outputSet ++ otherPlan.outputSet))
    if (joinConds.isEmpty) {
      // Cartesian product is very expensive, so we exclude them from candidate plans.
      // This also helps us to reduce the search space. Unjoinable items will be put at the end
      // of the plan when the reordering phase finishes.
      None
    } else {
      // Put the deeper side on the left, tend to build a left-deep tree.
      val (left, right) = if (oneJoinPlan.itemIds.size >= otherJoinPlan.itemIds.size) {
        (onePlan, otherPlan)
      } else {
        (otherPlan, onePlan)
      }
      val newJoin = Join(left, right, Inner, joinConds.reduceOption(And))
      val itemIds = oneJoinPlan.itemIds.union(otherJoinPlan.itemIds)

      // Now onePlan/otherPlan becomes an intermediate join (if it's a non-leaf item),
      // so the cost of the new join should also include their own cardinalities.
      val newCost = oneJoinPlan.cost + otherJoinPlan.cost +
        (if (oneJoinPlan.itemIds.size > 1) onePlan.stats(conf).rowCount.get else 0) +
        (if (otherJoinPlan.itemIds.size > 1) otherPlan.stats(conf).rowCount.get else 0)

      Some(JoinPlan(itemIds, newJoin, newCost))
    }
  }

  /** Map[set of item ids, join plan for these items] */
  type JoinPlanMap = Map[Set[Int], JoinPlan]

  /**
   * Partial join order in a specific level.
   *
   * @param itemIds Set of item ids participating in this partial plan.
   * @param plan The plan tree with the lowest cost for these items found so far.
   * @param cost Sum of cardinalities of all intermediate joins in the plan tree.
   */
  case class JoinPlan(itemIds: Set[Int], plan: LogicalPlan, cost: BigInt)
}
