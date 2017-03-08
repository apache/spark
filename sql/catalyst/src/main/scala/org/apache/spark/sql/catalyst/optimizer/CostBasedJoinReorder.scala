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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Expression, PredicateHelper}
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
      val result = plan transform {
        case p @ Project(projectList, j @ Join(_, _, _: InnerLike, _)) =>
          reorder(p, p.outputSet)
        case j @ Join(_, _, _: InnerLike, _) =>
          reorder(j, j.outputSet)
      }
      // After reordering is finished, convert OrderedJoin back to Join
      result transform {
        case oj: OrderedJoin => oj.join
      }
    }
  }

  def reorder(plan: LogicalPlan, output: AttributeSet): LogicalPlan = {
    val (items, conditions) = extractInnerJoins(plan)
    val result =
      // Do reordering if the number of items is appropriate and join conditions exist.
      // We also need to check if costs of all items can be evaluated.
      if (items.size > 2 && items.size <= conf.joinReorderDPThreshold && conditions.nonEmpty &&
          items.forall(_.stats(conf).rowCount.isDefined)) {
        JoinReorderDP.search(conf, items, conditions, output).getOrElse(plan)
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
      case Project(projectList, join) if projectList.forall(_.isInstanceOf[Attribute]) =>
        extractInnerJoins(join)
      case _ =>
        (Seq(plan), Set())
    }
  }

  private def replaceWithOrderedJoin(plan: LogicalPlan): LogicalPlan = plan match {
    case j @ Join(left, right, _: InnerLike, cond) =>
      val replacedLeft = replaceWithOrderedJoin(left)
      val replacedRight = replaceWithOrderedJoin(right)
      OrderedJoin(j.copy(left = replacedLeft, right = replacedRight))
    case p @ Project(_, join) =>
      p.copy(child = replaceWithOrderedJoin(join))
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
 * For cost evaluation, since physical costs for operators are not available currently, we use
 * cardinalities and sizes to compute costs.
 */
object JoinReorderDP extends PredicateHelper {

  def search(
      conf: CatalystConf,
      items: Seq[LogicalPlan],
      conditions: Set[Expression],
      topOutput: AttributeSet): Option[LogicalPlan] = {

    // Level i maintains all found plans for i + 1 items.
    // Create the initial plans: each plan is a single item with zero cost.
    val itemIndex = items.zipWithIndex
    val foundPlans = mutable.Buffer[JoinPlanMap](itemIndex.map {
      case (item, id) => Set(id) -> JoinPlan(Set(id), item, Set(), Cost(0, 0))
    }.toMap)

    for (lev <- 1 until items.length) {
      // Build plans for the next level.
      foundPlans += searchLevel(foundPlans, conf, conditions, topOutput)
    }

    val plansLastLevel = foundPlans(items.length - 1)
    if (plansLastLevel.isEmpty) {
      // Failed to find a plan, fall back to the original plan
      None
    } else {
      // There must be only one plan at the last level, which contains all items.
      assert(plansLastLevel.size == 1 && plansLastLevel.head._1.size == items.length)
      Some(plansLastLevel.head._2.plan)
    }
  }

  /** Find all possible plans at the next level, based on existing levels. */
  private def searchLevel(
      existingLevels: Seq[JoinPlanMap],
      conf: CatalystConf,
      conditions: Set[Expression],
      topOutput: AttributeSet): JoinPlanMap = {

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
            val joinPlan = buildJoin(oneSidePlan, otherSidePlan, conf, conditions, topOutput)
            // Check if it's the first plan for the item set, or it's a better plan than
            // the existing one due to lower cost.
            val existingPlan = nextLevel.get(joinPlan.itemIds)
            if (existingPlan.isEmpty || joinPlan.cost.lessThan(existingPlan.get.cost)) {
              nextLevel.update(joinPlan.itemIds, joinPlan)
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
      conditions: Set[Expression],
      topOutput: AttributeSet): JoinPlan = {

    val onePlan = oneJoinPlan.plan
    val otherPlan = otherJoinPlan.plan
    // Now both onePlan and otherPlan become intermediate joins, so the cost of the
    // new join should also include their own cardinalities and sizes.
    val newCost = if (isCartesianProduct(onePlan) || isCartesianProduct(otherPlan)) {
      // We consider cartesian product very expensive, thus set a very large cost for it.
      // This enables to plan all the cartesian products at the end, because having a cartesian
      // product as an intermediate join will significantly increase a plan's cost, making it
      // impossible to be selected as the best plan for the items, unless there's no other choice.
      Cost(
        rows = BigInt(Long.MaxValue) * BigInt(Long.MaxValue),
        size = BigInt(Long.MaxValue) * BigInt(Long.MaxValue))
    } else {
      val onePlanStats = onePlan.stats(conf)
      val otherPlanStats = otherPlan.stats(conf)
      Cost(
        rows = oneJoinPlan.cost.rows + onePlanStats.rowCount.get +
          otherJoinPlan.cost.rows + otherPlanStats.rowCount.get,
        size = oneJoinPlan.cost.size + onePlanStats.sizeInBytes +
          otherJoinPlan.cost.size + otherPlanStats.sizeInBytes)
    }

    // Put the deeper side on the left, tend to build a left-deep tree.
    val (left, right) = if (oneJoinPlan.itemIds.size >= otherJoinPlan.itemIds.size) {
      (onePlan, otherPlan)
    } else {
      (otherPlan, onePlan)
    }
    val joinConds = conditions
      .filterNot(l => canEvaluate(l, onePlan))
      .filterNot(r => canEvaluate(r, otherPlan))
      .filter(e => e.references.subsetOf(onePlan.outputSet ++ otherPlan.outputSet))
    // We use inner join whether join condition is empty or not. Since cross join is
    // equivalent to inner join without condition.
    val newJoin = Join(left, right, Inner, joinConds.reduceOption(And))
    val collectedJoinConds = joinConds ++ oneJoinPlan.joinConds ++ otherJoinPlan.joinConds
    val remainingConds = conditions -- collectedJoinConds
    val neededAttr = AttributeSet(remainingConds.flatMap(_.references)) ++ topOutput
    val neededFromNewJoin = newJoin.outputSet.filter(neededAttr.contains)
    val newPlan =
      if ((newJoin.outputSet -- neededFromNewJoin).nonEmpty) {
        Project(neededFromNewJoin.toSeq, newJoin)
      } else {
        newJoin
      }

    val itemIds = oneJoinPlan.itemIds.union(otherJoinPlan.itemIds)
    JoinPlan(itemIds, newPlan, collectedJoinConds, newCost)
  }

  private def isCartesianProduct(plan: LogicalPlan): Boolean = plan match {
    case Join(_, _, _, None) => true
    case Project(_, Join(_, _, _, None)) => true
    case _ => false
  }

  /** Map[set of item ids, join plan for these items] */
  type JoinPlanMap = Map[Set[Int], JoinPlan]

  /**
   * Partial join order in a specific level.
   *
   * @param itemIds Set of item ids participating in this partial plan.
   * @param plan The plan tree with the lowest cost for these items found so far.
   * @param joinConds Join conditions included in the plan.
   * @param cost The cost of this plan is the sum of costs of all intermediate joins.
   */
  case class JoinPlan(itemIds: Set[Int], plan: LogicalPlan, joinConds: Set[Expression], cost: Cost)
}

/** This class defines the cost model. */
case class Cost(rows: BigInt, size: BigInt) {
  /**
   * An empirical value for the weights of cardinality (number of rows) in the cost formula:
   * cost = rows * weight + size * (1 - weight), usually cardinality is more important than size.
   */
  val weight = 0.7

  def lessThan(other: Cost): Boolean = {
    if (other.rows == 0 || other.size == 0) {
      false
    } else {
      val relativeRows = BigDecimal(rows) / BigDecimal(other.rows)
      val relativeSize = BigDecimal(size) / BigDecimal(other.size)
      relativeRows * weight + relativeSize * (1 - weight) < 1
    }
  }
}
