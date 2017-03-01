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
import org.apache.spark.sql.catalyst.expressions.{And, AttributeSet, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{Inner, InnerLike}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
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
      plan transform {
        case p @ Project(projectList, j @ Join(_, _, _: InnerLike, _)) if !j.ordered =>
          p.copy(child = reorder(j, p.outputSet))
        case j @ Join(_, _, _: InnerLike, _) if !j.ordered =>
          reorder(j, j.outputSet)
      }
    }
  }

  def reorder(plan: LogicalPlan, output: AttributeSet): LogicalPlan = {
    val (items, conditions) = extractInnerJoins(plan)
    if (items.size <= conf.joinReorderDPThreshold && conditions.nonEmpty) {
      JoinReorderDP(conf, items, conditions, output).search().getOrElse(plan)
    } else {
      plan
    }
  }

  /**
   * Extract inner joinable items and join conditions.
   * This method works for bushy trees and left/right deep trees.
   */
  def extractInnerJoins(plan: LogicalPlan): (Seq[LogicalPlan], Seq[Expression]) = plan match {
    case j @ Join(left, right, _: InnerLike, cond) =>
      // Set ordered to true, such that if we fail to reorder the joins, we won't try to do again.
      j.ordered = true
      val (leftPlans, leftConditions) = extractInnerJoins(left)
      val (rightPlans, rightConditions) = extractInnerJoins(right)
      (leftPlans ++ rightPlans,
        cond.map(splitConjunctivePredicates).getOrElse(Nil) ++ leftConditions ++ rightConditions)
    case Project(_, j @ Join(left, right, _: InnerLike, cond)) =>
      j.ordered = true
      val (leftPlans, leftConditions) = extractInnerJoins(left)
      val (rightPlans, rightConditions) = extractInnerJoins(right)
      (leftPlans ++ rightPlans,
        cond.map(splitConjunctivePredicates).getOrElse(Nil) ++ leftConditions ++ rightConditions)
    case _ =>
      (Seq(plan), Seq())
  }
}

/**
 * Reorder the joins using a dynamic programming algorithm:
 * First we put all items (basic joined nodes) into level 1, then we build all two-way joins
 * at level 2 from plans at level 1 (single items), then build all 3-way joins from plans
 * at previous levels (two-way joins and single items), then 4-way joins ... etc, until we
 * build all n-way joins and pick the best plan among them.
 *
 * When building m-way joins, we only keep the best plan (with the lowest cost) for the same set
 * of m items. E.g., for 3-way joins, we keep only the best plan for items {A, B, C} among
 * plans (A J B) J C, (A J C) J B and (B J C) J A.
 *
 * Thus the plans maintained for each level when reordering four items A, B, C, D are as follows:
 * level 1: p({A}), p({B}), p({C}), p({D})
 * level 2: p({A, B}), p({A, C}), p({A, D}), p({B, C}), p({B, D}), p({C, D})
 * level 3: p({A, B, C}), p({A, B, D}), p({A, C, D}), p({B, C, D})
 * level 4: p({A, B, C, D})
 * where p({A, B, C, D}) is the final output plan.
 */
case class JoinReorderDP(
    conf: CatalystConf,
    items: Seq[LogicalPlan],
    conditions: Seq[Expression],
    output: AttributeSet) extends PredicateHelper{
  
  /** Level i maintains all found plans for sets of i joinable items. */
  val foundPlans = new Array[mutable.Map[Set[Int], JoinPlan]](items.length + 1)
  val itemIndex = items.zipWithIndex
  val totalRequiredAttr = AttributeSet(conditions.flatMap(_.references)) ++ output

  def search(): Option[LogicalPlan] = {
    // Start from the first level: each plan is a single item with zero cost.
    foundPlans(1) = mutable.Map[Set[Int], JoinPlan](
      itemIndex.map { case (item, id) => Set(id) -> JoinPlan(Set(id), item, Cost(0, 0)) }: _*)

    for (lev <- 2 to items.length) {
      searchForLevel(lev)
    }

    val plansLastLevel = foundPlans(items.length)
    if (plansLastLevel.isEmpty) {
      // Failed to find a plan, fall back to the original plan
      None
    } else {
      // There must be only one plan at the last level, which contains all items.
      assert(plansLastLevel.size == 1 && plansLastLevel.head._1.size == items.length)
      Some(plansLastLevel.head._2.plan)
    }
  }

  /** Find all possible plans in one level, based on previous levels. */
  private def searchForLevel(level: Int): Unit = {
    val foundPlansCurLevel = foundPlans(level)
    var k = 1
    var continue = true
    while (continue) {
      val otherLevel = level - k
      if (k > otherLevel) {
        // We can break from here, because when building a join from A and B, both A J B and B J A
        // are handled.
        continue = false
      } else {
        val joinPlansLevelK = foundPlans(k).values.toSeq
        for (i <- joinPlansLevelK.indices) {
          val curJoinPlan = joinPlansLevelK(i)

          val joinPlansOtherLevel = if (k == otherLevel) {
            // Both sides of a join are at the same level, no need to repeat for previous ones.
            joinPlansLevelK.drop(i)
          } else {
            foundPlans(otherLevel).values.toSeq
          }

          joinPlansOtherLevel.foreach { otherJoinPlan =>
            // Should not join two overlapping item sets.
            if (curJoinPlan.itemIds.intersect(otherJoinPlan.itemIds).isEmpty) {
              val joinPlan = buildJoin(curJoinPlan, otherJoinPlan)
              if (joinPlan.nonEmpty) {
                // Check if it's the first plan for the item set, or it's a better plan than
                // the existing one due to lower cost.
                val existingPlan = foundPlansCurLevel.get(joinPlan.get.itemIds)
                if (existingPlan.isEmpty || joinPlan.get.cost < existingPlan.get.cost) {
                  foundPlansCurLevel.update(joinPlan.get.itemIds, joinPlan.get)
                }
              }
            }
          }
        }

        k += 1
      }
    }
  }

  /** Build a new join node. */
  private def buildJoin(curJoinPlan: JoinPlan, otherJoinPlan: JoinPlan): Option[JoinPlan] = {
    // Check if these two nodes are inner joinable. We consider cartesian product very
    // costly, thus exclude such plans. This also helps us to reduce the search space.
    val curPlan = curJoinPlan.plan
    val otherPlan = otherJoinPlan.plan
    val joinCond = conditions
      .filterNot(l => canEvaluate(l, curPlan))
      .filterNot(r => canEvaluate(r, otherPlan))
      .filter(e => e.references.subsetOf(curPlan.outputSet ++ otherPlan.outputSet))

    if (joinCond.nonEmpty) {
      val curPlanStats = curPlan.stats(conf)
      val otherPlanStats = otherPlan.stats(conf)
      if (curPlanStats.rowCount.nonEmpty && otherPlanStats.rowCount.nonEmpty) {
        // Now both curPlan and otherPlan become intermediate joins, so the cost of the
        // new join should also include their cost.
        val cost = curJoinPlan.cost + otherJoinPlan.cost +
          Cost(curPlanStats.rowCount.get, curPlanStats.sizeInBytes) +
          Cost(otherPlanStats.rowCount.get, otherPlanStats.sizeInBytes)

        val itemIds = curJoinPlan.itemIds.union(otherJoinPlan.itemIds)
        val newJoin = Join(curPlan, otherPlan, Inner, joinCond.reduceOption(And))
        val newPlan =
          if ((newJoin.outputSet -- newJoin.outputSet.filter(totalRequiredAttr.contains))
            .nonEmpty) {
            Project(newJoin.output.filter(totalRequiredAttr.contains), newJoin)
          } else {
            newJoin
          }
        return Some(JoinPlan(itemIds, newPlan, cost))
      }
    }
    None
  }

  /**
   * Partial join order in a specific level.
   *
   * @param itemIds Set of item ids participating in this partial plan.
   * @param plan The plan tree with the lowest cost for these items found so far.
   * @param cost The cost of this plan is the sum of cost of all intermediate joins.
   */
  case class JoinPlan(itemIds: Set[Int], plan: LogicalPlan, cost: Cost)
}

/** This class defines the cost model. */
case class Cost(rows: BigInt, sizeInBytes: BigInt) {
  /**
   * An empirical value for the weights of cardinality (number of rows) in the cost formula:
   * cost = rows * weight + size * (1 - weight), usually cardinality is more important than size.
   */
  val weight = 0.7

  def +(other: Cost): Cost = Cost(rows + other.rows, sizeInBytes + other.sizeInBytes)

  def <(other: Cost): Boolean = {
    if (other.rows == 0 || other.sizeInBytes == 0) {
      false
    } else {
      val relativeRows = BigDecimal(rows) / BigDecimal(other.rows)
      val relativeSize = BigDecimal(sizeInBytes) / BigDecimal(other.sizeInBytes)
      relativeRows * weight + relativeSize * (1 - weight) < 1
    }
  }
}
