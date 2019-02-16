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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Complete}
import org.apache.spark.sql.catalyst.plans.LeftSemiOrAnti
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Pushes Left semi and Left Anti joins below the following operators.
 *  1) Project
 *  2) Window
 *  3) Union
 *  4) Aggregate
 *  5) Other permissible unary operators. please see [[PushDownPredicate.canPushThrough]].
 */

object PushDownLeftSemiAntiJoin extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Similar to the above Filter over Project
    // LeftSemi/LeftAnti over Project
    case Join(p @ Project(pList, gChild), rightOp, LeftSemiOrAnti(joinType), joinCond, hint)
      if pList.forall(_.deterministic) &&
        !pList.find(ScalarSubquery.hasScalarSubquery(_)).isDefined &&
        canPushThroughCondition(Seq(gChild), joinCond, rightOp) =>
      if (joinCond.isEmpty) {
        // No join condition, just push down the Join below Project
        p.copy(child = Join(gChild, rightOp, joinType, joinCond, hint))
      } else {
        val aliasMap = PushDownPredicate.getAliasMap(p)
        val newJoinCond = if (aliasMap.nonEmpty) {
          Option(replaceAlias(joinCond.get, aliasMap))
        } else {
          joinCond
        }
        p.copy(child = Join(gChild, rightOp, joinType, newJoinCond, hint))
      }

    // Similar to the above Filter over Aggregate
    // LeftSemi/LeftAnti over Aggregate
    case join @ Join(agg: Aggregate, rightOp, LeftSemiOrAnti(joinType), joinCond, hint)
      if agg.aggregateExpressions.forall(_.deterministic)
        && agg.groupingExpressions.nonEmpty =>
      if (joinCond.isEmpty) {
        // No join condition, just push down Join below Aggregate
        agg.copy(child = Join(agg.child, rightOp, joinType, joinCond, hint))
      } else {
        val aliasMap = PushDownPredicate.getAliasMap(agg)

        // For each join condition, expand the alias and
        // check if the condition can be evaluated using
        // attributes produced by the aggregate operator's child operator.
        val (pushDown, stayUp) = splitConjunctivePredicates(joinCond.get).partition { cond =>
          val replaced = replaceAlias(cond, aliasMap)
          cond.references.nonEmpty &&
            replaced.references.subsetOf(agg.child.outputSet ++ rightOp.outputSet)
        }

        // Check if the remaining predicates do not contain columns from subquery
        val rightOpColumns = AttributeSet(stayUp.toSet).intersect(rightOp.outputSet)

        if (pushDown.nonEmpty && rightOpColumns.isEmpty) {
          val pushDownPredicate = pushDown.reduce(And)
          val replaced = replaceAlias(pushDownPredicate, aliasMap)
          val newAgg = agg.copy(child = Join(agg.child, rightOp, joinType, Option(replaced), hint))
          // If there is no more filter to stay up, just return the Aggregate over Join.
          // Otherwise, create "Filter(stayUp) <- Aggregate <- Join(pushDownPredicate)".
          if (stayUp.isEmpty) newAgg else Filter(stayUp.reduce(And), newAgg)
        } else {
          // The join condition is not a subset of the Aggregate's GROUP BY columns,
          // no push down.
          join
        }
      }

    // Similar to the above Filter over Window
    // LeftSemi/LeftAnti over Window
    case join @ Join(w: Window, rightOp, LeftSemiOrAnti(joinType), joinCond, hint)
      if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
      if (joinCond.isEmpty) {
        // No join condition, just push down Join below Window
        w.copy(child = Join(w.child, rightOp, joinType, joinCond, hint))
      } else {
        val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references)) ++
          rightOp.outputSet

        val (pushDown, stayUp) = splitConjunctivePredicates(joinCond.get).partition { cond =>
          cond.references.subsetOf(partitionAttrs)
        }

        // Check if the remaining predicates do not contain columns from subquery
        val rightOpColumns = AttributeSet(stayUp.toSet).intersect(rightOp.outputSet)

        if (pushDown.nonEmpty && rightOpColumns.isEmpty) {
          val predicate = pushDown.reduce(And)
          val newPlan = w.copy(child = Join(w.child, rightOp, joinType, Option(predicate), hint))
          if (stayUp.isEmpty) newPlan else Filter(stayUp.reduce(And), newPlan)
        } else {
          // The join condition is not a subset of the Window's PARTITION BY clause,
          // no push down.
          join
        }
      }

    // Similar to the above Filter over Union
    // LeftSemi/LeftAnti over Union
    case join @ Join(union: Union, rightOp, LeftSemiOrAnti(joinType), joinCond, hint)
      if canPushThroughCondition(union.children, joinCond, rightOp) =>
      if (joinCond.isEmpty) {
        // Push down the Join below Union
        val newGrandChildren = union.children.map { Join(_, rightOp, joinType, joinCond, hint) }
        union.withNewChildren(newGrandChildren)
      } else {
        val pushDown = splitConjunctivePredicates(joinCond.get)

        if (pushDown.nonEmpty) {
          val pushDownCond = pushDown.reduceLeft(And)
          val output = union.output
          val newGrandChildren = union.children.map { grandchild =>
            val newCond = pushDownCond transform {
              case e if output.exists(_.semanticEquals(e)) =>
                grandchild.output(output.indexWhere(_.semanticEquals(e)))
            }
            assert(newCond.references.subsetOf(grandchild.outputSet ++ rightOp.outputSet))
            Join(grandchild, rightOp, joinType, Option(newCond), hint)
          }
          union.withNewChildren(newGrandChildren)
        } else {
          // Nothing to push down
          join
        }
      }

    // Similar to the above Filter over UnaryNode
    // LeftSemi/LeftAnti over UnaryNode
    case join @ Join(u: UnaryNode, rightOp, LeftSemiOrAnti(joinType), joinCond, hint)
      if PushDownPredicate.canPushThrough(u) =>
      pushDownJoin(join, u.child) { joinCond =>
        u.withNewChildren(Seq(Join(u.child, rightOp, joinType, Option(joinCond), hint)))
      }
  }

  /**
   * Check if we can safely push a join through a project or union by making sure that predicate
   * subqueries in the condition do not contain the same attributes as the plan they are moved
   * into. This can happen when the plan and predicate subquery have the same source.
   */
  private def canPushThroughCondition(plans: Seq[LogicalPlan], condition: Option[Expression],
    rightOp: LogicalPlan): Boolean = {
    val attributes = AttributeSet(plans.flatMap (_.output))
    if (condition.isDefined) {
      val matched = condition.get.references.intersect(rightOp.outputSet).intersect(attributes)
      matched.isEmpty
    } else true
  }


  private def pushDownJoin(
    join: Join,
    grandchild: LogicalPlan)(insertFilter: Expression => LogicalPlan): LogicalPlan = {
    val (pushDown, stayUp) = if (join.condition.isDefined) {
      splitConjunctivePredicates(join.condition.get)
        .partition {_.references.subsetOf(grandchild.outputSet ++ join.right.outputSet)}
    } else {
      (Nil, Nil)
    }

    if (pushDown.nonEmpty) {
      val newChild = insertFilter(pushDown.reduceLeft(And))
      if (stayUp.nonEmpty) {
        Filter(stayUp.reduceLeft(And), newChild)
      } else {
        newChild
      }
    } else {
      join
    }
  }
}

