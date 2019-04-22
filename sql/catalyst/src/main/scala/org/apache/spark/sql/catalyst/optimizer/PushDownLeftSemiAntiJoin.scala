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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This rule is a variant of [[PushDownPredicate]] which can handle
 * pushing down Left semi and Left Anti joins below the following operators.
 *  1) Project
 *  2) Window
 *  3) Union
 *  4) Aggregate
 *  5) Other permissible unary operators. please see [[PushDownPredicate.canPushThrough]].
 */
object PushDownLeftSemiAntiJoin extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // LeftSemi/LeftAnti over Project
    case Join(p @ Project(pList, gChild), rightOp, LeftSemiOrAnti(joinType), joinCond, hint)
        if pList.forall(_.deterministic) &&
        !pList.exists(ScalarSubquery.hasCorrelatedScalarSubquery) &&
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

    // LeftSemi/LeftAnti over Aggregate
    case join @ Join(agg: Aggregate, rightOp, LeftSemiOrAnti(_), _, _)
        if agg.aggregateExpressions.forall(_.deterministic) && agg.groupingExpressions.nonEmpty &&
        !agg.aggregateExpressions.exists(ScalarSubquery.hasCorrelatedScalarSubquery) =>
      val aliasMap = PushDownPredicate.getAliasMap(agg)
      val canPushDownPredicate = (predicate: Expression) => {
        val replaced = replaceAlias(predicate, aliasMap)
        predicate.references.nonEmpty &&
          replaced.references.subsetOf(agg.child.outputSet ++ rightOp.outputSet)
      }
      val makeJoinCondition = (predicates: Seq[Expression]) => {
        replaceAlias(predicates.reduce(And), aliasMap)
      }
      pushDownJoin(join, canPushDownPredicate, makeJoinCondition)

    // LeftSemi/LeftAnti over Window
    case join @ Join(w: Window, rightOp, LeftSemiOrAnti(_), _, _)
        if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references)) ++ rightOp.outputSet
      pushDownJoin(join, _.references.subsetOf(partitionAttrs), _.reduce(And))

    // LeftSemi/LeftAnti over Union
    case Join(union: Union, rightOp, LeftSemiOrAnti(joinType), joinCond, hint)
        if canPushThroughCondition(union.children, joinCond, rightOp) =>
      if (joinCond.isEmpty) {
        // Push down the Join below Union
        val newGrandChildren = union.children.map { Join(_, rightOp, joinType, joinCond, hint) }
        union.withNewChildren(newGrandChildren)
      } else {
        val output = union.output
        val newGrandChildren = union.children.map { grandchild =>
          val newCond = joinCond.get transform {
            case e if output.exists(_.semanticEquals(e)) =>
              grandchild.output(output.indexWhere(_.semanticEquals(e)))
          }
          assert(newCond.references.subsetOf(grandchild.outputSet ++ rightOp.outputSet))
          Join(grandchild, rightOp, joinType, Option(newCond), hint)
        }
        union.withNewChildren(newGrandChildren)
      }

    // LeftSemi/LeftAnti over UnaryNode
    case join @ Join(u: UnaryNode, rightOp, LeftSemiOrAnti(_), _, _)
        if PushDownPredicate.canPushThrough(u) && u.expressions.forall(_.deterministic) =>
      val validAttrs = u.child.outputSet ++ rightOp.outputSet
      pushDownJoin(join, _.references.subsetOf(validAttrs), _.reduce(And))
  }

  /**
   * Check if we can safely push a join through a project or union by making sure that attributes
   * referred in join condition do not contain the same attributes as the plan they are moved
   * into. This can happen when both sides of join refers to the same source (self join). This
   * function makes sure that the join condition refers to attributes that are not ambiguous (i.e
   * present in both the legs of the join) or else the resultant plan will be invalid.
   */
  private def canPushThroughCondition(
      plans: Seq[LogicalPlan],
      condition: Option[Expression],
      rightOp: LogicalPlan): Boolean = {
    val attributes = AttributeSet(plans.flatMap(_.output))
    if (condition.isDefined) {
      val matched = condition.get.references.intersect(rightOp.outputSet).intersect(attributes)
      matched.isEmpty
    } else {
      true
    }
  }

  private def pushDownJoin(
      join: Join,
      canPushDownPredicate: Expression => Boolean,
      makeJoinCondition: Seq[Expression] => Expression): LogicalPlan = {
    assert(join.left.children.length == 1)

    if (join.condition.isEmpty) {
      join.left.withNewChildren(Seq(join.copy(left = join.left.children.head)))
    } else {
      val (pushDown, stayUp) = splitConjunctivePredicates(join.condition.get)
        .partition(canPushDownPredicate)

      // Check if the remaining predicates do not contain columns from the right hand side of the
      // join. Since the remaining predicates will be kept as a filter over the operator under join,
      // this check is necessary after the left-semi/anti join is pushed down. The reason is, for
      // this kind of join, we only output from the left leg of the join.
      val referRightSideCols = AttributeSet(stayUp.toSet).intersect(join.right.outputSet).nonEmpty

      if (pushDown.isEmpty || referRightSideCols)  {
        join
      } else {
        val newPlan = join.left.withNewChildren(Seq(join.copy(
          left = join.left.children.head, condition = Some(makeJoinCondition(pushDown)))))
        // If there is no more filter to stay up, return the new plan that has join pushed down.
        if (stayUp.isEmpty) {
          newPlan
        } else {
          join.joinType match {
            // In case of Left semi join, the part of the join condition which does not refer to
            // to attributes of the grandchild are kept as a Filter above.
            case LeftSemi => Filter(stayUp.reduce(And), newPlan)
            // In case of left-anti join, the join is pushed down only when the entire join
            // condition is eligible to be pushed down to preserve the semantics of left-anti join.
            case _ => join
          }
        }
      }
    }
  }
}

/**
 * This rule is a variant of [[PushPredicateThroughJoin]] which can handle
 * pushing down Left semi and Left Anti joins below a join operator. The
 * allowable join types are:
 *  1) Inner
 *  2) Cross
 *  3) LeftOuter
 *  4) RightOuter
 *
 * TODO:
 * Currently this rule can push down the left semi or left anti joins to either
 * left or right leg of the child join. This matches the behaviour of `PushPredicateThroughJoin`
 * when the lefi semi or left anti join is in expression form. We need to explore the possibility
 * to push the left semi/anti joins to both legs of join if the join condition refers to
 * both left and right legs of the child join.
 */
object PushLeftSemiLeftAntiThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Define an enumeration to identify whether a LeftSemi/LeftAnti join can be pushed down to
   * the left leg or the right leg of the join.
   */
  object PushdownDirection extends Enumeration {
    val TO_LEFT_BRANCH, TO_RIGHT_BRANCH, NONE = Value
  }

  object AllowedJoin {
    def unapply(join: Join): Option[Join] = join.joinType match {
      case Inner | Cross | LeftOuter | RightOuter => Some(join)
      case _ => None
    }
  }

  /**
   * Determine which side of the join a LeftSemi/LeftAnti join can be pushed to.
   */
  private def pushTo(leftChild: Join, rightChild: LogicalPlan, joinCond: Option[Expression]) = {
    val left = leftChild.left
    val right = leftChild.right
    val joinType = leftChild.joinType
    val rightOutput = rightChild.outputSet

    if (joinCond.nonEmpty) {
      val conditions = splitConjunctivePredicates(joinCond.get)
      val (leftConditions, rest) =
        conditions.partition(_.references.subsetOf(left.outputSet ++ rightOutput))
      val (rightConditions, commonConditions) =
        rest.partition(_.references.subsetOf(right.outputSet ++ rightOutput))

      if (rest.isEmpty && leftConditions.nonEmpty) {
        // When the join conditions can be computed based on the left leg of
        // leftsemi/anti join then push the leftsemi/anti join to the left side.
        PushdownDirection.TO_LEFT_BRANCH
      } else if (leftConditions.isEmpty && rightConditions.nonEmpty && commonConditions.isEmpty) {
        // When the join conditions can be computed based on the attributes from right leg of
        // leftsemi/anti join then push the leftsemi/anti join to the right side.
        PushdownDirection.TO_RIGHT_BRANCH
      } else {
        PushdownDirection.NONE
      }
    } else {
      /**
       * When the join condition is empty,
       * 1) if this is a left outer join or inner join, push leftsemi/anti join down
       *    to the left leg of join.
       * 2) if a right outer join, to the right leg of join,
       */
      joinType match {
        case _: InnerLike | LeftOuter =>
          PushdownDirection.TO_LEFT_BRANCH
        case RightOuter =>
          PushdownDirection.TO_RIGHT_BRANCH
        case _ =>
          PushdownDirection.NONE
      }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push LeftSemi/LeftAnti down into the join below
    case j @ Join(AllowedJoin(left), right, LeftSemiOrAnti(joinType), joinCond, parentHint) =>
      val (childJoinType, childLeft, childRight, childCondition, childHint) =
        (left.joinType, left.left, left.right, left.condition, left.hint)
      val action = pushTo(left, right, joinCond)

      action match {
        case PushdownDirection.TO_LEFT_BRANCH
          if (childJoinType == LeftOuter || childJoinType.isInstanceOf[InnerLike]) =>
          // push down leftsemi/anti join to the left table
          val newLeft = Join(childLeft, right, joinType, joinCond, parentHint)
          Join(newLeft, childRight, childJoinType, childCondition, childHint)
        case PushdownDirection.TO_RIGHT_BRANCH
          if (childJoinType == RightOuter || childJoinType.isInstanceOf[InnerLike]) =>
          // push down leftsemi/anti join to the right table
          val newRight = Join(childRight, right, joinType, joinCond, parentHint)
          Join(childLeft, newRight, childJoinType, childCondition, childHint)
        case _ =>
          // Do nothing
          j
      }
  }
}


