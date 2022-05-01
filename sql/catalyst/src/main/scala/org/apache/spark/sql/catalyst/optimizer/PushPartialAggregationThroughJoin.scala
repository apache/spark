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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN
import org.apache.spark.sql.types.DecimalType

/**
 * Push down the partial aggregation through join if it cannot be planned as broadcast hash join.
 */
object PushPartialAggregationThroughJoin extends Rule[LogicalPlan]
  with PredicateHelper
  with JoinSelectionHelper {

  private def split(expressions: Seq[NamedExpression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftExpressions, rest) =
      expressions.partition(e => e.references.nonEmpty && canEvaluate(e, left))
    val (rightExpressions, remainingExpressions) =
      rest.partition(e => e.references.nonEmpty && canEvaluate(e, right))

    (leftExpressions, rightExpressions, remainingExpressions)
  }

  private def toAttributeMap(aggExps: Seq[AggregateExpression]) = {
    AttributeMap(aggExps.map { a =>
      a.references.head -> Alias(a, s"pushed_${a.toString}")()
    }.toMap[Attribute, Alias])
  }

  private def splitAggregateExpressions(
    aggExps: Seq[AggregateExpression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftAggExprs, rest) =
      aggExps.partition(e => e.references.nonEmpty && canEvaluate(e, left))
    val (rightAggExprs, others) =
      rest.partition(e => e.references.nonEmpty && canEvaluate(e, right))

    (toAttributeMap(leftAggExprs), toAttributeMap(rightAggExprs), others)
  }

  protected def replaceAliasName(
                                  expr: NamedExpression,
                                  aliasMap: AttributeMap[Alias],
                                  cnt: Alias): NamedExpression = {
    // Use transformUp to prevent infinite recursion when the replacement expression
    // redefines the same ExprId.
    expr.mapChildren(_.transformUp {
      case s @ Sum(a: Attribute, failOnError, dt) if aliasMap.get(a).nonEmpty =>
        val value = aliasMap(a)
        val multiply = Multiply(value.toAttribute, Cast(cnt.toAttribute, value.dataType))
        s.dataType match {
          case decType: DecimalType =>
            Sum(CheckOverflow(multiply, decType, !failOnError), failOnError,
              Some(dt.getOrElse(s.dataType)))
          case _ =>
            Sum(multiply, failOnError, Some(dt.getOrElse(s.dataType)))
        }
      case c @ Count(Seq(a: Attribute)) if aliasMap.get(a).nonEmpty =>
        Sum(Multiply(aliasMap(a).toAttribute, cnt.toAttribute), !conf.ansiEnabled, Some(c.dataType))
      case Min(a: Attribute) if aliasMap.get(a).nonEmpty =>
        Min(aliasMap(a).toAttribute)
      case Max(a: Attribute) if aliasMap.get(a).nonEmpty =>
        Max(aliasMap(a).toAttribute)
    }).asInstanceOf[NamedExpression]
  }

  private def pushableAggExp(ae: AggregateExpression): Boolean = ae match {
    case AggregateExpression(Sum(_: Attribute, _, _), Complete, false, None, _) => true
    case AggregateExpression(Min(_: Attribute), Complete, false, None, _) => true
    case AggregateExpression(Max(_: Attribute), Complete, false, None, _) => true
    case _ => false
  }

  private def pushableCountExp(ae: AggregateExpression): Boolean = ae match {
    case AggregateExpression(Count(Seq(_: Attribute)), Complete, false, None, _) => true
    case AggregateExpression(Count(Seq(IntegerLiteral(1))), Complete, false, None, _) => true
    case _ => false
  }

  private def pushdownAggThroughJoin(
                                      agg: Aggregate,
                                      projectList: Seq[NamedExpression],
                                      leftKeys: Seq[Expression],
                                      rightKeys: Seq[Expression],
                                      join: Join) = {
    val aggregateExpressions = agg.ggregateExprs

    val (leftProjectList, rightProjectList, remainingProjectList) =
      split(projectList ++ join.condition.map(_.references.toSeq).getOrElse(Nil),
        join.left, join.right)

    // INNER joins are supported for these cases:
    // 1: All aggregate expressions are pushable
    // 2: Only have one count distinct expression and groupingExpressions is not empty
    // 3: projectList contains complex expressions
    // 4: Join condition contains complex expressions
    if (remainingProjectList.isEmpty && (
      (agg.groupingExpressions.nonEmpty &&
        aggregateExpressions.forall(ae => pushableAggExp(ae) || pushableCountExp(ae))) ||
        (agg.groupingExpressions.isEmpty && aggregateExpressions.forall(pushableAggExp)))) {

      val leftPushProjectList = Project(leftProjectList, join.left)
      val rightPushProjectList = Project(rightProjectList, join.right)

      // If groupingExpressions.size > 0 has only on count distinct
      val groupAttrs = agg.groupingExpressions.map(_.asInstanceOf[Attribute])
      val (leftGroupExps, rightGroupExps, _) =
        split(groupAttrs, leftPushProjectList, rightPushProjectList)

      val (leftAliasMap, rightAliasMap, _) =
        splitAggregateExpressions(aggregateExpressions, leftPushProjectList, rightPushProjectList)

      val remainingAggregateExps = agg.aggregateExpressions
        .filterNot(_.collectFirst { case a: AggregateFunction => a }.nonEmpty)
      val (leftRemainingExps, rightRemainingExps, _) =
        split(remainingAggregateExps, leftPushProjectList, rightPushProjectList)

      val cntExp = Count(Seq(Literal(1))).toAggregateExpression()
      val leftCnt = Alias(cntExp, "cnt")()
      val rightCnt = Alias(cntExp, "cnt")()

      // pull out complex join condition begin
      val complexLeftJoinKeys = new ArrayBuffer[NamedExpression]()
      val complexRightJoinKeys = new ArrayBuffer[NamedExpression]()
      val newLeftJoinKeys: Seq[Attribute] = leftKeys.map {
        case a: Attribute => a
        case o =>
          val ne = Alias(o, o.toString)()
          complexLeftJoinKeys += ne
          ne.toAttribute
      }

      val newRightJoinKeys: Seq[Attribute] = rightKeys.map {
        case a: Attribute => a
        case o =>
          val ne = Alias(o, o.toString)()
          complexRightJoinKeys += ne
          ne.toAttribute
      }

      val pulloutLeft = leftPushProjectList
        .copy(projectList = leftPushProjectList.projectList ++ complexLeftJoinKeys)
      val pulloutRight = rightPushProjectList
        .copy(projectList = rightPushProjectList.projectList ++ complexRightJoinKeys)
      val newCond = newLeftJoinKeys.zip(newRightJoinKeys)
        .map { case (l, r) => EqualTo(l, r) }
        .reduceLeftOption(And)
      // pull out complex join condition end

      val newLeftAggregateExps =
        leftRemainingExps ++ (leftAliasMap.values.toSeq :+ leftCnt) ++ newLeftJoinKeys ++
          leftGroupExps
      val newRightAggregateExps =
        rightRemainingExps ++ (rightAliasMap.values.toSeq :+ rightCnt) ++ newRightJoinKeys ++
          rightGroupExps
      val newLeft = PartialAggregate(ExpressionSet(newLeftJoinKeys ++ leftGroupExps).toSeq,
        newLeftAggregateExps.distinct, pulloutLeft)
      val newRight = PartialAggregate(ExpressionSet(newRightJoinKeys ++ rightGroupExps).toSeq,
        newRightAggregateExps.distinct, pulloutRight)

      val newJoin = join.copy(left = newLeft, right = newRight, condition = newCond)

      val newAggregateExps = agg.aggregateExpressions
        .map(replaceAliasName(_, leftAliasMap, rightCnt))
        .map(replaceAliasName(_, rightAliasMap, leftCnt))
        .map { expr =>
          expr.mapChildren(_.transformUp {
            case Count(Seq(IntegerLiteral(1))) =>
              Sum(Multiply(leftCnt.toAttribute, rightCnt.toAttribute))
          }).asInstanceOf[NamedExpression]
        }

      val newAgg = agg.copy(
        child = newJoin,
        aggregateExpressions = newAggregateExps)

      val required = newJoin.references ++ newAgg.references
      if (!newJoin.inputSet.subsetOf(required)) {
        val newChildren = newJoin.children.map(ColumnPruning.prunedChild(_, required))
        CollapseProject(newAgg.copy(child = newJoin.withNewChildren(newChildren)))
      } else {
        newAgg
      }
    } else {
      agg
    }
  }

  private def isCountDistinct(agg: AggregateExpression): Boolean = agg match {
    case AggregateExpression(Count(Seq(_: Attribute)), Complete, true, None, _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(JOIN), ruleId) {
    case j @ Join(_, _: AggregateBase, LeftSemiOrAnti(_), _, _) =>
      j
    case j @ Join(_, Project(_, _: AggregateBase), LeftSemiOrAnti(_), _, _) =>
      j

    case agg @ Aggregate(_, _, join: Join)
        if join.children.exists(e => e.isInstanceOf[AggregateBase]) =>
      agg
    case agg @ Aggregate(_, _, Project(_, join: Join))
        if join.children.exists(e => e.isInstanceOf[AggregateBase]) =>
      agg

    case agg @ PartialAggregate(_, _, join: Join)
        if join.children.exists(e => e.isInstanceOf[AggregateBase]) =>
      agg
    case agg @ PartialAggregate(_, _, Project(_, join: Join))
        if join.children.exists(e => e.isInstanceOf[AggregateBase]) =>
      agg

    case agg @ PartialAggregate(_, aggregateExps,
      join @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter, _, _))
        if agg.ggregateExprs.isEmpty && aggregateExps.forall(_.deterministic) &&
          !canPlanAsBroadcastHashJoin(join, conf) =>
      Project(aggregateExps, join.copy(
        left = PartialAggregate(left.output, left.output, left),
        right = PartialAggregate(right.output, right.output, right)))

    case agg @ PartialAggregate(_, aggregateExps, Project(projectList,
      join @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter, _, _)))
        if agg.ggregateExprs.isEmpty && aggregateExps.forall(_.deterministic) &&
          projectList.forall(_.deterministic) && !canPlanAsBroadcastHashJoin(join, conf) =>
      Project(aggregateExps, Project(projectList, join.copy(
        left = PartialAggregate(left.output, left.output, left),
        right = PartialAggregate(right.output, right.output, right))))


    case agg @ Aggregate(_, _, join: Join)
        if agg.ggregateExprs.forall(isCountDistinct) && !canPlanAsBroadcastHashJoin(join, conf) =>
      val left = join.left
      val right = join.right
      agg.copy(child = join.copy(
        left = PartialAggregate(left.output, left.output, left),
        right = PartialAggregate(right.output, right.output, right)))
    case agg @ Aggregate(_, _, p @ Project(_, join: Join))
        if agg.ggregateExprs.forall(isCountDistinct) && !canPlanAsBroadcastHashJoin(join, conf) =>
      val left = join.left
      val right = join.right
      agg.copy(child = p.copy(child = join.copy(
        left = PartialAggregate(left.output, left.output, left),
        right = PartialAggregate(right.output, right.output, right))))

    case agg @ Aggregate(groupExps, aggregateExps,
      join @ ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, None, _, _, _, _))
        if groupExps.forall(_.isInstanceOf[Attribute]) && aggregateExps.forall(_.deterministic) &&
          leftKeys.nonEmpty &&
          agg.ggregateExprs.forall(ae => pushableAggExp(ae) || pushableCountExp(ae)) &&
          !canPlanAsBroadcastHashJoin(join, conf) =>
      pushdownAggThroughJoin(agg, join.output, leftKeys, rightKeys, join)

    case agg @ Aggregate(groupExps, aggregateExps, Project(projectList,
      join @ ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, None, _, _, _, _)))
        if groupExps.forall(_.isInstanceOf[Attribute]) && aggregateExps.forall(_.deterministic) &&
          projectList.forall(_.deterministic) && leftKeys.nonEmpty &&
          agg.ggregateExprs.forall(ae => pushableAggExp(ae) || pushableCountExp(ae)) &&
          !canPlanAsBroadcastHashJoin(join, conf) =>
      pushdownAggThroughJoin(agg, projectList, leftKeys, rightKeys, join)

    case j @ Join(_, right, LeftSemiOrAnti(_), _, _) if !canPlanAsBroadcastHashJoin(j, conf) =>
      j.copy(right = PartialAggregate(right.output, right.output, right))
    }
}
