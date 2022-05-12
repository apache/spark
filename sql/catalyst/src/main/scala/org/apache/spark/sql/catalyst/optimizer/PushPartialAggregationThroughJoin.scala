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

import org.apache.spark.sql.catalyst.analysis.DecimalPrecision
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, NumericType}

/**
 * Push down the partial aggregation through join.
 *
 * For example:
 * CREATE TABLE t1(a int, b int, c int) using parquet;
 * CREATE TABLE t2(x int, y int, z int) using parquet;
 * SELECT b, SUM(c) FROM t1 INNER JOIN t2 ON t1.a = t2.x GROUP BY b;
 *
 * The current optimized logical plan is:
 * Aggregate [b#2], [b#2, sum((_pushed_sum_c#13L * cnt#16L)) AS sum(c)#8L]
 * +- Project [b#2, pushed_sum_c#13L, cnt#16L]
 *    +- Join Inner, (a#1 = x#4)
 *       :- PartialAggregate [a#1, b#2], [a#1, b#2, sum(c#3) AS pushed_sum_c#13L]
 *       :  +- Project [b#2, c#3, a#1]
 *       :     +- Filter isnotnull(a#1)
 *       :        +- Relation default.t1[a#1,b#2,c#3] parquet
 *       +- PartialAggregate [x#4], [count(1) AS cnt#16L, x#4]
 *          +- Project [x#4]
 *             +- Filter isnotnull(x#4)
 *                +- Relation default.t2[x#4,y#5,z#6] parquet
 *
 * This rule should be applied after ColumnPruning.
 */
object PushPartialAggregationThroughJoin extends Rule[LogicalPlan]
  with JoinSelectionHelper
  with PredicateHelper {

  // Returns true if `expr`'s references is non empty and can be evaluated using only
  // the output of `plan`.
  private def canEvaluateOnly(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.nonEmpty && canEvaluate(expr, plan)

  // Splits expressions into three categories based on the attributes required to evaluate them.
  private def split(expressions: Seq[NamedExpression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftExprs, rest) = expressions.partition(canEvaluateOnly(_, left))
    val (rightExprs, remainingExps) = rest.partition(canEvaluateOnly(_, right))

    (leftExprs, rightExprs, remainingExps)
  }

  // Splits expressions into three categories based on the attributes required to evaluate them.
  private def splitAggregateExpressions(
      aggExps: Seq[AggregateExpression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftAggExprs, rest) = aggExps.partition(canEvaluateOnly(_, left))
    val (rightAggExprs, remainingAggExps) = rest.partition(canEvaluateOnly(_, right))

    (toExpressionMap(leftAggExprs), toExpressionMap(rightAggExprs), remainingAggExps)
  }

  // Convert aggregate expressions to a map, the key used to replace the current Aggregate
  // and the value used to push through Join. see the function of replaceAliasName.
  private def toExpressionMap(aggExps: Seq[AggregateExpression]) = {
    aggExps.map { a =>
      val name =
        s"_pushed_${a.aggregateFunction.prettyName}_${a.references.map(_.name).mkString("_")}"
      a.aggregateFunction.canonicalized -> Alias(a, name)()
    }.toMap[Expression, Alias]
  }

  // Replace the current Aggregate's aggregate expression references with pushed attribute.
  // Please note that:
  // 1. Replace the sum with the current side sum * the other side row count
  // 2. Replace the count with the current side row count * the other side row count
  private def replaceAliasName(
      expr: NamedExpression,
      aliasMap: Map[Expression, Alias],
      cnt: Alias): NamedExpression = {
    // Use transformUp to prevent infinite recursion when the replacement expression
    // redefines the same ExprId.
    expr.mapChildren(_.transformUp {
      case e @ Sum(_, useAnsiAdd, dt) if aliasMap.contains(e.canonicalized) =>
        val value = aliasMap(e.canonicalized).toAttribute
        val multiply = Multiply(value,
          cnt.toAttribute.cast(value.dataType, Some(conf.sessionLocalTimeZone)))
        e.dataType match {
          case decType: DecimalType =>
            // Do not use DecimalPrecision because it may be change the precision and scale
            Sum(CheckOverflow(multiply, decType, !useAnsiAdd), useAnsiAdd, Some(decType))
          case _ =>
            Sum(multiply, useAnsiAdd, Some(dt.getOrElse(e.dataType)))
        }
      case e: Count if aliasMap.contains(e.canonicalized) =>
        Sum(Multiply(aliasMap(e.canonicalized).toAttribute, cnt.toAttribute),
          conf.ansiEnabled, Some(e.dataType))
      case e: Min if aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
      case e: Max if aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
      case e: First if aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
      case e: Last if aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
    }).asInstanceOf[NamedExpression]
  }

  // The expression should't complex and it's references should not empty.
  // For example, We do not support following cases:
  // 1. sum((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price)
  // 2. sum(1)
  private def supportPushedAgg(e: Expression) = {
    e.collectLeaves().size <= 2 && e.references.nonEmpty}

  private def pushableAggExp(ae: AggregateExpression): Boolean = ae match {
    case AggregateExpression(e: Sum, Complete, false, None, _) => supportPushedAgg(e)
    case AggregateExpression(e: Min, Complete, false, None, _) => supportPushedAgg(e)
    case AggregateExpression(e: Max, Complete, false, None, _) => supportPushedAgg(e)
    case AggregateExpression(e: First, Complete, false, None, _) => supportPushedAgg(e)
    case AggregateExpression(e: Last, Complete, false, None, _) => supportPushedAgg(e)
    case AggregateExpression(Average(e, _), Complete, false, None, _) =>
      e.dataType.isInstanceOf[NumericType] && supportPushedAgg(e)
    case _ => false
  }

  // Support count(*), count(id) ...
  private def pushableCountExp(ae: AggregateExpression): Boolean = ae match {
    case AggregateExpression(_: Count, Complete, false, None, _) => true
    case _ => false
  }

  private def supportPushDownAgg(
      aggExps: Seq[AggregateExpression],
      left: LogicalPlan,
      right: LogicalPlan): Boolean = {
    // All aggregate expressions should be pushable aggregate expression or count expression,
    // and it should can be evaluated only on left or right
    val semanticSupport = aggExps.forall(e => (pushableAggExp(e) || pushableCountExp(e)) &&
      (canEvaluate(e, left) || canEvaluate(e, right)))
    // Will not push down Agg if all aggregate expression's size much larger than
    // all aggregate expression references's size because it may increase shuffle data
    val references = AttributeSet(aggExps.flatMap(_.references))
    semanticSupport && aggExps.size / math.max(references.size, 2).toFloat <= 2
  }

  // Deduplicate and reorder aggregate expressions to avoid some query can't reuse the exchange.
  // See tpcds-v2.7.0: q57 and q67a
  private def reorderAggregateExpressions(
      aggregateExpressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    ExpressionSet(aggregateExpressions)
      .toSeq
      .map(_.asInstanceOf[NamedExpression])
      .sortBy(_.name)
  }

  // Rewrite Average to Sum / Count(*). Please see AverageBase.getEvaluateExpression
  private def rewriteAverage(agg: Aggregate): Aggregate = {
    if (agg.aggregateExprs.exists(_.aggregateFunction.isInstanceOf[Average])) {
      val newAggAggregateExpressions = agg.aggregateExpressions.map { expr =>
        expr.mapChildren(_.transformUp {
          case ae @ AggregateExpression(af, _, _, _, _) => af match {
            case avg @ Average(e, useAnsiAdd) =>
              val sum = Sum(e, useAnsiAdd, Some(avg.sumDataType)).toAggregateExpression()
              val count = Count(Seq(Literal(1))).toAggregateExpression()
              e.dataType match {
                case _: DecimalType =>
                  DecimalPrecision.decimalAndDecimal()(
                    Divide(
                      CheckOverflowInSum(sum, avg.sumDataType.asInstanceOf[DecimalType],
                        !useAnsiAdd),
                      count.cast(DecimalType.LongDecimal, Some(conf.sessionLocalTimeZone)),
                      failOnError = false)).cast(avg.dataType, Some(conf.sessionLocalTimeZone))
                case _ =>
                  Divide(
                    sum.cast(avg.dataType, Some(conf.sessionLocalTimeZone)),
                    count.cast(avg.dataType, Some(conf.sessionLocalTimeZone)),
                    failOnError = false)
              }
            case _ => ae
          }
        }).asInstanceOf[NamedExpression]
      }

      agg.copy(aggregateExpressions = newAggAggregateExpressions)
    } else {
      agg
    }
  }

  private def pullOutJoinKeys(
      joinKeys: Seq[Expression]): (Seq[Attribute], ArrayBuffer[NamedExpression]) = {
    val complexJoinKeys = new ArrayBuffer[NamedExpression]()
    val newJoinKeys = joinKeys.map {
      case a: Attribute => a
      case o =>
        val ne = Alias(o, s"_pullout_${o.prettyName}_${o.references.map(_.name).mkString("_")}")()
        complexJoinKeys += ne
        ne.toAttribute
    }
    (newJoinKeys, complexJoinKeys)
  }

  private def constructPartialAgg(
      joinKeys: Seq[Attribute],
      groupExps: Seq[NamedExpression],
      remainingExps: Seq[NamedExpression],
      aliasMap: Map[Expression, Alias],
      rowCnt: Alias,
      plan: LogicalPlan): PartialAggregate = {
    val partialGroupingExps = ExpressionSet(joinKeys ++ groupExps).toSeq
    val partialAggExps = remainingExps ++ (aliasMap.values.toSeq :+ rowCnt) ++ joinKeys ++ groupExps
    PartialAggregate(partialGroupingExps, reorderAggregateExpressions(partialAggExps), plan)
  }

  // The entry of push down partial aggregate through join.
  // Will return the current aggregate if it can't push down.
  private def pushAggThroughJoin(
      agg: Aggregate,
      projectList: Seq[NamedExpression],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      join: Join): LogicalPlan = {
    val rewrittenAgg = rewriteAverage(agg)
    val aggregateExpressions = rewrittenAgg.aggregateExprs

    val (leftProjectList, rightProjectList, remainingProjectList) =
      split(projectList ++ join.condition.map(_.references.toSeq).getOrElse(Nil),
        join.left, join.right)

    // remainingProjectList must should be empty. We do not support this case:
    // SELECT b + y, SUM(c) FROM t1 INNER JOIN t2 ON t1.a = t2.x GROUP BY 1
    //
    // Supported cases:
    // 1. groupingExpressions is not empty and aggregateExpressions are pushableAggExp or
    //    pushableCountExp
    // 2. groupingExpressions is empty and aggregateExpressions are pushableAggExp
    if (remainingProjectList.isEmpty && (
      (rewrittenAgg.groupingExpressions.nonEmpty &&
        aggregateExpressions.forall(ae => pushableAggExp(ae) || pushableCountExp(ae))) ||
        (rewrittenAgg.groupingExpressions.isEmpty &&
          aggregateExpressions.forall(pushableAggExp)))) {

      val pushedLeftProject = Project(leftProjectList, join.left)
      val pushedRightProject = Project(rightProjectList, join.right)

      // All groupingExpressions are Attributes, see PullOutGroupingExpressions.
      // Splits groupingExpressions into three categories based on the attributes.
      // We will use it as aggregateExpressions in PartialAggregate
      val (leftGroupExps, rightGroupExps, _) =
        split(rewrittenAgg.groupingExpressions.map(_.asInstanceOf[Attribute]),
          pushedLeftProject, pushedRightProject)

      // Will push down to both left and right side even it's can be planed as broadcast join
      val (leftAliasMap, rightAliasMap, _) =
        splitAggregateExpressions(aggregateExpressions, pushedLeftProject, pushedRightProject)

      val remainingAggregateExps = rewrittenAgg.aggregateExpressions
        .filterNot(_.exists(_.isInstanceOf[AggregateFunction]))
      val (leftRemainingExps, rightRemainingExps, _) =
        split(remainingAggregateExps, pushedLeftProject, pushedRightProject)

      // pull out complex join condition
      val (newLeftJoinKeys, complexLeftJoinKeys) = pullOutJoinKeys(leftKeys)
      val (newRightJoinKeys, complexRightJoinKeys) = pullOutJoinKeys(rightKeys)

      val pullOutedLeft = pushedLeftProject
        .copy(projectList = leftProjectList ++ complexLeftJoinKeys)
      val pullOutedRight = pushedRightProject
        .copy(projectList = rightProjectList ++ complexRightJoinKeys)
      val newCond = newLeftJoinKeys.zip(newRightJoinKeys)
        .map { case (l, r) => EqualTo(l, r) }
        .reduceLeftOption(And)

      // Construct partial aggregate and new aggregate
      val cntExp = Count(Seq(Literal(1))).toAggregateExpression()
      val leftCnt = Alias(cntExp, "cnt")()
      val rightCnt = Alias(cntExp, "cnt")()

      val newLeft = constructPartialAgg(
        newLeftJoinKeys, leftGroupExps, leftRemainingExps,
        leftAliasMap, leftCnt, pullOutedLeft)
      val newRight = constructPartialAgg(newRightJoinKeys, rightGroupExps, rightRemainingExps,
        rightAliasMap, rightCnt, pullOutedRight)

      val newJoin = join.copy(left = newLeft, right = newRight, condition = newCond)

      val newAggregateExps = rewrittenAgg.aggregateExpressions
        .map(replaceAliasName(_, leftAliasMap, rightCnt))
        .map(replaceAliasName(_, rightAliasMap, leftCnt))
        .map { expr =>
          expr.mapChildren(_.transformUp {
            case e @ Count(Seq(IntegerLiteral(1))) =>
              Sum(Multiply(leftCnt.toAttribute, rightCnt.toAttribute),
                conf.ansiEnabled, Some(e.dataType))
          }).asInstanceOf[NamedExpression]
        }

      val newAgg = if (conf.getConf(SQLConf.REMOVE_CURRENT_PARTIAL_AGGREGATION) &&
        canPlanAsBroadcastHashJoin(newJoin, conf)) {
        FinalAggregate(rewrittenAgg.groupingExpressions, newAggregateExps, newJoin)
      } else {
        rewrittenAgg.copy(aggregateExpressions = newAggregateExps, child = newJoin)
      }

      val required = newJoin.references ++ newAgg.references
      if (!newJoin.inputSet.subsetOf(required)) {
        val newChildren = newJoin.children.map(ColumnPruning.prunedChild(_, required))
        CollapseProject(newAgg.withNewChildren(Seq(newJoin.withNewChildren(newChildren))))
      } else {
        newAgg
      }
    } else {
      // We will not rewrite average if it can't push down through join.
      agg
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(JOIN), ruleId) {
    case agg @ Aggregate(_, _, j: Join)
        if j.children.exists(_.isInstanceOf[AggregateBase]) =>
      agg
    case agg @ Aggregate(_, _, Project(_, j: Join))
        if j.children.exists(_.isInstanceOf[AggregateBase]) =>
      agg

    case agg @ PartialAggregate(_, _, j: Join)
        if j.children.exists(_.isInstanceOf[AggregateBase]) =>
      agg
    case agg @ PartialAggregate(_, _, Project(_, j: Join))
        if j.children.exists(_.isInstanceOf[AggregateBase]) =>
      agg

    case agg @ PartialAggregate(_, aggregateExps,
      j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter | Cross, _, _))
        if agg.aggregateExprs.isEmpty =>
      val newChild = j.copy(left =
        PartialAggregate(left.output, left.output, left),
        right = PartialAggregate(right.output, right.output, right))
      Project(aggregateExps, newChild)

    case agg @ PartialAggregate(_, aggregateExps, Project(projectList,
      j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter | Cross, _, _)))
        if agg.aggregateExprs.isEmpty && aggregateExps.forall(_.deterministic) =>
      val newChild = j.copy(left =
        PartialAggregate(left.output, left.output, left),
        right = PartialAggregate(right.output, right.output, right))
      Project(aggregateExps, Project(projectList, newChild))

    case agg @ Aggregate(_, aggregateExps,
      j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter | Cross, _, _))
        if agg.aggregateExprs.isEmpty && aggregateExps.forall(_.deterministic) =>
      val newChild = j.copy(left =
        PartialAggregate(left.output, left.output, left),
        right = PartialAggregate(right.output, right.output, right))
      if (conf.getConf(SQLConf.REMOVE_CURRENT_PARTIAL_AGGREGATION) &&
        canPlanAsBroadcastHashJoin(j, conf)) {
        FinalAggregate(agg.groupingExpressions, agg.aggregateExpressions, newChild)
      } else {
        agg.copy(child = newChild)
      }

    case agg @ Aggregate(_, aggregateExps, p @ Project(_,
      j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter | Cross, _, _)))
        if agg.aggregateExprs.isEmpty && aggregateExps.forall(_.deterministic) =>
      val newChild = j.copy(left =
        PartialAggregate(left.output, left.output, left),
        right = PartialAggregate(right.output, right.output, right))
      if (conf.getConf(SQLConf.REMOVE_CURRENT_PARTIAL_AGGREGATION) &&
        canPlanAsBroadcastHashJoin(j, conf)) {
        FinalAggregate(agg.groupingExpressions, agg.aggregateExpressions, p.copy(child = newChild))
      } else {
        agg.copy(child = p.copy(child = newChild))
      }

    case agg @ Aggregate(groupExps, aggregateExps,
      j @ ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, None, _, left, right, _))
        if groupExps.forall(_.isInstanceOf[Attribute]) && aggregateExps.forall(_.deterministic) &&
          leftKeys.nonEmpty && supportPushDownAgg(agg.aggregateExprs, left, right) =>
      pushAggThroughJoin(agg, j.output, leftKeys, rightKeys, j)

    case agg @ Aggregate(groupExps, aggregateExps, Project(projectList,
      j @ ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, None, _, left, right, _)))
        if groupExps.forall(_.isInstanceOf[Attribute]) && aggregateExps.forall(_.deterministic) &&
          projectList.forall(_.deterministic) && leftKeys.nonEmpty &&
          supportPushDownAgg(agg.aggregateExprs, left, right) =>
      pushAggThroughJoin(agg, projectList, leftKeys, rightKeys, j)

    case j @ Join(_, _: AggregateBase, _, _, _) =>
      j
    case j @ Join(_, Project(_, _: AggregateBase), _, _, _) =>
      j
    case j @ Join(_, right, LeftSemiOrAnti(_), _, _) if !canPlanAsBroadcastHashJoin(j, conf) =>
      j.copy(right = PartialAggregate(right.output, right.output, right))
    }
}
