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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, ExpressionSet, NamedExpression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, LeftSemiOrAnti, RightOuter}

/**
 * A visitor pattern for traversing a [[LogicalPlan]] tree and propagate the distinct attributes.
 */
object DistinctKeyVisitor extends LogicalPlanVisitor[Set[ExpressionSet]] {

  private def projectDistinctKeys(
      keys: Set[ExpressionSet], projectList: Seq[NamedExpression]): Set[ExpressionSet] = {
    val outputSet = ExpressionSet(projectList.map(_.toAttribute))
    val aliases = projectList.filter(_.isInstanceOf[Alias])
    if (aliases.isEmpty) {
      keys.filter(_.subsetOf(outputSet))
    } else {
      val aliasedDistinctKeys = keys.map { expressionSet =>
        expressionSet.map { expression =>
          expression transform {
            case expr: Expression =>
              // TODO: Expand distinctKeys for redundant aliases on the same expression
              aliases
                .collectFirst { case a: Alias if a.child.semanticEquals(expr) => a.toAttribute }
                .getOrElse(expr)
          }
        }
      }
      aliasedDistinctKeys.collect {
        case es: ExpressionSet if es.subsetOf(outputSet) => ExpressionSet(es)
      } ++ keys.filter(_.subsetOf(outputSet))
    }.filter(_.nonEmpty)
  }

  /**
   * Add a new ExpressionSet S into distinctKeys D.
   * To minimize the size of D:
   * 1. If there is a subset of S in D, return D.
   * 2. Otherwise, remove all the ExpressionSet containing S from D, and add the new one.
   */
  private def addDistinctKey(
      keys: Set[ExpressionSet],
      newExpressionSet: ExpressionSet): Set[ExpressionSet] = {
    if (keys.exists(_.subsetOf(newExpressionSet))) {
      keys
    } else {
      keys.filterNot(s => newExpressionSet.subsetOf(s)) + newExpressionSet
    }
  }

  override def default(p: LogicalPlan): Set[ExpressionSet] = Set.empty[ExpressionSet]

  override def visitAggregate(p: Aggregate): Set[ExpressionSet] = {
    val groupingExps = ExpressionSet(p.groupingExpressions) // handle group by a, a
    projectDistinctKeys(addDistinctKey(p.child.distinctKeys, groupingExps), p.aggregateExpressions)
  }

  override def visitDistinct(p: Distinct): Set[ExpressionSet] = Set(ExpressionSet(p.output))

  override def visitExcept(p: Except): Set[ExpressionSet] =
    if (!p.isAll) Set(ExpressionSet(p.output)) else default(p)

  override def visitExpand(p: Expand): Set[ExpressionSet] = default(p)

  override def visitFilter(p: Filter): Set[ExpressionSet] = p.child.distinctKeys

  override def visitGenerate(p: Generate): Set[ExpressionSet] = default(p)

  override def visitGlobalLimit(p: GlobalLimit): Set[ExpressionSet] = {
    p.maxRows match {
      case Some(value) if value <= 1 => p.output.map(attr => ExpressionSet(Seq(attr))).toSet
      case _ => p.child.distinctKeys
    }
  }

  override def visitIntersect(p: Intersect): Set[ExpressionSet] = {
    if (!p.isAll) Set(ExpressionSet(p.output)) else default(p)
  }

  override def visitJoin(p: Join): Set[ExpressionSet] = {
    p match {
      case Join(_, _, LeftSemiOrAnti(_), _, _) =>
        p.left.distinctKeys
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, _)
          if left.distinctKeys.nonEmpty || right.distinctKeys.nonEmpty =>
        val rightJoinKeySet = ExpressionSet(rightKeys)
        val leftJoinKeySet = ExpressionSet(leftKeys)
        joinType match {
          case Inner if left.distinctKeys.exists(_.subsetOf(leftJoinKeySet)) &&
            right.distinctKeys.exists(_.subsetOf(rightJoinKeySet)) =>
            left.distinctKeys ++ right.distinctKeys
          case Inner | LeftOuter if right.distinctKeys.exists(_.subsetOf(rightJoinKeySet)) =>
            p.left.distinctKeys
          case Inner | RightOuter if left.distinctKeys.exists(_.subsetOf(leftJoinKeySet)) =>
            p.right.distinctKeys
          case _ =>
            default(p)
        }
      case _ => default(p)
    }
  }

  override def visitLocalLimit(p: LocalLimit): Set[ExpressionSet] = p.child.distinctKeys

  override def visitPivot(p: Pivot): Set[ExpressionSet] = default(p)

  override def visitProject(p: Project): Set[ExpressionSet] = {
    if (p.child.distinctKeys.nonEmpty) {
      projectDistinctKeys(p.child.distinctKeys, p.projectList)
    } else {
      default(p)
    }
  }

  override def visitRepartition(p: Repartition): Set[ExpressionSet] = p.child.distinctKeys

  override def visitRepartitionByExpr(p: RepartitionByExpression): Set[ExpressionSet] =
    p.child.distinctKeys

  override def visitSample(p: Sample): Set[ExpressionSet] = {
    if (!p.withReplacement) p.child.distinctKeys else default(p)
  }

  override def visitScriptTransform(p: ScriptTransformation): Set[ExpressionSet] = default(p)

  override def visitUnion(p: Union): Set[ExpressionSet] = default(p)

  override def visitWindow(p: Window): Set[ExpressionSet] = p.child.distinctKeys

  override def visitTail(p: Tail): Set[ExpressionSet] = p.child.distinctKeys

  override def visitSort(p: Sort): Set[ExpressionSet] = p.child.distinctKeys

  override def visitRebalancePartitions(p: RebalancePartitions): Set[ExpressionSet] =
    p.child.distinctKeys

  override def visitWithCTE(p: WithCTE): Set[ExpressionSet] = p.plan.distinctKeys
}
