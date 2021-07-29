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

import org.apache.spark.sql.catalyst.expressions.{Alias, ExpressionSet}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}

/**
 * A visitor pattern for traversing a [[LogicalPlan]] tree and propagate the distinct attributes.
 */
object DistinctAttributesVisitor extends LogicalPlanVisitor[Set[ExpressionSet]] {
  override def default(p: LogicalPlan): Set[ExpressionSet] = {
    Set.empty[ExpressionSet]
  }

  override def visitAggregate(p: Aggregate): Set[ExpressionSet] = {
    val groupingExps = p.groupingExpressions.toSet
    p.aggregateExpressions.toSet.subsets(groupingExps.size).filter { s =>
      s.map {
        case a: Alias => a.child
        case o => o
      }.equals(groupingExps)
    }.map(s => ExpressionSet(s.map(_.toAttribute))).toSet
  }

  override def visitDistinct(p: Distinct): Set[ExpressionSet] = Set(ExpressionSet(p.output))

  override def visitExcept(p: Except): Set[ExpressionSet] =
    if (!p.isAll) Set(ExpressionSet(p.output)) else default(p)

  override def visitExpand(p: Expand): Set[ExpressionSet] = default(p)

  override def visitFilter(p: Filter): Set[ExpressionSet] = p.child.distinctAttributes

  override def visitGenerate(p: Generate): Set[ExpressionSet] = default(p)

  override def visitGlobalLimit(p: GlobalLimit): Set[ExpressionSet] = default(p)

  override def visitIntersect(p: Intersect): Set[ExpressionSet] = {
    if (!p.isAll) Set(ExpressionSet(p.output)) else default(p)
  }

  override def visitJoin(p: Join): Set[ExpressionSet] = {
    p.joinType match {
      case LeftSemi | LeftAnti => p.left.distinctAttributes
      case _ => default(p)
    }
  }

  override def visitLocalLimit(p: LocalLimit): Set[ExpressionSet] = default(p)

  override def visitPivot(p: Pivot): Set[ExpressionSet] = default(p)

  override def visitProject(p: Project): Set[ExpressionSet] = {
    if (p.child.distinctAttributes.nonEmpty) {
      val childDistinctAttributes = p.child.distinctAttributes
      p.projectList.toSet.subsets(childDistinctAttributes.map(_.size).min).filter { s =>
        val exps = s.map {
          case a: Alias => a.child
          case o => o
        }
        childDistinctAttributes.exists(_.equals(ExpressionSet(exps)))
      }.map(s => ExpressionSet(s.map(_.toAttribute))).toSet
    } else {
      default(p)
    }
  }

  override def visitRepartition(p: Repartition): Set[ExpressionSet] = p.child.distinctAttributes

  override def visitRepartitionByExpr(p: RepartitionByExpression): Set[ExpressionSet] =
    p.child.distinctAttributes

  override def visitSample(p: Sample): Set[ExpressionSet] = default(p)

  override def visitScriptTransform(p: ScriptTransformation): Set[ExpressionSet] = default(p)

  override def visitUnion(p: Union): Set[ExpressionSet] = default(p)

  override def visitWindow(p: Window): Set[ExpressionSet] = default(p)

  override def visitTail(p: Tail): Set[ExpressionSet] = default(p)

  override def visitSort(sort: Sort): Set[ExpressionSet] = sort.child.distinctAttributes
}
