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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSet, ExpressionSet, NamedExpression}
import org.apache.spark.sql.catalyst.plans.LeftExistence

/**
 * A visitor pattern for traversing a [[LogicalPlan]] tree and propagate the distinct attributes.
 */
object DistinctKeyVisitor extends LogicalPlanVisitor[Set[AttributeSet]] {

  private def projectDistinctKeys(
      keys: Set[ExpressionSet], projectList: Seq[NamedExpression]): Set[AttributeSet] = {
    val expressions = keys.flatMap(_.toSet)
    projectList.filter {
      case a: Alias => expressions.exists(_.semanticEquals(a.child))
      case ne => expressions.exists(_.semanticEquals(ne))
    }.toSet.subsets(keys.map(_.size).min).filter { s =>
      val references = s.map {
        case a: Alias => a.child
        case ne => ne
      }
      keys.exists(_.equals(ExpressionSet(references)))
    }.map(s => AttributeSet(s.map(_.toAttribute))).toSet
  }

  override def default(p: LogicalPlan): Set[AttributeSet] = Set.empty[AttributeSet]

  override def visitAggregate(p: Aggregate): Set[AttributeSet] = {
    val groupingExps = ExpressionSet(p.groupingExpressions) // handle group by a, a
    projectDistinctKeys(Set(groupingExps), p.aggregateExpressions)
  }

  override def visitDistinct(p: Distinct): Set[AttributeSet] = {
    Set(p.outputSet)
  }

  override def visitExcept(p: Except): Set[AttributeSet] =
    if (!p.isAll && p.deterministic) Set(p.outputSet) else default(p)

  override def visitExpand(p: Expand): Set[AttributeSet ] = default(p)

  override def visitFilter(p: Filter): Set[AttributeSet ] = p.child.distinctKeys

  override def visitGenerate(p: Generate): Set[AttributeSet ] = default(p)

  override def visitGlobalLimit(p: GlobalLimit): Set[AttributeSet ] = p.child.distinctKeys

  override def visitIntersect(p: Intersect): Set[AttributeSet ] = {
    if (!p.isAll && p.deterministic) Set(p.outputSet) else default(p)
  }

  override def visitJoin(p: Join): Set[AttributeSet] = {
    p.joinType match {
      case LeftExistence(_) => p.left.distinctKeys
      case _ => default(p)
    }
  }

  override def visitLocalLimit(p: LocalLimit): Set[AttributeSet] = p.child.distinctKeys

  override def visitPivot(p: Pivot): Set[AttributeSet] = default(p)

  override def visitProject(p: Project): Set[AttributeSet] = {
    if (p.child.distinctKeys.nonEmpty) {
      projectDistinctKeys(p.child.distinctKeys.map(ExpressionSet(_)), p.projectList)
    } else {
      default(p)
    }
  }

  override def visitRepartition(p: Repartition): Set[AttributeSet] = p.child.distinctKeys

  override def visitRepartitionByExpr(p: RepartitionByExpression): Set[AttributeSet] =
    p.child.distinctKeys

  override def visitSample(p: Sample): Set[AttributeSet] = default(p)

  override def visitScriptTransform(p: ScriptTransformation): Set[AttributeSet] = default(p)

  override def visitUnion(p: Union): Set[AttributeSet] = default(p)

  override def visitWindow(p: Window): Set[AttributeSet] = p.child.distinctKeys

  override def visitTail(p: Tail): Set[AttributeSet] = p.child.distinctKeys

  override def visitSort(p: Sort): Set[AttributeSet] = p.child.distinctKeys

  override def visitRebalancePartitions(p: RebalancePartitions): Set[AttributeSet] =
    p.child.distinctKeys

  override def visitWithCTE(p: WithCTE): Set[AttributeSet] = default(p)
}
