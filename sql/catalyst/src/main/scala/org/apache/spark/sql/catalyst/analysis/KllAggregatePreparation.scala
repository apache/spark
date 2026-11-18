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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.{KLL_CHECKPOINT_ALTERNATIVE, OUTER_REFERENCE, PLAN_EXPRESSION}

/** Retires compiler-owned producers after a batch or checkpoint resolution choice is selected. */
private[sql] object KllAggregatePreparation {
  def unchosenOutputs(resolution: KllAggregateResolution, chosen: Expression): Set[ExprId] = {
    val ordinary = (resolution.references -- chosen.references).map(_.exprId).toSet
    if (!resolution.containsPattern(OUTER_REFERENCE)) ordinary else {
      val outer = AttributeSet(SubExprUtils.getOuterReferences(resolution))
      val chosenOuter = AttributeSet(SubExprUtils.getOuterReferences(chosen))
      ordinary ++ (outer -- chosenOuter).map(_.exprId)
    }
  }

  def selectBatch(plan: LogicalPlan): LogicalPlan = {
    // Streaming startup/micro-batches own their checkpoint choice and perform the same retirement.
    if (plan.isStreaming || !plan.containsPattern(KLL_CHECKPOINT_ALTERNATIVE)) return plan
    val unchosen = mutable.Set.empty[ExprId]
    def selectExpression(expression: Expression): Expression = {
      if (!expression.containsPattern(KLL_CHECKPOINT_ALTERNATIVE)) return expression
      expression match {
        case resolution: KllAggregateResolution =>
          unchosen ++= unchosenOutputs(resolution, resolution.batch)
          selectExpression(resolution.batch)
        case query: SubqueryExpression =>
          val children = query.mapChildrenWithReferenceEquality(selectExpression)
            .asInstanceOf[SubqueryExpression]
          val nested = selectPlan(query.plan)
          if (nested eq query.plan) children else children.withNewPlan(nested)
        case other => other.mapChildrenWithReferenceEquality(selectExpression)
      }
    }
    def selectPlan(input: LogicalPlan): LogicalPlan = {
      if (!input.containsPattern(KLL_CHECKPOINT_ALTERNATIVE)) return input
      input.mapChildrenWithReferenceEquality(selectPlan)
        .mapExpressionsWithReferenceEquality(selectExpression)
    }
    val selected = selectPlan(plan)
    if (unchosen.isEmpty) selected else retireUnchosenOutputs(selected, unchosen.toSet)
  }

  def retireUnchosenOutputs(plan: LogicalPlan, candidates: Set[ExprId]): LogicalPlan = {
    val nestedPlans = new java.util.IdentityHashMap[LogicalPlan, LogicalPlan]
    val outerReferences = new java.util.IdentityHashMap[LogicalPlan, AttributeSet]
    def visit(input: LogicalPlan, required: AttributeSet): LogicalPlan = {
      def keep(expression: NamedExpression): Boolean =
        !candidates.contains(expression.exprId) || required.contains(expression.toAttribute)
      val trimmed = input match {
        case aggregate: Aggregate =>
          val retained = aggregate.aggregateExpressions.filter(keep)
          if (retained.size == aggregate.aggregateExpressions.size) aggregate else {
            aggregate.copy(aggregateExpressions = retained)
          }
        case project: Project =>
          val retained = project.projectList.filter(keep)
          if (retained.size == project.projectList.size) project else {
            project.copy(projectList = retained)
          }
        case other => other
      }
      def retireSubqueries(expression: Expression): Expression = {
        if (!expression.containsPattern(PLAN_EXPRESSION)) return expression
        val updated = expression match {
          case query: SubqueryExpression =>
            val nested = nestedPlans.computeIfAbsent(query.plan,
              child => visit(child, child.outputSet.filter(a => candidates.contains(a.exprId))))
            val updated = if (nested eq query.plan) query else query.withNewPlan(nested)
            query match {
              case _: ScalarSubquery | _: Exists | _: ListQuery | _: LateralSubquery
                  if query.references.exists(a => candidates.contains(a.exprId)) =>
                val referenced = outerReferences.computeIfAbsent(nested,
                  child => AttributeSet(SubExprUtils.getOuterReferences(child)))
                val outer = query.getOuterAttrs.filterNot {
                  case attribute: Attribute => candidates.contains(attribute.exprId) &&
                    !referenced.contains(attribute)
                  case _ => false
                }
                if (outer.size == query.getOuterAttrs.size) updated else {
                  updated.withNewOuterAttrs(outer)
                }
              case _ => updated
            }
          case other => other
        }
        updated.mapChildrenWithReferenceEquality(retireSubqueries)
      }
      val current = trimmed.mapExpressionsWithReferenceEquality(retireSubqueries)
      // Only candidate identities can affect deletion. Avoid recursive output/inputSet reads
      // for ordinary forwarded columns, especially through long chains of filters.
      val local = current.references.filter(a => candidates.contains(a.exprId))
      val needed = if (local.isEmpty) required else local ++ required
      current match {
        case distinct: Distinct =>
          // DISTINCT consumes its complete equality tuple, including keys hidden by a parent.
          val keys = distinct.child.outputSet.filter(a => candidates.contains(a.exprId))
          distinct.copy(child = visit(distinct.child, needed ++ keys))
        case _: Project | _: Aggregate | _: Filter | _: Sort | _: SubqueryAlias | _: Join |
            _: Repartition | _: RepartitionByExpression | _: RebalancePartitions |
            _: GlobalLimit | _: LocalLimit | _: Offset | _: Window | _: Deduplicate |
            _: CTERelationDef =>
          current.mapChildrenWithReferenceEquality(child => visit(child, needed))
        case _ =>
          // CTE definitions and positional set-operation branches need not share their
          // consumer's ExprIds. Preserve exposed candidate outputs at unmapped boundaries;
          // precise inner Projects can still retire their unused auxiliary producers.
          current.mapChildrenWithReferenceEquality { child =>
            val exposed = child.output.filter(a => candidates.contains(a.exprId))
            visit(child, needed ++ AttributeSet(exposed))
          }
      }
    }
    visit(plan, plan.outputSet.filter(a => candidates.contains(a.exprId)))
  }
}
