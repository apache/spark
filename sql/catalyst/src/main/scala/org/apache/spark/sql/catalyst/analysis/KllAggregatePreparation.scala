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
import org.apache.spark.sql.catalyst.trees.TreePattern.{KLL_CHECKPOINT_ALTERNATIVE, OUTER_REFERENCE}

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
    val selected = plan.transformAllExpressionsWithSubqueries {
      case resolution: KllAggregateResolution =>
        unchosen ++= unchosenOutputs(resolution, resolution.batch)
        resolution.batch
    }
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
          aggregate.copy(aggregateExpressions = aggregate.aggregateExpressions.filter(keep))
        case project: Project => project.copy(projectList = project.projectList.filter(keep))
        case other => other
      }
      val current = trimmed.mapExpressionsWithReferenceEquality { expression =>
        expression.transformDownWithReferenceEquality {
          case query: SubqueryExpression =>
            val nested = nestedPlans.computeIfAbsent(query.plan,
              child => visit(child, child.outputSet))
            val updated = query.withNewPlan(nested)
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
                updated.withNewOuterAttrs(outer)
              case _ => updated
            }
        }
      }
      val forwarded = if (required.isEmpty) AttributeSet.empty else {
        current.inputSet.intersect(required)
      }
      val needed = current.references ++ forwarded
      current.mapChildren(child => visit(child, needed))
    }
    visit(plan, plan.outputSet)
  }
}
