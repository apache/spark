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

import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.expressions.{Alias, OuterReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, Join, JoinHint, LogicalPlan, Project, Subquery, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}

/**
 * Inlines CTE definitions into corresponding references if either of the conditions satisfies:
 * 1. The CTE definition does not contain any non-deterministic expressions. If this CTE
 *    definition references another CTE definition that has non-deterministic expressions, it
 *    is still OK to inline the current CTE definition.
 * 2. The CTE definition is only referenced once throughout the main query and all the subqueries.
 *
 * In addition, due to the complexity of correlated subqueries, all CTE references in correlated
 * subqueries are inlined regardless of the conditions above.
 */
object InlineCTE extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.isInstanceOf[Subquery] && plan.containsPattern(CTE)) {
      val cteMap = mutable.HashMap.empty[Long, (CTERelationDef, Int)]
      buildCTEMap(plan, cteMap)
      inlineCTE(plan, cteMap, forceInline = false)
    } else {
      plan
    }
  }

  private def shouldInline(cteDef: CTERelationDef, refCount: Int): Boolean = {
    // We do not need to check enclosed `CTERelationRef`s for `deterministic` or `OuterReference`,
    // because:
    // 1) It is fine to inline a CTE if it references another CTE that is non-deterministic;
    // 2) Any `CTERelationRef` that contains `OuterReference` would have been inlined first.
    refCount == 1 ||
      cteDef.deterministic ||
      cteDef.child.exists(_.expressions.exists(_.isInstanceOf[OuterReference]))
  }

  private def buildCTEMap(
      plan: LogicalPlan,
      cteMap: mutable.HashMap[Long, (CTERelationDef, Int)]): Unit = {
    plan match {
      case WithCTE(_, cteDefs) =>
        cteDefs.foreach { cteDef =>
          cteMap.put(cteDef.id, (cteDef, 0))
        }

      case ref: CTERelationRef =>
        val (cteDef, refCount) = cteMap(ref.cteId)
        cteMap.update(ref.cteId, (cteDef, refCount + 1))

      case _ =>
    }

    if (plan.containsPattern(CTE)) {
      plan.children.foreach { child =>
        buildCTEMap(child, cteMap)
      }

      plan.expressions.foreach { expr =>
        if (expr.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          expr.foreach {
            case e: SubqueryExpression =>
              buildCTEMap(e.plan, cteMap)
            case _ =>
          }
        }
      }
    }
  }

  private def inlineCTE(
      plan: LogicalPlan,
      cteMap: mutable.HashMap[Long, (CTERelationDef, Int)],
      forceInline: Boolean): LogicalPlan = {
    val (stripped, notInlined) = plan match {
      case WithCTE(child, cteDefs) =>
        val notInlined = mutable.ArrayBuffer.empty[CTERelationDef]
        cteDefs.foreach { cteDef =>
          val (cte, refCount) = cteMap(cteDef.id)
          if (refCount > 0) {
            val inlined = cte.copy(child = inlineCTE(cte.child, cteMap, forceInline))
            cteMap.update(cteDef.id, (inlined, refCount))
            if (!forceInline && !shouldInline(inlined, refCount)) {
              notInlined.append(inlined)
            }
          }
        }
        (inlineCTE(child, cteMap, forceInline), notInlined.toSeq)

      case ref: CTERelationRef =>
        val (cteDef, refCount) = cteMap(ref.cteId)
        val newRef = if (forceInline || shouldInline(cteDef, refCount)) {
          if (ref.outputSet == cteDef.outputSet) {
            cteDef.child
          } else {
            val ctePlan = DeduplicateRelations(
              Join(cteDef.child, cteDef.child, Inner, None, JoinHint(None, None))).children(1)
            val projectList = ref.output.zip(ctePlan.output).map { case (tgtAttr, srcAttr) =>
              Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
            }
            Project(projectList, ctePlan)
          }
        } else {
          ref
        }
        (newRef, Seq.empty)

      case _ if plan.containsPattern(CTE) =>
        val newPlan = plan
          .withNewChildren(plan.children.map(child => inlineCTE(child, cteMap, forceInline)))
          .transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
            case e: SubqueryExpression =>
              e.withNewPlan(inlineCTE(e.plan, cteMap, forceInline = e.isCorrelated))
          }
        (newPlan, Seq.empty)

      case _ => (plan, Seq.empty)
    }

    if (notInlined.isEmpty) {
      stripped
    } else {
      WithCTE(stripped, notInlined)
    }
  }
}
