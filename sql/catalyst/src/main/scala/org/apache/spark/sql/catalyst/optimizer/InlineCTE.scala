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
 * 1. The CTE definition does not contain any non-deterministic expressions or contains attribute
 *    references to an outer query. If this CTE definition references another CTE definition that
 *    has non-deterministic expressions, it is still OK to inline the current CTE definition.
 * 2. The CTE definition is only referenced once throughout the main query and all the subqueries.
 *
 * CTE definitions that appear in subqueries and are not inlined will be pulled up to the main
 * query level.
 */
object InlineCTE extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.isInstanceOf[Subquery] && plan.containsPattern(CTE)) {
      val cteMap = mutable.HashMap.empty[Long, (CTERelationDef, Int)]
      buildCTEMap(plan, cteMap)
      inlineCTE(plan, cteMap, None)
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
      cteDef.child.find(_.expressions.exists(_.isInstanceOf[OuterReference])).isDefined
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
      notInlinedOpt: Option[mutable.ArrayBuffer[CTERelationDef]]): LogicalPlan = {
    plan match {
      case WithCTE(child, cteDefs) =>
        // If `notInlinedOpt` is defined, it means this `WithCTE` exists in a subquery, then we
        // are adding all CTE defs to the main query level `WithCTE`.
        // It is worth noting that if any CTE def depends on another CTE def in a subquery, the
        // CTE def being depended on is either pulled up to the main query level together (if it
        // is not inlined), or is inlined into the dependant CTE def, in which case the dependant
        // CTE def will inherit any outer references that may require itself to be inlined.
        val notInlined = notInlinedOpt.getOrElse(mutable.ArrayBuffer.empty[CTERelationDef])
        cteDefs.foreach { cteDef =>
          val (cte, refCount) = cteMap(cteDef.id)
          if (refCount > 0) {
            val inlined = cte.copy(child = inlineCTE(cte.child, cteMap, Some(notInlined)))
            cteMap.update(cteDef.id, (inlined, refCount))
            if (!shouldInline(inlined, refCount)) {
              notInlined.append(inlined)
            }
          }
        }
        val newChild = inlineCTE(child, cteMap, Some(notInlined))
        if (notInlinedOpt.isDefined || notInlined.isEmpty) {
          newChild
        } else {
          WithCTE(newChild, notInlined)
        }

      case ref: CTERelationRef =>
        val (cteDef, refCount) = cteMap(ref.cteId)
        if (shouldInline(cteDef, refCount)) {
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

      case _ if plan.containsPattern(CTE) =>
        plan
          .withNewChildren(plan.children.map(child => inlineCTE(child, cteMap, notInlinedOpt)))
          .transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
            case e: SubqueryExpression =>
              e.withNewPlan(inlineCTE(e.plan, cteMap, notInlinedOpt))
          }

      case _ => plan
    }
  }
}
