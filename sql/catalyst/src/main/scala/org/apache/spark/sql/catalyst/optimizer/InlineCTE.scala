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
 *
 * @param alwaysInline if true, inline all CTEs in the query plan.
 */
case class InlineCTE(alwaysInline: Boolean = false) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.isInstanceOf[Subquery] && plan.containsPattern(CTE)) {
      val cteMap = mutable.SortedMap.empty[Long, (CTERelationDef, Int, mutable.Map[Long, Int])]
      buildCTEMap(plan, cteMap)
      cleanCTEMap(cteMap)
      val notInlined = mutable.ArrayBuffer.empty[CTERelationDef]
      val inlined = inlineCTE(plan, cteMap, notInlined)
      // CTEs in SQL Commands have been inlined by `CTESubstitution` already, so it is safe to add
      // WithCTE as top node here.
      if (notInlined.isEmpty) {
        inlined
      } else {
        WithCTE(inlined, notInlined.toSeq)
      }
    } else {
      plan
    }
  }

  private def shouldInline(cteDef: CTERelationDef, refCount: Int): Boolean = alwaysInline || {
    // We do not need to check enclosed `CTERelationRef`s for `deterministic` or `OuterReference`,
    // because:
    // 1) It is fine to inline a CTE if it references another CTE that is non-deterministic;
    // 2) Any `CTERelationRef` that contains `OuterReference` would have been inlined first.
    refCount == 1 ||
      cteDef.deterministic ||
      cteDef.child.exists(_.expressions.exists(_.isInstanceOf[OuterReference]))
  }

  /**
   * Accumulates all the CTEs from a plan into a special map.
   *
   * @param plan The plan to collect the CTEs from
   * @param cteMap A mutable map that accumulates the CTEs and their reference information by CTE
   *               ids. The value of the map is tuple whose elements are:
   *               - The CTE definition
   *               - The number of incoming references to the CTE. This includes references from
   *                 other CTEs and regular places.
   *               - A mutable inner map that tracks outgoing references (counts) to other CTEs.
   * @param collectCTERefs A function to collect CTE references so that the caller side can do some
   *                       bookkeeping work.
   */
  def buildCTEMap(
      plan: LogicalPlan,
      cteMap: mutable.Map[Long, (CTERelationDef, Int, mutable.Map[Long, Int])],
      collectCTERefs: CTERelationRef => Unit = _ => ()): Unit = {
    plan match {
      case WithCTE(child, cteDefs) =>
        cteDefs.foreach { cteDef =>
          cteMap(cteDef.id) = (cteDef, 0, mutable.Map.empty.withDefaultValue(0))
        }
        cteDefs.foreach { cteDef =>
          buildCTEMap(cteDef, cteMap, ref => {
            // A CTE relation can references CTE relations defined before it in the same `WithCTE`.
            // Here we update the out-going-ref-count for it, in case this CTE relation is not
            // referenced at all and can be optimized out, and we need to decrease the ref counts
            // for CTE relations that are referenced by it.
            if (cteDefs.exists(_.id == ref.cteId)) {
              val (_, _, outerRefMap) = cteMap(ref.cteId)
              outerRefMap(ref.cteId) += 1
            }
            // Similarly, a CTE relation can reference CTE relations defined in the outer `WithCTE`.
            // Here we call the `collectCTERefs` function so that the outer CTE can also update the
            // out-going-ref-count if needed.
            collectCTERefs(ref)
          })
        }
        buildCTEMap(child, cteMap, collectCTERefs)

      case ref: CTERelationRef =>
        val (cteDef, refCount, refMap) = cteMap(ref.cteId)
        cteMap(ref.cteId) = (cteDef, refCount + 1, refMap)
        collectCTERefs(ref)

      case _ =>
        if (plan.containsPattern(CTE)) {
          plan.children.foreach { child =>
            buildCTEMap(child, cteMap, collectCTERefs)
          }

          plan.expressions.foreach { expr =>
            if (expr.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
              expr.foreach {
                case e: SubqueryExpression => buildCTEMap(e.plan, cteMap, collectCTERefs)
                case _ =>
              }
            }
          }
        }
    }
  }

  /**
   * Cleans the CTE map by removing those CTEs that are not referenced at all and corrects those
   * CTE's reference counts where the removed CTE referred to.
   *
   * @param cteMap A mutable map that accumulates the CTEs and their reference information by CTE
   *               ids. Needs to be sorted to speed up cleaning.
   */
  private def cleanCTEMap(
      cteMap: mutable.SortedMap[Long, (CTERelationDef, Int, mutable.Map[Long, Int])]
    ) = {
    cteMap.keys.toSeq.reverse.foreach { currentCTEId =>
      val (_, currentRefCount, refMap) = cteMap(currentCTEId)
      if (currentRefCount == 0) {
        refMap.foreach { case (referencedCTEId, uselessRefCount) =>
          val (cteDef, refCount, refMap) = cteMap(referencedCTEId)
          cteMap(referencedCTEId) = (cteDef, refCount - uselessRefCount, refMap)
        }
      }
    }
  }

  private def inlineCTE(
      plan: LogicalPlan,
      cteMap: mutable.Map[Long, (CTERelationDef, Int, mutable.Map[Long, Int])],
      notInlined: mutable.ArrayBuffer[CTERelationDef]): LogicalPlan = {
    plan match {
      case WithCTE(child, cteDefs) =>
        cteDefs.foreach { cteDef =>
          val (cte, refCount, refMap) = cteMap(cteDef.id)
          if (refCount > 0) {
            val inlined = cte.copy(child = inlineCTE(cte.child, cteMap, notInlined))
            cteMap(cteDef.id) = (inlined, refCount, refMap)
            if (!shouldInline(inlined, refCount)) {
              notInlined.append(inlined)
            }
          }
        }
        inlineCTE(child, cteMap, notInlined)

      case ref: CTERelationRef =>
        val (cteDef, refCount, _) = cteMap(ref.cteId)
        if (shouldInline(cteDef, refCount)) {
          if (ref.outputSet == cteDef.outputSet) {
            cteDef.child
          } else {
            val ctePlan = DeduplicateRelations(
              Join(cteDef.child, cteDef.child, Inner, None, JoinHint(None, None))).children(1)
            val projectList = ref.output.zip(ctePlan.output).map { case (tgtAttr, srcAttr) =>
              if (srcAttr.semanticEquals(tgtAttr)) {
                tgtAttr
              } else {
                Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
              }
            }
            Project(projectList, ctePlan)
          }
        } else {
          ref
        }

      case _ if plan.containsPattern(CTE) =>
        plan
          .withNewChildren(plan.children.map(child => inlineCTE(child, cteMap, notInlined)))
          .transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
            case e: SubqueryExpression =>
              e.withNewPlan(inlineCTE(e.plan, cteMap, notInlined))
          }

      case _ => plan
    }
  }
}
